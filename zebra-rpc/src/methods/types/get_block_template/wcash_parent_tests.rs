//! Private parent-template contract tests, including the exact NU7 branch.

use std::sync::Arc;
use zcash_keys::address::Address;
use zebra_chain::{
    amount::{Amount, NegativeOrZero, NonNegative},
    block::{self, Height},
    parameters::{Network, NetworkUpgrade},
    serialization::{DateTime32, ZcashDeserializeInto, ZcashSerialize},
    transaction::{HashType, LockTime, Transaction},
    transparent::{Input, Output, Script},
    work::difficulty::{CompactDifficulty, ExpandedDifficulty, U256},
};
use zebra_script::Sigops;
use zebra_state::GetBlockTemplateChainInfo;

use super::*;
use crate::{
    config::mining::{
        default_miner_address, Config, ExtraCoinbaseData, MinerAddressType,
        MAX_USER_COINBASE_DATA_LEN,
    },
    methods::{
        hex_data::HexData, tests::utils::fake_history_tree, types::long_poll::LongPollInput,
    },
};

fn configured_miner(net: &Network) -> MinerParams {
    MinerParams::new(
        net,
        Config {
            miner_address: Some(
                default_miner_address(net.kind(), &MinerAddressType::Transparent)
                    .parse()
                    .unwrap(),
            ),
            ..Default::default()
        },
    )
    .unwrap()
}

fn nu7_template() -> (Network, MinerParams, BlockTemplateResponse) {
    let net = Network::Mainnet;
    let height = NetworkUpgrade::Nu7.activation_height(&net).unwrap();
    let miner = configured_miner(&net);
    let chain_info = GetBlockTemplateChainInfo {
        expected_difficulty: CompactDifficulty::from(ExpandedDifficulty::from(U256::one())),
        tip_height: height.previous().unwrap(),
        tip_hash: block::Hash([0xab; 32]),
        cur_time: DateTime32::from(1_654_008_617),
        min_time: DateTime32::from(1_654_008_606),
        max_time: DateTime32::from(1_654_008_719),
        chain_history_root: fake_history_tree(&net).hash(),
        chain_value_pools: Default::default(),
    };
    let id = LongPollInput::new(
        chain_info.tip_height,
        chain_info.tip_hash,
        chain_info.max_time,
        std::iter::empty(),
    )
    .generate_id();
    let template = BlockTemplateResponse::new_internal(
        &net,
        &CoinbaseCache::default(),
        &miner,
        &chain_info,
        id,
        vec![],
        None,
    );
    (net, miner, template)
}

#[test]
fn parent_payout_attestation_is_canonical_and_redacted() {
    let net = Network::Mainnet;
    let miner = configured_miner(&net);
    let address = default_miner_address(net.kind(), &MinerAddressType::Transparent);
    let expected = hex::encode(wcash_zcash_aux::parent_payout_address_commitment(address));
    assert_eq!(
        miner.parent_payout_address_commitment(),
        Some(expected.as_str())
    );
    let debug = format!("{miner:?}");
    assert!(!debug.contains(address));
    assert!(!debug.contains(&expected));
    assert!(debug.contains("[REDACTED]"));
    let config = Config {
        miner_address: Some(address.parse().unwrap()),
        miner_memo: Some("private payout memo".to_string()),
        ..Default::default()
    };
    let debug = format!("{config:?}");
    assert!(!debug.contains(address));
    assert!(!debug.contains("private payout memo"));
    assert!(debug.contains("[REDACTED]"));
}

#[test]
fn wcash_aux_request_round_trips_and_rejects_ambiguous_modes() {
    let json = r#"{"mode":"template","wcashaux":{"blockhash":"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f","nonce":7}}"#;
    let request: GetBlockTemplateParameters = serde_json::from_str(json).unwrap();
    assert_eq!(serde_json::to_string(&request).unwrap(), json);
    assert_eq!(request.wcash_aux().unwrap().block_hash().0[0], 31);
    assert!(check_parameters(&Some(request.clone())).is_ok());
    let mut proposal = request.clone();
    proposal.mode = GetBlockTemplateRequestMode::Proposal;
    proposal.data = Some(HexData(vec![]));
    assert!(check_parameters(&Some(proposal)).is_err());
    let mut long_poll = request;
    long_poll.long_poll_id = Some(
        LongPollInput::new(
            Height(1),
            block::Hash([0; 32]),
            DateTime32::from(1),
            std::iter::empty(),
        )
        .generate_id(),
    );
    assert!(check_parameters(&Some(long_poll)).is_err());
    assert!(serde_json::from_str::<GetBlockTemplateParameters>(
        r#"{"wcashaux":{"blockhash":"00","nonce":0}}"#
    )
    .is_err());
    assert!(serde_json::from_str::<GetBlockTemplateParameters>(r#"{"wcashaux":{"blockhash":"0000000000000000000000000000000000000000000000000000000000000000","nonce":0,"other":1}}"#).is_err());
}

#[test]
fn nu7_v5_and_v6_carriers_preserve_exact_transaction_bindings() {
    // Canonical transaction/parser fixtures, not solved parent blocks.
    let height = Height(3_543_405);
    for version in [5, 6] {
        let inputs = vec![Input::Coinbase {
            height,
            data: b"parent".to_vec(),
            sequence: u32::MAX,
        }];
        let outputs = vec![Output::new(
            Amount::<NonNegative>::zero(),
            Script::new(&[0x51]),
        )];
        let transaction = if version == 5 {
            Transaction::test_v5(
                NetworkUpgrade::Nu7,
                inputs,
                outputs,
                LockTime::unlocked(),
                height,
            )
        } else {
            Transaction::test_v6(
                NetworkUpgrade::Nu7,
                inputs,
                outputs,
                LockTime::unlocked(),
                height,
            )
        };
        let bytes = transaction.zcash_serialize_to_vec().unwrap();
        assert_eq!(&bytes[8..12], &0x7719_0ad9u32.to_le_bytes());
        let original = TransactionTemplate::<NegativeOrZero> {
            data: bytes.into(),
            hash: transaction.hash(),
            auth_digest: transaction.auth_digest().unwrap(),
            depends: vec![],
            fee: Amount::zero(),
            sigops: transaction.sigops().unwrap(),
            required: true,
        };
        let original_bytes = original.data.as_ref().to_vec();
        let extended = original.clone().with_wcash_aux([0x42; 32], 7).unwrap();
        let parsed: Transaction = extended.data.as_ref().zcash_deserialize_into().unwrap();
        assert_eq!(parsed.version(), version);
        assert_eq!(parsed.network_upgrade(), Some(NetworkUpgrade::Nu7));
        assert_eq!(
            &extended.data.as_ref()[8..12],
            &0x7719_0ad9u32.to_le_bytes()
        );
        assert_eq!(extended.hash, original.hash);
        assert_eq!(extended.fee, original.fee);
        assert_ne!(extended.auth_digest, original.auth_digest);
        assert_eq!(extended.data.as_ref().len(), original_bytes.len() + 44);
        let input = parsed.inputs().remove(0);
        wcash_zcash_aux::validate_miner_data_commitment(
            input.miner_data().unwrap(),
            [0x42; 32],
            &[],
            0,
        )
        .unwrap();
        assert_eq!(
            transaction
                .sighash(
                    NetworkUpgrade::Nu7,
                    zebra_chain::transaction::HashType::ALL,
                    Arc::new(vec![]),
                    None
                )
                .unwrap(),
            parsed
                .sighash(
                    NetworkUpgrade::Nu7,
                    zebra_chain::transaction::HashType::ALL,
                    Arc::new(vec![]),
                    None
                )
                .unwrap()
        );
    }
}

#[test]
fn nu7_cached_transform_is_child_isolated_and_preserves_budget_and_fees() {
    let (net, miner, ordinary) = nu7_template();
    assert_eq!(
        &ordinary.coinbase_txn.data.as_ref()[8..12],
        &0x7719_0ad9u32.to_le_bytes()
    );
    let first = ordinary
        .clone()
        .with_wcash_aux(
            &net,
            &miner,
            WcashAuxRequest::new(block::Hash([0x11; 32]), 1),
        )
        .unwrap();
    let second = ordinary
        .clone()
        .with_wcash_aux(
            &net,
            &miner,
            WcashAuxRequest::new(block::Hash([0x22; 32]), 2),
        )
        .unwrap();
    assert!(ordinary.wcash_parent_payout_commitment.is_none());
    assert_eq!(first.transactions, ordinary.transactions);
    assert_eq!(first.previous_block_hash, ordinary.previous_block_hash);
    assert_eq!(first.coinbase_txn.fee, ordinary.coinbase_txn.fee);
    assert_eq!(first.coinbase_txn.hash, ordinary.coinbase_txn.hash);
    assert_ne!(
        first.coinbase_txn.auth_digest,
        second.coinbase_txn.auth_digest
    );
    assert_ne!(
        first.default_roots.auth_data_root,
        ordinary.default_roots.auth_data_root
    );
    assert_ne!(first.block_commitments_hash, second.block_commitments_hash);
    assert_eq!(
        first.wcash_parent_payout_commitment.as_deref(),
        miner.parent_payout_address_commitment()
    );
    assert!(first.fits_parent_template_limits(&net));
    let size = proposal::proposal_block_from_template(&ordinary, None, &net)
        .unwrap()
        .zcash_serialized_size();
    let mut full = ordinary.clone();
    full.size_limit = u64::try_from(size).unwrap();
    assert!(full.fits_parent_template_limits(&net));
    let extended = full
        .with_wcash_aux(
            &net,
            &miner,
            WcashAuxRequest::new(block::Hash([0x33; 32]), 3),
        )
        .unwrap();
    assert!(!extended.fits_parent_template_limits(&net));
}

#[test]
fn aux_carrier_rejects_maximum_configured_extra_coinbase_data() {
    let net = Network::Mainnet;
    let miner = MinerParams::new(
        &net,
        Config {
            miner_address: Some(
                default_miner_address(net.kind(), &MinerAddressType::Transparent)
                    .parse()
                    .unwrap(),
            ),
            extra_coinbase_data: Some(
                ExtraCoinbaseData::try_from("x".repeat(MAX_USER_COINBASE_DATA_LEN)).unwrap(),
            ),
            ..Default::default()
        },
    )
    .unwrap();
    let height = NetworkUpgrade::Nu7.activation_height(&net).unwrap();
    let coinbase = TransactionTemplate::new_coinbase(&net, height, &miner, Amount::zero()).unwrap();
    assert!(coinbase.with_wcash_aux([0x42; 32], 0).is_err());
}

#[test]
fn nu7_aux_commitment_preserves_shielded_coinbase_and_recomputes_roots() {
    let net = Network::Mainnet;
    let height = NetworkUpgrade::Nu7
        .activation_height(&net)
        .expect("NU7 is scheduled on Mainnet");
    let miner_params = MinerParams::from(
        Address::decode(
            &net,
            default_miner_address(net.kind(), &MinerAddressType::Unified),
        )
        .expect("hard-coded Unified address is valid"),
    );
    let block_hash = std::array::from_fn(|index| u8::try_from(index).expect("index fits in u8"));
    let nonce = 7;

    let original = TransactionTemplate::new_coinbase(&net, height, &miner_params, Amount::zero())
        .expect("valid shielded v6 coinbase");
    let original_bytes = original.data().as_ref().to_vec();
    let original_transaction: Transaction = original_bytes
        .as_slice()
        .zcash_deserialize_into()
        .expect("original coinbase deserializes");
    assert_eq!(original_transaction.version(), 6);
    assert_eq!(&original_bytes[8..12], &0x7719_0ad9u32.to_le_bytes());
    assert!(original_transaction.has_ironwood_shielded_data());

    let extended = original
        .clone()
        .with_wcash_aux(block_hash, nonce)
        .expect("the canonical commitment fits in ordinary miner data");
    let extended_bytes = extended.data().as_ref().to_vec();
    let extended_transaction: Transaction = extended_bytes
        .as_slice()
        .zcash_deserialize_into()
        .expect("extended coinbase deserializes");
    let commitment =
        wcash_zcash_aux::miner_data_commitment(block_hash, &[], 0, nonce).expect("valid leaf");
    assert_eq!(
        hex::encode(commitment),
        "fabe6d6d828c9a504f6f9792169ae8c5751624ce48aa43476dd83ca2e481a01cdd5980b30100000007000000",
        "the parent hook freezes the v2 domain, chain ID, root byte order, tree size, and nonce encoding",
    );

    assert_eq!(
        extended_bytes.len(),
        original_bytes.len() + commitment.len()
    );
    assert_eq!(
        extended.hash(),
        original.hash(),
        "ZIP-244 txid is unchanged"
    );
    assert_eq!(extended.hash(), extended_transaction.hash());
    assert_ne!(extended.auth_digest(), original.auth_digest());
    assert_eq!(
        extended.auth_digest(),
        extended_transaction
            .auth_digest()
            .expect("v6 has an auth digest")
    );
    assert_eq!(extended.sigops(), extended_transaction.sigops().unwrap());

    let extended_inputs = extended_transaction.inputs();
    let extended_input = extended_inputs.first().expect("coinbase has one input");
    assert!(
        extended_input.coinbase_script().unwrap().len()
            <= zcash_transparent::coinbase::MAX_COINBASE_SCRIPT_LEN
    );
    assert!(extended_input
        .miner_data()
        .expect("coinbase has miner data")
        .ends_with(&commitment));
    wcash_zcash_aux::validate_miner_data_commitment(
        extended_input.miner_data().unwrap(),
        block_hash,
        &[],
        0,
    )
    .expect("the committed child and nonce round-trip");

    // The original proof-bearing suffix is bit-for-bit preserved. Restoring
    // the one-byte script length and removing the appended carrier reconstructs
    // the exact original transaction.
    const V5_V6_COINBASE_SCRIPT_LENGTH_OFFSET: usize = 20 + 1 + 36;
    let commitment_offset = extended_bytes
        .windows(commitment.len())
        .position(|window| window == commitment)
        .expect("the complete commitment occurs in the serialized coinbase");
    let mut restored = extended_bytes.clone();
    restored.drain(commitment_offset..commitment_offset + commitment.len());
    restored[V5_V6_COINBASE_SCRIPT_LENGTH_OFFSET] =
        original_bytes[V5_V6_COINBASE_SCRIPT_LENGTH_OFFSET];
    // This exact byte equality covers every serialized Ironwood action,
    // proof, and binding authorization as well as all other coinbase fields.
    assert_eq!(restored, original_bytes);
    let previous_outputs = Arc::new(Vec::new());
    assert_eq!(
        original_transaction
            .sighash(
                NetworkUpgrade::Nu7,
                HashType::ALL,
                previous_outputs.clone(),
                None,
            )
            .expect("original shielded signature hash is defined"),
        extended_transaction
            .sighash(NetworkUpgrade::Nu7, HashType::ALL, previous_outputs, None,)
            .expect("extended shielded signature hash is defined"),
        "coinbase miner data is authorizing data but not part of the shielded signature hash",
    );
    zebra_consensus::transaction::check::coinbase_outputs_are_decryptable(
        &extended_transaction,
        &net,
        height,
    )
    .expect("the unchanged shielded coinbase authorization remains valid");

    let history_root = Some([0x55; 32].into());
    let original_roots = DefaultRoots::from_coinbase(&net, height, &original, history_root, &[]);
    let extended_roots = DefaultRoots::from_coinbase(&net, height, &extended, history_root, &[]);
    assert_eq!(
        original_roots.merkle_root(),
        extended_roots.merkle_root(),
        "the txid Merkle root stays unchanged",
    );
    assert_ne!(
        original_roots.auth_data_root(),
        extended_roots.auth_data_root(),
        "the auth-data root commits to the new miner data",
    );
    assert_ne!(
        original_roots.block_commitments_hash(),
        extended_roots.block_commitments_hash(),
        "the header commitment is rebuilt from the new auth-data root",
    );
}

#[tokio::test(start_paused = true)]
async fn committed_duplicate_gossip_admission_is_global_and_retryable() {
    use std::time::Duration;
    use zebra_chain::chain_sync_status::MockSyncStatus;
    use zebra_node_services::BoxError;
    use zebra_test::mock_service::MockService;

    let router: MockService<_, _, _, BoxError> = MockService::build().for_unit_tests();
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    let handler = super::GetBlockTemplateHandler::new(
        &Network::Mainnet,
        Config::default(),
        router,
        MockSyncStatus::default(),
        Some(sender),
    );
    let first = handler.clone();
    let second = handler.clone();
    let (a, b) = tokio::join!(
        tokio::spawn(async move {
            first.advertise_committed_duplicate(block::Hash([1; 32]), Height(1))
        }),
        tokio::spawn(async move {
            second.advertise_committed_duplicate(block::Hash([2; 32]), Height(2))
        }),
    );
    assert_ne!(a.unwrap().is_ok(), b.unwrap().is_ok());
    receiver.try_recv().unwrap();
    assert!(handler
        .advertise_committed_duplicate(block::Hash([3; 32]), Height(3))
        .is_err());

    // New consensus-accepted blocks are never throttled by duplicate traffic.
    handler
        .advertise_mined_block(block::Hash([4; 32]), Height(4))
        .unwrap();
    assert_eq!(receiver.try_recv().unwrap().0, block::Hash([4; 32]));
    tokio::time::advance(Duration::from_secs(1)).await;

    handler
        .advertise_mined_block(block::Hash([5; 32]), Height(5))
        .unwrap();
    assert!(handler
        .advertise_committed_duplicate(block::Hash([6; 32]), Height(6))
        .is_err());
    receiver.try_recv().unwrap();
    // Failed admission must not consume the interval: retry without advancing time.
    handler
        .advertise_committed_duplicate(block::Hash([6; 32]), Height(6))
        .unwrap();
    assert_eq!(receiver.try_recv().unwrap().0, block::Hash([6; 32]));
    // Even a contended guard cannot delay fresh submissions or wait for the lock.
    let lock = handler.duplicate_gossip_admission.lock().unwrap();
    assert!(handler
        .advertise_committed_duplicate(block::Hash([9; 32]), Height(9))
        .is_err());
    handler
        .advertise_mined_block(block::Hash([10; 32]), Height(10))
        .unwrap();
    assert_eq!(receiver.try_recv().unwrap().0, block::Hash([10; 32]));
    drop(lock);
    drop(receiver);
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(handler
        .advertise_committed_duplicate(block::Hash([7; 32]), Height(7))
        .is_err());
}

/// Carrier insertion must preserve aggregate NU7 fee rounding and parent-dependent
/// reissuance, including the cache key that separates different parent reserves.
#[test]
fn nu7_aux_preserves_nonzero_aggregate_fees_and_parent_nsm_context() {
    use zebra_chain::{
        parameters::subsidy::{block_subsidy_with_parent_nsm_value_balance, miner_subsidy},
        transaction::{UnminedTx, VerifiedUnminedTx},
        transparent::OutPoint,
    };

    let net = Network::Mainnet;
    let height = Height(8_939_664);
    assert_eq!(net.nsm_reissuance_height(), Some(height));
    let miner = configured_miner(&net);
    let cache = CoinbaseCache::default();
    // Both gross totals collect 8,001 zats, but require distinct gross-fee cache keys.
    for (first_fee, gross) in [(10_000u64, 20_001), (10_001, 20_002)] {
        let transactions: Vec<_> = [1u8, 2]
            .into_iter()
            .map(|tag| {
                let fee =
                    Amount::<NonNegative>::try_from(if tag == 1 { first_fee } else { 10_001 })
                        .unwrap();
                let tx = Transaction::test_v6(
                    NetworkUpgrade::Nu7,
                    vec![Input::PrevOut {
                        outpoint: OutPoint {
                            hash: zebra_chain::transaction::Hash([tag; 32]),
                            index: 0,
                        },
                        unlock_script: Script::new(&[]),
                        sequence: u32::MAX,
                    }],
                    vec![Output::new(Amount::zero(), Script::new(&[0x51]))],
                    LockTime::unlocked(),
                    height,
                );
                (
                    0,
                    VerifiedUnminedTx::new(
                        UnminedTx::from(Arc::new(tx)),
                        fee,
                        0,
                        0,
                        Arc::new(vec![]),
                    )
                    .unwrap(),
                )
            })
            .collect();
        let mut paid_by_parent = Vec::new();
        for reserve in [1_000_000_000_000u64, 2_000_000_000_000] {
            let parent_nsm = Amount::<NonNegative>::try_from(reserve).unwrap();
            let mut chain_info = GetBlockTemplateChainInfo {
                expected_difficulty: CompactDifficulty::from(ExpandedDifficulty::from(U256::one())),
                tip_height: height.previous().unwrap(),
                tip_hash: block::Hash([0xab; 32]),
                cur_time: DateTime32::from(1_654_008_617),
                min_time: DateTime32::from(1_654_008_606),
                max_time: DateTime32::from(1_654_008_719),
                chain_history_root: fake_history_tree(&net).hash(),
                chain_value_pools: Default::default(),
            };
            chain_info.chain_value_pools.set_nsm_amount(parent_nsm);
            let id = LongPollInput::new(
                chain_info.tip_height,
                chain_info.tip_hash,
                chain_info.max_time,
                std::iter::empty(),
            )
            .generate_id();
            let original = BlockTemplateResponse::new_internal(
                &net,
                &cache,
                &miner,
                &chain_info,
                id,
                transactions.clone(),
                None,
            );
            let extended = original
                .clone()
                .with_wcash_aux(&net, &miner, WcashAuxRequest::new(block::Hash([7; 32]), 3))
                .unwrap();
            let before: Transaction = original
                .coinbase_txn
                .data
                .as_ref()
                .zcash_deserialize_into()
                .unwrap();
            let after: Transaction = extended
                .coinbase_txn
                .data
                .as_ref()
                .zcash_deserialize_into()
                .unwrap();
            assert_eq!(
                &extended.coinbase_txn.data.as_ref()[8..12],
                &0x7719_0ad9u32.to_le_bytes()
            );
            // Both aggregate totals collect 8,001; per-transaction rounding fails at 20,002.
            assert_eq!(
                original
                    .transactions
                    .iter()
                    .map(|tx| tx.fee.zatoshis())
                    .sum::<i64>(),
                gross
            );
            assert_eq!(original.coinbase_txn.fee.zatoshis(), -8_001);
            assert_eq!(extended.coinbase_txn.fee, original.coinbase_txn.fee);
            assert_eq!(extended.transactions, original.transactions);
            assert_eq!(after.outputs(), before.outputs());
            assert_eq!(extended.coinbase_txn.hash, original.coinbase_txn.hash);
            assert_ne!(
                extended.coinbase_txn.auth_digest,
                original.coinbase_txn.auth_digest
            );
            let paid = before
                .outputs()
                .iter()
                .map(|output| output.value.zatoshis())
                .sum::<i64>();
            let subsidy =
                block_subsidy_with_parent_nsm_value_balance(height, &net, parent_nsm).unwrap();
            assert_eq!(
                paid,
                miner_subsidy(height, &net, subsidy).unwrap().zatoshis() + 8_001
            );
            paid_by_parent.push(paid);
            assert!(extended.fits_parent_template_limits(&net));
        }
        assert_ne!(
            paid_by_parent[0], paid_by_parent[1],
            "the parent reserve must affect the cached coinbase"
        );
    }
}

#[test]
fn parent_commitment_hash_helpers_match_the_nu7_chain_types() {
    use zebra_chain::{
        block::{
            merkle::AuthDataRoot, ChainHistoryBlockTxAuthCommitmentHash, ChainHistoryMmrRootHash,
        },
        transaction::AuthDigest,
    };
    let left = [0x11; 32];
    let right = [0x22; 32];
    let root = wcash_zcash_aux::auth_data_merkle_root(left, &[right], 0).unwrap();
    let chain_root: [u8; 32] = [AuthDigest(left), AuthDigest(right)]
        .into_iter()
        .collect::<AuthDataRoot>()
        .into();
    assert_eq!(root, chain_root);
    let commitment = wcash_zcash_aux::block_commitments_hash([0x33; 32], root);
    let chain_commitment: [u8; 32] = ChainHistoryBlockTxAuthCommitmentHash::from_commitments(
        &ChainHistoryMmrRootHash::from([0x33; 32]),
        &AuthDataRoot::from(root),
    )
    .into();
    assert_eq!(commitment, chain_commitment);
}

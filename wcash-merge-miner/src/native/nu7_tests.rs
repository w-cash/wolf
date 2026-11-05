//! Canonical template/parser fixtures. Transactions have synthetic prevouts and
//! headers are deliberately unsolved; these tests do not prove winning delivery.

use hex::ToHex;
use wcash_zec_parent::address::ToAddress;
use wcash_zec_parent::chain::{
    amount::{Amount, NonNegative},
    parameters::subsidy::{funding_stream_address, funding_stream_values, FundingStreamReceiver},
    transaction::LockTime,
    transparent::{Input, OutPoint, Output, Script},
};

use super::*;

const CHILD: [u8; 32] = [0x42; 32];
const NONCE: u32 = 7;
const NU7_HEIGHT: u32 = 3_543_405;

fn tx_template(transaction: Transaction, fee: i64) -> TransactionTemplate {
    TransactionTemplate {
        data: hex::encode(transaction.zcash_serialize_to_vec().unwrap()),
        hash: transaction.hash().encode_hex::<String>(),
        auth_digest: transaction.auth_digest().unwrap().encode_hex::<String>(),
        fee,
    }
}

fn fixture(
    height: u32,
    reserve: u64,
    fees: &[i64],
    version: u32,
) -> (BlockTemplateResponse, ZcashAddress) {
    let network = Network::Mainnet;
    let upgrade = NetworkUpgrade::current(&network, Height(height));
    let address = ZcashAddress::from_transparent_p2pkh(NetworkType::Main, [0x11; 20]);
    let miner_script = transparent::Address::from_pub_key_hash(network.kind(), [0x11; 20]).script();
    let gross = Amount::<NonNegative>::try_from(fees.iter().copied().sum::<i64>()).unwrap();
    let claimed = (gross - nsm_fee_contribution(Height(height), &network, gross)).unwrap();
    let subsidy = block_subsidy_with_parent_nsm_value_balance(
        Height(height),
        &network,
        Amount::try_from(reserve).unwrap(),
    )
    .unwrap();
    let reward = (miner_subsidy(Height(height), &network, subsidy).unwrap() + claimed).unwrap();
    let mut outputs = vec![Output::new(reward, miner_script)];
    for (receiver, value) in funding_stream_values(Height(height), &network, subsidy).unwrap() {
        if receiver != FundingStreamReceiver::Deferred {
            let recipient = funding_stream_address(Height(height), &network, receiver).unwrap();
            outputs.push(Output::new(value, recipient.script()));
        }
    }
    let inputs = vec![Input::Coinbase {
        height: Height(height),
        data: wcash_zcash_aux::miner_data_commitment(CHILD, &[], 0, NONCE)
            .unwrap()
            .to_vec(),
        sequence: u32::MAX,
    }];
    let coinbase = if version == 5 {
        Transaction::test_v5(
            upgrade,
            inputs,
            outputs,
            LockTime::unlocked(),
            Height(height),
        )
    } else {
        Transaction::test_v6(
            upgrade,
            inputs,
            outputs,
            LockTime::unlocked(),
            Height(height),
        )
    };
    let mut decoded = vec![Arc::new(coinbase.clone())];
    let transactions = fees
        .iter()
        .enumerate()
        .map(|(index, fee)| {
            let transaction = Transaction::test_v5(
                upgrade,
                vec![Input::PrevOut {
                    outpoint: OutPoint {
                        hash: TransactionHash([u8::try_from(index + 1).unwrap(); 32]),
                        index: 0,
                    },
                    unlock_script: Script::new(&[]),
                    sequence: u32::MAX,
                }],
                vec![Output::new(Amount::zero(), Script::new(&[0x51]))],
                LockTime::unlocked(),
                Height(height),
            );
            decoded.push(Arc::new(transaction.clone()));
            tx_template(transaction, *fee)
        })
        .collect();
    let merkle: block::merkle::Root = decoded.iter().collect();
    let auth: AuthDataRoot = decoded.iter().collect();
    let history: ChainHistoryMmrRootHash = [0x22; 32].into();
    let commitment = ChainHistoryBlockTxAuthCommitmentHash::from_commitments(&history, &auth);
    let mut template: BlockTemplateResponse = serde_json::from_str(include_str!(
        "../../../zebra-rpc/tests/vectors/getblocktemplate_response_template.json"
    ))
    .unwrap();
    template.height = height;
    template.coinbase_txn = tx_template(coinbase, -claimed.zatoshis());
    template.transactions = transactions;
    template.default_roots = TemplateRoots {
        merkle_root: merkle.encode_hex::<String>(),
        auth_data_root: auth.encode_hex::<String>(),
        chain_history_root: history.encode_hex::<String>(),
        block_commitments_hash: commitment.encode_hex::<String>(),
    };
    template.block_commitments_hash = commitment.encode_hex::<String>();
    template.parent_context = Some(ParentMiningContext {
        version: 1,
        previous_block_hash: template.previous_block_hash.clone(),
        height,
        parent_nsm_value_balance: reserve,
    });
    (template, address)
}

fn prepare(
    template: BlockTemplateResponse,
    address: &ZcashAddress,
    rules: ParentCoinbaseRules,
) -> Result<NativePreparedJob, MinerError> {
    NativePreparedJob::from_template(
        CHILD,
        Target::MAX,
        NONCE,
        template,
        address,
        NativeZcashNetwork::Mainnet,
        &Network::Mainnet,
        rules,
    )
}

#[test]
fn activation_templates_preserve_real_branch_and_aggregate_fee_rounding() {
    for height in [NU7_HEIGHT - 1, NU7_HEIGHT, NU7_HEIGHT + 1] {
        for version in [5, 6] {
            for fees in [[10_000, 10_001], [10_001, 10_001]] {
                let (template, address) = fixture(height, 0, &fees, version);
                let expected_fee = if height < NU7_HEIGHT {
                    fees.iter().sum()
                } else {
                    8_001
                };
                assert_eq!(
                    validate_template_fees(&template, &Network::Mainnet)
                        .unwrap()
                        .zatoshis(),
                    expected_fee
                );
                let prepared =
                    prepare(template, &address, ParentCoinbaseRules::LegacyAndNu7).unwrap();
                let coinbase = &prepared.parent_proposal.transactions[0];
                assert_eq!(coinbase.version(), version);
                assert_eq!(
                    coinbase.network_upgrade(),
                    Some(if height < NU7_HEIGHT {
                        NetworkUpgrade::Nu6_3
                    } else {
                        NetworkUpgrade::Nu7
                    })
                );
                assert_eq!(
                    prepared.job.parent_coinbase_rules(),
                    ParentCoinbaseRules::LegacyAndNu7
                );
                let parsed: Block = prepared
                    .proposal_bytes
                    .as_slice()
                    .zcash_deserialize_into()
                    .unwrap();
                assert_eq!(
                    parsed.zcash_serialize_to_vec().unwrap(),
                    prepared.proposal_bytes
                );
            }
        }
    }
}

#[test]
fn legacy_wec_policy_rejects_nu7_before_more_template_work() {
    let (mut template, address) = fixture(NU7_HEIGHT, 0, &[], 6);
    template.parent_context = None;
    template.transactions.push(TransactionTemplate {
        data: "invalid".to_string(),
        hash: String::new(),
        auth_digest: String::new(),
        fee: -1,
    });
    let error = prepare(template, &address, ParentCoinbaseRules::Legacy).unwrap_err();
    assert!(error
        .to_string()
        .contains("candidate Wcash parent-proof rules"));
}

#[test]
fn context_and_fee_metadata_fail_closed() {
    let (template, address) = fixture(NU7_HEIGHT, 0, &[10_001, 10_001], 6);
    for defect in 0..7 {
        let mut malformed = template.clone();
        match defect {
            0 => malformed.parent_context = None,
            1 => malformed.parent_context.as_mut().unwrap().version = 2,
            2 => malformed.parent_context.as_mut().unwrap().height += 1,
            3 => {
                malformed
                    .parent_context
                    .as_mut()
                    .unwrap()
                    .previous_block_hash = "11".repeat(32)
            }
            4 => {
                malformed
                    .parent_context
                    .as_mut()
                    .unwrap()
                    .parent_nsm_value_balance = u64::MAX
            }
            5 => malformed.transactions[0].fee = -1,
            _ => malformed.coinbase_txn.fee = -8_002,
        }
        assert!(
            prepare(malformed, &address, ParentCoinbaseRules::LegacyAndNu7).is_err(),
            "defect {defect}"
        );
    }
    assert!(serde_json::from_str::<ParentMiningContext>(
        r#"{"version":1,"previousblockhash":"00","height":1,"parentnsmvaluebalance":0.5}"#
    )
    .is_err());
    assert!(serde_json::from_str::<ParentMiningContext>(r#"{"version":1,"previousblockhash":"00","height":1,"parentnsmvaluebalance":0,"unexpected":1}"#).is_err());
    assert!(serde_json::from_str::<TransactionTemplate>(
        r#"{"data":"00","hash":"00","authdigest":"00"}"#
    )
    .is_err());
}

#[test]
fn nsm_reissuance_uses_the_exact_parent_reserve_without_a_scheduled_fallback() {
    let height = 8_939_664;
    assert_eq!(
        Network::Mainnet.nsm_reissuance_height(),
        Some(Height(height))
    );
    let mut rewards = Vec::new();
    for reserve in [1_000_000_000_000u64, 2_000_000_000_000] {
        let (template, address) = fixture(height, reserve, &[10_001, 10_001], 6);
        let prepared = prepare(
            template.clone(),
            &address,
            ParentCoinbaseRules::LegacyAndNu7,
        )
        .unwrap();
        rewards.push(prepared.parent_reward_zatoshis);
        let mut missing = template.clone();
        missing.parent_context = None;
        assert!(prepare(missing, &address, ParentCoinbaseRules::LegacyAndNu7).is_err());
        let mut inflated = template;
        inflated
            .parent_context
            .as_mut()
            .unwrap()
            .parent_nsm_value_balance = 3_000_000_000_000;
        assert!(prepare(inflated, &address, ParentCoinbaseRules::LegacyAndNu7).is_err());
    }
    assert!(rewards[1] > rewards[0]);
}

#[test]
fn independent_context_must_match_the_exact_template_without_building_another_coinbase() {
    use std::{
        io::{BufRead, BufReader, Read, Write},
        net::TcpListener,
    };

    for defect in 0..4 {
        let (template, address) = fixture(NU7_HEIGHT, 0, &[], 6);
        let expected = template.parent_context.clone().unwrap();
        let prepared = prepare(template, &address, ParentCoinbaseRules::LegacyAndNu7).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = RpcEndpoint::new(
            format!("http://{}/", listener.local_addr().unwrap()),
            None,
            None,
        )
        .unwrap();
        let request_previous = expected.previous_block_hash.clone();
        let request_height = expected.height;
        let mut reply = json!({"version":1,"previousblockhash":expected.previous_block_hash,"height":expected.height,"parentnsmvaluebalance":expected.parent_nsm_value_balance});
        match defect {
            1 => reply["parentnsmvaluebalance"] = json!(1),
            2 => reply["previousblockhash"] = json!("44".repeat(32)),
            3 => reply["height"] = json!(expected.height + 1),
            _ => {}
        }
        let server = thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(3);
            let mut stream = loop {
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(error)
                        if error.kind() == std::io::ErrorKind::WouldBlock
                            && Instant::now() < deadline =>
                    {
                        thread::sleep(Duration::from_millis(1))
                    }
                    Err(error) => panic!("bounded context fixture accept failed: {error}"),
                }
            };
            stream.set_nonblocking(false).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(1)))
                .unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            let mut length = None;
            loop {
                line.clear();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" {
                    break;
                }
                if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    length = Some(value.trim().parse::<usize>().unwrap());
                }
            }
            let length = length.unwrap();
            assert!(length < 4096);
            let mut body = vec![0; length];
            reader.read_exact(&mut body).unwrap();
            let request: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(request["method"], "getwcashparentcontext");
            assert_eq!(request["params"], json!([request_previous, request_height]));
            let response =
                serde_json::to_vec(&json!({"jsonrpc":"2.0","id":request["id"],"result":reply}))
                    .unwrap();
            write!(stream,"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",response.len()).unwrap();
            stream.write_all(&response).unwrap();
        });
        let config = NativeZcashConfig::from_encoded_payout(
            RpcEndpoint::new("http://127.0.0.1:1", None, None).unwrap(),
            vec![endpoint],
            NativeZcashNetwork::Mainnet,
            Network::Mainnet.genesis_hash().to_string(),
            address,
        )
        .unwrap();
        let provider = NativeZcashProvider::connect(config).unwrap();
        let result = provider
            .validate_independent_parent_context(&provider.proposal_validators[0], &prepared);
        assert_eq!(result.is_ok(), defect == 0, "context defect {defect}");
        server.join().unwrap();
    }
}

#[test]
fn mandatory_outputs_are_exact_and_remaining_transparent_value_has_one_recipient() {
    let (template, address) = fixture(NU7_HEIGHT, 0, &[10_001, 10_001], 6);
    let coinbase = decode_template_coinbase(&template.coinbase_txn).unwrap();
    let outputs = coinbase.outputs();
    validate_parent_transparent_recipients(
        &outputs,
        &address,
        NU7_HEIGHT,
        &Network::Mainnet,
        template.parent_context.as_ref(),
    )
    .unwrap();
    let mut diverted = outputs.clone();
    diverted.push(Output::new(
        Amount::try_from(1).unwrap(),
        transparent::Address::from_pub_key_hash(Network::Mainnet.kind(), [0x77; 20]).script(),
    ));
    assert!(validate_parent_transparent_recipients(
        &diverted,
        &address,
        NU7_HEIGHT,
        &Network::Mainnet,
        template.parent_context.as_ref()
    )
    .is_err());
    let mut missing = outputs;
    assert!(
        missing.len() > 1,
        "the activation fixture has a mandatory transparent funding output"
    );
    missing.pop();
    assert!(validate_parent_transparent_recipients(
        &missing,
        &address,
        NU7_HEIGHT,
        &Network::Mainnet,
        template.parent_context.as_ref()
    )
    .is_err());
}

#[test]
fn fee_sums_are_bounded_and_each_template_fee_is_required() {
    let (mut template, _) = fixture(NU7_HEIGHT, 0, &[10_001, 10_001], 6);
    for fee in [i64::MAX, -1] {
        template.transactions[0].fee = fee;
        assert!(validate_template_fees(&template, &Network::Mainnet).is_err());
    }
    template.transactions[0].fee = 2_100_000_000_000_000;
    template.transactions[1].fee = 1;
    assert!(validate_template_fees(&template, &Network::Mainnet).is_err());
}

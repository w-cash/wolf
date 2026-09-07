//! Tests for types and functions for the `getblocktemplate` RPC.

use anyhow::anyhow;
use std::iter;
use zebra_chain::amount::{Amount, MAX_WCASH_COINBASE_VALUE};

use strum::IntoEnumIterator;
use zcash_keys::address::Address;

use zebra_chain::parameters::testnet::ConfiguredFundingStreamRecipient;

use zebra_chain::{
    block::Height,
    parameters::{
        subsidy::FundingStreamReceiver::{Deferred, Ecc, MajorGrants, ZcashFoundation},
        testnet::{self, ConfiguredActivationHeights, ConfiguredFundingStreams},
        Network, NetworkUpgrade,
    },
    serialization::ZcashDeserializeInto,
    transaction::Transaction,
};

use crate::client::TransactionTemplate;
use crate::config::mining::{
    default_miner_address, default_miner_address_for_network, Config, MinerAddressType,
};

use super::MinerParams;

/// Tests that coinbase transactions can be generated.
///
/// This test needs to be run with the `--release` flag so that it runs for ~ 30 seconds instead of
/// ~ 90.
#[test]
#[ignore]
fn coinbase() -> anyhow::Result<()> {
    let regtest = testnet::Parameters::build()
        .with_slow_start_interval(Height::MIN)
        .with_activation_heights(ConfiguredActivationHeights {
            overwinter: Some(1),
            sapling: Some(2),
            blossom: Some(3),
            heartwood: Some(4),
            canopy: Some(5),
            nu5: Some(6),
            nu6: Some(7),
            nu6_1: Some(8),
            nu7: Some(9),
            ..Default::default()
        })?
        .with_funding_streams(vec![
            ConfiguredFundingStreams {
                height_range: Some(Height(1)..Height(100)),
                recipients: Some(vec![
                    ConfiguredFundingStreamRecipient::new_for(Ecc),
                    ConfiguredFundingStreamRecipient::new_for(ZcashFoundation),
                    ConfiguredFundingStreamRecipient::new_for(MajorGrants),
                ]),
            },
            ConfiguredFundingStreams {
                height_range: Some(Height(1)..Height(100)),
                recipients: Some(vec![
                    ConfiguredFundingStreamRecipient::new_for(MajorGrants),
                    ConfiguredFundingStreamRecipient {
                        receiver: Deferred,
                        numerator: 12,
                        addresses: None,
                    },
                ]),
            },
        ])
        .to_network()?;

    for net in Network::iter().chain(iter::once(regtest)) {
        for nu in NetworkUpgrade::iter().filter(|nu| nu >= &NetworkUpgrade::Sapling) {
            if let Some(height) = nu.activation_height(&net) {
                for addr_type in MinerAddressType::iter() {
                    TransactionTemplate::new_coinbase(
                        &net,
                        height,
                        &MinerParams::from(
                            Address::decode(&net, default_miner_address(net.kind(), &addr_type))
                                .ok_or(anyhow!("hard-coded addr must be valid"))?,
                        ),
                        Amount::zero(),
                    )?
                    .data()
                    .as_ref()
                    // Deserialization contains checks for elementary consensus rules, which must
                    // pass.
                    .zcash_deserialize_into::<Transaction>()?;
                }
            }
        }
    }

    Ok(())
}

/// The Zebra marker is always prepended, and `extra_coinbase_data` can't exceed the limit.
#[test]
fn coinbase_tag_and_limit() {
    use zcash_address::ZcashAddress;

    use crate::config::mining::{
        Config, ExtraCoinbaseData, MAX_USER_COINBASE_DATA_LEN, ZEBRA_COINBASE_MARKER,
        ZEBRA_COINBASE_SEPARATOR,
    };

    // `ExtraCoinbaseData` accepts data up to the limit and rejects one byte over. Its `Deserialize`
    // impl delegates here, so an oversized `mining.extra_coinbase_data` makes the config fail to
    // load and the node refuse to start.
    assert!(ExtraCoinbaseData::try_from("x".repeat(MAX_USER_COINBASE_DATA_LEN)).is_ok());
    assert!(ExtraCoinbaseData::try_from("x".repeat(MAX_USER_COINBASE_DATA_LEN + 1)).is_err());

    let net = Network::Mainnet;
    let addr: ZcashAddress = default_miner_address(net.kind(), &MinerAddressType::Transparent)
        .parse()
        .expect("default miner address parses");

    let params = |extra: Option<ExtraCoinbaseData>| {
        MinerParams::new(
            &net,
            Config {
                miner_address: Some(addr.clone().into()),
                extra_coinbase_data: extra,
                ..Default::default()
            },
        )
    };

    // The marker is prepended whether or not `extra_coinbase_data` is set, so every block Zebra
    // builds is tagged. Without extra data, the coinbase data is exactly the marker.
    let untagged = params(None).expect("valid config");
    let untagged = untagged.data().as_ref().expect("marker is always present");
    assert_eq!(
        untagged.value().as_slice(),
        ZEBRA_COINBASE_MARKER.as_bytes()
    );

    // With extra data, the marker and separator precede it.
    let tag = ExtraCoinbaseData::try_from("/pool/".to_string()).expect("within the limit");
    let tagged = params(Some(tag)).expect("valid config");
    let tagged = tagged.data().as_ref().expect("marker is always present");
    assert_eq!(
        tagged.value().as_slice(),
        [ZEBRA_COINBASE_MARKER, ZEBRA_COINBASE_SEPARATOR, "/pool/"]
            .concat()
            .as_bytes()
    );
}

/// Tests that the coinbase cache reuses a previously built coinbase for the same height and fees,
/// so a short-polling miner doesn't re-run the shielded-coinbase proof on every request.
#[test]
fn coinbase_cache_reuses_built_coinbase() {
    use super::CoinbaseCache;

    let net = Network::Mainnet;
    let height = NetworkUpgrade::Nu5
        .activation_height(&net)
        .expect("Nu5 is active on Mainnet");
    let miner_params = MinerParams::from(
        Address::decode(
            &net,
            default_miner_address(net.kind(), &MinerAddressType::Sapling),
        )
        .expect("hard-coded Sapling address is valid"),
    );
    let fee = Amount::zero();

    let build = || {
        TransactionTemplate::new_coinbase(&net, height, &miner_params, fee)
            .expect("valid coinbase tx")
    };

    // A shielded coinbase carries a randomized proof, so two fresh builds differ. Identical bytes
    // therefore prove the cache returned a reused transaction rather than rebuilding it.
    let coinbase = build();
    assert_ne!(
        build(),
        coinbase,
        "fresh shielded coinbases differ (randomized proof)"
    );

    let cache = CoinbaseCache::default();
    assert!(cache.get(height, fee).is_none(), "an empty cache misses");

    cache.store(height, fee, coinbase.clone());
    assert_eq!(
        cache.get(height, fee),
        Some(coinbase.clone()),
        "a cache hit reuses the stored coinbase",
    );

    // A different height key or a cleared cache misses, so the next request rebuilds.
    let next_height = height.next().expect("height is below Height::MAX");
    assert!(
        cache.get(next_height, fee).is_none(),
        "a different height misses"
    );
    cache.clear();
    assert!(cache.get(height, fee).is_none(), "a cleared cache misses");
}

/// Verifies the fix for #10907: the multi-entry coinbase cache retains both the zero-fee fake
/// coinbase (used for ZIP-317 weight sizing) and the real-fee coinbase simultaneously, so
/// `getblocktemplate` doesn't rebuild shielded proofs on every short-poll.
#[test]
fn coinbase_cache_retains_both_fake_and_real_fee_entries() {
    use super::CoinbaseCache;

    let height = Height(1_000_000);
    let zero_fee = Amount::zero();
    let real_fee: Amount<zebra_chain::amount::NonNegative> =
        Amount::try_from(10_000).expect("valid amount");

    let cache = CoinbaseCache::default();

    // Simulate what getblocktemplate does: store a fake coinbase at zero fee (ZIP-317 sizing),
    // then store the real coinbase at the actual fee.
    let fake_coinbase = TransactionTemplate::new_coinbase(
        &Network::Mainnet,
        height,
        &MinerParams::from(
            Address::decode(
                &Network::Mainnet,
                default_miner_address(
                    zebra_chain::parameters::NetworkKind::Mainnet,
                    &MinerAddressType::Sapling,
                ),
            )
            .unwrap(),
        ),
        zero_fee,
    )
    .unwrap();

    let real_coinbase = TransactionTemplate::new_coinbase(
        &Network::Mainnet,
        height,
        &MinerParams::from(
            Address::decode(
                &Network::Mainnet,
                default_miner_address(
                    zebra_chain::parameters::NetworkKind::Mainnet,
                    &MinerAddressType::Sapling,
                ),
            )
            .unwrap(),
        ),
        real_fee,
    )
    .unwrap();

    cache.store(height, zero_fee, fake_coinbase.clone());
    cache.store(height, real_fee, real_coinbase.clone());

    // Both entries coexist — the zero-fee sizing coinbase survives the real-fee store.
    assert_eq!(
        cache.get(height, zero_fee),
        Some(fake_coinbase),
        "zero-fee fake coinbase should still be cached after storing real-fee coinbase"
    );
    assert_eq!(
        cache.get(height, real_fee),
        Some(real_coinbase),
        "real-fee coinbase should be cached"
    );

    // Height transition: storing at a new height evicts the stale entries.
    let next_height = Height(height.0 + 1);
    let next_coinbase = TransactionTemplate::new_coinbase(
        &Network::Mainnet,
        next_height,
        &MinerParams::from(
            Address::decode(
                &Network::Mainnet,
                default_miner_address(
                    zebra_chain::parameters::NetworkKind::Mainnet,
                    &MinerAddressType::Sapling,
                ),
            )
            .unwrap(),
        ),
        zero_fee,
    )
    .unwrap();

    cache.store(next_height, zero_fee, next_coinbase.clone());
    assert_eq!(
        cache.get(next_height, zero_fee),
        Some(next_coinbase),
        "new-height entry should be cached"
    );
    assert!(
        cache.get(height, zero_fee).is_none(),
        "old-height entry should be evicted"
    );
}

/// Verifies that fee churn beyond the cache cap (4 entries) evicts stale nonzero-fee entries
/// while preserving the zero-fee sizing coinbase. Without this, the cap would clear the
/// entire map — including the zero-fee entry — recreating the original #10907 churn.
#[test]
fn coinbase_cache_preserves_zero_fee_entry_at_capacity() {
    use super::CoinbaseCache;

    let height = Height(2_000_000);
    let zero_fee = Amount::zero();
    let cache = CoinbaseCache::default();

    let miner_params = MinerParams::from(
        Address::decode(
            &Network::Mainnet,
            default_miner_address(
                zebra_chain::parameters::NetworkKind::Mainnet,
                &MinerAddressType::Sapling,
            ),
        )
        .unwrap(),
    );

    let make_coinbase = |fee: Amount<zebra_chain::amount::NonNegative>| {
        TransactionTemplate::new_coinbase(&Network::Mainnet, height, &miner_params, fee).unwrap()
    };

    // Store the zero-fee sizing coinbase first.
    let fake_coinbase = make_coinbase(zero_fee);
    cache.store(height, zero_fee, fake_coinbase.clone());

    // Fill to capacity with distinct fee values (simulating mempool fee churn).
    for i in 1..=5u64 {
        let fee = Amount::try_from(i * 1_000).expect("valid amount");
        cache.store(height, fee, make_coinbase(fee));
    }

    // The zero-fee entry must survive eviction at capacity.
    assert_eq!(
        cache.get(height, zero_fee),
        Some(fake_coinbase.clone()),
        "zero-fee sizing coinbase must survive fee churn at capacity"
    );

    // Updating an existing key at capacity should not trigger eviction.
    let fee_1k: Amount<zebra_chain::amount::NonNegative> =
        Amount::try_from(1_000).expect("valid amount");
    let updated_coinbase = make_coinbase(fee_1k);
    cache.store(height, fee_1k, updated_coinbase.clone());
    assert_eq!(
        cache.get(height, fee_1k),
        Some(updated_coinbase),
        "updating an existing key should replace in place"
    );
    assert_eq!(
        cache.get(height, zero_fee),
        Some(fake_coinbase),
        "zero-fee entry must still be present after in-place update"
    );
}

/// From NU6.3 onward, a shielded coinbase paid to a Unified miner address with an Orchard
/// receiver routes newly minted value into the Ironwood pool, not the Orchard pool, and remains
/// recoverable with the consensus-required all-zero outgoing viewing key.
#[test]
fn coinbase_at_nu6_3_routes_shielded_output_to_ironwood() {
    let net = Network::new_default_testnet();
    let height = NetworkUpgrade::Nu6_3
        .activation_height(&net)
        .expect("Nu6.3 is scheduled on Testnet");
    let miner_params = MinerParams::from(
        Address::decode(
            &net,
            default_miner_address(net.kind(), &MinerAddressType::Unified),
        )
        .expect("hard-coded Unified address is valid"),
    );

    let template = TransactionTemplate::new_coinbase(&net, height, &miner_params, Amount::zero())
        .expect("valid coinbase tx");
    let coinbase: Transaction = template.data.as_ref().zcash_deserialize_into().unwrap();

    // The coinbase is a v6 transaction with Ironwood shielded data and no Orchard shielded data.
    // ZIP-229: from NU6.3, coinbase MUST have an empty Orchard component.
    assert_eq!(coinbase.version(), 6, "coinbase is v6 at NU6.3");
    assert!(
        coinbase.ironwood_shielded_data().is_some(),
        "coinbase creates an Ironwood output"
    );
    assert!(
        coinbase.orchard_shielded_data().is_none(),
        "coinbase must not create Orchard components on NU6.3"
    );
    zebra_consensus::transaction::check::coinbase_outputs_are_decryptable(&coinbase, &net, height)
        .expect("Ironwood coinbase output is recoverable with the zero outgoing viewing key");
}

/// Wcash templates pay the entire coinbase reward privately into Ironwood and reject miner
/// addresses that cannot receive an Ironwood note.
#[test]
fn wcash_coinbase_is_private_and_ironwood_only() {
    use zebra_chain::parameters::subsidy::SubsidyError;
    use zebra_consensus::error::TransactionError;

    let net = Network::new_wcash_regtest();
    let height = Height(1);
    let unified_address = default_miner_address_for_network(&net, &MinerAddressType::Unified);
    let config_for = |address: &str| Config {
        miner_address: Some(address.parse().expect("hard-coded address parses")),
        ..Default::default()
    };
    let miner_params = MinerParams::new(&net, config_for(&unified_address))
        .expect("the Wcash Unified miner address is valid");
    let miner_fees = Amount::try_from(12_345).expect("valid fee amount");

    let template = TransactionTemplate::new_coinbase(&net, height, &miner_params, miner_fees)
        .expect("valid private Wcash coinbase tx");
    let coinbase: Transaction = template.data.as_ref().zcash_deserialize_into().unwrap();

    assert_eq!(coinbase.version(), 6);
    assert!(coinbase.is_coinbase());
    assert!(coinbase.outputs().is_empty());
    assert!(!coinbase.has_sapling_shielded_data());
    assert!(!coinbase.has_orchard_shielded_data());
    assert!(coinbase.ironwood_actions().next().is_some());
    assert!(
        zebra_chain::primitives::zcash_note_encryption::ironwood_outputs_are_private_from_zero_ovk(
            &coinbase, &net, height,
        ),
        "every Wcash reward action must reject public recovery with Zcash's zero OVK"
    );
    zebra_consensus::transaction::check::wcash_coinbase_outputs_are_private(
        &coinbase, &net, height,
    )
    .expect("the private Wcash coinbase passes its consensus privacy rule");
    assert_eq!(
        coinbase
            .ironwood_value_balance()
            .ironwood_amount()
            .zatoshis(),
        -1_000_012_345
    );

    assert!(matches!(
        zebra_consensus::transaction::check::coinbase_outputs_are_decryptable(
            &coinbase, &net, height
        ),
        Err(TransactionError::CoinbaseOutputsNotDecryptable)
    ));
    assert!(MinerParams::new(&net, config_for(&unified_address)).is_ok());

    let inherited_zcash_address = default_miner_address(net.kind(), &MinerAddressType::Unified);
    assert!(matches!(
        MinerParams::new(&net, config_for(inherited_zcash_address)),
        Err(super::MinerParamsError::WcashAddressNamespaceRequired)
    ));

    for invalid_type in [MinerAddressType::Sapling, MinerAddressType::Transparent] {
        let invalid_address = default_miner_address_for_network(&net, &invalid_type);
        assert!(matches!(
            MinerParams::new(&net, config_for(&invalid_address)),
            Err(super::MinerParamsError::WcashRequiresIronwoodReceiver)
        ));

        let invalid_params = MinerParams::new(&net, config_for(&invalid_address))
            .expect_err("the Wcash coinbase policy rejects non-Unified miner addresses");
        assert!(matches!(
            invalid_params,
            super::MinerParamsError::WcashRequiresIronwoodReceiver
        ));
    }

    let excessive_fees = Amount::try_from(MAX_WCASH_COINBASE_VALUE)
        .expect("the per-transaction cap is representable");
    assert!(matches!(
        TransactionTemplate::new_coinbase(&net, height, &miner_params, excessive_fees),
        Err(TransactionError::Subsidy(
            SubsidyError::WcashCoinbaseValueTooLarge
        ))
    ));
}

/// Once scheduled issuance reaches zero, Wcash still routes both zero and positive fee totals
/// through private Ironwood actions rather than falling back to a transparent or empty coinbase.
#[test]
fn wcash_zero_subsidy_tail_stays_private() {
    let net = Network::new_wcash_regtest();
    let height = Height(50_400_001);
    let unified_address = default_miner_address_for_network(&net, &MinerAddressType::Unified);
    let miner_params = MinerParams::new(
        &net,
        Config {
            miner_address: Some(
                unified_address
                    .parse()
                    .expect("hard-coded Wcash Unified address is valid"),
            ),
            ..Default::default()
        },
    )
    .expect("the Wcash Unified miner address is valid");

    for fee_zatoshis in [0i64, 12_345] {
        let fees = Amount::try_from(fee_zatoshis).expect("test fee is representable");
        let template = TransactionTemplate::new_coinbase(&net, height, &miner_params, fees)
            .expect("a zero-subsidy Wcash coinbase remains buildable");
        let coinbase: Transaction = template
            .data
            .as_ref()
            .zcash_deserialize_into()
            .expect("tail coinbase deserializes");

        assert!(coinbase.outputs().is_empty());
        assert!(!coinbase.has_sapling_shielded_data());
        assert!(!coinbase.has_orchard_shielded_data());
        assert!(coinbase.ironwood_actions().next().is_some());
        assert_eq!(
            coinbase
                .ironwood_value_balance()
                .ironwood_amount()
                .zatoshis(),
            -fee_zatoshis
        );
        zebra_consensus::transaction::check::wcash_coinbase_outputs_are_private(
            &coinbase, &net, height,
        )
        .expect("the tail Ironwood actions remain private");
    }
}

/// Wcash consensus can distinguish its private Ironwood rewards from a normal
/// Zcash NU6.3 coinbase that deliberately uses the public all-zero OVK.
#[test]
fn wcash_privacy_rule_rejects_public_zero_ovk_ironwood() {
    let zcash_net = Network::new_default_testnet();
    let zcash_height = NetworkUpgrade::Nu6_3
        .activation_height(&zcash_net)
        .expect("NU6.3 is scheduled on Testnet");
    let miner_params = MinerParams::from(
        Address::decode(
            &zcash_net,
            default_miner_address(zcash_net.kind(), &MinerAddressType::Unified),
        )
        .expect("hard-coded Unified address is valid"),
    );
    let public_template =
        TransactionTemplate::new_coinbase(&zcash_net, zcash_height, &miner_params, Amount::zero())
            .expect("valid standard Zcash coinbase");
    let public_coinbase: Transaction = public_template
        .data
        .as_ref()
        .zcash_deserialize_into()
        .expect("coinbase deserializes");

    let wcash_net = Network::new_wcash_regtest();
    assert!(
        !zebra_chain::primitives::zcash_note_encryption::ironwood_outputs_are_private_from_zero_ovk(
            &public_coinbase,
            &wcash_net,
            Height(1),
        ),
        "a standard publicly recoverable Zcash coinbase must fail Wcash privacy"
    );

    assert!(matches!(
        zebra_consensus::transaction::check::wcash_coinbase_outputs_are_private(
            &public_coinbase,
            &wcash_net,
            Height(1),
        ),
        Err(zebra_consensus::error::TransactionError::WcashCoinbaseOutputPubliclyRecoverable)
    ));
}

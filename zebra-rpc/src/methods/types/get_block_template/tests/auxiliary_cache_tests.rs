//! Regression coverage for recipient-isolated auxiliary coinbase proof reuse.

use super::*;
use std::sync::{mpsc, Condvar, Mutex};

type Handler = super::super::GetBlockTemplateHandler<
    MockService<zebra_consensus::Request, block::Hash, zebra_test::mock_service::PanicAssertion>,
    MockSyncStatus,
>;

fn handler() -> Handler {
    handler_for_network(&Network::Mainnet)
}

fn handler_for_network(network: &Network) -> Handler {
    super::super::GetBlockTemplateHandler::new(
        network,
        Config {
            miner_address: Some(
                default_miner_address_for_network(network, &MinerAddressType::Transparent)
                    .expect("transparent payout supported for fixture network")
                    .parse()
                    .expect("valid public transparent fixture address"),
            ),
            ..Default::default()
        },
        MockService::build().for_unit_tests(),
        MockSyncStatus::default(),
        None,
    )
}

fn params(tag: u8) -> MinerParams {
    MinerParams::from(Address::Transparent(
        zcash_transparent::address::TransparentAddress::PublicKeyHash([tag; 20]),
    ))
}

fn coinbase(
    params: &MinerParams,
    height: Height,
    fee: Amount<zebra_chain::amount::NonNegative>,
) -> TransactionTemplate<zebra_chain::amount::NegativeOrZero> {
    TransactionTemplate::new_coinbase(&Network::Mainnet, height, params, fee)
        .expect("valid exact public coinbase fixture")
}

#[test]
fn auxiliary_cache_reuses_repeated_recipient_across_rpc_handler_clones() {
    let handler = handler();
    let height = NetworkUpgrade::Nu5
        .activation_height(&Network::Mainnet)
        .unwrap();
    let fee = Amount::zero();
    let configured = handler.miner_params().unwrap().unwrap().clone();
    let configured_coinbase = coinbase(&configured, height, fee);
    handler
        .coinbase_cache()
        .store(height, fee, configured_coinbase.clone());
    let alternate = params(0x31);
    let alternate_coinbase = coinbase(&alternate, height, fee);
    assert_ne!(alternate_coinbase, configured_coinbase);
    let builds = AtomicUsize::new(0);
    for _ in 0..3 {
        let mut request = handler.clone();
        request.set_auxiliary_miner_params(alternate.clone());
        assert!(
            request.template_cache().is_none(),
            "an alternate recipient cannot receive a configured template"
        );
        assert_eq!(
            request.coinbase_cache().get_or_build(height, fee, || {
                builds.fetch_add(1, Ordering::SeqCst);
                alternate_coinbase.clone()
            }),
            alternate_coinbase
        );
    }
    assert_eq!(
        builds.load(Ordering::SeqCst),
        1,
        "repeated createauxblock clones must not repeat the same proof"
    );
    assert_eq!(
        handler.coinbase_cache().get(height, fee),
        Some(configured_coinbase)
    );
    assert!(
        handler.template_cache().is_some(),
        "the configured precompute path remains untouched"
    );
}

#[test]
fn auxiliary_cache_isolates_recipient_memo_data_commitment_height_and_fees() {
    let handler = handler();
    let height = NetworkUpgrade::Nu5
        .activation_height(&Network::Mainnet)
        .unwrap();
    let fee = Amount::zero();
    let first = params(0x41);
    let mut request = handler.clone();
    request.set_auxiliary_miner_params(first.clone());
    let original_cache = request.coinbase_cache();
    original_cache.store(height, fee, coinbase(&first, height, fee));
    let mut other_data = first.clone();
    other_data.randomize_data();
    let mut other_memo = first.clone();
    other_memo.randomize_memo();
    let mut other_commitment = first.clone();
    other_commitment.parent_payout_address_commitment = Some("11".repeat(32));
    for other in [params(0x42), other_data, other_memo, other_commitment] {
        let mut request = handler.clone();
        request.set_auxiliary_miner_params(other);
        assert!(
            request.coinbase_cache().get(height, fee).is_none(),
            "changed payout inputs must never reuse another recipient's proof"
        );
    }
    assert!(original_cache.get(height, fee).is_some());
    assert!(original_cache.get(height.next().unwrap(), fee).is_none());
    assert!(original_cache
        .get(height, Amount::try_from(1i64).unwrap())
        .is_none());
}

#[test]
fn auxiliary_cache_singleflight_does_not_hold_recipient_map_while_proving() {
    let handler = handler();
    let height = NetworkUpgrade::Nu5
        .activation_height(&Network::Mainnet)
        .unwrap();
    let fee = Amount::zero();
    let first = params(0x51);
    let expected = coinbase(&first, height, fee);
    let builds = Arc::new(AtomicUsize::new(0));
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut building = handler.clone();
    let first_params = first.clone();
    let building_expected = expected.clone();
    let building_count = Arc::clone(&builds);
    let builder = thread::spawn(move || {
        building.set_auxiliary_miner_params(first_params);
        building.coinbase_cache().get_or_build(height, fee, || {
            building_count.fetch_add(1, Ordering::SeqCst);
            entered_tx.send(()).unwrap();
            release_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("test releases the held local proof");
            building_expected
        })
    });
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let mut waiting = handler.clone();
    let waiting_count = Arc::clone(&builds);
    let waiting_expected = expected.clone();
    let waiter = thread::spawn(move || {
        waiting.set_auxiliary_miner_params(first);
        waiting.coinbase_cache().get_or_build(height, fee, || {
            waiting_count.fetch_add(1, Ordering::SeqCst);
            waiting_expected
        })
    });
    let (other_tx, other_rx) = mpsc::channel();
    let mut other = handler.clone();
    let other_worker = thread::spawn(move || {
        let other_params = params(0x52);
        other.set_auxiliary_miner_params(other_params.clone());
        let result = other
            .coinbase_cache()
            .get_or_build(height, fee, || coinbase(&other_params, height, fee));
        other_tx.send(result).unwrap();
    });
    let independent = other_rx.recv_timeout(Duration::from_secs(2));
    // Release before asserting or joining, including on regression failures.
    let _ = release_tx.send(());
    let built = builder.join().unwrap();
    let waited = waiter.join().unwrap();
    other_worker.join().unwrap();
    assert!(
        independent.is_ok(),
        "another recipient must finish before the first proof is released"
    );
    assert_ne!(independent.unwrap(), expected);
    assert_eq!(built, expected);
    assert_eq!(waited, expected);
    assert_eq!(
        builds.load(Ordering::SeqCst),
        1,
        "same-recipient concurrent clones share one proof build"
    );
}

#[test]
fn auxiliary_cache_capacity_eviction_keeps_inflight_handles_recipient_isolated() {
    let handler = handler();
    let height = NetworkUpgrade::Nu5
        .activation_height(&Network::Mainnet)
        .unwrap();
    let fee = Amount::zero();
    let oldest = params(0x61);
    let expected = coinbase(&oldest, height, fee);
    let mut retained_request = handler.clone();
    retained_request.set_auxiliary_miner_params(oldest.clone());
    let retained_cache = retained_request.coinbase_cache();
    retained_cache.store(height, fee, expected.clone());
    for tag in 0x62..0x72 {
        let mut request = handler.clone();
        request.set_auxiliary_miner_params(params(tag));
        assert!(request.coinbase_cache().get(height, fee).is_none());
        assert!(
            handler.auxiliary_coinbase_caches.0.lock().unwrap().len()
                <= super::super::MAX_AUXILIARY_COINBASE_RECIPIENTS
        );
    }
    assert_eq!(retained_cache.get(height, fee), Some(expected));
    let mut replacement = handler.clone();
    replacement.set_auxiliary_miner_params(oldest);
    assert!(
        replacement.coinbase_cache().get(height, fee).is_none(),
        "eviction permits rebuilding but never aliases a different recipient"
    );
    assert_eq!(
        handler.auxiliary_coinbase_caches.0.lock().unwrap().len(),
        super::super::MAX_AUXILIARY_COINBASE_RECIPIENTS
    );
}

#[test]
fn auxiliary_cache_fee_churn_is_bounded_on_the_real_build_insertion_path() {
    let handler = handler();
    let mut request = handler.clone();
    let recipient = params(0x65);
    request.set_auxiliary_miner_params(recipient.clone());
    let cache = request.coinbase_cache();
    let height = NetworkUpgrade::Nu5
        .activation_height(&Network::Mainnet)
        .unwrap();
    let zero = Amount::zero();
    let sizing = cache.get_or_build(height, zero, || coinbase(&recipient, height, zero));
    for fee in 1i64..=32 {
        let fee = Amount::try_from(fee).unwrap();
        let built = cache.get_or_build(height, fee, || coinbase(&recipient, height, fee));
        assert_eq!(built, coinbase(&recipient, height, fee));
        assert!(
            cache
                .0
                .lock()
                .unwrap()
                .keys()
                .filter(|(h, _)| *h == height)
                .count()
                <= 4
        );
        assert_eq!(
            cache.get(height, zero),
            Some(sizing.clone()),
            "fee churn cannot discard the zero-fee sizing proof"
        );
    }
}

#[test]
fn auxiliary_cache_fee_eviction_preserves_active_builders_and_their_waiters() {
    let cache = CoinbaseCache::default();
    let height = NetworkUpgrade::Nu5
        .activation_height(&Network::Mainnet)
        .unwrap();
    let recipient = params(0x66);
    let expected = (0i64..=4)
        .map(|fee| {
            let fee = Amount::try_from(fee).unwrap();
            (fee, coinbase(&recipient, height, fee))
        })
        .collect::<Vec<_>>();
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let (entered_tx, entered_rx) = mpsc::channel();
    let mut builders = Vec::new();
    let mut waiters = Vec::new();
    for (fee, value) in expected.iter().take(4).cloned() {
        let building_cache = cache.clone();
        let building_gate = Arc::clone(&gate);
        let entered = entered_tx.clone();
        builders.push(thread::spawn(move || {
            building_cache.get_or_build(height, fee, || {
                entered.send(()).unwrap();
                let (lock, changed) = &*building_gate;
                let (released, timeout) = changed
                    .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(5), |released| {
                        !*released
                    })
                    .unwrap();
                assert!(
                    !timeout.timed_out() || *released,
                    "test releases all held fee proofs"
                );
                value
            })
        }));
    }
    let all_building = (0..4).all(|_| entered_rx.recv_timeout(Duration::from_secs(2)).is_ok());
    for (fee, _) in expected.iter().take(4).cloned() {
        let waiting_cache = cache.clone();
        waiters.push(thread::spawn(move || {
            waiting_cache.get_or_build(height, fee, || {
                panic!("an admitted same-key waiter must share its active proof")
            })
        }));
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let mut all_waiting = false;
    while std::time::Instant::now() < deadline {
        all_waiting = {
            let map = cache.0.lock().unwrap();
            expected.iter().take(4).all(|(fee, _)| {
                map.get(&(height, *fee))
                    .is_some_and(|entry| Arc::strong_count(entry) >= 3)
            })
        };
        if all_waiting {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    let (new_fee, new_value) = expected[4].clone();
    let fifth = cache.get_or_build(height, new_fee, || new_value.clone());
    let (evicted, count, zero_retained) = {
        let map = cache.0.lock().unwrap();
        (
            expected
                .iter()
                .take(4)
                .enumerate()
                .find_map(|(index, (fee, _))| {
                    (!map.contains_key(&(height, *fee))).then_some(index)
                }),
            map.len(),
            map.contains_key(&(height, Amount::zero())),
        )
    };
    let (lock, changed) = &*gate;
    *lock.lock().unwrap() = true;
    changed.notify_all();
    let built = builders
        .into_iter()
        .map(|worker| worker.join())
        .collect::<Vec<_>>();
    let waited = waiters
        .into_iter()
        .map(|worker| worker.join())
        .collect::<Vec<_>>();
    assert!(
        all_building && all_waiting,
        "all original keys must be building with same-entry waiters before eviction"
    );
    assert_eq!(fifth, new_value);
    assert_eq!(count, 4);
    assert!(zero_retained);
    let evicted = evicted.expect("the fifth distinct fee evicts one nonzero Building entry");
    assert_ne!(evicted, 0);
    for (index, (built, waited)) in built.into_iter().zip(waited).enumerate() {
        assert_eq!(built.unwrap(), expected[index].1);
        assert_eq!(waited.unwrap(), expected[index].1);
    }
    let (fee, value) = expected[evicted].clone();
    let rebuilt = AtomicUsize::new(0);
    assert_eq!(
        cache.get_or_build(height, fee, || {
            rebuilt.fetch_add(1, Ordering::SeqCst);
            value.clone()
        }),
        value
    );
    assert_eq!(
        rebuilt.load(Ordering::SeqCst),
        1,
        "a new request may rebuild an evicted fee without redirecting existing waiters"
    );
    assert_eq!(cache.0.lock().unwrap().len(), 4);
}

#[cfg(feature = "wcash-consensus")]
#[test]
fn auxiliary_private_wcash_proof_repro_rebuilds_twice_before_fix_and_once_with_shared_cache() {
    use orchard::keys::{FullViewingKey, Scope, SpendingKey};
    use std::time::Instant;
    use zcash_address::unified::{Encoding, Receiver};
    use zcash_protocol::consensus::NetworkType;
    use zebra_chain::primitives::WcashAddress;

    let network = Network::new_wcash_regtest();
    let height = Height(1);
    let fee = Amount::zero();
    // Disposable public fixture keys only; no production wallet or address.
    let spending = Option::<SpendingKey>::from(SpendingKey::from_bytes([0x71; 32])).unwrap();
    let viewing = FullViewingKey::from(&spending);
    let ivk = viewing.to_ivk(Scope::External);
    let receiver = viewing.address_at(0u32, Scope::External);
    let address = WcashAddress::from_unified(
        NetworkType::Regtest,
        zcash_address::unified::Address::try_from_items(vec![Receiver::Orchard(
            receiver.to_raw_address_bytes(),
        )])
        .unwrap(),
    )
    .unwrap();
    let wrong_spending = Option::<SpendingKey>::from(SpendingKey::from_bytes([0x72; 32])).unwrap();
    let wrong_ivk = FullViewingKey::from(&wrong_spending).to_ivk(Scope::External);
    let private = MinerParams::new(
        &network,
        Config {
            miner_address: Some(address.encode().parse().unwrap()),
            ..Default::default()
        },
    )
    .unwrap();
    let handler = handler_for_network(&network);
    assert_ne!(
        handler.miner_params().unwrap().unwrap().addr(),
        private.addr()
    );
    let validate = |template: &TransactionTemplate<zebra_chain::amount::NegativeOrZero>| {
        let coinbase: Transaction = template.data.as_ref().zcash_deserialize_into().unwrap();
        assert_eq!(
            coinbase.zcash_serialize_to_vec().unwrap(),
            template.data.as_ref()
        );
        assert_eq!(coinbase.version(), 6);
        assert!(coinbase.is_coinbase());
        assert!(coinbase.outputs().is_empty());
        assert!(!coinbase.has_sapling_shielded_data() && !coinbase.has_orchard_shielded_data());
        zebra_consensus::transaction::check::wcash_coinbase_outputs_are_private(
            &coinbase, &network, height,
        )
        .unwrap();
        let bundle = coinbase.ironwood_bundle().unwrap();
        let notes = bundle.decrypt_outputs_with_keys(std::slice::from_ref(&ivk));
        assert!(!notes.is_empty());
        assert!(notes
            .iter()
            .all(|(_, _, _, recipient, _)| *recipient == receiver));
        assert_eq!(
            notes
                .iter()
                .map(|(_, _, note, _, _)| note.value().inner())
                .sum::<u64>(),
            625_000_000
        );
        assert!(
            bundle
                .decrypt_outputs_with_keys(std::slice::from_ref(&wrong_ivk))
                .is_empty(),
            "a different fixture recipient cannot recover the miner reward"
        );
    };
    let legacy_started = Instant::now();
    let mut legacy = Vec::new();
    let mut legacy_request_ms = Vec::new();
    for _ in 0..2 {
        let request_started = Instant::now();
        let mut request = handler.clone();
        // Exactly the old alternate-address branch: reset on every RPC clone.
        request.set_miner_params(private.clone());
        legacy.push(request.coinbase_cache().get_or_build(height, fee, || {
            TransactionTemplate::new_coinbase(&network, height, &private, fee).unwrap()
        }));
        legacy_request_ms.push(request_started.elapsed().as_millis());
    }
    let legacy_elapsed = legacy_started.elapsed();
    for coinbase in &legacy {
        validate(coinbase);
    }
    assert_ne!(
        legacy[0], legacy[1],
        "two genuine private proof builds have randomized bytes"
    );
    let fixed_started = Instant::now();
    let builds = AtomicUsize::new(0);
    let mut fixed = Vec::new();
    let mut fixed_request_ms = Vec::new();
    for _ in 0..2 {
        let request_started = Instant::now();
        let mut request = handler.clone();
        request.set_auxiliary_miner_params(private.clone());
        fixed.push(request.coinbase_cache().get_or_build(height, fee, || {
            builds.fetch_add(1, Ordering::SeqCst);
            TransactionTemplate::new_coinbase(&network, height, &private, fee).unwrap()
        }));
        fixed_request_ms.push(request_started.elapsed().as_millis());
    }
    let fixed_elapsed = fixed_started.elapsed();
    for coinbase in &fixed {
        validate(coinbase);
    }
    assert_eq!(
        fixed[0], fixed[1],
        "later pool requests retain the exact completed private proof"
    );
    assert_eq!(builds.load(Ordering::SeqCst), 1);
    #[expect(
        clippy::print_stderr,
        reason = "Expose the old/new proof measurements only in this local regression test"
    )]
    eprintln!("auxiliary_private_proof_repro legacy_builds=2 fixed_builds=1 legacy_request_ms={legacy_request_ms:?} fixed_request_ms={fixed_request_ms:?} legacy_total_ms={} fixed_total_ms={}", legacy_elapsed.as_millis(), fixed_elapsed.as_millis());
}

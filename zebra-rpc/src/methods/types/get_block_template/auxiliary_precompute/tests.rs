//! Offline regressions for bounded, recipient-specific speculative proof work.

use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    mpsc, Condvar,
};
use std::time::Duration;

use zcash_protocol::consensus::NetworkType;
use zebra_chain::{
    primitives::WcashAddress, serialization::ZcashDeserializeInto, transaction::Transaction,
    transparent,
};

use crate::config::mining::Config;

use super::*;

const DEADLINE: Duration = Duration::from_secs(10);

fn fee(value: i64) -> Amount<NonNegative> {
    Amount::try_from(value).unwrap()
}

fn params(network: &Network, tag: u8) -> MinerParams {
    let address = WcashAddress::from_transparent_p2pkh(NetworkType::Regtest, [tag; 20]);
    MinerParams::new(
        network,
        Config {
            miner_address: Some(address.encode().parse().unwrap()),
            ..Default::default()
        },
    )
    .unwrap()
}

fn coinbase(
    network: &Network,
    height: Height,
    params: &MinerParams,
    fee: Amount<NonNegative>,
) -> TransactionTemplate<NegativeOrZero> {
    TransactionTemplate::new_coinbase(network, height, params, fee).unwrap()
}

fn check_public_coinbase(
    template: &TransactionTemplate<NegativeOrZero>,
    network: &Network,
    height: Height,
    tag: u8,
    value: i64,
) {
    let tx: Transaction = template.data.as_ref().zcash_deserialize_into().unwrap();
    assert!(matches!(
        &tx.inputs()[0],
        transparent::Input::Coinbase { height: actual, .. } if *actual == height
    ));
    assert_eq!(tx.outputs().len(), 1);
    assert_eq!(
        tx.outputs()[0].address(network),
        Some(transparent::Address::from_pub_key_hash(
            network.kind(),
            [tag; 20]
        ))
    );
    assert_eq!(tx.outputs()[0].value().zatoshis(), value);
}

async fn register(
    worker: &AuxiliaryPrecomputer,
    network: &Network,
    params: &MinerParams,
    cache: &CoinbaseCache,
    height: Height,
    observed_fee: Amount<NonNegative>,
) {
    tokio::time::timeout(DEADLINE, async {
        loop {
            if worker.register(network, params, cache.clone(), height, observed_fee) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("optional registration becomes available without waiting for a proof");
}

async fn idle(worker: &AuxiliaryPrecomputer) {
    tokio::time::timeout(DEADLINE, worker.wait_for_idle())
        .await
        .expect("bounded fixture warming finishes");
}

#[derive(Default)]
struct Gate {
    released: Mutex<bool>,
    changed: Condvar,
}

impl Gate {
    fn wait(&self) {
        let released = self.released.lock().unwrap();
        let (released, timed) = self
            .changed
            .wait_timeout_while(released, DEADLINE, |released| !*released)
            .unwrap();
        assert!(
            !timed.timed_out() || *released,
            "test releases the proof gate"
        );
    }

    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn future_coinbases_use_exact_recipient_height_and_fee_without_touching_current_work() {
    let network = Network::new_wcash_regtest();
    let configured = params(&network, 0x21);
    let actual = params(&network, 0x22);
    let configured_cache = CoinbaseCache::default();
    let actual_cache = CoinbaseCache::default();
    let configured_current = coinbase(&network, Height(1), &configured, fee(12_345));
    let actual_current = coinbase(&network, Height(1), &actual, fee(12_345));
    configured_cache.store(Height(1), fee(12_345), configured_current.clone());
    actual_cache.store(Height(1), fee(12_345), actual_current.clone());
    let builds = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&builds);
    let worker = AuxiliaryPrecomputer::with_builder(Arc::new(move |net, height, miner, fee| {
        counted.fetch_add(1, Ordering::SeqCst);
        assert_eq!(rayon::current_num_threads(), 1);
        TransactionTemplate::new_coinbase(net, height, miner, fee)
    }));
    register(
        &worker,
        &network,
        &actual,
        &actual_cache,
        Height(1),
        fee(12_345),
    )
    .await;
    idle(&worker).await;
    assert_eq!(builds.load(Ordering::SeqCst), 4);
    let cloned_cache = actual_cache.clone();
    for amount in [0, 12_345] {
        let warmed = cloned_cache.get_or_build(Height(2), fee(amount), || {
            panic!("an exact future proof is already complete")
        });
        check_public_coinbase(&warmed, &network, Height(2), 0x22, 625_000_000 + amount);
    }
    assert_eq!(
        actual_cache.get(Height(1), fee(12_345)),
        Some(actual_current)
    );
    assert_eq!(
        configured_cache.get(Height(1), fee(12_345)),
        Some(configured_current)
    );
    assert!(configured_cache.get(Height(2), fee(0)).is_none());
    assert!(actual_cache.get(Height(2), fee(12_346)).is_none());
    let changed_fee = actual_cache.get_or_build(Height(2), fee(12_346), || {
        coinbase(&network, Height(2), &actual, fee(12_346))
    });
    check_public_coinbase(&changed_fee, &network, Height(2), 0x22, 625_012_346);
    register(
        &worker.clone(),
        &network,
        &actual,
        &actual_cache,
        Height(1),
        fee(12_345),
    )
    .await;
    idle(&worker).await;
    assert_eq!(
        builds.load(Ordering::SeqCst),
        4,
        "clones deduplicate completed exact keys"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_held_proof_does_not_block_registration_and_namespaces_stay_bounded() {
    let network = Network::new_wcash_regtest();
    let first = params(&network, 0x31);
    let cache = CoinbaseCache::default();
    let gate = Arc::new(Gate::default());
    let held = Arc::clone(&gate);
    let first_call = Arc::new(AtomicBool::new(true));
    let enter_once = Arc::clone(&first_call);
    let (entered_tx, entered_rx) = mpsc::channel();
    let active = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));
    let proof_count = Arc::new(AtomicUsize::new(0));
    let (building, maximum_seen, count) = (
        Arc::clone(&active),
        Arc::clone(&maximum),
        Arc::clone(&proof_count),
    );
    let worker = AuxiliaryPrecomputer::with_builder(Arc::new(move |net, height, miner, fee| {
        let now = building.fetch_add(1, Ordering::SeqCst) + 1;
        maximum_seen.fetch_max(now, Ordering::SeqCst);
        count.fetch_add(1, Ordering::SeqCst);
        if enter_once.swap(false, Ordering::SeqCst) {
            entered_tx.send(()).unwrap();
            held.wait();
        }
        let result = TransactionTemplate::new_coinbase(net, height, miner, fee);
        building.fetch_sub(1, Ordering::SeqCst);
        result
    }));
    register(&worker, &network, &first, &cache, Height(1), fee(0)).await;
    let entered = entered_rx.recv_timeout(DEADLINE);
    let lock_released_during_proof = worker.0.state.try_lock().is_ok();
    for _ in 0..16 {
        register(&worker.clone(), &network, &first, &cache, Height(1), fee(0)).await;
    }
    // More callers than the recipient bound must coalesce; an in-flight old
    // recipient keeps its exact cache rather than being redirected to a new one.
    let mut other_caches = Vec::new();
    for tag in 0x32..0x3a {
        let other = params(&network, tag);
        let other_cache = CoinbaseCache::default();
        register(&worker, &network, &other, &other_cache, Height(1), fee(1)).await;
        other_caches.push(other_cache);
    }
    let state_bounds = {
        let state = worker.0.state.lock().unwrap();
        (
            state.namespaces.len(),
            state.namespaces.iter().all(|entry| entry.slots.len() <= 4),
        )
    };
    let builds_before_release = proof_count.load(Ordering::SeqCst);
    gate.release();
    idle(&worker).await;
    assert!(entered.is_ok());
    assert!(lock_released_during_proof);
    assert_eq!(builds_before_release, 1);
    assert!(state_bounds.0 <= MAX_AUXILIARY_COINBASE_RECIPIENTS && state_bounds.1);
    assert_eq!(maximum.load(Ordering::SeqCst), 1);
    assert_eq!(active.load(Ordering::SeqCst), 0);
    check_public_coinbase(
        &cache.get(Height(2), fee(0)).unwrap(),
        &network,
        Height(2),
        0x31,
        625_000_000,
    );
    assert!(other_caches
        .iter()
        .all(|other| !Arc::ptr_eq(&cache.0, &other.0)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_inflight_future_work_cannot_evict_reorg_current_work() {
    let network = Network::new_wcash_regtest();
    let miner = params(&network, 0x41);
    let cache = CoinbaseCache::default();
    let gate = Arc::new(Gate::default());
    let held = Arc::clone(&gate);
    let (entered_tx, entered_rx) = mpsc::channel();
    let worker = AuxiliaryPrecomputer::with_builder(Arc::new(move |net, height, miner, fee| {
        if height == Height(101) {
            entered_tx.send(()).unwrap();
            held.wait();
        }
        TransactionTemplate::new_coinbase(net, height, miner, fee)
    }));
    cache.store(
        Height(100),
        fee(0),
        coinbase(&network, Height(100), &miner, fee(0)),
    );
    register(&worker, &network, &miner, &cache, Height(100), fee(0)).await;
    let entered = entered_rx.recv_timeout(DEADLINE);
    let new_current = coinbase(&network, Height(98), &miner, fee(0));
    cache.store(Height(98), fee(0), new_current.clone());
    register(&worker, &network, &miner, &cache, Height(98), fee(0)).await;
    gate.release();
    idle(&worker).await;
    assert!(entered.is_ok());
    assert_eq!(cache.get(Height(98), fee(0)), Some(new_current));
    assert!(
        cache.get(Height(101), fee(0)).is_none(),
        "a pruned speculative entry is never reinserted"
    );
    assert_eq!(
        cache.get(Height(99), fee(0)),
        Some(coinbase(&network, Height(99), &miner, fee(0)))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn evicted_recipient_cache_does_not_keep_queued_proof_work_alive() {
    let network = Network::new_wcash_regtest();
    let first = params(&network, 0x62);
    let first_cache = CoinbaseCache::default();
    let gate = Arc::new(Gate::default());
    let held = Arc::clone(&gate);
    let (entered_tx, entered_rx) = mpsc::channel();
    let builds = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&builds);
    let worker = AuxiliaryPrecomputer::with_builder(Arc::new(move |net, height, miner, amount| {
        if counted.fetch_add(1, Ordering::SeqCst) == 0 {
            entered_tx.send(()).unwrap();
            held.wait();
        }
        TransactionTemplate::new_coinbase(net, height, miner, amount)
    }));
    register(&worker, &network, &first, &first_cache, Height(1), fee(0)).await;
    let entered = entered_rx.recv_timeout(DEADLINE);
    let queued = params(&network, 0x63);
    let queued_cache = CoinbaseCache::default();
    register(&worker, &network, &queued, &queued_cache, Height(1), fee(0)).await;
    let queued_lifetime = Arc::downgrade(&queued_cache.0);
    drop(queued_cache);
    let queued_cache_released = queued_lifetime.upgrade().is_none();
    gate.release();
    idle(&worker).await;
    assert!(entered.is_ok());
    assert!(
        queued_cache_released,
        "registration retains no evicted recipient cache"
    );
    assert_eq!(builds.load(Ordering::SeqCst), 2);
    assert!(first_cache.get(Height(2), fee(0)).is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_speculation_is_not_spun_and_retries_only_after_registration() {
    let network = Network::new_wcash_regtest();
    let miner = params(&network, 0x51);
    let cache = CoinbaseCache::default();
    let zero_count = Arc::new(AtomicUsize::new(0));
    let fee_count = Arc::new(AtomicUsize::new(0));
    let (zero_builds, fee_builds) = (Arc::clone(&zero_count), Arc::clone(&fee_count));
    let worker = AuxiliaryPrecomputer::with_builder(Arc::new(move |net, height, miner, amount| {
        if amount == fee(0) {
            zero_builds.fetch_add(1, Ordering::SeqCst);
        } else if fee_builds.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(TransactionError::CoinbaseOutputsNotDecryptable);
        }
        TransactionTemplate::new_coinbase(net, height, miner, amount)
    }));
    register(&worker, &network, &miner, &cache, Height(1), fee(19)).await;
    idle(&worker).await;
    assert_eq!(zero_count.load(Ordering::SeqCst), 2);
    assert_eq!(fee_count.load(Ordering::SeqCst), 2);
    assert!(cache.get(Height(2), fee(0)).is_some());
    assert!(cache.get(Height(2), fee(19)).is_none());
    assert!(cache.get(Height(3), fee(19)).is_some());
    register(&worker, &network, &miner, &cache, Height(1), fee(19)).await;
    idle(&worker).await;
    assert_eq!(zero_count.load(Ordering::SeqCst), 2);
    assert_eq!(fee_count.load(Ordering::SeqCst), 3);
    check_public_coinbase(
        &cache.get(Height(2), fee(19)).unwrap(),
        &network,
        Height(2),
        0x51,
        625_000_019,
    );
}

#[test]
fn warming_is_optional_without_runtime_or_valid_next_height() {
    let network = Network::new_wcash_regtest();
    let miner = params(&network, 0x61);
    let cache = CoinbaseCache::default();
    let worker = AuxiliaryPrecomputer::default();
    assert!(!worker.register(&network, &miner, cache.clone(), Height(1), fee(0)));
    assert!(!worker.register(&network, &miner, cache, Height(u32::MAX), fee(0)));
    assert!(worker.0.state.lock().unwrap().worker.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alternate_two_tip_burst_consumes_prepared_third_candidate_without_reproving() {
    let network = Network::new_wcash_regtest();
    let miner = params(&network, 0x73);
    let cache = CoinbaseCache::default();
    let current = coinbase(&network, Height(1), &miner, fee(29));
    cache.store(Height(1), fee(29), current.clone());
    let builds = Arc::new(Mutex::new(Vec::new()));
    let active = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(Gate::default());
    let (entered_tx, entered_rx) = mpsc::channel();
    let (recorded, building, max_seen, held) = (
        Arc::clone(&builds),
        Arc::clone(&active),
        Arc::clone(&maximum),
        Arc::clone(&gate),
    );
    let worker = AuxiliaryPrecomputer::with_builder(Arc::new(move |net, height, miner, amount| {
        let now = building.fetch_add(1, Ordering::SeqCst) + 1;
        max_seen.fetch_max(now, Ordering::SeqCst);
        recorded.lock().unwrap().push((height, amount));
        if height == Height(4) && amount == fee(0) {
            entered_tx.send(()).unwrap();
            held.wait();
        }
        let result = TransactionTemplate::new_coinbase(net, height, miner, amount);
        building.fetch_sub(1, Ordering::SeqCst);
        result
    }));
    register(&worker, &network, &miner, &cache, Height(1), fee(29)).await;
    idle(&worker).await;

    // This is a behavioral prerequisite: the third candidate must have been
    // prepared before either new tip arrives. A new foreground proof at H3
    // cannot be disguised as a successful lookahead hit.
    let before_burst = builds.lock().unwrap().clone();
    for amount in [fee(0), fee(29)] {
        assert_eq!(
            before_burst
                .iter()
                .filter(|&&(height, value)| height == Height(3) && value == amount)
                .count(),
            1,
            "the alternate recipient must prepare the third candidate before a two-tip burst"
        );
    }

    // Ordinary same-height requests retain only the three adjacent foreground
    // heights. They must not prune the independently retained future H3 proof.
    assert_eq!(
        cache.get_or_build(Height(1), fee(29), || {
            panic!("the current exact coinbase was already cached")
        }),
        current
    );
    for amount in [0, 29] {
        let second = cache.get_or_build(Height(2), fee(amount), || {
            panic!("the second candidate must use its already prepared proof")
        });
        check_public_coinbase(&second, &network, Height(2), 0x73, 625_000_000 + amount);
    }
    register(&worker, &network, &miner, &cache, Height(2), fee(29)).await;
    let later_build_started = entered_rx.recv_timeout(DEADLINE);
    let foreground_builds = Arc::new(AtomicUsize::new(0));
    let mut third_candidates = Vec::new();
    for amount in [0, 29] {
        let fallback = Arc::clone(&foreground_builds);
        third_candidates.push(cache.get_or_build(Height(3), fee(amount), || {
            fallback.fetch_add(1, Ordering::SeqCst);
            coinbase(&network, Height(3), &miner, fee(amount))
        }));
    }
    let worker_still_held = active.load(Ordering::SeqCst);
    // Registration for the next tip is optional bookkeeping and cannot wait
    // for the unrelated H4 proof held above.
    register(&worker, &network, &miner, &cache, Height(3), fee(29)).await;
    gate.release();
    idle(&worker).await;

    assert!(later_build_started.is_ok());
    assert_eq!(worker_still_held, 1);
    assert_eq!(foreground_builds.load(Ordering::SeqCst), 0);
    for (template, amount) in third_candidates.iter().zip([0, 29]) {
        check_public_coinbase(template, &network, Height(3), 0x73, 625_000_000 + amount);
    }
    let after_burst = builds.lock().unwrap();
    for amount in [fee(0), fee(29)] {
        assert_eq!(
            after_burst
                .iter()
                .filter(|&&(height, value)| height == Height(3) && value == amount)
                .count(),
            1,
            "tip rotation must consume the exact reserve instead of proving it again"
        );
    }
    assert_eq!(maximum.load(Ordering::SeqCst), 1);
    assert_eq!(active.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alternate_future_reserve_reorg_and_recipient_fee_keys_stay_exact() {
    let network = Network::new_wcash_regtest();
    let first = params(&network, 0x74);
    let second = params(&network, 0x75);
    let first_cache = CoinbaseCache::default();
    let second_cache = CoinbaseCache::default();
    let worker = AuxiliaryPrecomputer::default();
    for (miner, cache, amount) in [(&first, &first_cache, 17), (&second, &second_cache, 19)] {
        cache.store(
            Height(10),
            fee(amount),
            coinbase(&network, Height(10), miner, fee(amount)),
        );
        register(&worker, &network, miner, cache, Height(10), fee(amount)).await;
    }
    idle(&worker).await;
    let fallback = Arc::new(AtomicUsize::new(0));
    for (miner, cache, tag, amount) in [
        (&first, &first_cache, 0x74, 17),
        (&second, &second_cache, 0x75, 19),
    ] {
        // Re-read the current foreground height before the two-height jump.
        // Its ordinary cache pruning must leave the independent H12 reserve.
        cache.get_or_build(Height(10), fee(amount), || {
            panic!("the exact current recipient coinbase was stored")
        });
        let counted = Arc::clone(&fallback);
        let third = cache.get_or_build(Height(12), fee(amount), || {
            counted.fetch_add(1, Ordering::SeqCst);
            coinbase(&network, Height(12), miner, fee(amount))
        });
        check_public_coinbase(&third, &network, Height(12), tag, 625_000_000 + amount);
    }
    assert_eq!(
        fallback.load(Ordering::SeqCst),
        0,
        "each third candidate must come from its own exact recipient/fee reserve"
    );

    let wrong_fee_builds = AtomicUsize::new(0);
    let changed = first_cache.get_or_build(Height(12), fee(19), || {
        wrong_fee_builds.fetch_add(1, Ordering::SeqCst);
        coinbase(&network, Height(12), &first, fee(19))
    });
    assert_eq!(wrong_fee_builds.load(Ordering::SeqCst), 1);
    check_public_coinbase(&changed, &network, Height(12), 0x74, 625_000_019);

    // A backward reorg changes the desired height range. It must preserve a
    // newly installed current proof and never redirect another namespace.
    let reorg_current = coinbase(&network, Height(8), &first, fee(0));
    first_cache.store(Height(8), fee(0), reorg_current.clone());
    register(&worker, &network, &first, &first_cache, Height(8), fee(23)).await;
    idle(&worker).await;
    assert_eq!(first_cache.get(Height(8), fee(0)), Some(reorg_current));
    for height in [Height(9), Height(10)] {
        let template = first_cache.get_or_build(height, fee(23), || {
            panic!("the reorg reserve must prepare the exact new height and fee")
        });
        check_public_coinbase(&template, &network, height, 0x74, 625_000_023);
    }
    let untouched_second = second_cache.get_or_build(Height(12), fee(19), || {
        panic!("another recipient's reorg cannot discard this exact candidate")
    });
    check_public_coinbase(&untouched_second, &network, Height(12), 0x75, 625_000_019);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alternate_future_reserve_cancelled_registration_keeps_one_bounded_creator() {
    let network = Network::new_wcash_regtest();
    let miner = params(&network, 0x76);
    let cache = CoinbaseCache::default();
    let gate = Arc::new(Gate::default());
    let held = Arc::clone(&gate);
    let builds = Arc::new(AtomicUsize::new(0));
    let active = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));
    let (counted, building, max_seen) = (
        Arc::clone(&builds),
        Arc::clone(&active),
        Arc::clone(&maximum),
    );
    let (entered_tx, entered_rx) = mpsc::channel();
    let worker = AuxiliaryPrecomputer::with_builder(Arc::new(move |net, height, miner, amount| {
        counted.fetch_add(1, Ordering::SeqCst);
        let now = building.fetch_add(1, Ordering::SeqCst) + 1;
        max_seen.fetch_max(now, Ordering::SeqCst);
        assert_eq!((height, amount), (Height(21), fee(0)));
        entered_tx.send(()).unwrap();
        held.wait();
        let result = TransactionTemplate::new_coinbase(net, height, miner, amount);
        building.fetch_sub(1, Ordering::SeqCst);
        result
    }));
    register(&worker, &network, &miner, &cache, Height(20), fee(0)).await;
    let entered = entered_rx.recv_timeout(DEADLINE);
    for _ in 0..64 {
        register(
            &worker.clone(),
            &network,
            &miner,
            &cache,
            Height(20),
            fee(0),
        )
        .await;
    }
    let registration = {
        let worker = worker.clone();
        let network = network.clone();
        let miner = miner.clone();
        let cache = cache.clone();
        tokio::spawn(async move {
            register(&worker, &network, &miner, &cache, Height(20), fee(0)).await;
            std::future::pending::<()>().await;
        })
    };
    tokio::task::yield_now().await;
    registration.abort();
    assert!(registration.await.unwrap_err().is_cancelled());

    let foreground_cache = cache.clone();
    let foreground_network = network.clone();
    let foreground_miner = miner.clone();
    let fallback = Arc::new(AtomicUsize::new(0));
    let counted_fallback = Arc::clone(&fallback);
    let foreground = tokio::task::spawn_blocking(move || {
        foreground_cache.get_or_build(Height(21), fee(0), || {
            counted_fallback.fetch_add(1, Ordering::SeqCst);
            coinbase(&foreground_network, Height(21), &foreground_miner, fee(0))
        })
    });
    let joined = tokio::time::timeout(DEADLINE, async {
        loop {
            if cache
                .0
                .lock()
                .unwrap()
                .foreground
                .contains_key(&(Height(21), fee(0)))
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    let builds_before_release = builds.load(Ordering::SeqCst);
    let maximum_before_release = maximum.load(Ordering::SeqCst);
    // Removing every optional scheduler owner cancels queued lookahead, while
    // an already running proof still has one exact completion responsibility.
    drop(worker);
    gate.release();
    let result = tokio::time::timeout(DEADLINE, foreground).await;
    assert!(entered.is_ok());
    assert!(joined.is_ok());
    let result = result.unwrap().unwrap();
    check_public_coinbase(&result, &network, Height(21), 0x76, 625_000_000);
    assert_eq!(builds_before_release, 1);
    assert_eq!(maximum_before_release, 1);
    assert_eq!(builds.load(Ordering::SeqCst), 1);
    assert_eq!(maximum.load(Ordering::SeqCst), 1);
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert_eq!(fallback.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queued_future_proof_foreground_promotion_survives_tip_reorg_and_worker_drop() {
    let network = Network::new_wcash_regtest();
    let miner = params(&network, 0x77);
    let cache = CoinbaseCache::default();
    let gate = Arc::new(Gate::default());
    let held = Arc::clone(&gate);
    let background_keys = Arc::new(Mutex::new(Vec::new()));
    let observed_keys = Arc::clone(&background_keys);
    let (entered_tx, entered_rx) = mpsc::channel();
    let worker = AuxiliaryPrecomputer::with_builder(Arc::new(move |net, height, miner, amount| {
        observed_keys.lock().unwrap().push((height, amount));
        if height == Height(11) && amount == fee(0) {
            entered_tx.send(()).unwrap();
            held.wait();
        }
        TransactionTemplate::new_coinbase(net, height, miner, amount)
    }));
    register(&worker, &network, &miner, &cache, Height(10), fee(0)).await;
    let entered = entered_rx.recv_timeout(DEADLINE);

    let foreground_builds = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&foreground_builds);
    let foreground_cache = cache.clone();
    let foreground_network = network.clone();
    let foreground_miner = miner.clone();
    let (foreground_tx, mut foreground_rx) = tokio::sync::oneshot::channel();
    let foreground = std::thread::spawn(move || {
        let built = foreground_cache.get_or_build(Height(12), fee(0), || {
            counted.fetch_add(1, Ordering::SeqCst);
            coinbase(&foreground_network, Height(12), &foreground_miner, fee(0))
        });
        let _ = foreground_tx.send(built);
    });
    // This bounds the ordering check, not proof performance: H11 remains held
    // until this wait completes. The fixture proof itself is transparent.
    let before_release = tokio::time::timeout(Duration::from_secs(2), &mut foreground_rx).await;
    let finished_before_release = matches!(&before_release, Ok(Ok(_)));
    if !finished_before_release {
        // Failure cleanup only: complete the exact promoted foreground entry,
        // cancel optional queued work and release the held real builder before
        // reporting the ordering failure. Never leave a held blocking proof.
        drop(worker);
        cache.store(
            Height(12),
            fee(0),
            coinbase(&network, Height(12), &miner, fee(0)),
        );
        gate.release();
        let settled = tokio::time::timeout(DEADLINE, foreground_rx).await;
        if settled.is_ok() {
            foreground.join().unwrap();
        } else {
            drop(foreground);
        }
        assert!(
            finished_before_release,
            "foreground H12 cannot wait behind the unrelated optional H11 proof"
        );
        return;
    }
    let completed = before_release.unwrap().unwrap();
    foreground.join().unwrap();

    // Foreground claimed H12 before the optional worker reached it. Its same
    // reserved EntryArc must survive a lower current-height lookup and an
    // unchanged registration window, without another proof or worker release.
    let claimed_entry = cache.0.lock().unwrap().foreground[&(Height(12), fee(0))].clone();
    cache.get_or_build(Height(10), fee(0), || {
        coinbase(&network, Height(10), &miner, fee(0))
    });
    register(&worker, &network, &miner, &cache, Height(10), fee(0)).await;
    let retained = cache.get_or_build(Height(12), fee(0), || {
        foreground_builds.fetch_add(1, Ordering::SeqCst);
        coinbase(&network, Height(12), &miner, fee(0))
    });
    let retained_entry = cache.0.lock().unwrap().foreground[&(Height(12), fee(0))].clone();
    let same_entry_after_pruning = Arc::ptr_eq(&claimed_entry, &retained_entry);

    // H12 became current while optional H11 is still running. Further
    // registration cannot authorize a duplicate queued H12 creator.
    register(&worker, &network, &miner, &cache, Height(12), fee(0)).await;
    let reorg_current = coinbase(&network, Height(8), &miner, fee(0));
    cache.store(Height(8), fee(0), reorg_current.clone());
    register(&worker, &network, &miner, &cache, Height(8), fee(0)).await;
    let before_drop_keys = background_keys.lock().unwrap().clone();
    let weak_owner = Arc::downgrade(&worker.0);
    drop(worker);
    let optional_owner_dropped = weak_owner.upgrade().is_none();
    gate.release();

    // The detached in-flight proof may finish, but queued reserve work is not
    // kept alive by foreground entry ownership after the scheduler is gone.
    tokio::time::timeout(DEADLINE, async {
        while Arc::strong_count(&background_keys) > 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the released optional builder drops its last observer clone");
    assert!(entered.is_ok());
    assert!(optional_owner_dropped);
    assert!(same_entry_after_pruning);
    assert_eq!(retained, completed);
    assert_eq!(before_drop_keys, vec![(Height(11), fee(0))]);
    assert_eq!(*background_keys.lock().unwrap(), vec![(Height(11), fee(0))]);
    assert_eq!(foreground_builds.load(Ordering::SeqCst), 1);
    check_public_coinbase(&completed, &network, Height(12), 0x77, 625_000_000);
    assert_eq!(cache.get(Height(8), fee(0)), Some(reorg_current));
    assert!(cache.get(Height(11), fee(0)).is_none());
    assert!(cache.get(Height(12), fee(0)).is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumed_future_reserve_entry_survives_same_window_foreground_pruning() {
    let network = Network::new_wcash_regtest();
    let miner = params(&network, 0x78);
    let cache = CoinbaseCache::default();
    let current = coinbase(&network, Height(1), &miner, fee(17));
    cache.store(Height(1), fee(17), current.clone());
    let builds = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&builds);
    let worker = AuxiliaryPrecomputer::with_builder(Arc::new(move |net, height, miner, amount| {
        counted.fetch_add(1, Ordering::SeqCst);
        TransactionTemplate::new_coinbase(net, height, miner, amount)
    }));
    register(&worker, &network, &miner, &cache, Height(1), fee(17)).await;
    idle(&worker).await;
    let fallback = AtomicUsize::new(0);
    for amount in [0, 17] {
        let first = cache.get_or_build(Height(3), fee(amount), || {
            fallback.fetch_add(1, Ordering::SeqCst);
            coinbase(&network, Height(3), &miner, fee(amount))
        });
        let first_entry = cache.0.lock().unwrap().foreground[&(Height(3), fee(amount))].clone();
        // This valid lower-height foreground request prunes H3 from the
        // ordinary adjacent-height map. The unchanged reserve window still
        // owns it and registration must not leave a Done-but-missing key.
        cache.get_or_build(Height(1), fee(17), || current.clone());
        assert!(!cache
            .0
            .lock()
            .unwrap()
            .foreground
            .contains_key(&(Height(3), fee(amount))));
        register(&worker, &network, &miner, &cache, Height(1), fee(17)).await;
        idle(&worker).await;
        let again = cache.get_or_build(Height(3), fee(amount), || {
            fallback.fetch_add(1, Ordering::SeqCst);
            coinbase(&network, Height(3), &miner, fee(amount))
        });
        let again_entry = cache.0.lock().unwrap().foreground[&(Height(3), fee(amount))].clone();
        assert!(Arc::ptr_eq(&first_entry, &again_entry));
        assert_eq!(first, again);
        check_public_coinbase(&again, &network, Height(3), 0x78, 625_000_000 + amount);
    }
    assert_eq!(fallback.load(Ordering::SeqCst), 0);
    assert_eq!(builds.load(Ordering::SeqCst), 4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn removed_then_reintroduced_running_key_requires_fresh_slot_ownership() {
    let network = Network::new_wcash_regtest();
    let miner = params(&network, 0x79);
    let cache = CoinbaseCache::default();
    let gate = Arc::new(Gate::default());
    let held = Arc::clone(&gate);
    let builds = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&builds);
    let (entered_tx, entered_rx) = mpsc::channel();
    let worker = AuxiliaryPrecomputer::with_builder(Arc::new(move |net, height, miner, amount| {
        if height == Height(101) && counted.fetch_add(1, Ordering::SeqCst) == 0 {
            entered_tx.send(()).unwrap();
            held.wait();
        }
        TransactionTemplate::new_coinbase(net, height, miner, amount)
    }));
    register(&worker, &network, &miner, &cache, Height(100), fee(0)).await;
    let entered = entered_rx.recv_timeout(DEADLINE);
    let old_entry = cache
        .0
        .lock()
        .unwrap()
        .reserve
        .get(&(Height(101), fee(0)))
        .cloned();
    register(&worker, &network, &miner, &cache, Height(98), fee(0)).await;
    let removed = !cache
        .0
        .lock()
        .unwrap()
        .reserve
        .contains_key(&(Height(101), fee(0)));
    register(&worker, &network, &miner, &cache, Height(100), fee(0)).await;
    let absent_before_release = cache.get(Height(101), fee(0)).is_none();
    gate.release();
    idle(&worker).await;

    assert!(entered.is_ok());
    assert!(removed && absent_before_release);
    assert_eq!(
        builds.load(Ordering::SeqCst),
        2,
        "the old completion cannot mark the newly introduced H101 slot Done"
    );
    let current_entry = cache.0.lock().unwrap().reserve[&(Height(101), fee(0))].clone();
    assert!(
        !Arc::ptr_eq(&old_entry.unwrap(), &current_entry),
        "an old pruned EntryArc must not be reinserted for the new slot"
    );
    let ready = cache.get_or_build(Height(101), fee(0), || {
        panic!("the newly owned H101 proof is complete")
    });
    check_public_coinbase(&ready, &network, Height(101), 0x79, 625_000_000);
}

#[cfg(feature = "wcash-consensus")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real private Ironwood proofs; run this exact test explicitly off the VPS"]
#[expect(
    clippy::print_stderr,
    reason = "Expose real proof measurements only in the explicitly selected offline regression"
)]
async fn actual_private_recipient_two_tip_burst_is_warmed_and_reused_without_reproving() {
    use std::time::Instant;

    use orchard::keys::{FullViewingKey, Scope, SpendingKey};
    use zcash_address::unified::{Encoding, Receiver};

    let network = Network::new_wcash_regtest();
    // Public disposable fixture only; never a production key, address, or wallet.
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
    let miner = MinerParams::new(
        &network,
        Config {
            miner_address: Some(address.encode().parse().unwrap()),
            ..Default::default()
        },
    )
    .unwrap();
    let wrong = Option::<SpendingKey>::from(SpendingKey::from_bytes([0x72; 32])).unwrap();
    let wrong_ivk = FullViewingKey::from(&wrong).to_ivk(Scope::External);
    let current_started = Instant::now();
    let current = coinbase(&network, Height(1), &miner, fee(12_345));
    let current_ms = current_started.elapsed().as_millis();
    let cache = CoinbaseCache::default();
    cache.store(Height(1), fee(12_345), current.clone());
    let builds = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&builds);
    let worker = AuxiliaryPrecomputer::with_builder(Arc::new(move |net, height, miner, amount| {
        counted.fetch_add(1, Ordering::SeqCst);
        assert_eq!(rayon::current_num_threads(), 1);
        TransactionTemplate::new_coinbase(net, height, miner, amount)
    }));
    let warm_started = Instant::now();
    register(&worker, &network, &miner, &cache, Height(1), fee(12_345)).await;
    tokio::time::timeout(Duration::from_secs(120), worker.wait_for_idle())
        .await
        .expect("the four explicit future private proofs finish off the VPS");
    let warm_ms = warm_started.elapsed().as_millis();
    let request_cache = cache.clone();
    let mut consumption_ms = Vec::new();
    for height in [Height(2), Height(3)] {
        // A normal current-height request must not discard the independent H3
        // proof before the second rapid tip arrives.
        assert_eq!(
            request_cache.get_or_build(Height(1), fee(12_345), || current.clone()),
            current
        );
        for amount in [0, 12_345] {
            let started = Instant::now();
            let warmed = request_cache.get_or_build(height, fee(amount), || {
                panic!("the future private proof must be reused")
            });
            consumption_ms.push(started.elapsed().as_millis());
            let tx: Transaction = warmed.data.as_ref().zcash_deserialize_into().unwrap();
            assert!(
                matches!(&tx.inputs()[0], transparent::Input::Coinbase { height: actual, .. } if *actual == height)
            );
            assert!(tx.outputs().is_empty());
            assert_eq!(tx.version(), 6);
            zebra_consensus::transaction::check::wcash_coinbase_outputs_are_private(
                &tx, &network, height,
            )
            .unwrap();
            let bundle = tx.ironwood_bundle().unwrap();
            let notes = bundle.decrypt_outputs_with_keys(std::slice::from_ref(&ivk));
            assert!(!notes.is_empty());
            assert!(notes
                .iter()
                .all(|(_, _, _, address, _)| *address == receiver));
            assert_eq!(
                notes
                    .iter()
                    .map(|(_, _, note, _, _)| note.value().inner())
                    .sum::<u64>(),
                u64::try_from(625_000_000 + amount).unwrap()
            );
            assert!(bundle
                .decrypt_outputs_with_keys(std::slice::from_ref(&wrong_ivk))
                .is_empty());
        }
    }
    // Reconsume H3 after a lower-height request prunes its foreground copy.
    request_cache.get_or_build(Height(1), fee(12_345), || current.clone());
    for amount in [0, 12_345] {
        request_cache.get_or_build(Height(3), fee(amount), || {
            panic!("consumed private reserve entries must survive foreground pruning")
        });
    }
    assert_eq!(builds.load(Ordering::SeqCst), 4);
    assert!(cache.get(Height(3), fee(12_346)).is_none());
    eprintln!("actual_private_two_tip_burst current_build_ms={current_ms} next_height_warm_ms={warm_ms} exact_future_consume_ms={consumption_ms:?} worker_builds=4");
}

#[test]
fn reserve_registration_lock_skips_are_diagnosed_before_held_locks_are_released() {
    use super::super::coinbase_diagnostics::{self as diagnostics, Event, Outcome, Phase};

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let network = Network::new_wcash_regtest();
    let miner = params(&network, 0x7a);
    let cache = CoinbaseCache::default();
    let builds = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&builds);
    let worker = AuxiliaryPrecomputer::with_builder(Arc::new(move |net, height, miner, fee| {
        counted.fetch_add(1, Ordering::SeqCst);
        TransactionTemplate::new_coinbase(net, height, miner, fee)
    }));
    let mut observations = Vec::new();
    for hold_scheduler in [true, false] {
        let state_guard = hold_scheduler.then(|| worker.0.state.lock().unwrap());
        let cache_guard = (!hold_scheduler).then(|| cache.0.lock().unwrap());
        let registering = worker.clone();
        let selected_cache = cache.clone();
        let selected_miner = miner.clone();
        let selected_network = network.clone();
        let handle = runtime.handle().clone();
        let (result_tx, result_rx) = mpsc::channel();
        let registration = std::thread::spawn(move || {
            let _runtime = handle.enter();
            let _context = diagnostics::enter(701, Phase::Template);
            let mut accepted = true;
            let records = diagnostics::capture_records(|| {
                accepted = registering.register(
                    &selected_network,
                    &selected_miner,
                    selected_cache,
                    Height(10),
                    fee(17),
                );
            });
            result_tx.send((accepted, records)).unwrap();
        });
        let before_release = result_rx.recv_timeout(Duration::from_secs(2));
        drop(cache_guard);
        drop(state_guard);
        registration.join().unwrap();
        observations.push((hold_scheduler, before_release));
    }
    assert_eq!(builds.load(Ordering::SeqCst), 0);
    assert!(cache.0.lock().unwrap().reserve_keys.is_empty());
    assert!(worker.0.state.lock().unwrap().namespaces.is_empty());
    for (scheduler, observation) in observations {
        let (accepted, records) = observation.expect("registration cannot wait for a held lock");
        assert!(!accepted);
        assert_eq!(records.len(), 1);
        let record = records[0];
        assert_eq!(record.event, Event::ReserveRegistration);
        assert_eq!(
            record.outcome,
            if scheduler {
                Outcome::StateContended
            } else {
                Outcome::CacheContended
            }
        );
        assert_eq!((record.request_id, record.phase), (701, Phase::Prewarm));
        assert_eq!(record.cache_id, cache.1);
        assert_eq!(record.height, Some(10));
        assert_eq!(record.observed_fee_zat, Some(17));
        assert_eq!(
            (record.reserve_start, record.reserve_end),
            (Some(11), Some(12))
        );
    }
}

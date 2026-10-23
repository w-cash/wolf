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
    assert_eq!(builds.load(Ordering::SeqCst), 2);
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
        2,
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
            state.namespaces.iter().all(|entry| entry.slots.len() <= 2),
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
    assert_eq!(builds.load(Ordering::SeqCst), 1);
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
    assert_eq!(zero_count.load(Ordering::SeqCst), 1);
    assert_eq!(fee_count.load(Ordering::SeqCst), 1);
    assert!(cache.get(Height(2), fee(0)).is_some());
    assert!(cache.get(Height(2), fee(19)).is_none());
    register(&worker, &network, &miner, &cache, Height(1), fee(19)).await;
    idle(&worker).await;
    assert_eq!(zero_count.load(Ordering::SeqCst), 1);
    assert_eq!(fee_count.load(Ordering::SeqCst), 2);
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

#[cfg(feature = "wcash-consensus")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real private Ironwood proofs; run this exact test explicitly off the VPS"]
#[expect(
    clippy::print_stderr,
    reason = "Expose real proof measurements only in the explicitly selected offline regression"
)]
async fn actual_private_recipient_future_coinbase_is_warmed_and_reused_without_reproving() {
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
        .expect("the two explicit private fixture proofs finish off the VPS");
    let warm_ms = warm_started.elapsed().as_millis();
    let request_cache = cache.clone();
    let mut consumption_ms = Vec::new();
    for amount in [0, 12_345] {
        let started = Instant::now();
        let warmed = request_cache.get_or_build(Height(2), fee(amount), || {
            panic!("the future private proof must be reused")
        });
        consumption_ms.push(started.elapsed().as_millis());
        let tx: Transaction = warmed.data.as_ref().zcash_deserialize_into().unwrap();
        assert!(
            matches!(&tx.inputs()[0], transparent::Input::Coinbase { height, .. } if *height == Height(2))
        );
        assert!(tx.outputs().is_empty());
        assert_eq!(tx.version(), 6);
        zebra_consensus::transaction::check::wcash_coinbase_outputs_are_private(
            &tx,
            &network,
            Height(2),
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
    assert_eq!(builds.load(Ordering::SeqCst), 2);
    assert_eq!(cache.get(Height(1), fee(12_345)), Some(current));
    assert!(cache.get(Height(2), fee(12_346)).is_none());
    eprintln!("actual_private_future_prewarm current_build_ms={current_ms} next_height_warm_ms={warm_ms} exact_future_consume_ms={consumption_ms:?} worker_builds=2");
}

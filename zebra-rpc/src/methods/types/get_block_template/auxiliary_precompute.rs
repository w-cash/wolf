//! Bounded, optional next-height coinbase work for alternate auxiliary payouts.
//!
//! A successful `createauxblock` has already built its current-height coinbase. It only records
//! the exact recipient/cache namespace and last selected fee here. One lazy worker may prepare
//! the next height's zero-fee sizing proof and that exact fee total. It does not create candidates,
//! acquire leases, read chain state, or change any submission path.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, Weak},
    time::Instant,
};

use tokio::{runtime::Handle, sync::Notify, task::JoinHandle};
use zebra_chain::{
    amount::{Amount, NegativeOrZero, NonNegative},
    block::Height,
    parameters::Network,
};
use zebra_consensus::error::TransactionError;

use crate::methods::types::transaction::TransactionTemplate;

use super::{CoinbaseCache, MinerParams, WeakCoinbaseCache, MAX_AUXILIARY_COINBASE_RECIPIENTS};

#[cfg(test)]
mod tests;

type CoinbaseBuilder = dyn Fn(
        &Network,
        Height,
        &MinerParams,
        Amount<NonNegative>,
    ) -> Result<TransactionTemplate<NegativeOrZero>, TransactionError>
    + Send
    + Sync;

/// Shared by clones of one RPC handler; no worker exists until a successful alternate request.
#[derive(Clone)]
pub(super) struct AuxiliaryPrecomputer(Arc<Shared>);

struct Shared {
    state: Mutex<State>,
    notify: Arc<Notify>,
    builder: Arc<CoinbaseBuilder>,
    #[cfg(test)]
    changed: Notify,
}

#[derive(Default)]
struct State {
    namespaces: VecDeque<Namespace>,
    active: bool,
    worker: Option<JoinHandle<()>>,
}

struct Namespace {
    id: Arc<()>,
    network: Network,
    params: MinerParams,
    cache: WeakCoinbaseCache,
    current_height: Height,
    target_height: Height,
    observed_fee: Amount<NonNegative>,
    slots: Vec<FeeSlot>,
}

struct FeeSlot {
    fee: Amount<NonNegative>,
    state: WorkState,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum WorkState {
    Pending,
    Running,
    Done,
    Failed,
}

#[derive(Clone)]
struct Work {
    namespace: Arc<()>,
    network: Network,
    params: MinerParams,
    cache: CoinbaseCache,
    height: Height,
    fee: Amount<NonNegative>,
}

enum WorkOutcome {
    Built,
    Skipped,
    Failed,
}

impl Default for AuxiliaryPrecomputer {
    fn default() -> Self {
        Self::from_builder(Arc::new(TransactionTemplate::new_coinbase))
    }
}

impl AuxiliaryPrecomputer {
    fn from_builder(builder: Arc<CoinbaseBuilder>) -> Self {
        Self(Arc::new(Shared {
            state: Mutex::new(State::default()),
            notify: Arc::new(Notify::new()),
            builder,
            #[cfg(test)]
            changed: Notify::new(),
        }))
    }

    #[cfg(test)]
    fn with_builder(builder: Arc<CoinbaseBuilder>) -> Self {
        Self::from_builder(builder)
    }

    /// Coalesces optional work after successful candidate publication, without waiting for locks.
    ///
    /// False means this best-effort registration was skipped. Failure, saturation, or a stale
    /// speculative result never changes the successful foreground response or its current proof.
    pub(super) fn register(
        &self,
        network: &Network,
        params: &MinerParams,
        cache: CoinbaseCache,
        current_height: Height,
        observed_fee: Amount<NonNegative>,
    ) -> bool {
        if !network.uses_wcash_consensus() {
            return false;
        }
        let Some(next_height) = current_height.0.checked_add(1).map(Height) else {
            return false;
        };
        let Ok(runtime) = Handle::try_current() else {
            return false;
        };
        let mut state = match self.0.state.try_lock() {
            Ok(state) => state,
            Err(std::sync::TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => return false,
        };

        let existing = state.namespaces.iter().position(|namespace| {
            namespace.network == *network
                && namespace.cache.ptr_eq(&cache)
                && same_params(&namespace.params, params)
        });
        let mut namespace = if let Some(index) = existing {
            state
                .namespaces
                .remove(index)
                .expect("the matching prewarm namespace is present")
        } else {
            if state.namespaces.len() == MAX_AUXILIARY_COINBASE_RECIPIENTS {
                // An in-flight build keeps its own exact cache Arc. Eviction never redirects it.
                state.namespaces.pop_front();
            }
            Namespace {
                id: Arc::new(()),
                network: network.clone(),
                params: params.clone(),
                cache: cache.downgrade(),
                current_height,
                target_height: next_height,
                observed_fee,
                slots: Vec::with_capacity(2),
            }
        };
        namespace.update(current_height, next_height, observed_fee);
        state.namespaces.push_back(namespace);

        // A terminal task failure must not permanently disable later best-effort registrations.
        if state.worker.as_ref().is_some_and(JoinHandle::is_finished) {
            state.worker = None;
            state.active = false;
            for namespace in &mut state.namespaces {
                for slot in &mut namespace.slots {
                    if slot.state == WorkState::Running {
                        slot.state = WorkState::Pending;
                    }
                }
            }
        }
        if state.worker.is_none() {
            let shared = Arc::downgrade(&self.0);
            let notify = Arc::clone(&self.0.notify);
            state.worker = Some(runtime.spawn(run(shared, notify)));
        }
        drop(state);
        self.0.notify.notify_one();
        #[cfg(test)]
        self.0.changed.notify_waiters();
        true
    }

    #[cfg(test)]
    async fn wait_for_idle(&self) {
        loop {
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let state = self
                    .0
                    .state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if !state.active
                    && state.namespaces.iter().all(|namespace| {
                        namespace
                            .slots
                            .iter()
                            .all(|slot| matches!(slot.state, WorkState::Done | WorkState::Failed))
                    })
                {
                    return;
                }
            }
            changed.await;
        }
    }
}

impl Namespace {
    fn update(
        &mut self,
        current_height: Height,
        target_height: Height,
        observed_fee: Amount<NonNegative>,
    ) {
        let zero = Amount::<NonNegative>::zero();
        let fees = [zero, observed_fee];
        let same_height = self.target_height == target_height;
        let mut slots = Vec::with_capacity(2);
        for fee in fees {
            if slots.iter().any(|slot: &FeeSlot| slot.fee == fee) {
                continue;
            }
            let prior = same_height
                .then(|| self.slots.iter().find(|slot| slot.fee == fee))
                .flatten();
            slots.push(FeeSlot {
                fee,
                state: match prior.map(|slot| slot.state) {
                    // Retry an actual failure only after another explicit successful request.
                    None | Some(WorkState::Failed) => WorkState::Pending,
                    Some(state) => state,
                },
            });
        }
        self.current_height = current_height;
        self.target_height = target_height;
        self.observed_fee = observed_fee;
        self.slots = slots;
    }
}

fn same_params(left: &MinerParams, right: &MinerParams) -> bool {
    left.addr == right.addr
        && left.data == right.data
        && left.memo == right.memo
        && left.parent_payout_address_commitment == right.parent_payout_address_commitment
}

/// Selects from coalesced state, not a queue of historical requests.
fn next_work(shared: &Weak<Shared>) -> Option<(Work, Arc<CoinbaseBuilder>)> {
    let shared = shared.upgrade()?;
    let mut state = shared
        .state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    debug_assert!(!state.active, "only one optional proof can be active");
    // The recipient bank, rather than a stale scheduler request, owns cache lifetime. In-flight
    // work may finish, but an actually evicted namespace must not start another speculative proof.
    state
        .namespaces
        .retain(|namespace| namespace.cache.upgrade().is_some());
    #[cfg(test)]
    shared.changed.notify_waiters();
    let (mut namespace, cache) = loop {
        let index = state.namespaces.iter().position(|namespace| {
            namespace
                .slots
                .iter()
                .any(|slot| slot.state == WorkState::Pending)
        })?;
        let namespace = state.namespaces.remove(index)?;
        // The foreground bank can drop its last owner after retain's temporary upgrade. Continue
        // past that entry rather than parking while another live namespace has pending work.
        if let Some(cache) = namespace.cache.upgrade() {
            break (namespace, cache);
        }
        #[cfg(test)]
        shared.changed.notify_waiters();
    };
    let slot = namespace
        .slots
        .iter_mut()
        .find(|slot| slot.state == WorkState::Pending)?;
    slot.state = WorkState::Running;
    let work = Work {
        namespace: Arc::clone(&namespace.id),
        network: namespace.network.clone(),
        params: namespace.params.clone(),
        cache,
        height: namespace.target_height,
        fee: slot.fee,
    };
    // Rotate serviced namespaces so a busy recipient cannot starve another registered recipient.
    state.namespaces.push_back(namespace);
    state.active = true;
    Some((work, Arc::clone(&shared.builder)))
}

fn is_current(shared: &Weak<Shared>, work: &Work) -> bool {
    let Some(shared) = shared.upgrade() else {
        return false;
    };
    let state = shared
        .state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    state.namespaces.iter().any(|namespace| {
        Arc::ptr_eq(&namespace.id, &work.namespace)
            && namespace.target_height == work.height
            && namespace.current_height.0.checked_add(1) == Some(work.height.0)
            && (work.fee == Amount::<NonNegative>::zero() || work.fee == namespace.observed_fee)
            && namespace.slots.iter().any(|slot| slot.fee == work.fee)
    })
}

fn finish_work(shared: &Weak<Shared>, work: &Work, outcome: &WorkOutcome) {
    let Some(shared) = shared.upgrade() else {
        return;
    };
    let mut state = shared
        .state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    state.active = false;
    if let Some(namespace) = state.namespaces.iter_mut().find(|namespace| {
        Arc::ptr_eq(&namespace.id, &work.namespace) && namespace.target_height == work.height
    }) {
        if let Some(slot) = namespace.slots.iter_mut().find(|slot| slot.fee == work.fee) {
            slot.state = match outcome {
                WorkOutcome::Built | WorkOutcome::Skipped => WorkState::Done,
                WorkOutcome::Failed => WorkState::Failed,
            };
        }
    }
    drop(state);
    #[cfg(test)]
    shared.changed.notify_waiters();
}

async fn run(shared: Weak<Shared>, notify: Arc<Notify>) {
    // Thread creation and all proof work are off the successful RPC's foreground path. Nested
    // Rayon work uses this private one-thread pool; global Rayon settings are never changed.
    let pool = tokio::task::spawn_blocking(|| {
        rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .thread_name(|_| "wcash-aux-prewarm".to_owned())
            .build()
    })
    .await;
    let pool = match pool {
        Ok(Ok(pool)) => Arc::new(pool),
        _ => {
            tracing::warn!(
                failure_class = "worker_initialization",
                "auxiliary coinbase prewarm unavailable"
            );
            return;
        }
    };

    loop {
        let Some((work, builder)) = next_work(&shared) else {
            if shared.strong_count() == 0 {
                return;
            }
            // The worker owns only a Weak reference while idle, so dropping all handlers can
            // abort it without an Arc/task cycle. notify_one retains a registration permit.
            notify.notified().await;
            continue;
        };
        let started = Instant::now();
        let pool = Arc::clone(&pool);
        let worker_shared = shared.clone();
        let worker_work = work.clone();
        let result = tokio::task::spawn_blocking(move || {
            if !is_current(&worker_shared, &worker_work) {
                return WorkOutcome::Skipped;
            }
            pool.install(|| {
                match worker_work
                    .cache
                    .try_prewarm(worker_work.height, worker_work.fee, || {
                        builder(
                            &worker_work.network,
                            worker_work.height,
                            &worker_work.params,
                            worker_work.fee,
                        )
                    }) {
                    Ok(Some(_)) => WorkOutcome::Built,
                    Ok(None) => WorkOutcome::Skipped,
                    Err(_) => WorkOutcome::Failed,
                }
            })
        })
        .await;
        let outcome = result.unwrap_or(WorkOutcome::Failed);
        finish_work(&shared, &work, &outcome);
        // Never format recipient parameters, raw error strings, or transactions in diagnostics.
        match outcome {
            WorkOutcome::Built => tracing::debug!(
                height = work.height.0,
                proof_ms = started.elapsed().as_millis(),
                "filled alternate auxiliary next-height coinbase proof"
            ),
            WorkOutcome::Failed => tracing::warn!(
                height = work.height.0,
                failure_class = "proof_worker",
                "auxiliary coinbase prewarm attempt failed"
            ),
            WorkOutcome::Skipped => {}
        }
    }
}

impl Drop for Shared {
    fn drop(&mut self) {
        let state = self
            .state
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(worker) = state.worker.take() {
            worker.abort();
        }
        // Tokio cannot cancel an already running blocking proof. It owns only its exact cache
        // Arc and finishes without reinserting a pruned entry or touching a candidate/lease.
    }
}

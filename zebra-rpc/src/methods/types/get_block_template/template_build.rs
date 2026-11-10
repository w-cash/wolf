//! Bounded foreground template CPU work, separate from async RPC execution.

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use jsonrpsee::core::RpcResult;
use jsonrpsee_types::{ErrorCode, ErrorObject};
use tokio::sync::Semaphore;

use crate::server::error::LegacyCode;

use super::coinbase_diagnostics::{self as diagnostics, Event, Outcome, Phase, Record};

// A builder and one same-proof waiter may run together. Saturation must not
// turn short-polling into an unbounded queue of blocking proof workers.
const MAX_FOREGROUND_TEMPLATE_BUILDS: usize = 2;
const TEMPLATE_BUILD_WAIT_LIMIT: Duration = Duration::from_secs(2);
const TEMPLATE_BUILD_BUSY: &str = "block template construction is busy; retry";
const TEMPLATE_BUILD_PENDING: &str = "block template construction is pending; retry";
pub(super) const CACHE_JOIN_POLL_INTERVAL: Duration = Duration::from_millis(25);

/// Request-local restrictions; these never revoke an already running proof's entry ownership.
#[derive(Clone)]
pub(crate) struct TemplateBuildContext {
    ready_only: bool,
    deadline: Instant,
    cancelled: Arc<AtomicBool>,
    obsolete: Arc<dyn Fn() -> bool + Send + Sync>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TemplateBuildError {
    #[error("block template construction is pending")]
    Pending,
    #[error("block template snapshot was replaced")]
    Obsolete,
    #[error(transparent)]
    Transaction(#[from] zebra_consensus::error::TransactionError),
}

impl From<zebra_chain::amount::Error> for TemplateBuildError {
    fn from(error: zebra_chain::amount::Error) -> Self {
        Self::Transaction(error.into())
    }
}

impl TemplateBuildError {
    pub(crate) fn into_transaction_error(self) -> zebra_consensus::error::TransactionError {
        match self {
            Self::Transaction(error) => error,
            other => {
                zebra_consensus::error::TransactionError::CoinbaseConstruction(other.to_string())
            }
        }
    }

    pub(crate) fn into_rpc_error(self) -> jsonrpsee_types::ErrorObjectOwned {
        match self {
            Self::Pending => {
                ErrorObject::borrowed(LegacyCode::InWarmup.into(), TEMPLATE_BUILD_PENDING, None)
            }
            Self::Obsolete => ErrorObject::borrowed(
                crate::methods::WCASH_AUX_TIP_CHANGED_ERROR_CODE,
                "Wcash tip changed while constructing the candidate; retry createauxblock",
                None,
            ),
            Self::Transaction(error) => ErrorObject::owned(
                ErrorCode::InvalidParams.code(),
                format!("invalid wcashaux parent-template request: {error}"),
                None::<()>,
            ),
        }
    }
}

impl TemplateBuildContext {
    fn new<Tip>(latest_chain_tip: Tip, snapshot: zebra_chain::block::Hash) -> Self
    where
        Tip: zebra_chain::chain_tip::ChainTip + Send + Sync + 'static,
    {
        let observed = AtomicBool::new(latest_chain_tip.best_tip_hash() == Some(snapshot));
        let obsolete = AtomicBool::new(false);
        Self {
            ready_only: false,
            deadline: Instant::now() + TEMPLATE_BUILD_WAIT_LIMIT,
            cancelled: Arc::new(AtomicBool::new(false)),
            obsolete: Arc::new(move || {
                match latest_chain_tip.best_tip_hash() {
                    Some(hash) if hash == snapshot => {
                        observed.store(true, Ordering::Relaxed);
                    }
                    Some(_) if observed.load(Ordering::Relaxed) => {
                        obsolete.store(true, Ordering::Relaxed);
                    }
                    _ => {}
                }
                obsolete.load(Ordering::Relaxed)
            }),
        }
    }

    pub(super) fn check(&self) -> Result<(), TemplateBuildError> {
        if (self.obsolete)() {
            Err(TemplateBuildError::Obsolete)
        } else if self.cancelled.load(Ordering::Relaxed) || Instant::now() >= self.deadline {
            Err(TemplateBuildError::Pending)
        } else {
            Ok(())
        }
    }

    pub(super) fn ready_only(&self) -> bool {
        self.ready_only
    }
}

struct TemplateWaitGuard(Arc<AtomicBool>);

impl Drop for TemplateWaitGuard {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

pub(super) fn is_pending_template_build(error: &ErrorObject<'_>) -> bool {
    error.code() == i32::from(LegacyCode::InWarmup)
        && matches!(
            error.message(),
            TEMPLATE_BUILD_BUSY | TEMPLATE_BUILD_PENDING
        )
        && error.data().is_none()
}

#[derive(Clone)]
pub(super) struct TemplateBuildExecutor {
    permits: Arc<Semaphore>,
    // Assembly can use ready proofs while both expensive workers finish old work.
    // This lane is shared across clones and never builds or joins a cache entry.
    ready_permits: Arc<Semaphore>,
}

impl Default for TemplateBuildExecutor {
    fn default() -> Self {
        diagnostics::initialize();
        Self {
            permits: Arc::new(Semaphore::new(MAX_FOREGROUND_TEMPLATE_BUILDS)),
            ready_permits: Arc::new(Semaphore::new(1)),
        }
    }
}

impl TemplateBuildExecutor {
    pub(super) async fn run<T, F>(&self, build: F) -> RpcResult<T>
    where
        T: Send + 'static,
        F: FnOnce() -> RpcResult<T> + Send + 'static,
    {
        self.run_inner(false, move |_| build()).await
    }

    pub(super) async fn run_for_tip<T, F, Tip>(
        &self,
        latest_chain_tip: Tip,
        snapshot: zebra_chain::block::Hash,
        build: F,
    ) -> RpcResult<T>
    where
        T: Send + 'static,
        F: FnOnce(TemplateBuildContext) -> RpcResult<T> + Send + 'static,
        Tip: zebra_chain::chain_tip::ChainTip + Send + Sync + 'static,
    {
        let context = TemplateBuildContext::new(latest_chain_tip, snapshot);
        let _waiter = TemplateWaitGuard(Arc::clone(&context.cancelled));
        self.run_inner(true, move |ready_only| {
            build(TemplateBuildContext {
                ready_only,
                ..context
            })
        })
        .await
    }

    async fn run_inner<T, F>(&self, allow_ready: bool, build: F) -> RpcResult<T>
    where
        T: Send + 'static,
        F: FnOnce(bool) -> RpcResult<T> + Send + 'static,
    {
        let deadline = tokio::time::Instant::now() + TEMPLATE_BUILD_WAIT_LIMIT;
        let started = std::time::Instant::now();
        let request_id = diagnostics::next_id();
        let admission = Arc::clone(&self.permits)
            .try_acquire_owned()
            .map(|permit| (permit, false))
            .or_else(|error| {
                if allow_ready {
                    Arc::clone(&self.ready_permits)
                        .try_acquire_owned()
                        .map(|permit| (permit, true))
                } else {
                    Err(error)
                }
            });
        let (permit, ready_only) = admission.map_err(|_| {
            let mut record = Record::new(Event::TemplateWorker, Outcome::Busy);
            record.request_id = request_id;
            record.phase = Phase::Template;
            record.elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
            diagnostics::emit(record);
            ErrorObject::borrowed(LegacyCode::InWarmup.into(), TEMPLATE_BUILD_BUSY, None)
        })?;
        let span = tracing::Span::current();
        let build = tokio::task::spawn_blocking(move || {
            // Cancellation of the async waiter cannot release capacity while
            // a proof or a singleflight cache wait is still running.
            let _permit = permit;
            let _span = span.enter();
            let _context = diagnostics::enter(request_id, Phase::Template);
            let dispatch_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
            let mut record = Record::new(Event::TemplateWorker, Outcome::Started);
            record.dispatch_ms = Some(dispatch_ms);
            diagnostics::emit(record);
            let result = build(ready_only);
            drop(_permit);
            let mut record = Record::new(
                Event::TemplateWorker,
                if result.is_ok() {
                    Outcome::Finished
                } else {
                    Outcome::Error
                },
            );
            record.dispatch_ms = Some(dispatch_ms);
            // Includes blocking dispatch, all cache calls, and remaining assembly.
            record.elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
            diagnostics::emit(record);
            result
        });
        // A pending proof has not created a candidate or a retirement token.
        // Return a retryable result before the pool's ordinary RPC deadline;
        // the detached worker keeps its permit and warms the exact cache key.
        let result = tokio::time::timeout_at(deadline, build).await;
        if tokio::time::Instant::now() >= deadline {
            let mut record = Record::new(Event::TemplateWorker, Outcome::Deadline);
            record.request_id = request_id;
            record.phase = Phase::Template;
            record.elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
            diagnostics::emit(record);
            return Err(ErrorObject::borrowed(
                LegacyCode::InWarmup.into(),
                TEMPLATE_BUILD_PENDING,
                None,
            ));
        }
        result
            .map_err(|_| {
                ErrorObject::borrowed(LegacyCode::InWarmup.into(), TEMPLATE_BUILD_PENDING, None)
            })?
            .map_err(|_| {
                let mut record = Record::new(Event::TemplateWorker, Outcome::Error);
                record.request_id = request_id;
                record.phase = Phase::Template;
                record.elapsed_ms =
                    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
                diagnostics::emit(record);
                ErrorObject::borrowed(
                    ErrorCode::InternalError.code(),
                    "block template worker failed",
                    None,
                )
            })?
    }
}

/// Wait until the exact fetched snapshot has been observed and then replaced.
///
/// Read-state can publish a committed tip before the tip watch catches up, so
/// an initial different hash does not prove that the fetched snapshot is old.
/// Watch failure falls back to the existing bounded template build, without
/// repeatedly polling a closed watch.
pub(super) async fn wait_for_obsolete_snapshot<Tip: zebra_chain::chain_tip::ChainTip>(
    mut latest_chain_tip: Tip,
    snapshot: zebra_chain::block::Hash,
) {
    let mut observed_snapshot = false;
    loop {
        // Mark first, then inspect: an update between inspection and waiting
        // stays unseen and wakes best_tip_changed instead of being lost.
        latest_chain_tip.mark_best_tip_seen();
        match latest_chain_tip.best_tip_hash() {
            Some(hash) if hash == snapshot => observed_snapshot = true,
            Some(_) if observed_snapshot => return,
            _ => {}
        }
        if latest_chain_tip.best_tip_changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

#[cfg(test)]
mod tests;

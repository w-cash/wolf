//! Bounded foreground template CPU work, separate from async RPC execution.

use std::{sync::Arc, time::Duration};

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
}

impl Default for TemplateBuildExecutor {
    fn default() -> Self {
        diagnostics::initialize();
        Self {
            permits: Arc::new(Semaphore::new(MAX_FOREGROUND_TEMPLATE_BUILDS)),
        }
    }
}

impl TemplateBuildExecutor {
    pub(super) async fn run<T, F>(&self, build: F) -> RpcResult<T>
    where
        T: Send + 'static,
        F: FnOnce() -> RpcResult<T> + Send + 'static,
    {
        let deadline = tokio::time::Instant::now() + TEMPLATE_BUILD_WAIT_LIMIT;
        let started = std::time::Instant::now();
        let request_id = diagnostics::next_id();
        let permit = Arc::clone(&self.permits).try_acquire_owned().map_err(|_| {
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
            let result = build();
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

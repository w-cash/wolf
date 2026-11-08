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

#[cfg(test)]
mod tests;

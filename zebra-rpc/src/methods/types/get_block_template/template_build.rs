//! Bounded foreground template CPU work, separate from async RPC execution.

use std::{sync::Arc, time::Duration};

use jsonrpsee::core::RpcResult;
use jsonrpsee_types::{ErrorCode, ErrorObject};
use tokio::sync::Semaphore;

use crate::server::error::LegacyCode;

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
        let permit = Arc::clone(&self.permits).try_acquire_owned().map_err(|_| {
            ErrorObject::borrowed(LegacyCode::InWarmup.into(), TEMPLATE_BUILD_BUSY, None)
        })?;
        let span = tracing::Span::current();
        let build = tokio::task::spawn_blocking(move || {
            // Cancellation of the async waiter cannot release capacity while
            // a proof or a singleflight cache wait is still running.
            let _permit = permit;
            let _span = span.enter();
            let result = build();
            match &result {
                Err(error) if started.elapsed() >= TEMPLATE_BUILD_WAIT_LIMIT => {
                    tracing::warn!(
                        rpc_error_code = error.code(),
                        "delayed foreground block template construction failed"
                    );
                }
                _ => {}
            }
            result
        });
        // A pending proof has not created a candidate or a retirement token.
        // Return a retryable result before the pool's ordinary RPC deadline;
        // the detached worker keeps its permit and warms the exact cache key.
        let result = tokio::time::timeout_at(deadline, build).await;
        if tokio::time::Instant::now() >= deadline {
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

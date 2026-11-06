//! Bounded foreground template CPU work, separate from async RPC execution.

use std::sync::Arc;

use jsonrpsee::core::RpcResult;
use jsonrpsee_types::{ErrorCode, ErrorObject};
use tokio::sync::Semaphore;

use crate::server::error::LegacyCode;

// A builder and one same-proof waiter may run together. Saturation must not
// turn short-polling into an unbounded queue of blocking proof workers.
const MAX_FOREGROUND_TEMPLATE_BUILDS: usize = 2;

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
        let permit = Arc::clone(&self.permits).try_acquire_owned().map_err(|_| {
            ErrorObject::borrowed(
                LegacyCode::InWarmup.into(),
                "block template construction is busy; retry",
                None,
            )
        })?;
        let span = tracing::Span::current();
        tokio::task::spawn_blocking(move || {
            // Cancellation of the async waiter cannot release capacity while
            // a proof or a singleflight cache wait is still running.
            let _permit = permit;
            let _span = span.enter();
            build()
        })
        .await
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

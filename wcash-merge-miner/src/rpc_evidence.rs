//! Fixed public metadata for explicitly attributed preparation RPC failures.
//!
//! Context travels with temporary RPC-client clones, so parallel checks retain
//! their origin without a thread-local guess. It never changes RPC results.

use std::sync::{
    atomic::{AtomicU64, AtomicU8, Ordering},
    Arc,
};

use crate::{preparation_timing::PreparationPhase, MinerError};

static NEXT_ATTEMPT_ID: AtomicU64 = AtomicU64::new(1);

/// One process-local preparation origin shared by its temporary client clones.
#[derive(Clone, Debug)]
pub(crate) struct PreparationRpcContext {
    attempt_id: u64,
    phase: Arc<AtomicU8>,
    #[cfg(test)]
    capture: Option<Arc<std::sync::Mutex<Vec<PreparationRpcFailure>>>>,
}

impl PreparationRpcContext {
    pub(crate) fn new() -> Self {
        // Zero means unavailable after exhaustion; diagnostics must never wrap
        // into another preparation's origin or make mining fail for telemetry.
        let attempt_id = NEXT_ATTEMPT_ID
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .unwrap_or(0);
        Self {
            attempt_id,
            phase: Arc::new(AtomicU8::new(0)),
            #[cfg(test)]
            capture: None,
        }
    }

    pub(crate) const fn attempt_id(&self) -> u64 {
        self.attempt_id
    }

    pub(crate) fn enter(&self, phase: PreparationPhase) {
        let index = u8::try_from(phase.index()).expect("the fixed phase table fits in one byte");
        self.phase.store(index, Ordering::Relaxed);
    }

    pub(crate) fn snapshot_phase(&self) -> PreparationPhase {
        PreparationPhase::ALL
            .get(usize::from(self.phase.load(Ordering::Relaxed)))
            .copied()
            .unwrap_or(PreparationPhase::Other)
    }

    pub(crate) fn record_failure(&self, failure: PreparationRpcFailure) {
        #[cfg(test)]
        if let Some(capture) = &self.capture {
            capture.lock().unwrap().push(failure);
        }
        crate::diagnostics::preparation_rpc_failure(failure);
    }

    #[cfg(test)]
    pub(crate) fn with_failure_capture(
        mut self,
    ) -> (Self, Arc<std::sync::Mutex<Vec<PreparationRpcFailure>>>) {
        let captured = Arc::new(std::sync::Mutex::new(Vec::new()));
        self.capture = Some(Arc::clone(&captured));
        (self, captured)
    }
}

/// Source-assigned endpoint role; arbitrary endpoint labels are never copied.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RpcRole {
    Unspecified,
    Wcash,
    Template,
    Validator,
    Broadcaster,
    Observer,
}

impl RpcRole {
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
            Self::Wcash => "wcash",
            Self::Template => "template",
            Self::Validator => "validator",
            Self::Broadcaster => "broadcaster",
            Self::Observer => "observer",
        }
    }
}

/// Allowlisted RPC operation; unknown method names remain a fixed label.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RpcMethod {
    CreateAuxBlock,
    GetAuxBlockStatus,
    GetBestBlockHash,
    GetBlock,
    GetBlockchainInfo,
    GetBlockCount,
    GetBlockHash,
    GetBlockHeader,
    GetBlockStatus,
    GetBlockTemplate,
    RetireAuxBlock,
    SubmitAuxBlock,
    SubmitBlock,
    Other,
}

impl RpcMethod {
    pub(crate) fn from_name(method: &str) -> Self {
        match method {
            "createauxblock" => Self::CreateAuxBlock,
            "getauxblockstatus" => Self::GetAuxBlockStatus,
            "getbestblockhash" => Self::GetBestBlockHash,
            "getblock" => Self::GetBlock,
            "getblockchaininfo" => Self::GetBlockchainInfo,
            "getblockcount" => Self::GetBlockCount,
            "getblockhash" => Self::GetBlockHash,
            "getblockheader" => Self::GetBlockHeader,
            "getblockstatus" => Self::GetBlockStatus,
            "getblocktemplate" => Self::GetBlockTemplate,
            "retireauxblock" => Self::RetireAuxBlock,
            "submitauxblock" => Self::SubmitAuxBlock,
            "submitblock" => Self::SubmitBlock,
            _ => Self::Other,
        }
    }

    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::CreateAuxBlock => "createauxblock",
            Self::GetAuxBlockStatus => "getauxblockstatus",
            Self::GetBestBlockHash => "getbestblockhash",
            Self::GetBlock => "getblock",
            Self::GetBlockchainInfo => "getblockchaininfo",
            Self::GetBlockCount => "getblockcount",
            Self::GetBlockHash => "getblockhash",
            Self::GetBlockHeader => "getblockheader",
            Self::GetBlockStatus => "getblockstatus",
            Self::GetBlockTemplate => "getblocktemplate",
            Self::RetireAuxBlock => "retireauxblock",
            Self::SubmitAuxBlock => "submitauxblock",
            Self::SubmitBlock => "submitblock",
            Self::Other => "other",
        }
    }
}

/// A completed failed request, without response bodies, URLs or credentials.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PreparationRpcFailure {
    pub(crate) attempt_id: u64,
    pub(crate) phase: PreparationPhase,
    pub(crate) role: RpcRole,
    pub(crate) recipient_index: Option<u16>,
    pub(crate) method: RpcMethod,
    pub(crate) request_id: u64,
    pub(crate) elapsed_ms: u64,
    pub(crate) error_class: &'static str,
    pub(crate) rpc_error_code: Option<i64>,
    pub(crate) rpc_http_status: Option<u16>,
}

impl PreparationRpcFailure {
    pub(crate) fn with_error(mut self, error: &MinerError) -> Self {
        self.error_class = match error {
            MinerError::RpcTransport(error) if error.is_timeout() => "rpc_timeout",
            MinerError::RpcTransport(_) => "rpc_transport",
            MinerError::RpcHttpStatus(status) => {
                self.rpc_http_status = Some(status.as_u16());
                "rpc_http_status"
            }
            MinerError::RpcError { code, .. } => {
                self.rpc_error_code = *code;
                "rpc_error"
            }
            MinerError::RpcProtocol(_) | MinerError::Json(_) => "rpc_response",
            MinerError::RpcResponseTooLarge(_) => "rpc_response_too_large",
            MinerError::Io(_) => "rpc_io",
            _ => "rpc_failure",
        };
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contexts_have_nonzero_distinct_origins_and_shared_explicit_phases() {
        let first = PreparationRpcContext::new();
        let second = PreparationRpcContext::new();
        assert_ne!(first.attempt_id(), 0);
        assert!(second.attempt_id() > first.attempt_id());
        first.enter(PreparationPhase::ZcashProposal);
        assert_eq!(
            first.clone().snapshot_phase(),
            PreparationPhase::ZcashProposal
        );
        assert_eq!(second.snapshot_phase(), PreparationPhase::Other);
    }

    #[test]
    fn method_names_and_error_metadata_are_closed_public_sets() {
        let secret = "PRIVATE_METHOD_OR_ENDPOINT";
        assert_eq!(RpcMethod::from_name(secret), RpcMethod::Other);
        assert_eq!(RpcMethod::from_name(secret).name(), "other");
        assert_eq!(RpcRole::Unspecified.name(), "unspecified");
        let failure = PreparationRpcFailure {
            attempt_id: 1,
            phase: PreparationPhase::ZcashTemplate,
            role: RpcRole::Template,
            recipient_index: Some(0),
            method: RpcMethod::GetBlockTemplate,
            request_id: 3,
            elapsed_ms: 0,
            error_class: "rpc_failure",
            rpc_error_code: None,
            rpc_http_status: None,
        }
        .with_error(&MinerError::RpcError {
            endpoint: secret.into(),
            code: Some(-32001),
            message: secret.into(),
        });
        assert_eq!(failure.error_class, "rpc_error");
        assert_eq!(failure.rpc_error_code, Some(-32001));
        assert!(!format!("{failure:?}").contains(secret));
    }
}

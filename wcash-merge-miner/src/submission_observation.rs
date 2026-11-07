//! Process-local attempt observations, independent of receipts and accounting.

use wcash_pool_protocol::{BackendErrorCode, BackendMessage};

use crate::pool_backend_listener::PoolBackendHandlerError;

pub(crate) const RESPONSE_LABELS: [&str; 12] = [
    "committed",
    "replayed",
    "invalid_request",
    "stale_job",
    "low_difficulty",
    "invalid_equihash",
    "target_out_of_range",
    "attribution_conflict",
    "overloaded",
    "backend_unhealthy",
    "recovery_deferred",
    "unexpected_response",
];
pub(crate) const LATE_LABELS: [&str; 6] = [
    "target_prefilter_passed",
    "parent_changed",
    "authority_deferred",
    "validation_failed",
    "journal_append_error",
    "durable_receipt_observed",
];

/// Saturating process-cumulative observations, reset on restart and never journaled.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NativeSubmissionStats {
    pub(crate) attempts: u64,
    pub(crate) responses: [u64; 12],
    pub(crate) late: [u64; 6],
    pub(crate) fresh_durable_receipts: u64,
    pub(crate) replayed_durable_receipts: u64,
    pub(crate) overflowed: bool,
}

pub(crate) enum LateSubmissionOutcome {
    TargetPrefilterPassed,
    ParentChanged,
    AuthorityDeferred,
    ValidationFailed,
    JournalAppendError,
    DurableReceiptObserved,
}

impl NativeSubmissionStats {
    pub(crate) fn record_response(
        &mut self,
        result: &Result<Vec<BackendMessage>, PoolBackendHandlerError>,
    ) {
        let index = match result {
            Ok(messages) => match messages.last() {
                Some(BackendMessage::ShareCommitted {
                    replayed: false, ..
                }) => 0,
                Some(BackendMessage::ShareCommitted { replayed: true, .. }) => 1,
                _ => 11,
            },
            Err(error) => match error.code() {
                BackendErrorCode::InvalidRequest => 2,
                BackendErrorCode::StaleJob => 3,
                BackendErrorCode::LowDifficulty => 4,
                BackendErrorCode::InvalidEquihash => 5,
                BackendErrorCode::TargetOutOfRange => 6,
                BackendErrorCode::AttributionConflict => 7,
                BackendErrorCode::Overloaded => 8,
                BackendErrorCode::BackendUnhealthy => 9,
                BackendErrorCode::RecoveryDeferred => 10,
            },
        };
        increment(&mut self.attempts, &mut self.overflowed);
        increment(&mut self.responses[index], &mut self.overflowed);
    }

    pub(crate) fn record_late(&mut self, outcome: LateSubmissionOutcome) {
        let index = match outcome {
            LateSubmissionOutcome::TargetPrefilterPassed => 0,
            LateSubmissionOutcome::ParentChanged => 1,
            LateSubmissionOutcome::AuthorityDeferred => 2,
            LateSubmissionOutcome::ValidationFailed => 3,
            LateSubmissionOutcome::JournalAppendError => 4,
            LateSubmissionOutcome::DurableReceiptObserved => 5,
        };
        increment(&mut self.late[index], &mut self.overflowed);
    }

    pub(crate) fn record_durable_receipt(&mut self, replayed: bool) {
        let value = if replayed {
            &mut self.replayed_durable_receipts
        } else {
            &mut self.fresh_durable_receipts
        };
        increment(value, &mut self.overflowed);
    }
}

fn increment(value: &mut u64, overflowed: &mut bool) {
    match value.checked_add(1) {
        Some(next) => *value = next,
        None => *overflowed = true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn submission_attempt_observations_saturate_and_do_not_infer_durability_from_errors() {
        let mut stats = NativeSubmissionStats::default();
        // A wakeup or response failure can occur after the real receipt exists.
        stats.record_durable_receipt(false);
        stats.record_response(&Err(PoolBackendHandlerError::new(
            BackendErrorCode::BackendUnhealthy,
            "private fixture",
            true,
        )));
        assert_eq!(stats.attempts, 1);
        assert_eq!(stats.responses[9], 1);
        assert_eq!(stats.fresh_durable_receipts, 1);
        stats.attempts = u64::MAX;
        stats.responses[9] = u64::MAX;
        stats.record_response(&Err(PoolBackendHandlerError::new(
            BackendErrorCode::BackendUnhealthy,
            "private fixture",
            true,
        )));
        assert_eq!(stats.attempts, u64::MAX);
        assert_eq!(stats.responses[9], u64::MAX);
        assert!(stats.overflowed);
        assert_eq!(stats.fresh_durable_receipts, 1);
        assert_eq!(stats.late, [0; 6]);
    }
}

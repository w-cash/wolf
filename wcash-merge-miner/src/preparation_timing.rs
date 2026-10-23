//! Fixed-size, post-result timings for native merged-mining job preparation.
//!
//! Callers mark exclusive phases on the preparation thread. This records no
//! RPC payloads or wall-clock timestamps and never participates in share or
//! winner admission. Only slow or failed completed attempts reach diagnostics.

use std::time::{Duration, Instant};

use crate::MinerError;

const SLOW_PREPARATION: Duration = Duration::from_secs(1);
pub(crate) const PHASE_COUNT: usize = 13;

/// Exclusive preparation phases; repeated entries accumulate without overlap.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PreparationPhase {
    /// Local orchestration before or between explicitly named phases.
    Other,
    /// Network and backend-capability identity checks.
    NetworkIdentity,
    /// Retirement of an unpublished child candidate from a prior attempt.
    CandidateRetirement,
    /// Catch-up of work nodes against the public-chain observer.
    ObserverCatchup,
    /// The Wcash candidate RPC, including its node-side construction/checks.
    WcashCandidate,
    /// Local Wcash candidate decoding and metadata checks.
    WcashDecode,
    /// Local Wcash payout-authority and value checks.
    WcashPayout,
    /// The Zcash template RPC and its node-side work.
    ZcashTemplate,
    /// Local Zcash template decoding and payout checks.
    ZcashDecode,
    /// Independent Zcash proposal checks.
    ZcashProposal,
    /// Exact chain-currentness fences.
    CurrentnessFences,
    /// Durable native generation journal activation.
    GenerationJournal,
    /// Backend actor activation, including its durable job event.
    Activation,
}

impl PreparationPhase {
    pub(crate) const ALL: [Self; PHASE_COUNT] = [
        Self::Other,
        Self::NetworkIdentity,
        Self::CandidateRetirement,
        Self::ObserverCatchup,
        Self::WcashCandidate,
        Self::WcashDecode,
        Self::WcashPayout,
        Self::ZcashTemplate,
        Self::ZcashDecode,
        Self::ZcashProposal,
        Self::CurrentnessFences,
        Self::GenerationJournal,
        Self::Activation,
    ];

    pub(crate) const fn index(self) -> usize {
        match self {
            Self::Other => 0,
            Self::NetworkIdentity => 1,
            Self::CandidateRetirement => 2,
            Self::ObserverCatchup => 3,
            Self::WcashCandidate => 4,
            Self::WcashDecode => 5,
            Self::WcashPayout => 6,
            Self::ZcashTemplate => 7,
            Self::ZcashDecode => 8,
            Self::ZcashProposal => 9,
            Self::CurrentnessFences => 10,
            Self::GenerationJournal => 11,
            Self::Activation => 12,
        }
    }

    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Other => "other",
            Self::NetworkIdentity => "network_identity",
            Self::CandidateRetirement => "candidate_retirement",
            Self::ObserverCatchup => "observer_catchup",
            Self::WcashCandidate => "wcash_candidate",
            Self::WcashDecode => "wcash_decode",
            Self::WcashPayout => "wcash_payout",
            Self::ZcashTemplate => "zcash_template",
            Self::ZcashDecode => "zcash_decode",
            Self::ZcashProposal => "zcash_proposal",
            Self::CurrentnessFences => "currentness_fences",
            Self::GenerationJournal => "generation_journal",
            Self::Activation => "activation",
        }
    }
}

/// One explicit preparation attempt, ending only after success or failure.
///
/// This is a fixed-size stack value with no writer, RPC, shared mutex or cache.
/// Dropping it emits nothing; the owner must finish the attempt after its result.
pub struct NativePreparationTiming {
    started: Instant,
    phase_started: Instant,
    phase: PreparationPhase,
    phases: [Duration; PHASE_COUNT],
    wcash_height: Option<u32>,
    zcash_height: Option<u32>,
}

impl Default for NativePreparationTiming {
    fn default() -> Self {
        Self::new()
    }
}

impl NativePreparationTiming {
    /// Starts a preparation attempt using the local monotonic clock.
    pub fn new() -> Self {
        Self::at(Instant::now())
    }

    fn at(now: Instant) -> Self {
        Self {
            started: now,
            phase_started: now,
            phase: PreparationPhase::Other,
            phases: [Duration::ZERO; PHASE_COUNT],
            wcash_height: None,
            zcash_height: None,
        }
    }

    /// Closes the preceding exclusive phase and begins the selected phase.
    ///
    /// A parallel RPC batch is measured as one caller phase, not as a sum of
    /// overlapping child requests. No text or diagnostics are emitted here.
    pub fn enter(&mut self, phase: PreparationPhase) {
        self.enter_at(phase, Instant::now());
    }

    fn enter_at(&mut self, phase: PreparationPhase, now: Instant) {
        self.close_phase(now);
        self.phase = phase;
        self.phase_started = now;
    }

    /// Adds the public Wcash candidate height once known.
    pub fn set_wcash_height(&mut self, height: u32) {
        self.wcash_height = Some(height);
    }

    /// Adds the public Zcash candidate height once known.
    pub fn set_zcash_height(&mut self, height: u32) {
        self.zcash_height = Some(height);
    }

    /// Adds both public candidate heights once known.
    pub fn set_heights(&mut self, wcash: u32, zcash: u32) {
        self.set_wcash_height(wcash);
        self.set_zcash_height(zcash);
    }

    /// Finishes a successfully activated attempt; fast success emits nothing.
    pub fn finish_ok(self) {
        self.finish(PreparationResult::Activated);
    }

    /// Finishes a failed attempt using only a fixed error-class label.
    ///
    /// Error bodies, endpoint labels, credentials and payout data are discarded.
    pub fn finish_error(self, error: &MinerError) {
        self.finish(PreparationResult::Failed(preparation_error_class(error)));
    }

    /// Finishes an actor-activation failure without formatting its error.
    pub fn finish_activation_failed(self) {
        self.finish(PreparationResult::Failed("activation_failed"));
    }

    fn finish(self, result: PreparationResult) {
        if let Some(report) = self.report_at(result, Instant::now()) {
            crate::diagnostics::preparation_result(report);
        }
    }

    fn close_phase(&mut self, now: Instant) {
        let index = self.phase.index();
        self.phases[index] =
            self.phases[index].saturating_add(now.saturating_duration_since(self.phase_started));
    }

    fn report_at(
        mut self,
        result: PreparationResult,
        finished: Instant,
    ) -> Option<PreparationReport> {
        self.close_phase(finished);
        let elapsed = finished.saturating_duration_since(self.started);
        if matches!(result, PreparationResult::Activated) && elapsed < SLOW_PREPARATION {
            return None;
        }
        Some(PreparationReport {
            elapsed_ms: milliseconds(elapsed),
            phases_ms: self.phases.map(milliseconds),
            finished_phase: self.phase,
            wcash_height: self.wcash_height,
            zcash_height: self.zcash_height,
            result,
        })
    }
}

#[derive(Clone, Copy)]
pub(crate) enum PreparationResult {
    Activated,
    Failed(&'static str),
}

impl PreparationResult {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Activated => "activated",
            Self::Failed(_) => "failed",
        }
    }

    pub(crate) const fn error_class(self) -> Option<&'static str> {
        match self {
            Self::Activated => None,
            Self::Failed(class) => Some(class),
        }
    }
}

/// Only fixed public metadata crosses the existing bounded diagnostic channel.
#[derive(Clone, Copy)]
pub(crate) struct PreparationReport {
    pub(crate) elapsed_ms: u64,
    pub(crate) phases_ms: [u64; PHASE_COUNT],
    pub(crate) finished_phase: PreparationPhase,
    pub(crate) wcash_height: Option<u32>,
    pub(crate) zcash_height: Option<u32>,
    pub(crate) result: PreparationResult,
}

fn milliseconds(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn preparation_error_class(error: &MinerError) -> &'static str {
    match error {
        MinerError::ParentTipMismatch { .. } => "parent_tip_mismatch",
        MinerError::ChildTipMismatch { .. } => "child_tip_mismatch",
        MinerError::StaleNativeJob(_) => "stale_job",
        MinerError::RpcTransport(_) => "rpc_transport",
        MinerError::RpcHttpStatus(_) => "rpc_http_status",
        MinerError::RpcError { .. } => "rpc_error",
        MinerError::RpcConfiguration(_) => "rpc_configuration",
        MinerError::RpcProtocol(_) | MinerError::RpcResponseTooLarge(_) => "rpc_response",
        MinerError::ParentProposalRejected { .. } => "proposal_rejected",
        MinerError::NetworkIdentityMismatch { .. } => "network_identity",
        MinerError::InvalidParentTemplate(_) => "invalid_template",
        MinerError::Io(_) => "io_error",
        _ => "preparation_error",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exclusive_repeated_phases_account_for_the_complete_attempt() {
        let started = Instant::now();
        let mut timing = NativePreparationTiming::at(started);
        timing.enter_at(
            PreparationPhase::NetworkIdentity,
            started + Duration::from_millis(50),
        );
        timing.enter_at(
            PreparationPhase::WcashCandidate,
            started + Duration::from_millis(150),
        );
        timing.enter_at(
            PreparationPhase::NetworkIdentity,
            started + Duration::from_millis(950),
        );
        timing.enter_at(
            PreparationPhase::Activation,
            started + Duration::from_millis(1_050),
        );
        timing.set_heights(38_789, 3_528_090);
        let report = timing
            .report_at(
                PreparationResult::Activated,
                started + Duration::from_millis(1_100),
            )
            .unwrap();
        assert_eq!(report.elapsed_ms, 1_100);
        assert_eq!(report.phases_ms[PreparationPhase::Other.index()], 50);
        assert_eq!(
            report.phases_ms[PreparationPhase::NetworkIdentity.index()],
            200
        );
        assert_eq!(
            report.phases_ms[PreparationPhase::WcashCandidate.index()],
            800
        );
        assert_eq!(report.phases_ms[PreparationPhase::Activation.index()], 50);
        assert_eq!(report.phases_ms.iter().sum::<u64>(), report.elapsed_ms);
        assert_eq!(report.wcash_height, Some(38_789));
        assert_eq!(report.zcash_height, Some(3_528_090));
    }

    #[test]
    fn fast_success_is_silent_but_failure_and_exact_threshold_are_reportable() {
        let now = Instant::now();
        assert!(NativePreparationTiming::at(now)
            .report_at(
                PreparationResult::Activated,
                now + SLOW_PREPARATION - Duration::from_nanos(1)
            )
            .is_none());
        assert!(NativePreparationTiming::at(now)
            .report_at(PreparationResult::Activated, now + SLOW_PREPARATION)
            .is_some());
        let failed = NativePreparationTiming::at(now)
            .report_at(PreparationResult::Failed("rpc_error"), now)
            .unwrap();
        assert_eq!(failed.elapsed_ms, 0);
        assert_eq!(failed.wcash_height, None);
        assert_eq!(failed.zcash_height, None);
    }

    #[test]
    fn failed_phase_and_partial_public_heights_remain_exact() {
        let now = Instant::now();
        let mut timing = NativePreparationTiming::at(now);
        timing.set_wcash_height(38_789);
        timing.enter_at(
            PreparationPhase::ZcashProposal,
            now + Duration::from_millis(200),
        );
        let report = timing
            .report_at(
                PreparationResult::Failed("parent_tip_mismatch"),
                now + Duration::from_millis(250),
            )
            .unwrap();
        assert_eq!(report.finished_phase, PreparationPhase::ZcashProposal);
        assert_eq!(
            report.phases_ms[PreparationPhase::ZcashProposal.index()],
            50
        );
        assert_eq!(report.wcash_height, Some(38_789));
        assert_eq!(report.zcash_height, None);
    }

    #[test]
    fn fixed_error_class_never_copies_private_error_bodies() {
        let secret = "PRIVATE_ERROR_ENDPOINT_OR_PAYOUT";
        for (error, class) in [
            (MinerError::RpcProtocol(secret.into()), "rpc_response"),
            (
                MinerError::ParentProposalRejected {
                    endpoint: secret.into(),
                    reason: secret.into(),
                },
                "proposal_rejected",
            ),
            (
                MinerError::ParentTipMismatch {
                    expected: secret.into(),
                    endpoint: secret.into(),
                    actual: secret.into(),
                },
                "parent_tip_mismatch",
            ),
            (
                MinerError::InvalidParentTemplate(secret.into()),
                "invalid_template",
            ),
        ] {
            assert_eq!(preparation_error_class(&error), class);
            assert!(!preparation_error_class(&error).contains(secret));
        }
    }
}

//! Pool-backend retention for one exact native merged-mining generation.

use std::fmt;

use wcash_pool_protocol::{Hex32, JobDescriptor, TargetLe};
use wcash_zcash_aux::{AuxPowError, Target};

use crate::{
    coordinator::{GenerationRetirement, NativeMiningCoordinator},
    pool_backend::{job_descriptor_from_native, PoolBackendAdapterError},
    pool_backend_actor::{
        PoolBackendRetainedJob, PoolBackendShareRequest, PoolBackendShareValidationError,
        PoolBackendValidatedShare,
    },
    MinerError, EQUIHASH_SOLUTION_BYTES,
};

/// One proposal-validated native generation retained for backend shares.
///
/// The adapter owns the exact [`NativeMiningCoordinator`] which produced its
/// descriptor. It validates submitted fields through that coordinator's frozen
/// [`crate::NativePreparedJob`] and materializes winner blocks through the same
/// coordinator. It never calls the legacy share processor or winner outbox, so
/// only [`crate::PoolBackendActor`]'s journal can acknowledge a backend share.
///
/// Pool orchestration should store this value in an `Arc`, give a clone to
/// [`crate::PoolBackendActor::activate_job`], then durably close the job and
/// recover unique ownership before calling [`Self::retire_generation`]. Requiring
/// `&mut self` for retirement prevents candidate retirement while any admitted
/// validation still borrows this exact adapter.
pub struct NativePoolBackendRetainedJob {
    coordinator: NativeMiningCoordinator,
    descriptor: JobDescriptor,
}

impl NativePoolBackendRetainedJob {
    /// Binds one exact coordinator to its backend descriptor.
    pub fn new(coordinator: NativeMiningCoordinator) -> Result<Self, PoolBackendAdapterError> {
        let descriptor = job_descriptor_from_native(coordinator.generation_descriptor())?;
        Ok(Self {
            coordinator,
            descriptor,
        })
    }

    /// Retires the underlying Wcash candidate after validation ownership is unique.
    ///
    /// When this value is shared through an `Arc`, callers must first use
    /// `Arc::get_mut` or `Arc::try_unwrap`. Both operations fail while the actor
    /// or an admitted validation retains another owner.
    pub fn retire_generation(&mut self) -> Result<GenerationRetirement, MinerError> {
        self.coordinator.retire_generation()
    }
}

impl fmt::Debug for NativePoolBackendRetainedJob {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Do not expose the coordinator, descriptor, proof material, block
        // bytes, reward recipients, or payout amounts through diagnostics.
        formatter
            .debug_struct("NativePoolBackendRetainedJob")
            .finish_non_exhaustive()
    }
}

impl PoolBackendRetainedJob for NativePoolBackendRetainedJob {
    fn descriptor(&self) -> JobDescriptor {
        self.descriptor.clone()
    }

    fn wcash_payout_commitment(&self) -> Hex32 {
        Hex32::new(
            self.coordinator
                .generation_descriptor()
                .wcash_payout_commitment(),
        )
    }

    fn zcash_payout_commitment(&self) -> Hex32 {
        Hex32::new(
            self.coordinator
                .generation_descriptor()
                .zcash_payout_commitment(),
        )
    }

    fn remaining_lifetime(&self) -> Option<std::time::Duration> {
        let remaining = self.coordinator.remaining_job_lifetime();
        (!remaining.is_zero()).then_some(remaining)
    }

    fn is_healthy(&self) -> bool {
        self.coordinator.assert_current().is_ok()
    }

    fn validate_share(
        &self,
        share: PoolBackendShareRequest<'_>,
    ) -> Result<PoolBackendValidatedShare, PoolBackendShareValidationError> {
        validate_retained_share(
            &self.coordinator,
            *share.time().as_bytes(),
            share.nonce().as_bytes(),
            share.solution().as_bytes(),
            share.target_le(),
        )
    }
}

/// Private test seam for the exact native validation and materialization call.
///
/// This trait is deliberately not exported: normal builds cannot substitute a
/// pool-controlled implementation for [`NativeMiningCoordinator`].
trait NativeValidationBackend: Send + Sync {
    fn assert_current(&self) -> Result<(), MinerError>;

    fn validate_and_materialize(
        &self,
        header_time: [u8; 4],
        nonce: &[u8; 32],
        solution: &[u8; EQUIHASH_SOLUTION_BYTES],
        share_target: Target,
    ) -> Result<ValidatedWinnerBlocks, MinerError>;
}

impl NativeValidationBackend for NativeMiningCoordinator {
    fn assert_current(&self) -> Result<(), MinerError> {
        NativeMiningCoordinator::assert_current(self)
    }

    fn validate_and_materialize(
        &self,
        header_time: [u8; 4],
        nonce: &[u8; 32],
        solution: &[u8; EQUIHASH_SOLUTION_BYTES],
        share_target: Target,
    ) -> Result<ValidatedWinnerBlocks, MinerError> {
        let validated =
            self.job()
                .validate_submitted_share(header_time, nonce, solution, share_target)?;
        let material = self.materialize_winners(&validated)?;
        Ok(ValidatedWinnerBlocks {
            wcash: material.wcash().map(|winner| winner.block_bytes().to_vec()),
            zcash: material.zcash().map(|winner| winner.block_bytes().to_vec()),
        })
    }
}

struct ValidatedWinnerBlocks {
    wcash: Option<Vec<u8>>,
    zcash: Option<Vec<u8>>,
}

fn validate_retained_share<B: NativeValidationBackend + ?Sized>(
    backend: &B,
    header_time: [u8; 4],
    nonce: &[u8; 32],
    solution: &[u8; EQUIHASH_SOLUTION_BYTES],
    target_le: &TargetLe,
) -> Result<PoolBackendValidatedShare, PoolBackendShareValidationError> {
    let share_target = Target::from_le_bytes(*target_le.as_bytes())
        .map_err(|error| map_native_validation_error(error.into()))?;
    let winners = backend
        .validate_and_materialize(header_time, nonce, solution, share_target)
        .map_err(map_native_validation_error)?;

    // Mirror the coordinator's winner-first invariant. A tip change or outage
    // on either chain must not erase exact winner material for the other chain;
    // each chain's later consensus submission is authoritative. Currentness is
    // only an admission condition for an ordinary share.
    if winners.wcash.is_none() && winners.zcash.is_none() {
        backend
            .assert_current()
            .map_err(map_native_validation_error)?;
    }

    Ok(PoolBackendValidatedShare::with_winners(
        winners.wcash,
        winners.zcash,
    ))
}

fn map_native_validation_error(error: MinerError) -> PoolBackendShareValidationError {
    match error {
        MinerError::AuxPow(AuxPowError::InsufficientParentWork { .. }) => {
            PoolBackendShareValidationError::LowDifficulty
        }
        MinerError::AuxPow(AuxPowError::InvalidEquihash) => {
            PoolBackendShareValidationError::InvalidEquihash
        }
        MinerError::StaleNativeJob(_)
        | MinerError::ChildTipMismatch { .. }
        | MinerError::ParentTipMismatch { .. } => PoolBackendShareValidationError::StaleJob,
        // The actor has already checked the submitted time against the exact
        // retained descriptor. A second-layer mismatch therefore means the
        // descriptor and coordinator diverged, not that the miner sent stale
        // work.
        MinerError::SubmittedTimeMismatch => PoolBackendShareValidationError::BackendUnavailable,
        // Fixed-width protocol types make nonce/solution length failures
        // unreachable here. All other native failures indicate a dependency or
        // invariant failure and must never be attributed as valid miner work.
        _ => PoolBackendShareValidationError::BackendUnavailable,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    };

    use wcash_pool_protocol::{Hex1344, Hex32, Hex4};

    use super::*;

    #[derive(Clone)]
    enum CurrentOutcome {
        Current,
        Stale,
        DependencyUnavailable,
    }

    #[derive(Clone)]
    enum ValidationOutcome {
        Winners {
            wcash: Option<Vec<u8>>,
            zcash: Option<Vec<u8>>,
        },
        LowDifficulty,
        InvalidEquihash,
        SubmittedTimeMismatch,
        InvariantFailure,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct ObservedSubmission {
        header_time: [u8; 4],
        nonce: [u8; 32],
        solution: Box<[u8; EQUIHASH_SOLUTION_BYTES]>,
        share_target_le: [u8; 32],
    }

    struct TestNativeBackend {
        current: CurrentOutcome,
        outcome: ValidationOutcome,
        currentness_calls: AtomicUsize,
        validation_calls: AtomicUsize,
        observed: Mutex<Option<ObservedSubmission>>,
    }

    impl TestNativeBackend {
        fn new(current: CurrentOutcome, outcome: ValidationOutcome) -> Self {
            Self {
                current,
                outcome,
                currentness_calls: AtomicUsize::new(0),
                validation_calls: AtomicUsize::new(0),
                observed: Mutex::new(None),
            }
        }
    }

    impl NativeValidationBackend for TestNativeBackend {
        fn assert_current(&self) -> Result<(), MinerError> {
            self.currentness_calls.fetch_add(1, Ordering::Relaxed);
            match self.current {
                CurrentOutcome::Current => Ok(()),
                CurrentOutcome::Stale => Err(MinerError::StaleNativeJob("test detail".to_string())),
                CurrentOutcome::DependencyUnavailable => {
                    Err(MinerError::RpcProtocol("test detail".to_string()))
                }
            }
        }

        fn validate_and_materialize(
            &self,
            header_time: [u8; 4],
            nonce: &[u8; 32],
            solution: &[u8; EQUIHASH_SOLUTION_BYTES],
            share_target: Target,
        ) -> Result<ValidatedWinnerBlocks, MinerError> {
            self.validation_calls.fetch_add(1, Ordering::Relaxed);
            *self.observed.lock().expect("test observation lock") = Some(ObservedSubmission {
                header_time,
                nonce: *nonce,
                solution: Box::new(*solution),
                share_target_le: share_target.to_le_bytes(),
            });
            match &self.outcome {
                ValidationOutcome::Winners { wcash, zcash } => Ok(ValidatedWinnerBlocks {
                    wcash: wcash.clone(),
                    zcash: zcash.clone(),
                }),
                ValidationOutcome::LowDifficulty => Err(AuxPowError::InsufficientParentWork {
                    hash_le: [0xff; 32],
                    target_le: [1; 32],
                }
                .into()),
                ValidationOutcome::InvalidEquihash => Err(AuxPowError::InvalidEquihash.into()),
                ValidationOutcome::SubmittedTimeMismatch => Err(MinerError::SubmittedTimeMismatch),
                ValidationOutcome::InvariantFailure => {
                    Err(MinerError::InvalidParentTemplate("test detail".to_string()))
                }
            }
        }
    }

    fn proof_fields() -> (Hex4, Hex32, Hex1344, TargetLe) {
        (
            Hex4::new([0x11, 0x22, 0x33, 0x44]),
            Hex32::new([0x55; 32]),
            Hex1344::new([0x66; EQUIHASH_SOLUTION_BYTES]),
            TargetLe::new([0x77; 32]),
        )
    }

    #[test]
    fn exact_native_fields_are_forwarded_without_endian_conversion() {
        let backend = TestNativeBackend::new(
            CurrentOutcome::Current,
            ValidationOutcome::Winners {
                wcash: None,
                zcash: None,
            },
        );
        let (time, nonce, solution, target) = proof_fields();

        let result = validate_retained_share(
            &backend,
            *time.as_bytes(),
            nonce.as_bytes(),
            solution.as_bytes(),
            &target,
        );

        assert_eq!(result, Ok(PoolBackendValidatedShare::ordinary()));
        assert_eq!(backend.validation_calls.load(Ordering::Relaxed), 1);
        assert_eq!(backend.currentness_calls.load(Ordering::Relaxed), 1);
        assert_eq!(
            *backend.observed.lock().expect("test observation lock"),
            Some(ObservedSubmission {
                header_time: *time.as_bytes(),
                nonce: *nonce.as_bytes(),
                solution: Box::new(*solution.as_bytes()),
                share_target_le: *target.as_bytes(),
            })
        );
    }

    #[test]
    fn native_winner_material_is_preserved_for_each_chain_combination() {
        let cases = [
            (Some(vec![0x41, 0x42]), None),
            (None, Some(vec![0x51, 0x52, 0x53])),
            (Some(vec![0x61, 0x62]), Some(vec![0x71, 0x72, 0x73])),
        ];
        let (time, nonce, solution, target) = proof_fields();

        for (wcash, zcash) in cases {
            let backend = TestNativeBackend::new(
                CurrentOutcome::Current,
                ValidationOutcome::Winners {
                    wcash: wcash.clone(),
                    zcash: zcash.clone(),
                },
            );
            assert_eq!(
                validate_retained_share(
                    &backend,
                    *time.as_bytes(),
                    nonce.as_bytes(),
                    solution.as_bytes(),
                    &target,
                ),
                Ok(PoolBackendValidatedShare::with_winners(wcash, zcash))
            );
            assert_eq!(backend.validation_calls.load(Ordering::Relaxed), 1);
            assert_eq!(backend.currentness_calls.load(Ordering::Relaxed), 0);
        }
    }

    #[test]
    fn stale_cross_chain_and_dual_winners_are_never_suppressed() {
        let cases = [
            (Some(vec![0x41]), None),
            (None, Some(vec![0x51])),
            (Some(vec![0x61]), Some(vec![0x71])),
        ];
        let (time, nonce, solution, target) = proof_fields();

        for (wcash, zcash) in cases {
            let backend = TestNativeBackend::new(
                CurrentOutcome::Stale,
                ValidationOutcome::Winners {
                    wcash: wcash.clone(),
                    zcash: zcash.clone(),
                },
            );
            assert_eq!(
                validate_retained_share(
                    &backend,
                    *time.as_bytes(),
                    nonce.as_bytes(),
                    solution.as_bytes(),
                    &target,
                ),
                Ok(PoolBackendValidatedShare::with_winners(wcash, zcash))
            );
            assert_eq!(backend.validation_calls.load(Ordering::Relaxed), 1);
            assert_eq!(backend.currentness_calls.load(Ordering::Relaxed), 0);
        }
    }

    #[test]
    fn dependency_outage_cannot_suppress_either_winner() {
        let (time, nonce, solution, target) = proof_fields();
        let backend = TestNativeBackend::new(
            CurrentOutcome::DependencyUnavailable,
            ValidationOutcome::Winners {
                wcash: Some(vec![0xaa]),
                zcash: Some(vec![0xbb]),
            },
        );

        assert_eq!(
            validate_retained_share(
                &backend,
                *time.as_bytes(),
                nonce.as_bytes(),
                solution.as_bytes(),
                &target,
            ),
            Ok(PoolBackendValidatedShare::with_winners(
                Some(vec![0xaa]),
                Some(vec![0xbb]),
            ))
        );
        assert_eq!(backend.validation_calls.load(Ordering::Relaxed), 1);
        assert_eq!(backend.currentness_calls.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn native_validation_errors_map_to_stable_fail_closed_classes() {
        let cases = [
            (
                ValidationOutcome::LowDifficulty,
                PoolBackendShareValidationError::LowDifficulty,
            ),
            (
                ValidationOutcome::InvalidEquihash,
                PoolBackendShareValidationError::InvalidEquihash,
            ),
            (
                ValidationOutcome::SubmittedTimeMismatch,
                PoolBackendShareValidationError::BackendUnavailable,
            ),
            (
                ValidationOutcome::InvariantFailure,
                PoolBackendShareValidationError::BackendUnavailable,
            ),
        ];
        let (time, nonce, solution, target) = proof_fields();

        for (outcome, expected) in cases {
            let backend = TestNativeBackend::new(CurrentOutcome::Current, outcome);
            assert_eq!(
                validate_retained_share(
                    &backend,
                    *time.as_bytes(),
                    nonce.as_bytes(),
                    solution.as_bytes(),
                    &target,
                ),
                Err(expected)
            );
        }
    }

    #[test]
    fn stale_or_unavailable_currentness_rejects_only_after_ordinary_validation() {
        let (time, nonce, solution, target) = proof_fields();
        for (current, expected) in [
            (
                CurrentOutcome::Stale,
                PoolBackendShareValidationError::StaleJob,
            ),
            (
                CurrentOutcome::DependencyUnavailable,
                PoolBackendShareValidationError::BackendUnavailable,
            ),
        ] {
            let backend = TestNativeBackend::new(
                current,
                ValidationOutcome::Winners {
                    wcash: None,
                    zcash: None,
                },
            );
            assert_eq!(
                validate_retained_share(
                    &backend,
                    *time.as_bytes(),
                    nonce.as_bytes(),
                    solution.as_bytes(),
                    &target,
                ),
                Err(expected)
            );
            assert_eq!(backend.validation_calls.load(Ordering::Relaxed), 1);
            assert_eq!(backend.currentness_calls.load(Ordering::Relaxed), 1);
            assert!(backend
                .observed
                .lock()
                .expect("test observation lock")
                .is_some());
        }
    }

    #[test]
    fn impossible_zero_wire_target_fails_as_backend_invariant() {
        let backend = TestNativeBackend::new(
            CurrentOutcome::Current,
            ValidationOutcome::Winners {
                wcash: None,
                zcash: None,
            },
        );
        let (time, nonce, solution, _) = proof_fields();
        assert_eq!(
            validate_retained_share(
                &backend,
                *time.as_bytes(),
                nonce.as_bytes(),
                solution.as_bytes(),
                &TargetLe::new([0; 32]),
            ),
            Err(PoolBackendShareValidationError::BackendUnavailable)
        );
        assert_eq!(backend.validation_calls.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn remaining_native_error_classes_never_become_miner_acceptance() {
        let stale_tip = MinerError::ParentTipMismatch {
            expected: "expected".to_string(),
            endpoint: "endpoint".to_string(),
            actual: "actual".to_string(),
        };
        assert_eq!(
            map_native_validation_error(stale_tip),
            PoolBackendShareValidationError::StaleJob
        );
        assert_eq!(
            map_native_validation_error(MinerError::InvalidNonceLength(0)),
            PoolBackendShareValidationError::BackendUnavailable
        );
        assert_eq!(
            map_native_validation_error(AuxPowError::ZeroTarget.into()),
            PoolBackendShareValidationError::BackendUnavailable
        );
    }
}

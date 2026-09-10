//! Strict conversion from proposal-validated native generations to pool jobs.

use std::cmp::Ordering;

use thiserror::Error;
use wcash_pool_protocol::{Hex108, Hex32, JobDescriptor, ProtocolError, TargetLe};

use crate::{NativeGenerationDescriptor, NativeWcashPayoutVerification};

pub use crate::{
    pool_backend_connection::BackendRequestKind,
    pool_backend_identity::{PoolBackendIdentity, PoolBackendIdentityError},
    pool_backend_journal::{
        JournalEventPage, JournalShareCommit, JournalWinnerBlocks, JournalWinnerLifecycle,
        JournalWinnerSummary, PoolBackendJournal, PoolBackendJournalError, JOURNAL_FORMAT_VERSION,
        MAX_JOURNAL_BYTES, MAX_JOURNAL_EVENTS, MAX_JOURNAL_RECORD_BYTES, MAX_WINNER_BLOCK_BYTES,
    },
    pool_backend_listener::{
        PoolBackendAuthority, PoolBackendConnectionOutcome, PoolBackendHandlerError,
        PoolBackendListener, PoolBackendListenerConfig, PoolBackendListenerError,
        PoolBackendRequestContext, PoolBackendRequestHandler, PoolBackendSession,
    },
    pool_backend_transport::BackendTransportError,
};

/// A native generation could not be represented by the backend-v1 protocol.
#[derive(Debug, Eq, Error, PartialEq)]
pub enum PoolBackendAdapterError {
    /// The Wcash reward recipient was not authenticated exactly.
    #[error("backend-v1 requires an exactly verified transparent Wcash payout recipient")]
    UnverifiedWcashPayout,

    /// The converted descriptor violated a backend-v1 protocol invariant.
    #[error("native generation is not a valid backend-v1 job descriptor: {0}")]
    InvalidJobDescriptor(#[source] ProtocolError),
}

/// Invalid operator or submitted share-target policy.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum PoolShareTargetPolicyError {
    /// A zero operator target cannot admit any share.
    #[error("operator easiest share target must be nonzero")]
    ZeroOperatorTarget,

    /// The Wcash network target was zero.
    #[error("Wcash network target must be nonzero")]
    ZeroWcashNetworkTarget,

    /// The Zcash network target was zero.
    #[error("Zcash network target must be nonzero")]
    ZeroZcashNetworkTarget,

    /// The configured ceiling is too hard to retain every network winner.
    #[error(
        "operator easiest target {configured:?} is harder than the required network target {required:?}"
    )]
    OperatorTargetExcludesNetworkWinner {
        /// The easier, numerically larger, of the two network targets.
        required: TargetLe,
        /// The invalid operator-configured easiest target.
        configured: TargetLe,
    },

    /// A zero submitted target cannot admit any share.
    #[error("submitted share target must be nonzero")]
    ZeroSubmittedTarget,

    /// The submitted target could reject a valid winner on one chain.
    #[error(
        "submitted share target {submitted:?} is harder than the required network target {required:?}"
    )]
    SubmittedTargetExcludesNetworkWinner {
        /// The invalid submitted target.
        submitted: TargetLe,
        /// The easier, numerically larger, of the two network targets.
        required: TargetLe,
    },

    /// The submitted target exceeds the operator's validation-work ceiling.
    #[error(
        "submitted share target {submitted:?} is easier than the operator ceiling {operator_easiest:?}"
    )]
    SubmittedTargetExceedsOperatorCeiling {
        /// The invalid submitted target.
        submitted: TargetLe,
        /// The operator-configured easiest accepted target.
        operator_easiest: TargetLe,
    },
}

/// Validated deployment-wide ceiling for miner share targets.
///
/// Numerically larger proof-of-work targets are easier. The configured target
/// therefore caps backend validation load, while each job supplies the lower
/// bound needed to retain every possible Wcash or Zcash network winner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PoolShareTargetPolicy {
    operator_easiest: TargetLe,
}

impl PoolShareTargetPolicy {
    /// Creates a policy from the operator-configured easiest accepted target.
    pub fn new(operator_easiest: TargetLe) -> Result<Self, PoolShareTargetPolicyError> {
        if operator_easiest.is_zero() {
            return Err(PoolShareTargetPolicyError::ZeroOperatorTarget);
        }

        Ok(Self { operator_easiest })
    }

    /// Returns the exact operator-configured easiest accepted target.
    pub const fn operator_easiest(&self) -> &TargetLe {
        &self.operator_easiest
    }

    /// Freezes safe target bounds for one exact backend job.
    pub fn bounds_for_job(
        &self,
        job: &JobDescriptor,
    ) -> Result<PoolShareTargetBounds, PoolShareTargetPolicyError> {
        if job.wcash_target_le.is_zero() {
            return Err(PoolShareTargetPolicyError::ZeroWcashNetworkTarget);
        }
        if job.zcash_target_le.is_zero() {
            return Err(PoolShareTargetPolicyError::ZeroZcashNetworkTarget);
        }

        // A hash winning the easier chain might not win the harder chain. The
        // lower bound must therefore be the numerically larger network target;
        // using the smaller target would silently discard some network winners.
        let required_network = easier_target(&job.wcash_target_le, &job.zcash_target_le).clone();
        if target_numeric_cmp(&self.operator_easiest, &required_network) == Ordering::Less {
            return Err(
                PoolShareTargetPolicyError::OperatorTargetExcludesNetworkWinner {
                    required: required_network,
                    configured: self.operator_easiest.clone(),
                },
            );
        }

        Ok(PoolShareTargetBounds {
            required_network,
            operator_easiest: self.operator_easiest.clone(),
        })
    }

    /// Checks a submitted target against one exact job before Equihash work.
    pub fn validate_for_job(
        &self,
        job: &JobDescriptor,
        submitted: &TargetLe,
    ) -> Result<(), PoolShareTargetPolicyError> {
        self.bounds_for_job(job)?.validate_submitted(submitted)
    }
}

/// Immutable safe share-target interval for one backend job.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PoolShareTargetBounds {
    required_network: TargetLe,
    operator_easiest: TargetLe,
}

impl PoolShareTargetBounds {
    /// Returns the easiest network target, the inclusive numeric lower bound.
    pub const fn required_network(&self) -> &TargetLe {
        &self.required_network
    }

    /// Returns the inclusive operator-configured easiest target.
    pub const fn operator_easiest(&self) -> &TargetLe {
        &self.operator_easiest
    }

    /// Checks a submitted target before any Equihash verification is attempted.
    pub fn validate_submitted(
        &self,
        submitted: &TargetLe,
    ) -> Result<(), PoolShareTargetPolicyError> {
        if submitted.is_zero() {
            return Err(PoolShareTargetPolicyError::ZeroSubmittedTarget);
        }
        if target_numeric_cmp(submitted, &self.required_network) == Ordering::Less {
            return Err(
                PoolShareTargetPolicyError::SubmittedTargetExcludesNetworkWinner {
                    submitted: submitted.clone(),
                    required: self.required_network.clone(),
                },
            );
        }
        if target_numeric_cmp(submitted, &self.operator_easiest) == Ordering::Greater {
            return Err(
                PoolShareTargetPolicyError::SubmittedTargetExceedsOperatorCeiling {
                    submitted: submitted.clone(),
                    operator_easiest: self.operator_easiest.clone(),
                },
            );
        }

        Ok(())
    }
}

fn easier_target<'a>(left: &'a TargetLe, right: &'a TargetLe) -> &'a TargetLe {
    if target_numeric_cmp(left, right) == Ordering::Less {
        right
    } else {
        left
    }
}

fn target_numeric_cmp(left: &TargetLe, right: &TargetLe) -> Ordering {
    left.as_bytes()
        .iter()
        .rev()
        .cmp(right.as_bytes().iter().rev())
}

/// Converts exact native generation metadata into a validated backend-v1 job.
///
/// This boundary rejects private Wcash payouts because their recipient is
/// trusted to the template node rather than authenticated from the candidate.
pub fn job_descriptor_from_native(
    native: &NativeGenerationDescriptor,
) -> Result<JobDescriptor, PoolBackendAdapterError> {
    match native.wcash_payout_verification() {
        NativeWcashPayoutVerification::ExactTransparentRecipient => {}
        NativeWcashPayoutVerification::TrustedPrivateTemplateNode => {
            return Err(PoolBackendAdapterError::UnverifiedWcashPayout);
        }
    }

    let descriptor = JobDescriptor {
        job_id: Hex32::new(native.job_id()),
        wcash_candidate_hash_le: Hex32::new(native.wcash_candidate_hash_le()),
        header_input: Hex108::new(*native.parent_header_input()),
        wcash_previous_hash_le: Hex32::new(native.wcash_previous_hash_le()),
        zcash_previous_hash_le: Hex32::new(native.zcash_previous_hash_le()),
        wcash_coinbase_txid_le: Hex32::new(native.wcash_coinbase_txid_le()),
        zcash_coinbase_txid_le: Hex32::new(native.zcash_coinbase_txid_le()),
        wcash_target_le: TargetLe::new(native.wcash_target_le()),
        zcash_target_le: TargetLe::new(native.zcash_target_le()),
        wcash_height: native.wcash_height(),
        zcash_height: native.zcash_height(),
        wcash_reward_zat: native.wcash_reward_zatoshis(),
        zcash_reward_zat: native.zcash_reward_zatoshis(),
        wcash_maturity_confirmations: native.wcash_maturity_confirmations(),
        zcash_maturity_confirmations: native.zcash_maturity_confirmations(),
        max_age_ms: native.max_age_milliseconds(),
    };

    descriptor
        .validate()
        .map_err(PoolBackendAdapterError::InvalidJobDescriptor)?;
    Ok(descriptor)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target_with_limbs(low: u8, high: u8) -> TargetLe {
        let mut bytes = [0; 32];
        bytes[0] = low;
        bytes[31] = high;
        TargetLe::new(bytes)
    }

    fn descriptor(wcash_target_le: TargetLe, zcash_target_le: TargetLe) -> JobDescriptor {
        JobDescriptor {
            job_id: Hex32::new([1; 32]),
            wcash_candidate_hash_le: Hex32::new([2; 32]),
            header_input: Hex108::new([3; 108]),
            wcash_previous_hash_le: Hex32::new([4; 32]),
            zcash_previous_hash_le: Hex32::new([5; 32]),
            wcash_coinbase_txid_le: Hex32::new([6; 32]),
            zcash_coinbase_txid_le: Hex32::new([7; 32]),
            wcash_target_le,
            zcash_target_le,
            wcash_height: 11,
            zcash_height: 22,
            wcash_reward_zat: 625_000_000,
            zcash_reward_zat: 312_500_000,
            wcash_maturity_confirmations: 100,
            zcash_maturity_confirmations: 100,
            max_age_ms: 45_000,
        }
    }

    #[test]
    fn policy_rejects_zero_operator_and_network_targets() {
        assert_eq!(
            PoolShareTargetPolicy::new(TargetLe::new([0; 32])),
            Err(PoolShareTargetPolicyError::ZeroOperatorTarget)
        );

        let policy = PoolShareTargetPolicy::new(TargetLe::new([0xff; 32])).unwrap();
        let zero_wcash = descriptor(TargetLe::new([0; 32]), target_with_limbs(1, 0));
        assert_eq!(
            policy.bounds_for_job(&zero_wcash),
            Err(PoolShareTargetPolicyError::ZeroWcashNetworkTarget)
        );
        let zero_zcash = descriptor(target_with_limbs(1, 0), TargetLe::new([0; 32]));
        assert_eq!(
            policy.bounds_for_job(&zero_zcash),
            Err(PoolShareTargetPolicyError::ZeroZcashNetworkTarget)
        );
    }

    #[test]
    fn little_endian_order_uses_the_easier_network_target() {
        // Lexicographic byte order would rank `low_limb` above `high_limb` due
        // to byte zero. Numeric little-endian order must inspect byte 31 first.
        let low_limb = target_with_limbs(0xff, 0);
        let high_limb = target_with_limbs(0, 1);
        assert_eq!(target_numeric_cmp(&low_limb, &high_limb), Ordering::Less);

        let operator_easiest = target_with_limbs(0, 2);
        let policy = PoolShareTargetPolicy::new(operator_easiest.clone()).unwrap();
        let job = descriptor(low_limb.clone(), high_limb.clone());
        let bounds = policy.bounds_for_job(&job).unwrap();

        assert_eq!(bounds.required_network(), &high_limb);
        assert_eq!(bounds.operator_easiest(), &operator_easiest);
        assert_eq!(bounds.validate_submitted(&high_limb), Ok(()));
        assert_eq!(bounds.validate_submitted(&operator_easiest), Ok(()));

        assert!(matches!(
            bounds.validate_submitted(&low_limb),
            Err(PoolShareTargetPolicyError::SubmittedTargetExcludesNetworkWinner {
                submitted,
                required,
            }) if submitted == low_limb && required == high_limb
        ));

        let above_ceiling = target_with_limbs(0, 3);
        assert!(matches!(
            bounds.validate_submitted(&above_ceiling),
            Err(PoolShareTargetPolicyError::SubmittedTargetExceedsOperatorCeiling {
                submitted,
                operator_easiest: ceiling,
            }) if submitted == above_ceiling && ceiling == operator_easiest
        ));
        assert_eq!(
            bounds.validate_submitted(&TargetLe::new([0; 32])),
            Err(PoolShareTargetPolicyError::ZeroSubmittedTarget)
        );
    }

    #[test]
    fn operator_ceiling_cannot_exclude_a_network_winner() {
        let harder_network = target_with_limbs(0xff, 0);
        let easier_network = target_with_limbs(0, 1);
        let policy = PoolShareTargetPolicy::new(harder_network.clone()).unwrap();
        let job = descriptor(harder_network.clone(), easier_network.clone());

        assert!(matches!(
            policy.bounds_for_job(&job),
            Err(PoolShareTargetPolicyError::OperatorTargetExcludesNetworkWinner {
                required,
                configured,
            }) if required == easier_network && configured == harder_network
        ));
    }
}

//! Strict conversion from proposal-validated native generations to pool jobs.

use thiserror::Error;
use wcash_pool_protocol::{Hex108, Hex32, JobDescriptor, ProtocolError, TargetLe};

use crate::{NativeGenerationDescriptor, NativeWcashPayoutVerification};

pub use crate::{
    pool_backend_connection::BackendRequestKind,
    pool_backend_identity::{PoolBackendIdentity, PoolBackendIdentityError},
    pool_backend_journal::{
        JournalEventPage, PoolBackendJournal, PoolBackendJournalError, JOURNAL_FORMAT_VERSION,
        MAX_JOURNAL_BYTES, MAX_JOURNAL_EVENTS, MAX_JOURNAL_RECORD_BYTES,
    },
    pool_backend_listener::{
        PoolBackendAuthority, PoolBackendConnectionOutcome, PoolBackendHandlerError,
        PoolBackendListener, PoolBackendListenerConfig, PoolBackendListenerError,
        PoolBackendRequestHandler, PoolBackendSession,
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

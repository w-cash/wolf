//! Local Wcash-to-Zcash merged-mining harness.
//!
//! This crate constructs a canonical NU6.3 Zcash coinbase transaction carrying
//! the Wcash AuxPoW commitment, creates a one-transaction parent block template,
//! and either solves or validates real Equihash `(200, 9)` work. It does not
//! relax the production verifier in `wcash-zcash-aux`.
//!
//! The included JSON-lines server is intentionally a loopback development
//! interface. A production pool needs a modified parent template producer or a
//! complete coinbase builder that includes the commitment before shielded
//! proving and authorization, preserves every required parent-chain payout,
//! recomputes all affected block commitments, and submits winning parent blocks
//! to a Zcash node independently.

#![forbid(unsafe_code)]

pub mod accounting;
mod coinbase;
mod coordinator;
mod error;
mod job;
pub mod native;
pub mod protocol;
pub mod rpc;
pub mod zip301;
pub mod zip301_client;

/// Maximum time one native generation can remain available to miners.
pub const NATIVE_JOB_MAX_AGE_SECONDS: u64 = 45;

/// Millisecond representation of [`NATIVE_JOB_MAX_AGE_SECONDS`] for pool protocols.
pub const NATIVE_JOB_MAX_AGE_MILLISECONDS: u32 = 45_000;

/// Confirmation depth applied to native Wcash and Zcash coinbase rewards.
pub const NATIVE_COINBASE_MATURITY_CONFIRMATIONS: u32 =
    zcash_protocol::consensus::COINBASE_MATURITY_BLOCKS;

pub use coinbase::{build_parent_coinbase, ParentOutput};
pub use coordinator::{
    CoordinatorConfig, GenerationRetirement, NativeMiningCoordinator, NativeMiningSupervisor,
    WinnerOutboxStatus,
};
pub use error::MinerError;
pub use job::{JobConfig, PreparedJob, SolvedAuxPow, EQUIHASH_SOLUTION_BYTES};
pub use native::{
    NativeGenerationDescriptor, NativePreparedJob, NativeWcashPayoutVerification,
    NativeZcashConfig, NativeZcashProvider, ParentNodeOutcome, ParentSubmissionReport,
    ValidatedNativeShare,
};
pub use zip301::{serve_zip301_loopback, ShareProcessor, Zip301Config, Zip301LoopbackListener};
pub use zip301_client::{mine_zip301_once, Zip301AcceptedShare, Zip301ClientConfig};

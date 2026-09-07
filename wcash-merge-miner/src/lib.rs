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

mod coinbase;
mod coordinator;
mod error;
mod job;
pub mod native;
pub mod protocol;
pub mod rpc;
pub mod zip301;

pub use coinbase::{build_parent_coinbase, ParentOutput};
pub use coordinator::{CoordinatorConfig, NativeMiningCoordinator, WinnerOutboxStatus};
pub use error::MinerError;
pub use job::{JobConfig, PreparedJob, SolvedAuxPow, EQUIHASH_SOLUTION_BYTES};
pub use native::{
    NativePreparedJob, NativeZcashConfig, NativeZcashProvider, ParentNodeOutcome,
    ParentSubmissionReport, ValidatedNativeShare,
};
pub use zip301::{serve_zip301_loopback, ShareProcessor, Zip301Config};

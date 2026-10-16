//! Persisted coverage of the Wcash foundation consensus rule.
//!
//! Legacy executables can preserve unknown database columns while appending blocks. Coverage
//! therefore identifies an exact chain tip (or backup record), rather than an upgrade flag.

use zebra_chain::{
    block::{Hash, Height},
    parameters::{
        subsidy::{
            wcash_foundation_activation_height, wcash_foundation_expiry_height,
            WCASH_FOUNDATION_DENOMINATOR, WCASH_FOUNDATION_NUMERATOR,
        },
        Network,
    },
};

/// Identifies the consensus rule, including its fixed recipient and matching/accounting rules.
/// Increment this identifier whenever any of those rules change.
const RULE_VERSION: &[u8] = b"wcash-foundation-fund-validation-v2\0";

/// Exact coverage bytes binding the allocation ratio and history, including after expiry.
// A missing paired expiry is a programming error in fixed consensus parameters.
// Returning None would silently exempt affected history, so this must fail closed.
#[allow(clippy::unwrap_in_result)]
pub(super) fn coverage(network: &Network, height: Height, hash: Hash) -> Option<Vec<u8>> {
    let activation = wcash_foundation_activation_height(network)?;
    if height < activation {
        return None;
    }

    let expiry = wcash_foundation_expiry_height(network)
        .expect("foundation activation and expiry are defined together");
    let mut bytes = RULE_VERSION.to_vec();
    bytes.extend_from_slice(&WCASH_FOUNDATION_NUMERATOR.to_be_bytes());
    bytes.extend_from_slice(&WCASH_FOUNDATION_DENOMINATOR.to_be_bytes());
    bytes.extend_from_slice(&activation.0.to_be_bytes());
    bytes.extend_from_slice(&expiry.0.to_be_bytes());
    bytes.extend_from_slice(&height.0.to_be_bytes());
    bytes.extend_from_slice(&hash.0);
    Some(bytes)
}

//! Equihash Solution and related items.

use std::{fmt, io};

use hex::{FromHex, FromHexError, ToHex};
use serde::{de::Error as _, Deserialize, Deserializer, Serialize, Serializer};
use serde_big_array::BigArray;

use crate::{
    block::Header,
    parameters::Network,
    serialization::{
        zcash_deserialize_bytes_external_count, zcash_serialize_bytes, CompactSizeMessage,
        SerializationError, ZcashDeserialize, ZcashDeserializeInto, ZcashSerialize,
    },
};

#[cfg(feature = "internal-miner")]
use crate::serialization::AtLeastOne;

/// The error type for Equihash validation.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The Equihash solver rejected a Zcash solution.
    #[error("invalid equihash solution for BlockHeader")]
    Equihash(#[from] equihash::Error),

    /// A Wcash AuxPoW witness was passed to the native Zcash verifier.
    #[error("Wcash blocks require AuxPoW validation, not native Equihash validation")]
    WcashAuxPow,
}

/// The error type for Equihash solving.
#[derive(Copy, Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("solver was cancelled")]
pub struct SolverCancelled;

/// The size of an Equihash solution in bytes (always 1344).
pub(crate) const SOLUTION_SIZE: usize = 1344;

/// The size of an Equihash solution in bytes on Regtest (always 36).
pub(crate) const REGTEST_SOLUTION_SIZE: usize = 36;

/// The dedicated Wcash block-header wire version.
///
/// The lower 31 bits retain the `WC` marker and format version. The high bit is
/// deliberately set because native Zcash consensus forbids it, making this
/// variable-length header format disjoint from every valid Zcash header.
pub const WCASH_BLOCK_WIRE_VERSION: u32 = 0xd743_0001;

/// Maximum serialized Zcash-parent AuxPoW witness carried by a Wcash header.
///
/// Profile-specific decoding applies tighter component limits before doing
/// cryptographic work. This outer limit exists to bound allocation at the
/// network boundary.
pub const MAX_WCASH_AUXPOW_BYTES: usize = 256 * 1024;

const fn compact_size_len(value: usize) -> usize {
    match value {
        0..=252 => 1,
        253..=65_535 => 3,
        _ => 5,
    }
}

/// A bounded opaque Wcash AuxPoW witness.
///
/// Consensus code must decode these bytes with the frozen
/// `wcash-zcash-aux` proof parser. An empty witness is reserved for genesis
/// and block-template proposals.
#[derive(Clone, Eq, PartialEq)]
pub struct WcashSolution(Box<[u8]>);

impl WcashSolution {
    /// Constructs a bounded Wcash witness.
    pub fn new(bytes: impl Into<Box<[u8]>>) -> Result<Self, SerializationError> {
        let bytes = bytes.into();
        if bytes.len() > MAX_WCASH_AUXPOW_BYTES {
            return Err(SerializationError::Parse(
                "Wcash AuxPoW witness exceeds the consensus size limit",
            ));
        }

        Ok(Self(bytes))
    }

    /// Returns the canonical proof bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Returns true for the genesis/proposal placeholder witness.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for WcashSolution {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WcashAuxPow")
            .field("proof_bytes", &self.0.len())
            .finish()
    }
}

impl Serialize for WcashSolution {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(self.as_bytes())
    }
}

impl<'de> Deserialize<'de> for WcashSolution {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes = Vec::<u8>::deserialize(deserializer)?;
        Self::new(bytes).map_err(D::Error::custom)
    }
}

/// Equihash Solution in compressed format.
///
/// A wrapper around `[u8; n]` where `n` is the solution size because
/// Rust doesn't implement common traits like `Debug`, `Clone`, etc.
/// for collections like arrays beyond lengths 0 to 32.
///
/// The size of an Equihash solution in bytes is always 1344 on Mainnet and Testnet, and
/// is always 36 on Regtest so the length of this type is fixed.
#[derive(Clone, Deserialize, Serialize)]
// It's okay to use the extra space on Regtest
#[allow(clippy::large_enum_variant)]
pub enum Solution {
    /// Equihash solution on Mainnet or Testnet
    Common(#[serde(with = "BigArray")] [u8; SOLUTION_SIZE]),
    /// Equihash solution on Regtest
    Regtest(#[serde(with = "BigArray")] [u8; REGTEST_SOLUTION_SIZE]),
    /// A Zcash-parent AuxPoW witness on a Wcash network.
    Wcash(WcashSolution),
}

impl Solution {
    /// The length of the portion of the header used as input when verifying
    /// equihash solutions, in bytes.
    ///
    /// Excludes the 32-byte nonce, which is passed as a separate argument
    /// to the verification function.
    pub const INPUT_LENGTH: usize = 4 + 32 * 3 + 4 * 2;

    /// Returns the inner value of the [`Solution`] as a byte slice.
    fn value(&self) -> &[u8] {
        match self {
            Solution::Common(solution) => solution.as_slice(),
            Solution::Regtest(solution) => solution.as_slice(),
            Solution::Wcash(solution) => solution.as_bytes(),
        }
    }

    /// Returns `Ok(())` if `EquihashSolution` is valid for `header`
    #[allow(clippy::unwrap_in_result)]
    pub fn check(&self, header: &Header) -> Result<(), Error> {
        if matches!(self, Self::Wcash(_)) {
            return Err(Error::WcashAuxPow);
        }

        // TODO:
        // - Add Equihash parameters field to `testnet::Parameters`
        // - Update `Solution::Regtest` variant to hold a `Vec` to support arbitrary parameters - rename to `Other`
        let n = 200;
        let k = 9;
        let nonce = &header.nonce;

        let mut input = Vec::new();
        header
            .zcash_serialize(&mut input)
            .expect("serialization into a vec can't fail");

        // The part of the header before the nonce and solution.
        // This data is kept constant during solver runs, so the verifier API takes it separately.
        let input = &input[0..Solution::INPUT_LENGTH];

        equihash::is_valid_solution(n, k, input, nonce.as_ref(), self.value())?;

        Ok(())
    }

    /// Returns a [`Solution`] containing the bytes from `solution`.
    /// Returns an error if `solution` is the wrong length.
    pub fn from_bytes(solution: &[u8]) -> Result<Self, SerializationError> {
        match solution.len() {
            // Won't panic, because we just checked the length.
            SOLUTION_SIZE => {
                let mut bytes = [0; SOLUTION_SIZE];
                bytes.copy_from_slice(solution);
                Ok(Self::Common(bytes))
            }
            REGTEST_SOLUTION_SIZE => {
                let mut bytes = [0; REGTEST_SOLUTION_SIZE];
                bytes.copy_from_slice(solution);
                Ok(Self::Regtest(bytes))
            }
            _unexpected_len => Err(SerializationError::Parse(
                "incorrect equihash solution size",
            )),
        }
    }

    /// Constructs a bounded Wcash AuxPoW witness.
    pub fn for_wcash(bytes: impl Into<Box<[u8]>>) -> Result<Self, SerializationError> {
        WcashSolution::new(bytes).map(Self::Wcash)
    }

    /// Returns the Wcash witness when this is a Wcash header solution.
    pub const fn as_wcash(&self) -> Option<&WcashSolution> {
        match self {
            Self::Wcash(solution) => Some(solution),
            Self::Common(_) | Self::Regtest(_) => None,
        }
    }

    pub(crate) fn zcash_deserialize_for_version<R: io::Read>(
        mut reader: R,
        header_version: u32,
    ) -> Result<Self, SerializationError> {
        let len: CompactSizeMessage = (&mut reader).zcash_deserialize_into()?;
        let len: usize = len.into();
        let maximum = if header_version == WCASH_BLOCK_WIRE_VERSION {
            MAX_WCASH_AUXPOW_BYTES
        } else {
            SOLUTION_SIZE
        };

        if len > maximum {
            return Err(SerializationError::Parse(
                "proof solution exceeds its version-specific size limit",
            ));
        }

        let solution = zcash_deserialize_bytes_external_count(len, &mut reader)?;
        if header_version == WCASH_BLOCK_WIRE_VERSION {
            Self::for_wcash(solution)
        } else {
            Self::from_bytes(&solution)
        }
    }

    /// The serialized size of a solution on Mainnet and Testnet (except Regtest), in bytes:
    /// the 1344-byte solution and its 3-byte CompactSize length prefix (`0xfd` + `u16`).
    pub const SERIALIZED_SIZE: usize = 3 + SOLUTION_SIZE;

    /// The serialized size of a solution on Regtest, in bytes:
    /// the 36-byte solution and its 1-byte CompactSize length prefix.
    pub const REGTEST_SERIALIZED_SIZE: usize = 1 + REGTEST_SOLUTION_SIZE;

    /// Minimum serialized Wcash solution size: an empty CompactSize-prefixed witness.
    pub const WCASH_MIN_SERIALIZED_SIZE: usize = 1;

    /// Maximum serialized Wcash solution size, including its CompactSize prefix.
    pub const WCASH_MAX_SERIALIZED_SIZE: usize =
        compact_size_len(MAX_WCASH_AUXPOW_BYTES) + MAX_WCASH_AUXPOW_BYTES;

    /// Returns this solution's exact serialized size, including its length prefix.
    pub fn serialized_len(&self) -> usize {
        compact_size_len(self.value().len()) + self.value().len()
    }

    /// Returns the size reserved for a serialized solution on `network`, in
    /// bytes, including its CompactSize length prefix.
    ///
    /// Native Zcash solutions have a fixed size. Wcash reserves the maximum
    /// AuxPoW witness size so a block template cannot become oversized when a
    /// miner attaches its proof.
    pub fn serialized_size(network: &Network) -> usize {
        if network.uses_wcash_consensus() {
            Self::WCASH_MAX_SERIALIZED_SIZE
        } else if network.is_regtest() {
            Self::REGTEST_SERIALIZED_SIZE
        } else {
            Self::SERIALIZED_SIZE
        }
    }

    /// Returns a [`Solution`] of `[0; SOLUTION_SIZE]` to be used in block proposals.
    pub fn for_proposal() -> Self {
        // TODO: Accept network as an argument, and if it's Regtest, return the shorter null solution.
        Self::Common([0; SOLUTION_SIZE])
    }

    /// Returns the placeholder solution used in a block template for
    /// `network`.
    pub fn for_proposal_on(network: &Network) -> Self {
        if network.uses_wcash_consensus() {
            Self::for_wcash(Vec::new()).expect("an empty Wcash witness is within the size limit")
        } else if network.is_regtest() {
            Self::Regtest([0; REGTEST_SOLUTION_SIZE])
        } else {
            Self::for_proposal()
        }
    }

    /// Mines and returns one or more [`Solution`]s based on a template `header`.
    /// The returned header contains a valid `nonce` and `solution`.
    ///
    /// If `cancel_fn()` returns an error, returns early with `Err(SolverCancelled)`.
    ///
    /// The `nonce` in the header template is taken as the starting nonce. If you are running multiple
    /// solvers at the same time, start them with different nonces.
    /// The `solution` in the header template is ignored.
    ///
    /// This method is CPU and memory-intensive. It uses 144 MB of RAM and one CPU core while running.
    /// It can run for minutes or hours if the network difficulty is high.
    #[cfg(feature = "internal-miner")]
    #[allow(clippy::unwrap_in_result)]
    pub fn solve<F>(
        mut header: Header,
        mut cancel_fn: F,
    ) -> Result<AtLeastOne<Header>, SolverCancelled>
    where
        F: FnMut() -> Result<(), SolverCancelled>,
    {
        use crate::shutdown::is_shutting_down;

        let mut input = Vec::new();
        header
            .zcash_serialize(&mut input)
            .expect("serialization into a vec can't fail");
        // Take the part of the header before the nonce and solution.
        // This data is kept constant for this solver run.
        let input = &input[0..Solution::INPUT_LENGTH];

        while !is_shutting_down() {
            // Don't run the solver if we'd just cancel it anyway.
            cancel_fn()?;

            let solutions = equihash::tromp::solve_200_9(input, || {
                // Cancel the solver if we have a new template.
                if cancel_fn().is_err() {
                    return None;
                }

                // This skips the first nonce, which doesn't matter in practice.
                Self::next_nonce(&mut header.nonce);
                Some(*header.nonce)
            });

            let mut valid_solutions = Vec::new();

            for solution in &solutions {
                header.solution = Self::from_bytes(solution)
                    .expect("unexpected invalid solution: incorrect length");

                // TODO: work out why we sometimes get invalid solutions here
                if let Err(error) = header.solution.check(&header) {
                    info!(?error, "found invalid solution for header");
                    continue;
                }

                if Self::difficulty_is_valid(&header) {
                    valid_solutions.push(header.clone());
                }
            }

            match valid_solutions.try_into() {
                Ok(at_least_one_solution) => return Ok(at_least_one_solution),
                Err(_is_empty_error) => debug!(
                    solutions = ?solutions.len(),
                    "found valid solutions which did not pass the validity or difficulty checks"
                ),
            }
        }

        Err(SolverCancelled)
    }

    /// Returns `true` if the `nonce` and `solution` in `header` meet the difficulty threshold.
    ///
    /// # Panics
    ///
    /// - If `header` contains an invalid difficulty threshold.
    #[cfg(feature = "internal-miner")]
    fn difficulty_is_valid(header: &Header) -> bool {
        // Simplified from zebra_consensus::block::check::difficulty_is_valid().
        let difficulty_threshold = header
            .difficulty_threshold
            .to_expanded()
            .expect("unexpected invalid header template: invalid difficulty threshold");

        // TODO: avoid calculating this hash multiple times
        let hash = header.hash();

        // Note: this comparison is a u256 integer comparison, like zcashd and bitcoin. Greater
        // values represent *less* work.
        hash <= difficulty_threshold
    }

    /// Modifies `nonce` to be the next integer in big-endian order.
    /// Wraps to zero if the next nonce would overflow.
    #[cfg(feature = "internal-miner")]
    fn next_nonce(nonce: &mut [u8; 32]) {
        let _ignore_overflow = crate::primitives::byte_array::increment_big_endian(&mut nonce[..]);
    }
}

impl PartialEq<Solution> for Solution {
    fn eq(&self, other: &Solution) -> bool {
        match (self, other) {
            (Self::Common(left), Self::Common(right)) => left == right,
            (Self::Regtest(left), Self::Regtest(right)) => left == right,
            (Self::Wcash(left), Self::Wcash(right)) => left == right,
            _ => false,
        }
    }
}

impl fmt::Debug for Solution {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Wcash(solution) => solution.fmt(f),
            Self::Common(_) | Self::Regtest(_) => f
                .debug_tuple("EquihashSolution")
                .field(&hex::encode(self.value()))
                .finish(),
        }
    }
}

impl Eq for Solution {}

#[cfg(any(test, feature = "proptest-impl"))]
impl Default for Solution {
    fn default() -> Self {
        Self::Common([0; SOLUTION_SIZE])
    }
}

impl ZcashSerialize for Solution {
    fn zcash_serialize<W: io::Write>(&self, writer: W) -> Result<(), io::Error> {
        zcash_serialize_bytes(&self.value().to_vec(), writer)
    }
}

impl ZcashDeserialize for Solution {
    fn zcash_deserialize<R: io::Read>(reader: R) -> Result<Self, SerializationError> {
        Self::zcash_deserialize_for_version(reader, crate::block::ZCASH_BLOCK_VERSION)
    }
}

impl ToHex for &Solution {
    fn encode_hex<T: FromIterator<char>>(&self) -> T {
        self.value().encode_hex()
    }

    fn encode_hex_upper<T: FromIterator<char>>(&self) -> T {
        self.value().encode_hex_upper()
    }
}

impl ToHex for Solution {
    fn encode_hex<T: FromIterator<char>>(&self) -> T {
        (&self).encode_hex()
    }

    fn encode_hex_upper<T: FromIterator<char>>(&self) -> T {
        (&self).encode_hex_upper()
    }
}

impl FromHex for Solution {
    type Error = FromHexError;

    fn from_hex<T: AsRef<[u8]>>(hex: T) -> Result<Self, Self::Error> {
        let bytes = Vec::from_hex(hex)?;
        Solution::from_bytes(&bytes).map_err(|_| FromHexError::InvalidStringLength)
    }
}

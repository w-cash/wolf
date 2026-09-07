//! Deterministic identity and Bitcoin-anchored genesis data for Wcash.
//!
//! Public-network anchors are deliberately unavailable until their Bitcoin
//! blocks have been mined, confirmed, independently checked, and frozen in a
//! reviewed release. Consensus code never fetches data from the network.

#![forbid(unsafe_code)]

use std::{error::Error, fmt, str::FromStr};

use blake2b_simd::Params;
use sha2::{Digest, Sha256};

/// Human-readable project name.
pub const PROJECT_NAME: &str = "Wcash";

/// Currency ticker used by Wcash software.
pub const CURRENCY_TICKER: &str = "WCASH";

/// Canonical configuration-file name.
pub const CONFIG_FILE_NAME: &str = "wcash.toml";

/// Canonical application data-directory name.
pub const DATA_DIRECTORY_NAME: &str = "wcash";

/// Prefix used by Wcash peer user-agent strings.
pub const USER_AGENT_PREFIX: &str = "/Wcash:";

/// Human-readable timestamp prefix embedded in every Wcash genesis statement.
pub const GENESIS_TIMESTAMP_TEXT: &str = "06/Sep/2026 Wcash";

/// Bitcoin mainnet height announced as the future Wcash mainnet anchor.
pub const REQUESTED_MAINNET_BITCOIN_HEIGHT: u32 = 965_954;

/// Number of confirmations required by the release procedure before freezing a public anchor.
pub const MIN_PUBLIC_ANCHOR_CONFIRMATIONS: u32 = 100;

/// Version of the fixed-width Bitcoin anchor encoding.
pub const ANCHOR_ENCODING_VERSION: u8 = 1;

/// Length of a canonical encoded Bitcoin anchor.
pub const ANCHOR_ENCODING_LEN: usize = 40;

/// `BLAKE2b` personalization used for Wcash genesis anchor commitments.
pub const ANCHOR_PERSONALIZATION: &[u8; 16] = b"WcashBtcAnchorV1";

const BITCOIN_MAINNET_SOURCE_ID: u8 = 0;
const RESERVED_BYTE: u8 = 0;
const BITCOIN_HEADER_LEN: usize = 80;
#[cfg(test)]
const BITCOIN_MAINNET_POW_LIMIT_BITS: u32 = 0x1d00_ffff;
const BITCOIN_MAINNET_POW_LIMIT: [u8; 32] = [
    0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
];

/// A Wcash network with its own identity and genesis block.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(u8)]
pub enum WcashNetwork {
    /// Wcash production network.
    Mainnet = 0,
    /// Wcash public testing network.
    Testnet = 1,
    /// Process-local Wcash regression-test network.
    Regtest = 2,
}

impl WcashNetwork {
    /// Returns the canonical lowercase network name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Mainnet => "mainnet",
            Self::Testnet => "testnet",
            Self::Regtest => "regtest",
        }
    }
}

impl fmt::Display for WcashNetwork {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for WcashNetwork {
    type Err = NetworkParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "mainnet" => Ok(Self::Mainnet),
            "testnet" => Ok(Self::Testnet),
            "regtest" => Ok(Self::Regtest),
            _ => Err(NetworkParseError),
        }
    }
}

impl TryFrom<u8> for WcashNetwork {
    type Error = AnchorDecodeError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Mainnet),
            1 => Ok(Self::Testnet),
            2 => Ok(Self::Regtest),
            other => Err(AnchorDecodeError::UnknownWcashNetwork(other)),
        }
    }
}

/// Error returned when a Wcash network name is unknown.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NetworkParseError;

impl fmt::Display for NetworkParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("network must be mainnet, testnet, or regtest")
    }
}

impl Error for NetworkParseError {}

/// Consensus-facing Wcash network identity values.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NetworkIdentity {
    network: WcashNetwork,
    domain_label: &'static str,
    p2p_magic: [u8; 4],
}

impl NetworkIdentity {
    /// Returns this identity's network.
    #[must_use]
    pub const fn network(self) -> WcashNetwork {
        self.network
    }

    /// Returns the ASCII domain label from which the P2P magic was derived.
    #[must_use]
    pub const fn domain_label(self) -> &'static str {
        self.domain_label
    }

    /// Returns the first four SHA-256 bytes of the domain label.
    #[must_use]
    pub const fn p2p_magic(self) -> [u8; 4] {
        self.p2p_magic
    }
}

/// Returns the frozen Wcash identity for a network.
#[must_use]
pub const fn network_identity(network: WcashNetwork) -> NetworkIdentity {
    match network {
        WcashNetwork::Mainnet => NetworkIdentity {
            network,
            domain_label: "Wcash/mainnet/v1",
            p2p_magic: [0xc1, 0xe0, 0xdc, 0x1b],
        },
        WcashNetwork::Testnet => NetworkIdentity {
            network,
            domain_label: "Wcash/testnet/v1",
            p2p_magic: [0x69, 0xc7, 0x5f, 0xba],
        },
        WcashNetwork::Regtest => NetworkIdentity {
            network,
            domain_label: "Wcash/regtest/v1",
            p2p_magic: [0x99, 0xcc, 0xd2, 0x65],
        },
    }
}

/// A Bitcoin block hash stored in raw `SHA256d(header)` byte order.
///
/// Bitcoin explorers conventionally display these bytes in reverse order.
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct BitcoinBlockHash([u8; 32]);

impl BitcoinBlockHash {
    /// Constructs a hash from raw `SHA256d(header)` bytes.
    #[must_use]
    pub const fn from_raw_digest(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the raw `SHA256d(header)` bytes committed by the anchor.
    #[must_use]
    pub const fn raw_digest(self) -> [u8; 32] {
        self.0
    }
}

impl fmt::Debug for BitcoinBlockHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "BitcoinBlockHash({self})")
    }
}

impl fmt::Display for BitcoinBlockHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0.iter().rev() {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl FromStr for BitcoinBlockHash {
    type Err = HexParseError;

    fn from_str(display_hash: &str) -> Result<Self, Self::Err> {
        let mut display_bytes = parse_hex_array::<32>(display_hash)?;
        display_bytes.reverse();
        Ok(Self(display_bytes))
    }
}

/// An exact 80-byte Bitcoin block header.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct BitcoinHeader([u8; BITCOIN_HEADER_LEN]);

impl BitcoinHeader {
    /// Constructs a header from its exact Bitcoin wire encoding.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; BITCOIN_HEADER_LEN]) -> Self {
        Self(bytes)
    }

    /// Returns the exact Bitcoin wire encoding.
    #[must_use]
    pub const fn bytes(self) -> [u8; BITCOIN_HEADER_LEN] {
        self.0
    }

    /// Computes this header's raw double-SHA-256 digest.
    #[must_use]
    pub fn block_hash(self) -> BitcoinBlockHash {
        let first = Sha256::digest(self.0);
        BitcoinBlockHash::from_raw_digest(Sha256::digest(first).into())
    }

    /// Returns the compact target field in host byte order.
    #[must_use]
    pub fn bits(self) -> u32 {
        let mut bytes = [0_u8; 4];
        bytes.copy_from_slice(&self.0[72..76]);
        u32::from_le_bytes(bytes)
    }

    /// Checks the header hash against a structurally valid Bitcoin-mainnet target.
    ///
    /// This proves only that the isolated header has valid work. It does not
    /// prove the claimed height, best-chain membership, or confirmations.
    ///
    /// # Errors
    ///
    /// Returns an error if `nBits` is malformed, easier than the Bitcoin
    /// mainnet proof-of-work limit, or the header hash is above its target.
    pub fn validate_isolated_mainnet_work(self) -> Result<(), BitcoinPowError> {
        let target = decode_compact_target(self.bits())?;
        if target > BITCOIN_MAINNET_POW_LIMIT {
            return Err(BitcoinPowError::TargetAboveMainnetLimit);
        }

        let mut hash = self.block_hash().raw_digest();
        hash.reverse();
        if hash > target {
            return Err(BitcoinPowError::HashAboveTarget);
        }

        Ok(())
    }
}

impl fmt::Debug for BitcoinHeader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("BitcoinHeader")
            .field(&self.block_hash())
            .finish()
    }
}

impl FromStr for BitcoinHeader {
    type Err = HexParseError;

    fn from_str(header_hex: &str) -> Result<Self, Self::Err> {
        Ok(Self(parse_hex_array(header_hex)?))
    }
}

/// Error returned when fixed-width hexadecimal input is malformed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HexParseError {
    /// The input did not have the required number of hexadecimal characters.
    InvalidLength {
        /// Actual number of bytes in the input string.
        actual: usize,
        /// Required number of bytes in the input string.
        expected: usize,
    },
    /// The input contained a non-hexadecimal byte.
    InvalidHex {
        /// Zero-based byte index in the input string.
        index: usize,
    },
}

impl fmt::Display for HexParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLength { actual, expected } => {
                write!(
                    formatter,
                    "hex input has length {actual}, expected {expected}"
                )
            }
            Self::InvalidHex { index } => {
                write!(formatter, "hex input has invalid character at byte {index}")
            }
        }
    }
}

impl Error for HexParseError {}

/// Error returned by isolated Bitcoin proof-of-work validation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BitcoinPowError {
    /// The compact target's sign bit was set.
    NegativeTarget,
    /// The compact target represented zero.
    ZeroTarget,
    /// The compact target overflowed 256 bits.
    TargetOverflow,
    /// The compact target was easier than Bitcoin mainnet permits.
    TargetAboveMainnetLimit,
    /// The header hash did not satisfy the encoded target.
    HashAboveTarget,
}

impl fmt::Display for BitcoinPowError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::NegativeTarget => "Bitcoin compact target is negative",
            Self::ZeroTarget => "Bitcoin compact target is zero",
            Self::TargetOverflow => "Bitcoin compact target overflows 256 bits",
            Self::TargetAboveMainnetLimit => {
                "Bitcoin target is above the mainnet proof-of-work limit"
            }
            Self::HashAboveTarget => "Bitcoin header hash is above its target",
        };
        formatter.write_str(message)
    }
}

impl Error for BitcoinPowError {}

/// A fixed-width Bitcoin block anchor for one Wcash network.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BitcoinAnchor {
    wcash_network: WcashNetwork,
    bitcoin_height: u32,
    bitcoin_block_hash: BitcoinBlockHash,
}

impl BitcoinAnchor {
    const fn new(
        wcash_network: WcashNetwork,
        bitcoin_height: u32,
        bitcoin_block_hash: BitcoinBlockHash,
    ) -> Self {
        Self {
            wcash_network,
            bitcoin_height,
            bitcoin_block_hash,
        }
    }

    /// Constructs a reviewed anchor from explicit fields.
    ///
    /// This API is intentionally restricted to this crate. Public-network
    /// activation requires changing the frozen constants in a reviewed build.
    const fn frozen(
        wcash_network: WcashNetwork,
        bitcoin_height: u32,
        raw_block_hash: [u8; 32],
    ) -> Self {
        Self::new(
            wcash_network,
            bitcoin_height,
            BitcoinBlockHash::from_raw_digest(raw_block_hash),
        )
    }

    /// Returns the target Wcash network.
    #[must_use]
    pub const fn wcash_network(self) -> WcashNetwork {
        self.wcash_network
    }

    /// Returns the anchored Bitcoin mainnet height.
    #[must_use]
    pub const fn bitcoin_height(self) -> u32 {
        self.bitcoin_height
    }

    /// Returns the anchored Bitcoin block hash.
    #[must_use]
    pub const fn bitcoin_block_hash(self) -> BitcoinBlockHash {
        self.bitcoin_block_hash
    }

    /// Encodes this anchor in its canonical 40-byte representation.
    ///
    /// The layout is encoding version, Wcash network, Bitcoin source network,
    /// reserved byte, little-endian height, and raw double-SHA-256 bytes.
    #[must_use]
    pub fn encode(self) -> [u8; ANCHOR_ENCODING_LEN] {
        let mut encoding = [0_u8; ANCHOR_ENCODING_LEN];
        encoding[0] = ANCHOR_ENCODING_VERSION;
        encoding[1] = self.wcash_network as u8;
        encoding[2] = BITCOIN_MAINNET_SOURCE_ID;
        encoding[3] = RESERVED_BYTE;
        encoding[4..8].copy_from_slice(&self.bitcoin_height.to_le_bytes());
        encoding[8..].copy_from_slice(&self.bitcoin_block_hash.raw_digest());
        encoding
    }

    /// Strictly decodes one canonical 40-byte anchor.
    ///
    /// Decoding does not activate an anchor for a public network.
    ///
    /// # Errors
    ///
    /// Returns an error for a wrong length, unknown version or network, or a
    /// non-zero reserved byte.
    pub fn decode(encoding: &[u8]) -> Result<Self, AnchorDecodeError> {
        if encoding.len() != ANCHOR_ENCODING_LEN {
            return Err(AnchorDecodeError::InvalidLength(encoding.len()));
        }
        if encoding[0] != ANCHOR_ENCODING_VERSION {
            return Err(AnchorDecodeError::UnknownVersion(encoding[0]));
        }

        let wcash_network = encoding[1].try_into()?;
        if encoding[2] != BITCOIN_MAINNET_SOURCE_ID {
            return Err(AnchorDecodeError::UnknownBitcoinNetwork(encoding[2]));
        }
        if encoding[3] != RESERVED_BYTE {
            return Err(AnchorDecodeError::NonZeroReservedByte(encoding[3]));
        }

        let mut height = [0_u8; 4];
        height.copy_from_slice(&encoding[4..8]);
        let mut raw_digest = [0_u8; 32];
        raw_digest.copy_from_slice(&encoding[8..]);

        Ok(Self::new(
            wcash_network,
            u32::from_le_bytes(height),
            BitcoinBlockHash::from_raw_digest(raw_digest),
        ))
    }

    /// Returns the deterministic ASCII text to embed in the genesis coinbase.
    #[must_use]
    pub fn genesis_statement(self) -> String {
        format!(
            "{GENESIS_TIMESTAMP_TEXT}: BTC #{} {}",
            self.bitcoin_height, self.bitcoin_block_hash,
        )
    }

    /// Returns the 32-byte Wcash genesis commitment for this anchor.
    #[must_use]
    pub fn commitment(self) -> [u8; 32] {
        let hash = Params::new()
            .hash_length(32)
            .personal(ANCHOR_PERSONALIZATION)
            .hash(&self.encode());
        let mut commitment = [0_u8; 32];
        commitment.copy_from_slice(hash.as_bytes());
        commitment
    }
}

/// Error returned by the strict anchor decoder.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AnchorDecodeError {
    /// The encoding was not exactly 40 bytes.
    InvalidLength(usize),
    /// The structural encoding version is not supported.
    UnknownVersion(u8),
    /// The target Wcash network byte is unknown.
    UnknownWcashNetwork(u8),
    /// The Bitcoin source-network byte is unknown.
    UnknownBitcoinNetwork(u8),
    /// The reserved byte was not canonically zero.
    NonZeroReservedByte(u8),
}

impl fmt::Display for AnchorDecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLength(length) => {
                write!(
                    formatter,
                    "anchor has length {length}, expected {ANCHOR_ENCODING_LEN}"
                )
            }
            Self::UnknownVersion(version) => write!(formatter, "unknown anchor version {version}"),
            Self::UnknownWcashNetwork(network) => {
                write!(formatter, "unknown Wcash network {network}")
            }
            Self::UnknownBitcoinNetwork(network) => {
                write!(formatter, "unknown Bitcoin source network {network}")
            }
            Self::NonZeroReservedByte(byte) => {
                write!(formatter, "anchor reserved byte is non-zero: {byte}")
            }
        }
    }
}

impl Error for AnchorDecodeError {}

/// A validated, explicit local-regtest anchor override.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RegtestAnchorOverride(BitcoinAnchor);

impl RegtestAnchorOverride {
    /// Builds a local-only override from a claimed height and exact header.
    ///
    /// # Errors
    ///
    /// Returns an error unless the isolated header satisfies its canonical
    /// Bitcoin-mainnet proof-of-work target.
    pub fn from_header(height: u32, header: BitcoinHeader) -> Result<Self, BitcoinPowError> {
        header.validate_isolated_mainnet_work()?;
        Ok(Self(BitcoinAnchor::new(
            WcashNetwork::Regtest,
            height,
            header.block_hash(),
        )))
    }

    /// Returns the local anchor represented by this override.
    #[must_use]
    pub const fn anchor(self) -> BitcoinAnchor {
        self.0
    }
}

/// The Bitcoin mainnet genesis block used by default for local Wcash regtest.
pub const REGTEST_ANCHOR: BitcoinAnchor = BitcoinAnchor::frozen(
    WcashNetwork::Regtest,
    0,
    [
        0x6f, 0xe2, 0x8c, 0x0a, 0xb6, 0xf1, 0xb3, 0x72, 0xc1, 0xa6, 0xa2, 0x46, 0xae, 0x63, 0xf7,
        0x4f, 0x93, 0x1e, 0x83, 0x65, 0xe1, 0x5a, 0x08, 0x9c, 0x68, 0xd6, 0x19, 0x00, 0x00, 0x00,
        0x00, 0x00,
    ],
);

// These `None` values are the fail-closed public-network activation gate. A
// reviewed release must replace the applicable value with a checked anchor.
const MAINNET_ANCHOR: Option<BitcoinAnchor> = None;
const TESTNET_ANCHOR: Option<BitcoinAnchor> = None;

/// Error returned when no reviewed anchor is frozen for a Wcash network.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PublicNetworkDisabled {
    network: WcashNetwork,
}

impl PublicNetworkDisabled {
    /// Returns the public network that has no frozen anchor.
    #[must_use]
    pub const fn network(self) -> WcashNetwork {
        self.network
    }
}

impl fmt::Display for PublicNetworkDisabled {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Wcash {} is disabled because no reviewed Bitcoin anchor is frozen",
            self.network
        )
    }
}

impl Error for PublicNetworkDisabled {}

/// Selects the consensus anchor for a Wcash network.
///
/// An override is honored only on regtest. Mainnet and testnet stay disabled
/// until a reviewed source release freezes their anchor constants.
///
/// # Errors
///
/// Returns an error for a public network whose anchor has not been frozen.
pub const fn select_anchor(
    network: WcashNetwork,
    regtest_override: Option<RegtestAnchorOverride>,
) -> Result<BitcoinAnchor, PublicNetworkDisabled> {
    match network {
        WcashNetwork::Mainnet => match MAINNET_ANCHOR {
            Some(anchor) => Ok(anchor),
            None => Err(PublicNetworkDisabled { network }),
        },
        WcashNetwork::Testnet => match TESTNET_ANCHOR {
            Some(anchor) => Ok(anchor),
            None => Err(PublicNetworkDisabled { network }),
        },
        WcashNetwork::Regtest => match regtest_override {
            Some(anchor_override) => Ok(anchor_override.anchor()),
            None => Ok(REGTEST_ANCHOR),
        },
    }
}

fn parse_hex_array<const N: usize>(input: &str) -> Result<[u8; N], HexParseError> {
    let expected = N
        .checked_mul(2)
        .expect("fixed-width hex size fits in usize");
    if input.len() != expected {
        return Err(HexParseError::InvalidLength {
            actual: input.len(),
            expected,
        });
    }

    let mut output = [0_u8; N];
    for (index, pair) in input.as_bytes().chunks_exact(2).enumerate() {
        let high = decode_nibble(pair[0]).ok_or(HexParseError::InvalidHex { index: index * 2 })?;
        let low = decode_nibble(pair[1]).ok_or(HexParseError::InvalidHex {
            index: index * 2 + 1,
        })?;
        output[index] = (high << 4) | low;
    }
    Ok(output)
}

const fn decode_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn decode_compact_target(bits: u32) -> Result<[u8; 32], BitcoinPowError> {
    let size = bits >> 24;
    let word = bits & 0x007f_ffff;
    if bits & 0x0080_0000 != 0 {
        return Err(BitcoinPowError::NegativeTarget);
    }
    if word == 0 {
        return Err(BitcoinPowError::ZeroTarget);
    }
    if size > 34 || (word > 0xff && size > 33) || (word > 0xffff && size > 32) {
        return Err(BitcoinPowError::TargetOverflow);
    }

    let mut target = [0_u8; 32];
    if size <= 3 {
        let right_shift = 8_u32 * (3 - size);
        let value = word >> right_shift;
        if value == 0 {
            return Err(BitcoinPowError::ZeroTarget);
        }
        target[28..].copy_from_slice(&value.to_be_bytes());
        return Ok(target);
    }

    let size = usize::try_from(size).expect("compact target size is at most 34");
    let bytes = [
        u8::try_from(word >> 16).expect("the high compact-target byte fits in u8"),
        u8::try_from((word >> 8) & 0xff).expect("the middle compact-target byte fits in u8"),
        u8::try_from(word & 0xff).expect("the low compact-target byte fits in u8"),
    ];
    let start = 32_i32 - i32::try_from(size).expect("compact target size fits in i32");
    for (offset, byte) in bytes.into_iter().enumerate() {
        let index = start + i32::try_from(offset).expect("three-byte offset fits in i32");
        if index < 0 {
            if byte != 0 {
                return Err(BitcoinPowError::TargetOverflow);
            }
        } else {
            let index = usize::try_from(index).expect("non-negative target index fits in usize");
            if index < target.len() {
                target[index] = byte;
            } else if byte != 0 {
                return Err(BitcoinPowError::TargetOverflow);
            }
        }
    }
    Ok(target)
}

#[cfg(test)]
mod tests {
    use std::fmt::Write as _;

    use super::*;

    const BITCOIN_GENESIS_HEADER: &str = concat!(
        "0100000000000000000000000000000000000000000000000000000000000000",
        "000000003ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa",
        "4b1e5e4a29ab5f49ffff001d1dac2b7c",
    );
    const BITCOIN_GENESIS_DISPLAY_HASH: &str =
        "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f";
    const REGTEST_ENCODING: &str =
        "01020000000000006fe28c0ab6f1b372c1a6a246ae63f74f931e8365e15a089c68d6190000000000";
    const REGTEST_COMMITMENT: &str =
        "da9850231c2edb7728da1f99d9f31a938f56f3724209953da2ae03a0e767cba9";

    fn encode_hex(bytes: impl IntoIterator<Item = u8>) -> String {
        bytes.into_iter().fold(String::new(), |mut output, byte| {
            write!(&mut output, "{byte:02x}")
                .expect("writing hexadecimal bytes to a String cannot fail");
            output
        })
    }

    #[test]
    fn identity_values_are_domain_derived_and_not_zcash_magic() {
        let zcash_magics = [
            [0x24, 0xe9, 0x27, 0x64],
            [0xfa, 0x1a, 0xf9, 0xbf],
            [0xaa, 0xe8, 0x3f, 0x5f],
        ];

        for network in [
            WcashNetwork::Mainnet,
            WcashNetwork::Testnet,
            WcashNetwork::Regtest,
        ] {
            let identity = network_identity(network);
            let digest = Sha256::digest(identity.domain_label().as_bytes());
            assert_eq!(identity.network(), network);
            assert_eq!(identity.p2p_magic(), digest[..4]);
            assert!(!zcash_magics.contains(&identity.p2p_magic()));
        }
    }

    #[test]
    fn bitcoin_genesis_header_and_hash_are_reproducible() {
        let header: BitcoinHeader = BITCOIN_GENESIS_HEADER
            .parse()
            .expect("the frozen Bitcoin genesis header is valid hex");
        let displayed_hash: BitcoinBlockHash = BITCOIN_GENESIS_DISPLAY_HASH
            .parse()
            .expect("the frozen Bitcoin genesis hash is valid hex");

        assert_eq!(header.block_hash(), displayed_hash);
        assert_eq!(header.bits(), BITCOIN_MAINNET_POW_LIMIT_BITS);
        assert_eq!(header.validate_isolated_mainnet_work(), Ok(()));
        assert_eq!(REGTEST_ANCHOR.bitcoin_block_hash(), displayed_hash);
    }

    #[test]
    fn regtest_vectors_are_frozen() {
        assert_eq!(encode_hex(REGTEST_ANCHOR.encode()), REGTEST_ENCODING);
        assert_eq!(encode_hex(REGTEST_ANCHOR.commitment()), REGTEST_COMMITMENT);
        assert_eq!(
            BitcoinAnchor::decode(&REGTEST_ANCHOR.encode()),
            Ok(REGTEST_ANCHOR)
        );
        assert_eq!(
            REGTEST_ANCHOR.genesis_statement(),
            concat!(
                "06/Sep/2026 Wcash: BTC #0 ",
                "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
            )
        );
        assert!(REGTEST_ANCHOR.genesis_statement().len() <= 100);
    }

    #[test]
    fn both_public_networks_fail_closed() {
        for network in [WcashNetwork::Mainnet, WcashNetwork::Testnet] {
            let error = select_anchor(network, None)
                .expect_err("public anchors must stay unavailable before review");
            assert_eq!(error.network(), network);
        }
        assert_eq!(REQUESTED_MAINNET_BITCOIN_HEIGHT, 965_954);
    }

    #[test]
    fn explicit_regtest_override_is_local_and_deterministic() {
        let header: BitcoinHeader = BITCOIN_GENESIS_HEADER
            .parse()
            .expect("the frozen Bitcoin genesis header is valid hex");
        let anchor_override = RegtestAnchorOverride::from_header(42, header)
            .expect("the Bitcoin genesis header has valid isolated work");

        let first = select_anchor(WcashNetwork::Regtest, Some(anchor_override))
            .expect("regtest accepts an explicit override");
        let second = select_anchor(WcashNetwork::Regtest, Some(anchor_override))
            .expect("the same regtest override is deterministic");
        assert_eq!(first, second);
        assert_eq!(first.bitcoin_height(), 42);
        assert_ne!(first.commitment(), REGTEST_ANCHOR.commitment());

        for public_network in [WcashNetwork::Mainnet, WcashNetwork::Testnet] {
            assert!(select_anchor(public_network, Some(anchor_override)).is_err());
        }
    }

    #[test]
    fn malformed_or_invalid_headers_fail_closed() {
        assert!(matches!(
            "00".parse::<BitcoinHeader>(),
            Err(HexParseError::InvalidLength { .. })
        ));

        let mut header = BITCOIN_GENESIS_HEADER
            .parse::<BitcoinHeader>()
            .expect("the frozen Bitcoin genesis header is valid hex")
            .bytes();
        header[72..76].copy_from_slice(&0_u32.to_le_bytes());
        assert_eq!(
            BitcoinHeader::from_bytes(header).validate_isolated_mainnet_work(),
            Err(BitcoinPowError::ZeroTarget)
        );

        header[72..76].copy_from_slice(&0x0012_3456_u32.to_le_bytes());
        assert_eq!(
            BitcoinHeader::from_bytes(header).validate_isolated_mainnet_work(),
            Err(BitcoinPowError::ZeroTarget)
        );

        let mut header = BITCOIN_GENESIS_HEADER
            .parse::<BitcoinHeader>()
            .expect("the frozen Bitcoin genesis header is valid hex")
            .bytes();
        header[0] ^= 1;
        assert_eq!(
            BitcoinHeader::from_bytes(header).validate_isolated_mainnet_work(),
            Err(BitcoinPowError::HashAboveTarget)
        );
    }

    #[test]
    fn strict_decoder_rejects_noncanonical_encodings() {
        assert_eq!(
            BitcoinAnchor::decode(&REGTEST_ANCHOR.encode()[..39]),
            Err(AnchorDecodeError::InvalidLength(39))
        );

        for (index, value, expected) in [
            (0, 2, AnchorDecodeError::UnknownVersion(2)),
            (1, 3, AnchorDecodeError::UnknownWcashNetwork(3)),
            (2, 1, AnchorDecodeError::UnknownBitcoinNetwork(1)),
            (3, 1, AnchorDecodeError::NonZeroReservedByte(1)),
        ] {
            let mut encoding = REGTEST_ANCHOR.encode();
            encoding[index] = value;
            assert_eq!(BitcoinAnchor::decode(&encoding), Err(expected));
        }
    }
}

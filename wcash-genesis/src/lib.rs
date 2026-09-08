//! Deterministic identity and Bitcoin-anchored genesis data for Wcash.
//!
//! Public-network anchors are enabled only after their Bitcoin headers and
//! confirmation history are independently checked and frozen in a reviewed
//! release. Consensus code never fetches data from the network.

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

/// Bitcoin mainnet height designated for a possible Wcash mainnet anchor.
pub const DESIGNATED_MAINNET_BITCOIN_HEIGHT: u32 = 965_954;

/// Bitcoin mainnet height frozen into the public Wcash testnet genesis.
pub const PUBLIC_TESTNET_BITCOIN_HEIGHT: u32 = 965_900;

/// Bitcoin best-chain height used when the public testnet anchor was frozen.
///
/// Counting the anchor block itself, this records 112 confirmations. It is an
/// immutable audit vector, not a value consulted from the network at runtime.
pub const PUBLIC_TESTNET_VERIFICATION_HEIGHT: u32 = 966_011;

/// Bitcoin header timestamp frozen into the public Wcash testnet genesis.
pub const PUBLIC_TESTNET_BITCOIN_TIME: u32 = 1_788_768_709;

/// Exact Bitcoin wire header frozen into the public Wcash testnet test vector.
pub const PUBLIC_TESTNET_BITCOIN_HEADER_HEX: &str = concat!(
    "00203220700b5cbd51c3511db177feb5891754ee7e3ec5f4849a010000000000",
    "000000000444905fd34cdb083f512a1957ec2c7ffedf3a7ff5098d1f20a5aed9",
    "c8484b46c5719e6a5e35021706442381",
);

/// Bitcoin mainnet height frozen into the local Wcash regtest genesis.
pub const LOCAL_REGTEST_BITCOIN_HEIGHT: u32 = 965_910;

/// Bitcoin header timestamp frozen into the local Wcash regtest genesis.
pub const LOCAL_REGTEST_BITCOIN_TIME: u32 = 1_788_772_786;

/// Exact Bitcoin wire header frozen into the local Wcash regtest test vector.
pub const LOCAL_REGTEST_BITCOIN_HEADER_HEX: &str = concat!(
    "00c02123b4c556cf9cfeadd7419e61a554f2fb4f84aa89bd2c9e010000000000",
    "00000000ec36381fa721f932e3c24ded1ea557bc0babc74c686c4233f5e6ec92",
    "ba277d2ab2819e6a5e3502173c197783",
);

/// Number of confirmations required by the release procedure before freezing a public anchor.
pub const MIN_PUBLIC_ANCHOR_CONFIRMATIONS: u32 = 100;

/// Canonical compact `nBits` value for the Wcash public-testnet proof-of-work limit.
///
/// This is the compact encoding of `2^251 - 1`, matching the reviewed Zcash
/// testnet limit while retaining a separate Wcash chain and proof format.
pub const PUBLIC_TESTNET_POW_LIMIT_BITS: u32 = 0x2007_ffff;

/// Version of the fixed-width Bitcoin anchor encoding.
pub const ANCHOR_ENCODING_VERSION: u8 = 1;

/// Length of a canonical encoded Bitcoin anchor.
pub const ANCHOR_ENCODING_LEN: usize = 40;

/// `BLAKE2b` personalization used for Wcash genesis anchor commitments.
pub const ANCHOR_PERSONALIZATION: &[u8; 16] = b"WcashBtcAnchorV1";

const BITCOIN_MAINNET_SOURCE_ID: u8 = 0;
const RESERVED_BYTE: u8 = 0;
const BITCOIN_HEADER_LEN: usize = 80;
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
            domain_label: "Wcash/regtest/v2",
            p2p_magic: [0xd5, 0xe2, 0xfc, 0xae],
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

    /// Returns the header timestamp as seconds since the Unix epoch.
    #[must_use]
    pub fn time(self) -> u32 {
        let mut bytes = [0_u8; 4];
        bytes.copy_from_slice(&self.0[68..72]);
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

/// The frozen Bitcoin mainnet block used by default for local Wcash regtest.
pub const REGTEST_ANCHOR: BitcoinAnchor = BitcoinAnchor::frozen(
    WcashNetwork::Regtest,
    LOCAL_REGTEST_BITCOIN_HEIGHT,
    [
        0x46, 0xb1, 0x46, 0xd1, 0x92, 0x7c, 0x70, 0x4a, 0xd8, 0x5f, 0x0a, 0xde, 0x6f, 0x2c, 0x64,
        0x98, 0xf0, 0x2f, 0x8d, 0xb2, 0xbd, 0xbb, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00,
    ],
);

/// The frozen Bitcoin mainnet block used for the public Wcash testnet.
///
/// The Wcash network discriminator is committed alongside the Bitcoin hash,
/// so this cannot collide with a regtest or future mainnet anchor even if a
/// source block were reused.
pub const TESTNET_ANCHOR: BitcoinAnchor = BitcoinAnchor::frozen(
    WcashNetwork::Testnet,
    PUBLIC_TESTNET_BITCOIN_HEIGHT,
    [
        0x51, 0x98, 0x6b, 0x3e, 0x1c, 0x27, 0x3a, 0xd8, 0xc5, 0xea, 0xf2, 0x37, 0x78, 0xb4, 0xa8,
        0x3c, 0xaf, 0xf4, 0xf5, 0x9f, 0xb5, 0x56, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00,
    ],
);

// This `None` value is the fail-closed mainnet activation gate. A reviewed
// release must replace it with the announced anchor only after confirmation.
const MAINNET_ANCHOR: Option<BitcoinAnchor> = None;

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
/// An override is honored only on regtest. Mainnet stays disabled until a
/// reviewed source release freezes its announced anchor.
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
        WcashNetwork::Testnet => Ok(TESTNET_ANCHOR),
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

    const LOCAL_REGTEST_BITCOIN_DISPLAY_HASH: &str =
        "00000000000000000000bbbdb28d2ff098642c6fde0a5fd84a707c92d146b146";
    const LOCAL_REGTEST_BITCOIN_BITS: u32 = 0x1702_355e;
    const PUBLIC_TESTNET_BITCOIN_DISPLAY_HASH: &str =
        "0000000000000000000056b59ff5f4af3ca8b47837f2eac5d83a271c3e6b9851";
    const PUBLIC_TESTNET_BITCOIN_BITS: u32 = 0x1702_355e;
    const TESTNET_ENCODING: &str =
        "010100000cbd0e0051986b3e1c273ad8c5eaf23778b4a83caff4f59fb55600000000000000000000";
    const TESTNET_COMMITMENT: &str =
        "95940842f305c76339570ea54bde7987d3aa90193cb872baab0beb3528bcd6d5";
    const REGTEST_ENCODING: &str =
        "0102000016bd0e0046b146d1927c704ad85f0ade6f2c6498f02f8db2bdbb00000000000000000000";
    const REGTEST_COMMITMENT: &str =
        "87abb83be6bc263c6b1eb58119a4b39ce614bce722b1eb4f7363c5a000289e88";

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

        let wcash_networks = [
            WcashNetwork::Mainnet,
            WcashNetwork::Testnet,
            WcashNetwork::Regtest,
        ];
        let mut wcash_magics = std::collections::HashSet::new();
        for network in wcash_networks {
            let identity = network_identity(network);
            let digest = Sha256::digest(identity.domain_label().as_bytes());
            assert_eq!(identity.network(), network);
            assert_eq!(identity.p2p_magic(), digest[..4]);
            assert!(!zcash_magics.contains(&identity.p2p_magic()));
            assert!(
                wcash_magics.insert(identity.p2p_magic()),
                "each Wcash network must have unique P2P magic"
            );
        }
    }

    #[test]
    fn local_regtest_bitcoin_header_and_hash_are_reproducible() {
        let header: BitcoinHeader = LOCAL_REGTEST_BITCOIN_HEADER_HEX
            .parse()
            .expect("the frozen Bitcoin anchor header is valid hex");
        let displayed_hash: BitcoinBlockHash = LOCAL_REGTEST_BITCOIN_DISPLAY_HASH
            .parse()
            .expect("the frozen Bitcoin anchor hash is valid hex");

        assert_eq!(header.block_hash(), displayed_hash);
        assert_eq!(header.time(), LOCAL_REGTEST_BITCOIN_TIME);
        assert_eq!(header.bits(), LOCAL_REGTEST_BITCOIN_BITS);
        assert_eq!(header.validate_isolated_mainnet_work(), Ok(()));
        assert_eq!(
            REGTEST_ANCHOR.bitcoin_height(),
            LOCAL_REGTEST_BITCOIN_HEIGHT
        );
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
                "06/Sep/2026 Wcash: BTC #965910 ",
                "00000000000000000000bbbdb28d2ff098642c6fde0a5fd84a707c92d146b146"
            )
        );
        assert!(REGTEST_ANCHOR.genesis_statement().len() <= 100);
    }

    #[test]
    fn public_testnet_bitcoin_header_and_hash_are_reproducible() {
        let header: BitcoinHeader = PUBLIC_TESTNET_BITCOIN_HEADER_HEX
            .parse()
            .expect("the frozen Bitcoin anchor header is valid hex");
        let displayed_hash: BitcoinBlockHash = PUBLIC_TESTNET_BITCOIN_DISPLAY_HASH
            .parse()
            .expect("the frozen Bitcoin anchor hash is valid hex");

        assert_eq!(header.block_hash(), displayed_hash);
        assert_eq!(header.time(), PUBLIC_TESTNET_BITCOIN_TIME);
        assert_eq!(header.bits(), PUBLIC_TESTNET_BITCOIN_BITS);
        assert_eq!(header.validate_isolated_mainnet_work(), Ok(()));
        assert_eq!(
            TESTNET_ANCHOR.bitcoin_height(),
            PUBLIC_TESTNET_BITCOIN_HEIGHT
        );
        assert_eq!(TESTNET_ANCHOR.bitcoin_block_hash(), displayed_hash);
        assert_eq!(
            select_anchor(WcashNetwork::Testnet, None),
            Ok(TESTNET_ANCHOR)
        );
        assert!(
            PUBLIC_TESTNET_VERIFICATION_HEIGHT - PUBLIC_TESTNET_BITCOIN_HEIGHT + 1
                >= MIN_PUBLIC_ANCHOR_CONFIRMATIONS,
            "the frozen audit height must record at least the release minimum confirmations"
        );
    }

    #[test]
    fn public_testnet_anchor_vectors_are_frozen() {
        assert_eq!(
            TESTNET_ANCHOR.genesis_statement(),
            concat!(
                "06/Sep/2026 Wcash: BTC #965900 ",
                "0000000000000000000056b59ff5f4af3ca8b47837f2eac5d83a271c3e6b9851"
            )
        );
        assert!(TESTNET_ANCHOR.genesis_statement().len() <= 100);
        assert_eq!(encode_hex(TESTNET_ANCHOR.encode()), TESTNET_ENCODING);
        assert_eq!(encode_hex(TESTNET_ANCHOR.commitment()), TESTNET_COMMITMENT);
        assert_eq!(
            BitcoinAnchor::decode(&TESTNET_ANCHOR.encode()),
            Ok(TESTNET_ANCHOR)
        );
        assert_ne!(TESTNET_ANCHOR.commitment(), REGTEST_ANCHOR.commitment());
    }

    #[test]
    fn mainnet_stays_fail_closed() {
        let error = select_anchor(WcashNetwork::Mainnet, None)
            .expect_err("mainnet must stay unavailable before its anchor has enough confirmations");
        assert_eq!(error.network(), WcashNetwork::Mainnet);
        assert_eq!(DESIGNATED_MAINNET_BITCOIN_HEIGHT, 965_954);
    }

    #[test]
    fn explicit_regtest_override_is_local_and_deterministic() {
        let header: BitcoinHeader = LOCAL_REGTEST_BITCOIN_HEADER_HEX
            .parse()
            .expect("the frozen Bitcoin anchor header is valid hex");
        let anchor_override = RegtestAnchorOverride::from_header(42, header)
            .expect("the Bitcoin anchor header has valid isolated work");

        let first = select_anchor(WcashNetwork::Regtest, Some(anchor_override))
            .expect("regtest accepts an explicit override");
        let second = select_anchor(WcashNetwork::Regtest, Some(anchor_override))
            .expect("the same regtest override is deterministic");
        assert_eq!(first, second);
        assert_eq!(first.bitcoin_height(), 42);
        assert_ne!(first.commitment(), REGTEST_ANCHOR.commitment());

        assert!(select_anchor(WcashNetwork::Mainnet, Some(anchor_override)).is_err());
        assert_eq!(
            select_anchor(WcashNetwork::Testnet, Some(anchor_override)),
            Ok(TESTNET_ANCHOR),
            "a local override must not replace the frozen testnet anchor"
        );
    }

    #[test]
    fn malformed_or_invalid_headers_fail_closed() {
        assert!(matches!(
            "00".parse::<BitcoinHeader>(),
            Err(HexParseError::InvalidLength { .. })
        ));

        let mut header = LOCAL_REGTEST_BITCOIN_HEADER_HEX
            .parse::<BitcoinHeader>()
            .expect("the frozen Bitcoin anchor header is valid hex")
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

        let mut header = LOCAL_REGTEST_BITCOIN_HEADER_HEX
            .parse::<BitcoinHeader>()
            .expect("the frozen Bitcoin anchor header is valid hex")
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

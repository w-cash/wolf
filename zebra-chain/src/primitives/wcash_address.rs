//! Wcash payment-address encoding.
//!
//! Wcash reuses the receiver payloads and Unified Address container defined by
//! Zcash, but it deliberately uses a disjoint textual namespace. This prevents
//! wallets and operators from accidentally copying a Zcash address into Wcash.
//!
//! The Unified Address implementation preserves ZIP 316's F4Jumble construction:
//! the Wcash HRP is included in the padded, jumbled payload as well as the
//! Bech32m checksum. A Wcash Unified Address is therefore not a Zcash address
//! with its visible prefix replaced.

use std::{fmt, str::FromStr};

use bech32::{primitives::decode::CheckedHrpstring, Bech32m, Hrp};
use zcash_address::{
    unified::{Address as UnifiedAddress, Bech32mZip316, Container, Encoding, Item, Receiver},
    ConversionError, TryFromAddress,
};
use zcash_encoding::CompactSize;
use zcash_protocol::consensus::NetworkType;

/// Mainnet Wcash Unified Address HRP.
pub const HRP_UNIFIED_MAINNET: &str = "wu";
/// Testnet Wcash Unified Address HRP.
pub const HRP_UNIFIED_TESTNET: &str = "wutest";
/// Regtest Wcash Unified Address HRP.
pub const HRP_UNIFIED_REGTEST: &str = "wuregtest";

// Permanently reserved legacy-pool namespaces. Wcash never encodes or accepts
// Sapling payment addresses, but reserving the prefixes prevents them from
// being reinterpreted as another Wcash address type in the future.
const RESERVED_SAPLING_MAINNET: &str = "ws";
const RESERVED_SAPLING_TESTNET: &str = "wtestsapling";
const RESERVED_SAPLING_REGTEST: &str = "wregtestsapling";

/// Mainnet Wcash transparent-source-only address HRP.
pub const HRP_TEX_MAINNET: &str = "wtex";
/// Testnet Wcash transparent-source-only address HRP.
pub const HRP_TEX_TESTNET: &str = "wtextest";
/// Regtest Wcash transparent-source-only address HRP.
pub const HRP_TEX_REGTEST: &str = "wtexregtest";

/// Mainnet Wcash P2PKH Base58Check version bytes (`W1...`).
pub const B58_P2PKH_MAINNET: [u8; 2] = [0x10, 0x55];
/// Mainnet Wcash P2SH Base58Check version bytes (`W3...`).
pub const B58_P2SH_MAINNET: [u8; 2] = [0x10, 0x5a];
/// Testnet Wcash P2PKH Base58Check version bytes (`WT...`).
pub const B58_P2PKH_TESTNET: [u8; 2] = [0x10, 0x95];
/// Testnet Wcash P2SH Base58Check version bytes (`WU...`).
pub const B58_P2SH_TESTNET: [u8; 2] = [0x10, 0x98];
/// Regtest Wcash P2PKH Base58Check version bytes (`WR...`).
pub const B58_P2PKH_REGTEST: [u8; 2] = [0x10, 0x90];
/// Regtest Wcash P2SH Base58Check version bytes (`WS...`).
pub const B58_P2SH_REGTEST: [u8; 2] = [0x10, 0x93];

const ZIP316_PADDING_LEN: usize = 16;

/// A decoded Wcash payment-address payload.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum WcashAddressKind {
    /// A ZIP 316 container. Ironwood recipients use the Orchard receiver slot.
    Unified(UnifiedAddress),
    /// A transparent pay-to-public-key-hash receiver.
    P2pkh([u8; 20]),
    /// A transparent pay-to-script-hash receiver.
    P2sh([u8; 20]),
    /// A transparent-source-only P2PKH receiver.
    Tex([u8; 20]),
}

/// A Wcash payment address.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct WcashAddress {
    network: NetworkType,
    kind: WcashAddressKind,
}

/// An error encountered while parsing a Wcash payment address.
#[derive(Debug, Eq, PartialEq, thiserror::Error)]
pub enum WcashAddressParseError {
    /// The string does not use any Wcash address namespace.
    #[error("not a Wcash address")]
    NotWcash,

    /// The string uses a Wcash namespace but its encoded payload is malformed.
    #[error("invalid Wcash {0} encoding")]
    InvalidEncoding(&'static str),

    /// The Unified Address payload violates ZIP 316 container rules.
    #[error("invalid Wcash Unified Address: {0}")]
    InvalidUnified(String),

    /// The Unified Address does not contain the receiver used by Ironwood.
    #[error("Wcash Unified Addresses must contain an Ironwood receiver")]
    MissingIronwoodReceiver,

    /// The address contains a receiver for a pool Wcash does not activate.
    #[error("Wcash does not support {0} receivers")]
    UnsupportedReceiver(&'static str),
}

impl WcashAddress {
    /// Constructs an Ironwood-capable Unified Address.
    pub fn from_unified(
        network: NetworkType,
        data: UnifiedAddress,
    ) -> Result<Self, WcashAddressParseError> {
        validate_unified_receivers(&data)?;
        Ok(Self {
            network,
            kind: WcashAddressKind::Unified(data),
        })
    }

    /// Constructs a transparent pay-to-public-key-hash address.
    pub fn from_transparent_p2pkh(network: NetworkType, data: [u8; 20]) -> Self {
        Self {
            network,
            kind: WcashAddressKind::P2pkh(data),
        }
    }

    /// Constructs a transparent pay-to-script-hash address.
    pub fn from_transparent_p2sh(network: NetworkType, data: [u8; 20]) -> Self {
        Self {
            network,
            kind: WcashAddressKind::P2sh(data),
        }
    }

    /// Constructs a transparent-source-only address.
    pub fn from_tex(network: NetworkType, data: [u8; 20]) -> Self {
        Self {
            network,
            kind: WcashAddressKind::Tex(data),
        }
    }

    /// Parses a Wcash address from its canonical string representation.
    pub fn try_from_encoded(encoded: &str) -> Result<Self, WcashAddressParseError> {
        encoded.parse()
    }

    /// Encodes this address in its canonical Wcash representation.
    pub fn encode(&self) -> String {
        self.to_string()
    }

    /// Returns this address's network.
    pub fn network(&self) -> NetworkType {
        self.network
    }

    /// Re-encodes the same receiver payload for `network`.
    ///
    /// This is intended for deterministic fixtures and wallet derivation code that
    /// already owns the receiver. It must not be used to bypass network checks on an
    /// address supplied by an external caller.
    pub fn with_network(mut self, network: NetworkType) -> Self {
        self.network = network;
        self
    }

    /// Returns this address's decoded kind and receiver payload.
    pub fn kind(&self) -> &WcashAddressKind {
        &self.kind
    }

    /// Converts this address using the existing Zcash receiver conversion API.
    ///
    /// The textual encoding is Wcash-specific, but receiver payloads retain the
    /// same protocol representation. This makes the codec directly compatible
    /// with Zebra and `zcash_keys` address types without teaching the Zcash
    /// parser to accept Wcash strings.
    pub fn convert<T: TryFromAddress>(self) -> Result<T, ConversionError<T::Error>> {
        match self.kind {
            WcashAddressKind::Unified(data) => T::try_from_unified(self.network, data),
            WcashAddressKind::P2pkh(data) => T::try_from_transparent_p2pkh(self.network, data),
            WcashAddressKind::P2sh(data) => T::try_from_transparent_p2sh(self.network, data),
            WcashAddressKind::Tex(data) => T::try_from_tex(self.network, data),
        }
    }

    /// Converts this address if it belongs to `expected_network`.
    pub fn convert_if_network<T: TryFromAddress>(
        self,
        expected_network: NetworkType,
    ) -> Result<T, ConversionError<T::Error>> {
        if self.network != expected_network {
            return Err(ConversionError::IncorrectNetwork {
                expected: expected_network,
                actual: self.network,
            });
        }

        self.convert()
    }
}

impl TryFromAddress for WcashAddress {
    type Error = WcashAddressParseError;

    fn try_from_unified(
        network: NetworkType,
        data: UnifiedAddress,
    ) -> Result<Self, ConversionError<Self::Error>> {
        Self::from_unified(network, data).map_err(ConversionError::User)
    }

    fn try_from_transparent_p2pkh(
        network: NetworkType,
        data: [u8; 20],
    ) -> Result<Self, ConversionError<Self::Error>> {
        Ok(Self::from_transparent_p2pkh(network, data))
    }

    fn try_from_transparent_p2sh(
        network: NetworkType,
        data: [u8; 20],
    ) -> Result<Self, ConversionError<Self::Error>> {
        Ok(Self::from_transparent_p2sh(network, data))
    }

    fn try_from_tex(
        network: NetworkType,
        data: [u8; 20],
    ) -> Result<Self, ConversionError<Self::Error>> {
        Ok(Self::from_tex(network, data))
    }
}

impl fmt::Display for WcashAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let encoded = match &self.kind {
            WcashAddressKind::Unified(address) => encode_unified(self.network, address),
            WcashAddressKind::P2pkh(data) => encode_base58(p2pkh_prefix(self.network), data),
            WcashAddressKind::P2sh(data) => encode_base58(p2sh_prefix(self.network), data),
            WcashAddressKind::Tex(data) => encode_bech32::<Bech32m>(tex_hrp(self.network), data),
        };

        f.write_str(&encoded)
    }
}

impl FromStr for WcashAddress {
    type Err = WcashAddressParseError;

    fn from_str(encoded: &str) -> Result<Self, Self::Err> {
        if let Ok(parsed) = CheckedHrpstring::new::<Bech32mZip316>(encoded) {
            let parsed_hrp = parsed.hrp();
            let hrp = parsed_hrp.as_str();
            if let Some(network) = unified_network(hrp) {
                let data = parsed.byte_iter().collect::<Vec<_>>();
                let address = decode_unified(hrp, data)?;

                return Self::from_unified(network, address);
            }
        }

        if let Ok(parsed) = CheckedHrpstring::new::<Bech32m>(encoded) {
            let parsed_hrp = parsed.hrp();
            let hrp = parsed_hrp.as_str();
            if let Some(network) = tex_network(hrp) {
                let data: [u8; 20] = parsed
                    .byte_iter()
                    .collect::<Vec<_>>()
                    .try_into()
                    .map_err(|_| WcashAddressParseError::InvalidEncoding("TEX"))?;

                return Ok(Self::from_tex(network, data));
            }
        }

        if let Ok(decoded) = bs58::decode(encoded).with_check(None).into_vec() {
            if decoded.len() == 22 {
                let prefix = [decoded[0], decoded[1]];
                let data: [u8; 20] = decoded[2..]
                    .try_into()
                    .map_err(|_| WcashAddressParseError::InvalidEncoding("transparent"))?;

                if let Some(network) = p2pkh_network(prefix) {
                    return Ok(Self::from_transparent_p2pkh(network, data));
                }
                if let Some(network) = p2sh_network(prefix) {
                    return Ok(Self::from_transparent_p2sh(network, data));
                }
            }
        }

        if looks_like_wcash_address(encoded) {
            Err(WcashAddressParseError::InvalidEncoding("address"))
        } else {
            Err(WcashAddressParseError::NotWcash)
        }
    }
}

fn encode_unified(network: NetworkType, address: &UnifiedAddress) -> String {
    encode_unified_with_hrp(unified_hrp(network), address)
}

fn encode_unified_with_hrp(hrp: &str, address: &UnifiedAddress) -> String {
    let mut raw = Vec::new();
    for receiver in address.items_as_parsed() {
        raw.extend(receiver.typed_encoding());
    }

    let mut padding = [0u8; ZIP316_PADDING_LEN];
    padding[..hrp.len()].copy_from_slice(hrp.as_bytes());
    raw.extend(padding);

    let jumbled = f4jumble::f4jumble(&raw)
        .expect("a valid Unified Address is within the F4Jumble length bounds");
    encode_bech32::<Bech32mZip316>(hrp, &jumbled)
}

fn decode_unified(
    hrp: &str,
    mut jumbled: Vec<u8>,
) -> Result<UnifiedAddress, WcashAddressParseError> {
    f4jumble::f4jumble_inv_mut(&mut jumbled)
        .map_err(|_| WcashAddressParseError::InvalidEncoding("Unified Address"))?;

    if jumbled.len() < ZIP316_PADDING_LEN {
        return Err(WcashAddressParseError::InvalidEncoding("Unified Address"));
    }

    let raw_len = jumbled.len() - ZIP316_PADDING_LEN;
    let (raw, padding) = jumbled.split_at(raw_len);
    let mut expected_padding = [0u8; ZIP316_PADDING_LEN];
    expected_padding[..hrp.len()].copy_from_slice(hrp.as_bytes());
    if padding != expected_padding {
        return Err(WcashAddressParseError::InvalidEncoding(
            "Unified Address padding",
        ));
    }

    let mut raw = raw;
    let mut receivers = Vec::new();
    let mut previous_typecode = None;
    while !raw.is_empty() {
        let typecode = CompactSize::read(&mut raw)
            .map_err(|_| WcashAddressParseError::InvalidEncoding("Unified Address typecode"))?;
        let typecode = u32::try_from(typecode)
            .map_err(|_| WcashAddressParseError::InvalidEncoding("Unified Address typecode"))?;
        if previous_typecode.is_some_and(|previous| typecode <= previous) {
            return Err(WcashAddressParseError::InvalidUnified(
                "receiver typecodes are duplicated or out of canonical order".to_string(),
            ));
        }
        previous_typecode = Some(typecode);
        let length = CompactSize::read(&mut raw)
            .map_err(|_| WcashAddressParseError::InvalidEncoding("Unified Address length"))?;
        let length = usize::try_from(length)
            .map_err(|_| WcashAddressParseError::InvalidEncoding("Unified Address length"))?;
        if raw.len() < length {
            return Err(WcashAddressParseError::InvalidEncoding(
                "Unified Address receiver",
            ));
        }

        let (receiver, remaining) = raw.split_at(length);
        receivers.push(
            Receiver::try_from((typecode, receiver))
                .map_err(|error| WcashAddressParseError::InvalidUnified(error.to_string()))?,
        );
        raw = remaining;
    }

    UnifiedAddress::try_from_items(receivers)
        .map_err(|error| WcashAddressParseError::InvalidUnified(error.to_string()))
}

fn validate_unified_receivers(address: &UnifiedAddress) -> Result<(), WcashAddressParseError> {
    let mut has_ironwood_receiver = false;

    for receiver in address.items() {
        match receiver {
            Receiver::Orchard(_) => has_ironwood_receiver = true,
            Receiver::Sapling(_) => {
                return Err(WcashAddressParseError::UnsupportedReceiver("Sapling"));
            }
            Receiver::Unknown { .. } => {
                return Err(WcashAddressParseError::UnsupportedReceiver("unknown"));
            }
            _ => {}
        }
    }

    if has_ironwood_receiver {
        Ok(())
    } else {
        Err(WcashAddressParseError::MissingIronwoodReceiver)
    }
}

fn encode_bech32<Ck: bech32::Checksum>(hrp: &str, data: &[u8]) -> String {
    bech32::encode::<Ck>(
        Hrp::parse(hrp).expect("Wcash address HRPs are compile-time constants"),
        data,
    )
    .expect("Wcash address length is bounded by its payload type")
}

fn encode_base58(prefix: [u8; 2], data: &[u8; 20]) -> String {
    let mut bytes = Vec::with_capacity(22);
    bytes.extend(prefix);
    bytes.extend(data);
    bs58::encode(bytes).with_check().into_string()
}

fn unified_hrp(network: NetworkType) -> &'static str {
    match network {
        NetworkType::Main => HRP_UNIFIED_MAINNET,
        NetworkType::Test => HRP_UNIFIED_TESTNET,
        NetworkType::Regtest => HRP_UNIFIED_REGTEST,
    }
}

fn unified_network(hrp: &str) -> Option<NetworkType> {
    match hrp {
        HRP_UNIFIED_MAINNET => Some(NetworkType::Main),
        HRP_UNIFIED_TESTNET => Some(NetworkType::Test),
        HRP_UNIFIED_REGTEST => Some(NetworkType::Regtest),
        _ => None,
    }
}

fn tex_hrp(network: NetworkType) -> &'static str {
    match network {
        NetworkType::Main => HRP_TEX_MAINNET,
        NetworkType::Test => HRP_TEX_TESTNET,
        NetworkType::Regtest => HRP_TEX_REGTEST,
    }
}

fn tex_network(hrp: &str) -> Option<NetworkType> {
    match hrp {
        HRP_TEX_MAINNET => Some(NetworkType::Main),
        HRP_TEX_TESTNET => Some(NetworkType::Test),
        HRP_TEX_REGTEST => Some(NetworkType::Regtest),
        _ => None,
    }
}

fn p2pkh_prefix(network: NetworkType) -> [u8; 2] {
    match network {
        NetworkType::Main => B58_P2PKH_MAINNET,
        NetworkType::Test => B58_P2PKH_TESTNET,
        NetworkType::Regtest => B58_P2PKH_REGTEST,
    }
}

fn p2pkh_network(prefix: [u8; 2]) -> Option<NetworkType> {
    match prefix {
        B58_P2PKH_MAINNET => Some(NetworkType::Main),
        B58_P2PKH_TESTNET => Some(NetworkType::Test),
        B58_P2PKH_REGTEST => Some(NetworkType::Regtest),
        _ => None,
    }
}

fn p2sh_prefix(network: NetworkType) -> [u8; 2] {
    match network {
        NetworkType::Main => B58_P2SH_MAINNET,
        NetworkType::Test => B58_P2SH_TESTNET,
        NetworkType::Regtest => B58_P2SH_REGTEST,
    }
}

fn p2sh_network(prefix: [u8; 2]) -> Option<NetworkType> {
    match prefix {
        B58_P2SH_MAINNET => Some(NetworkType::Main),
        B58_P2SH_TESTNET => Some(NetworkType::Test),
        B58_P2SH_REGTEST => Some(NetworkType::Regtest),
        _ => None,
    }
}

fn looks_like_wcash_address(encoded: &str) -> bool {
    const BECH32_PREFIXES: [&str; 9] = [
        HRP_UNIFIED_MAINNET,
        HRP_UNIFIED_TESTNET,
        HRP_UNIFIED_REGTEST,
        RESERVED_SAPLING_MAINNET,
        RESERVED_SAPLING_TESTNET,
        RESERVED_SAPLING_REGTEST,
        HRP_TEX_MAINNET,
        HRP_TEX_TESTNET,
        HRP_TEX_REGTEST,
    ];

    BECH32_PREFIXES
        .iter()
        .any(|hrp| encoded.starts_with(&format!("{hrp}1")))
        || ["W1", "W3", "WT", "WU", "WR", "WS"]
            .iter()
            .any(|prefix| encoded.starts_with(prefix))
}

#[cfg(test)]
mod tests {
    use super::*;
    use zcash_address::{unified::Receiver, ZcashAddress};

    fn orchard_unified_address() -> UnifiedAddress {
        let encoded = "uregtest1pszqlgxaf5w8mu2yd9uygg8cswp0ec4f7eejqnqc35tztw4tk0sxnt3pym2f3s2872cy2ruuc5n8y9cen5q6ngzlmzu8ztrjesv8zm9j";
        let (network, unified) = UnifiedAddress::decode(encoded).unwrap();
        assert_eq!(network, NetworkType::Regtest);
        unified
    }

    fn round_trip(address: WcashAddress, expected: &str) {
        assert_eq!(address.encode(), expected);
        assert_eq!(expected.parse::<WcashAddress>(), Ok(address));
    }

    #[test]
    fn unified_golden_vectors() {
        let unified = orchard_unified_address();

        round_trip(
            WcashAddress::from_unified(NetworkType::Main, unified.clone())
                .expect("the Orchard-only fixture is supported"),
            "wu1fup0tyn04mp2hktdx25hk6tvh4gm8me04egwhdtycdxqvsphl5dna89ghj3j3gahlxsqxdy35vezp2r7xzvmcx4cs2p9c5yqsswmrjwq",
        );
        round_trip(
            WcashAddress::from_unified(NetworkType::Test, unified.clone())
                .expect("the Orchard-only fixture is supported"),
            "wutest12ky95e9c6mu3qveefsekul4tk949ahkllsu8yglndurppu8j8qeleyd20r7z6jaacwhnwar5wlrmw0nynugqr2yldvc9cv9y9gfktz8k",
        );
        round_trip(
            WcashAddress::from_unified(NetworkType::Regtest, unified)
                .expect("the Orchard-only fixture is supported"),
            "wuregtest1ctr282fk80mmwz0t69s4kywtstpyh0lr23u6ynpy54m7lufs0qyrautm3kjg7sxk5mu0lp0ck4hea672xhvrdzz2863afkz6ss423hgj",
        );
    }

    #[test]
    fn unified_codec_matches_zip_316_reference_for_zcash_hrps() {
        let unified = orchard_unified_address();

        for (network, hrp) in [
            (NetworkType::Main, "u"),
            (NetworkType::Test, "utest"),
            (NetworkType::Regtest, "uregtest"),
        ] {
            let reference = unified.encode(&network);
            assert_eq!(encode_unified_with_hrp(hrp, &unified), reference);

            let parsed = CheckedHrpstring::new::<Bech32mZip316>(&reference).unwrap();
            assert_eq!(
                decode_unified(hrp, parsed.byte_iter().collect()).unwrap(),
                unified
            );
        }
    }

    #[test]
    fn local_miner_unified_address_has_wcash_encoding() {
        let address = WcashAddress::from_unified(NetworkType::Regtest, orchard_unified_address())
            .expect("the Orchard-only fixture is supported");
        let encoded = address.encode();
        assert!(encoded.starts_with(concat!("w", "uregtest1")));
        assert_eq!(encoded.parse::<WcashAddress>().unwrap(), address);

        let converted = address
            .convert_if_network::<crate::primitives::Address>(NetworkType::Regtest)
            .unwrap();
        assert!(matches!(
            converted,
            crate::primitives::Address::Unified {
                orchard: Some(_),
                ..
            }
        ));
    }

    #[test]
    fn reserved_sapling_namespaces_are_rejected() {
        for encoded in [
            "ws1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqpp4jev",
            "wtestsapling1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqwrl5x9",
            "wregtestsapling1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq9ezcf5",
        ] {
            assert!(matches!(
                encoded.parse::<WcashAddress>(),
                Err(WcashAddressParseError::InvalidEncoding(_))
            ));
        }
    }

    #[test]
    fn unified_addresses_reject_sapling_receivers() {
        let sapling = UnifiedAddress::try_from_items(vec![Receiver::Sapling([0; 43])])
            .expect("a Sapling-only Unified Address is structurally valid");
        assert_eq!(
            WcashAddress::from_unified(NetworkType::Main, sapling.clone()),
            Err(WcashAddressParseError::UnsupportedReceiver("Sapling")),
        );

        let encoded = encode_unified(NetworkType::Main, &sapling);
        assert_eq!(
            encoded.parse::<WcashAddress>(),
            Err(WcashAddressParseError::UnsupportedReceiver("Sapling")),
        );
    }

    #[test]
    fn unified_addresses_require_an_ironwood_receiver_and_known_receivers() {
        let unknown_only = UnifiedAddress::try_from_items(vec![Receiver::Unknown {
            typecode: 65_536,
            data: vec![0; 43],
        }])
        .expect("an unknown shielded receiver is structurally valid");

        assert_eq!(
            WcashAddress::from_unified(NetworkType::Main, unknown_only.clone()),
            Err(WcashAddressParseError::UnsupportedReceiver("unknown")),
        );

        let orchard_with_unknown = UnifiedAddress::try_from_items(vec![
            Receiver::Orchard([0; 43]),
            Receiver::Unknown {
                typecode: 65_536,
                data: vec![0; 43],
            },
        ])
        .expect("the mixed receiver fixture is structurally valid");
        assert_eq!(
            WcashAddress::from_unified(NetworkType::Main, orchard_with_unknown),
            Err(WcashAddressParseError::UnsupportedReceiver("unknown")),
        );
    }

    #[test]
    fn transparent_golden_vectors() {
        round_trip(
            WcashAddress::from_transparent_p2pkh(NetworkType::Main, [0; 20]),
            "W1M7uk1EZYGQGJCwCL5cWTE1FU5CuVSL6bU",
        );
        round_trip(
            WcashAddress::from_transparent_p2sh(NetworkType::Main, [0; 20]),
            "W3MovfYj16AiePNddTBH6srNBcbVd5oHSZQ",
        );
        round_trip(
            WcashAddress::from_transparent_p2pkh(NetworkType::Test, [0; 20]),
            "WT6kWkxJzyp4LdwrjtvvuVFRbkMhH2SsBeq",
        );
        round_trip(
            WcashAddress::from_transparent_p2sh(NetworkType::Test, [0; 20]),
            "WUJmKiHCs7MSy6FGzyBvrwdExdsU75uiFgz",
        );
        round_trip(
            WcashAddress::from_transparent_p2pkh(NetworkType::Regtest, [0; 20]),
            "WR64VqQpZRujxYnAJmqGK4d4fbqQZRZHazG",
        );
        round_trip(
            WcashAddress::from_transparent_p2sh(NetworkType::Regtest, [0; 20]),
            "WSJ5JnjiRZT8b15aZr6GGWzt2VMBPapmhBQ",
        );
    }

    #[test]
    fn tex_golden_vectors() {
        round_trip(
            WcashAddress::from_tex(NetworkType::Main, [0; 20]),
            "wtex1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqkxpljy",
        );
        round_trip(
            WcashAddress::from_tex(NetworkType::Test, [0; 20]),
            "wtextest1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqf4k4nr",
        );
        round_trip(
            WcashAddress::from_tex(NetworkType::Regtest, [0; 20]),
            "wtexregtest1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqx3mu38",
        );
    }

    #[test]
    fn base58_prefixes_cover_the_entire_payload_range() {
        let prefixes = [
            (B58_P2PKH_MAINNET, "W1"),
            (B58_P2SH_MAINNET, "W3"),
            (B58_P2PKH_TESTNET, "WT"),
            (B58_P2SH_TESTNET, "WU"),
            (B58_P2PKH_REGTEST, "WR"),
            (B58_P2SH_REGTEST, "WS"),
        ];

        for (version, expected_text) in prefixes {
            // These are the inclusive numerical envelope around every possible
            // 20-byte receiver and four-byte checksum for this version.
            for (payload, checksum) in [([0; 20], [0; 4]), ([u8::MAX; 20], [u8::MAX; 4])] {
                let mut bytes = Vec::with_capacity(26);
                bytes.extend(version);
                bytes.extend(payload);
                bytes.extend(checksum);
                let encoded = bs58::encode(bytes).into_string();
                assert!(
                    encoded.starts_with(expected_text),
                    "{version:02x?} escaped its {expected_text} namespace at an endpoint: {encoded}"
                );
            }
        }
    }

    #[test]
    fn wcash_and_zcash_namespaces_are_mutually_rejected() {
        let zcash_addresses = [
            "u1qpatys4zruk99pg59gcscrt7y6akvl9vrhcfyhm9yxvxz7h87q6n8cgrzzpe9zru68uq39uhmlpp5uefxu0su5uqyqfe5zp3tycn0ecl",
            "utest10c5kutapazdnf8ztl3pu43nkfsjx89fy3uuff8tsmxm6s86j37pe7uz94z5jhkl49pqe8yz75rlsaygexk6jpaxwx0esjr8wm5ut7d5s",
            "uregtest15xk7vj4grjkay6mnfl93dhsflc2yeunhxwdh38rul0rq3dfhzzxgm5szjuvtqdha4t4p2q02ks0jgzrhjkrav70z9xlvq0plpcjkd5z3",
            "zs1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqpq6d8g",
            "ztestsapling1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqfhgwqu",
            "zregtestsapling1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqknpr3m",
            "t1Hsc1LR8yKnbbe3twRp88p6vFfC5t7DLbs",
            "tm9iMLAuYMzJ6jtFLcA7rzUmfreGuKvr7Ma",
            "t3JZcvsuaXE6ygokL4XUiZSTrQBUoPYFnXJ",
            "t26YoyZ1iPgiMEWL4zGUm74eVWfhyDMXzY2",
            "tex1s2rt77ggv6q989lr49rkgzmh5slsksa9khdgte",
            "textest1qyqszqgpqyqszqgpqyqszqgpqyqszqgpfcjgfy",
            "texregtest1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqz7rhv3",
        ];

        for encoded in zcash_addresses {
            assert_eq!(
                encoded.parse::<WcashAddress>(),
                Err(WcashAddressParseError::NotWcash),
                "accepted Zcash address {encoded}"
            );
            assert!(encoded.parse::<ZcashAddress>().is_ok());
        }

        let wcash_addresses = [
            WcashAddress::from_unified(NetworkType::Main, orchard_unified_address())
                .expect("the Orchard-only fixture is supported"),
            WcashAddress::from_transparent_p2pkh(NetworkType::Main, [0; 20]),
            WcashAddress::from_transparent_p2sh(NetworkType::Main, [0; 20]),
            WcashAddress::from_tex(NetworkType::Main, [0; 20]),
        ];

        for address in wcash_addresses {
            let encoded = address.encode();
            assert!(encoded.parse::<ZcashAddress>().is_err(), "{encoded}");
            assert_eq!(
                format!(" {encoded}").parse::<WcashAddress>(),
                Err(WcashAddressParseError::NotWcash),
                "accepted leading whitespace around {encoded}"
            );
        }
    }

    #[test]
    fn rejects_wrong_network_during_conversion() {
        let address = WcashAddress::from_transparent_p2pkh(NetworkType::Regtest, [0; 20]);
        let result = address.convert_if_network::<crate::primitives::Address>(NetworkType::Test);
        assert!(matches!(
            result,
            Err(ConversionError::IncorrectNetwork {
                expected: NetworkType::Test,
                actual: NetworkType::Regtest,
            })
        ));
    }

    #[test]
    fn converts_into_existing_zebra_address_type() {
        let valid_unified = orchard_unified_address();
        let wcash = WcashAddress::from_unified(NetworkType::Regtest, valid_unified)
            .expect("the fixture has an Orchard receiver and no Sapling receiver");
        let converted = wcash
            .convert_if_network::<crate::primitives::Address>(NetworkType::Regtest)
            .unwrap();

        assert_eq!(converted.network(), crate::parameters::NetworkKind::Regtest);
        assert!(!converted.is_transparent());
    }

    #[test]
    fn rejects_corrupted_and_cross_network_unified_addresses() {
        let unified = orchard_unified_address();
        let regtest = WcashAddress::from_unified(NetworkType::Regtest, unified.clone())
            .expect("the Orchard-only fixture is supported")
            .encode();

        let mut corrupted = regtest.clone().into_bytes();
        let last = corrupted.last_mut().unwrap();
        *last = if *last == b'q' { b'p' } else { b'q' };
        let corrupted = String::from_utf8(corrupted).unwrap();
        assert!(matches!(
            corrupted.parse::<WcashAddress>(),
            Err(WcashAddressParseError::InvalidEncoding(_))
        ));

        // Replacing only the visible HRP invalidates both the Bech32m checksum
        // and ZIP 316's HRP-bound padding.
        let replaced_hrp = regtest.replacen("wuregtest1", "wu1", 1);
        assert!(matches!(
            replaced_hrp.parse::<WcashAddress>(),
            Err(WcashAddressParseError::InvalidEncoding(_))
        ));

        let mainnet = WcashAddress::from_unified(NetworkType::Main, unified)
            .expect("the Orchard-only fixture is supported")
            .encode();
        assert_ne!(mainnet, regtest);
    }

    #[test]
    fn rejects_noncanonical_unified_receiver_order() {
        let hrp = HRP_UNIFIED_REGTEST;
        let mut raw = Receiver::Sapling([0; 43]).typed_encoding();
        raw.extend(Receiver::P2pkh([0; 20]).typed_encoding());

        let mut padding = [0u8; ZIP316_PADDING_LEN];
        padding[..hrp.len()].copy_from_slice(hrp.as_bytes());
        raw.extend(padding);

        let jumbled = f4jumble::f4jumble(&raw).unwrap();
        let encoded = encode_bech32::<Bech32mZip316>(hrp, &jumbled);
        assert!(matches!(
            encoded.parse::<WcashAddress>(),
            Err(WcashAddressParseError::InvalidUnified(message))
                if message.contains("canonical order")
        ));
    }
}

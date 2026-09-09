//! Wire types for Wcash auxiliary-block mining RPCs.

use std::fmt;

use derive_getters::Getters;
use schemars::JsonSchema;
use serde::{de::Visitor, Deserialize, Deserializer, Serialize, Serializer};

use zebra_chain::{
    block,
    work::difficulty::{CompactDifficulty, ExpandedDifficulty},
};

use wcash_zcash_aux::{MAX_PROOF_BYTES, WCASH_AUXILIARY_CHAIN_ID};

/// An unguessable, fixed-size capability for retiring one cached candidate.
#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct RetireToken(
    #[serde(with = "hex")]
    #[schemars(with = "String")]
    [u8; 32],
);

impl RetireToken {
    /// Constructs a retirement capability from cryptographically random bytes.
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the capability bytes for constant-time authentication.
    pub const fn into_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl fmt::Debug for RetireToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RetireToken([REDACTED])")
    }
}

/// The maximum number of hexadecimal characters accepted by `submitauxblock`.
pub const MAX_AUX_POW_HEX_CHARS: usize = MAX_PROOF_BYTES * 2;

/// A canonical, size-bounded hex-encoded Wcash AuxPoW proof.
///
/// The bound is checked before hex decoding, so an oversized request cannot
/// cause a second, proof-sized allocation inside the RPC implementation.
#[derive(Clone, Debug, Eq, PartialEq, JsonSchema)]
pub struct AuxPowHex(#[schemars(with = "String")] Vec<u8>);

impl AuxPowHex {
    /// Constructs a bounded proof parameter from raw proof bytes.
    pub fn new(bytes: Vec<u8>) -> Result<Self, AuxPowHexError> {
        if bytes.len() > MAX_PROOF_BYTES {
            return Err(AuxPowHexError::TooLarge {
                actual: bytes.len(),
                maximum: MAX_PROOF_BYTES,
            });
        }

        Ok(Self(bytes))
    }

    /// Returns the raw canonical proof bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Consumes the parameter and returns its raw proof bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

/// An invalid `submitauxblock` proof parameter.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum AuxPowHexError {
    /// The decoded proof is larger than the consensus proof limit.
    #[error("AuxPoW proof is {actual} bytes; maximum is {maximum} bytes")]
    TooLarge {
        /// Actual decoded proof length.
        actual: usize,
        /// Maximum accepted proof length.
        maximum: usize,
    },
}

impl Serialize for AuxPowHex {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&hex::encode(&self.0))
    }
}

impl<'de> Deserialize<'de> for AuxPowHex {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct AuxPowHexVisitor;

        impl Visitor<'_> for AuxPowHexVisitor {
            type Value = AuxPowHex;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(
                    formatter,
                    "an even-length hexadecimal AuxPoW proof of at most {MAX_AUX_POW_HEX_CHARS} characters"
                )
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                if value.len() > MAX_AUX_POW_HEX_CHARS {
                    return Err(E::invalid_length(value.len(), &self));
                }
                if !value.len().is_multiple_of(2) {
                    return Err(E::custom("AuxPoW proof hex must contain complete bytes"));
                }

                let bytes = hex::decode(value)
                    .map_err(|error| E::custom(format!("invalid AuxPoW proof hex: {error}")))?;
                AuxPowHex::new(bytes).map_err(E::custom)
            }
        }

        deserializer.deserialize_str(AuxPowHexVisitor)
    }
}

/// A proof-independent Wcash block candidate for parent-chain commitment.
///
/// `hash` identifies the exact child block. `data` lets a pool preserve and
/// later submit the candidate through the standard `submitblock` RPC even if
/// the node that created it restarts or evicts its convenience cache.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Getters, JsonSchema)]
pub struct CreateAuxBlockResponse {
    /// Proof-independent Wcash block ID in RPC display byte order.
    #[serde(with = "hex")]
    #[schemars(with = "String")]
    #[getter(copy)]
    hash: block::Hash,

    /// Capability required to retire this candidate before its cache TTL.
    ///
    /// Pools must keep this value private. It authorizes only this exact
    /// candidate and cannot retire a job after an AuxPoW submission begins.
    #[serde(rename = "retiretoken")]
    #[schemars(with = "String")]
    #[getter(copy)]
    retire_token: RetireToken,

    /// Canonical serialized proof-free Wcash candidate.
    ///
    /// Its Wcash solution carrier is empty. A pool attaches the AuxPoW witness
    /// without changing `hash`, then submits the completed block.
    #[serde(with = "hex")]
    #[schemars(with = "String")]
    data: Vec<u8>,

    /// Frozen Wcash auxiliary chain ID.
    #[serde(rename = "chainid")]
    #[getter(copy)]
    chain_id: u32,

    /// Current Wcash tip extended by this candidate.
    #[serde(rename = "previousblockhash", with = "hex")]
    #[schemars(with = "String")]
    #[getter(copy)]
    previous_block_hash: block::Hash,

    /// Total miner reward, including selected transaction fees, in zatoshis.
    ///
    /// This amount is always public supply accounting. The recipient is public
    /// for transparent payouts and hidden for private Ironwood payouts.
    #[serde(rename = "coinbasevalue")]
    #[getter(copy)]
    coinbase_value: i64,

    /// Required parent-work target in 32-byte RPC display order.
    #[serde(with = "hex")]
    #[schemars(with = "String")]
    #[getter(copy)]
    target: ExpandedDifficulty,

    /// Compact representation of `target`.
    #[serde(with = "hex")]
    #[schemars(with = "String")]
    #[getter(copy)]
    bits: CompactDifficulty,

    /// Height encoded in the cached candidate's coinbase transaction.
    #[getter(copy)]
    height: u32,
}

impl CreateAuxBlockResponse {
    /// Constructs a response for one exact cached Wcash candidate.
    pub fn new(
        candidate_lease: (block::Hash, RetireToken),
        data: Vec<u8>,
        previous_block_hash: block::Hash,
        coinbase_value: i64,
        target: ExpandedDifficulty,
        bits: CompactDifficulty,
        height: u32,
    ) -> Self {
        let (hash, retire_token) = candidate_lease;
        Self {
            hash,
            retire_token,
            data,
            chain_id: WCASH_AUXILIARY_CHAIN_ID,
            previous_block_hash,
            coinbase_value,
            target,
            bits,
            height,
        }
    }
}

/// Result of an authenticated `retireauxblock` request.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum RetireAuxBlockResponse {
    /// The active candidate was removed.
    Retired,
    /// The candidate was already absent, including after a retry or restart.
    AlreadyAbsent,
    /// A validly encoded AuxPoW submission began, so the candidate was retained.
    SubmissionStarted,
}

/// Exact witness-bound state of a submitted Wcash auxiliary block.
///
/// Only `best_chain` carries confirmations, and those confirmations are read
/// atomically with the committed block bytes used to compare the AuxPoW
/// witness.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum GetAuxBlockStatusResponse {
    /// The exact witness is committed to the current best chain.
    BestChain {
        /// Confirmations including the candidate block itself.
        confirmations: u32,
    },
    /// The exact witness is committed only to a side chain.
    SideChain,
    /// The proof-independent block ID is committed with different witness bytes.
    ConflictingWitness,
    /// Validation or state writing for this block ID is still in progress.
    Pending,
    /// The node has no committed or pending block with this ID.
    Unknown,
}

#[cfg(test)]
mod tests {
    use super::*;
    use hex::FromHex;

    #[test]
    fn aux_pow_hex_round_trips_and_rejects_bad_encoding() {
        let proof = AuxPowHex::new(vec![0x00, 0xab, 0xff]).expect("small proof is valid");
        let json = serde_json::to_string(&proof).expect("proof serializes");
        assert_eq!(json, "\"00abff\"");
        assert_eq!(
            serde_json::from_str::<AuxPowHex>(&json).expect("proof deserializes"),
            proof
        );

        assert!(serde_json::from_str::<AuxPowHex>("\"0\"").is_err());
        assert!(serde_json::from_str::<AuxPowHex>("\"xz\"").is_err());
    }

    #[test]
    fn aux_pow_hex_rejects_oversized_input_before_decode() {
        let oversized = format!("\"{}\"", "00".repeat(MAX_PROOF_BYTES + 1));
        assert!(serde_json::from_str::<AuxPowHex>(&oversized).is_err());
        assert!(matches!(
            AuxPowHex::new(vec![0; MAX_PROOF_BYTES + 1]),
            Err(AuxPowHexError::TooLarge { .. })
        ));
    }

    #[test]
    fn retire_token_is_fixed_size_and_redacted_from_debug_output() {
        let token = RetireToken::new([0x5a; 32]);
        let encoded = serde_json::to_string(&token).expect("token serializes");
        assert_eq!(encoded, format!("\"{}\"", "5a".repeat(32)));
        assert_eq!(
            serde_json::from_str::<RetireToken>(&encoded).expect("token deserializes"),
            token
        );
        assert!(serde_json::from_str::<RetireToken>("\"5a\"").is_err());
        assert!(!format!("{token:?}").contains("5a5a"));

        for response in [
            RetireAuxBlockResponse::Retired,
            RetireAuxBlockResponse::AlreadyAbsent,
            RetireAuxBlockResponse::SubmissionStarted,
        ] {
            let encoded = serde_json::to_value(response).expect("response serializes");
            let decoded = serde_json::from_value(encoded).expect("response deserializes");
            assert_eq!(response, decoded);
        }
    }

    #[test]
    fn create_aux_block_response_uses_pool_compatible_fields() {
        let hash = block::Hash([0x11; 32]);
        let previous = block::Hash([0x22; 32]);
        let bits = CompactDifficulty::from_hex("1f07ffff").expect("valid compact target");
        let target = bits.to_expanded().expect("valid expanded target");
        let data = vec![0x01, 0x23, 0x45];
        let response = CreateAuxBlockResponse::new(
            (hash, RetireToken::new([0x33; 32])),
            data.clone(),
            previous,
            1_000_000_000,
            target,
            bits,
            42,
        );

        assert_eq!(response.hash(), hash);
        assert_eq!(response.retire_token().into_bytes(), [0x33; 32]);
        assert_eq!(response.data(), data.as_slice());
        assert_eq!(response.chain_id(), WCASH_AUXILIARY_CHAIN_ID);
        assert_eq!(response.previous_block_hash(), previous);
        assert_eq!(response.coinbase_value(), 1_000_000_000);
        assert_eq!(response.target(), target);
        assert_eq!(response.bits(), bits);
        assert_eq!(response.height(), 42);

        let json = serde_json::to_value(response).expect("response serializes");
        assert_eq!(json["hash"], "11".repeat(32));
        assert_eq!(json["data"], "012345");
        assert_eq!(json["chainid"], WCASH_AUXILIARY_CHAIN_ID);
        assert_eq!(json["previousblockhash"], "22".repeat(32));
        assert_eq!(json["coinbasevalue"], 1_000_000_000i64);
        assert_eq!(json["bits"], "1f07ffff");
        assert_eq!(json["height"], 42);
        assert_eq!(json["target"], target.to_string());
    }

    #[test]
    fn aux_block_status_response_is_explicit_and_strict() {
        let best = GetAuxBlockStatusResponse::BestChain { confirmations: 7 };
        assert_eq!(
            serde_json::to_value(&best).expect("status serializes"),
            serde_json::json!({"state": "best_chain", "confirmations": 7})
        );
        assert_eq!(
            serde_json::from_value::<GetAuxBlockStatusResponse>(serde_json::json!({
                "state": "conflicting_witness"
            }))
            .expect("known status deserializes"),
            GetAuxBlockStatusResponse::ConflictingWitness
        );
        assert!(
            serde_json::from_value::<GetAuxBlockStatusResponse>(serde_json::json!({
                "state": "best_chain",
                "confirmations": 1,
                "unexpected": true
            }))
            .is_err()
        );
    }
}

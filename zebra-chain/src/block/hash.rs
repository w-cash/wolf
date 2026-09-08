use std::{fmt, io, sync::Arc};

use byteorder::{LittleEndian, WriteBytesExt};
use hex::{FromHex, ToHex};
use serde::{Deserialize, Serialize};

use crate::serialization::{
    sha256d, BytesInDisplayOrder, ReadZcashExt, SerializationError, ZcashDeserialize,
    ZcashSerialize,
};

use super::Header;
use crate::work::equihash::WCASH_BLOCK_WIRE_VERSION;

/// BLAKE2b personalization for proof-independent Wcash block identifiers.
const WCASH_BLOCK_ID_PERSONALIZATION: &[u8; 16] = b"WcashBlockIdV1\0\0";

#[cfg(any(test, feature = "proptest-impl"))]
use proptest_derive::Arbitrary;

/// A hash of a block, used to identify blocks and link blocks into a chain. ⛓️
///
/// For Zcash, this is the SHA256d hash of the full block header. For Wcash, it
/// is a domain-separated BLAKE2b-256 hash of every header field except the
/// attached AuxPoW witness. Excluding the witness prevents proof malleability
/// from changing the identity of an otherwise identical Wcash block.
///
/// Note: Zebra displays transaction and block hashes in big-endian byte-order,
/// following the u256 convention set by Bitcoin and zcashd.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "proptest-impl"), derive(Arbitrary, Default))]
pub struct Hash(pub [u8; 32]);

impl BytesInDisplayOrder<true> for Hash {
    fn bytes_in_serialized_order(&self) -> [u8; 32] {
        self.0
    }

    fn from_bytes_in_serialized_order(bytes: [u8; 32]) -> Self {
        Hash(bytes)
    }
}

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(&self.encode_hex::<String>())
    }
}

impl fmt::Debug for Hash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_tuple("block::Hash")
            .field(&self.encode_hex::<String>())
            .finish()
    }
}

impl ToHex for &Hash {
    fn encode_hex<T: FromIterator<char>>(&self) -> T {
        self.bytes_in_display_order().encode_hex()
    }

    fn encode_hex_upper<T: FromIterator<char>>(&self) -> T {
        self.bytes_in_display_order().encode_hex_upper()
    }
}

impl ToHex for Hash {
    fn encode_hex<T: FromIterator<char>>(&self) -> T {
        (&self).encode_hex()
    }

    fn encode_hex_upper<T: FromIterator<char>>(&self) -> T {
        (&self).encode_hex_upper()
    }
}

impl FromHex for Hash {
    type Error = <[u8; 32] as FromHex>::Error;

    fn from_hex<T: AsRef<[u8]>>(hex: T) -> Result<Self, Self::Error> {
        let hash = <[u8; 32]>::from_hex(hex)?;

        Ok(Self::from_bytes_in_display_order(&hash))
    }
}

impl From<[u8; 32]> for Hash {
    fn from(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

impl<'a> From<&'a Header> for Hash {
    fn from(block_header: &'a Header) -> Self {
        if block_header.version == WCASH_BLOCK_WIRE_VERSION {
            return Self(wcash_block_id(block_header));
        }

        let mut hash_writer = sha256d::Writer::default();
        block_header
            .zcash_serialize(&mut hash_writer)
            .expect("Sha256dWriter is infallible");
        Self(hash_writer.finish())
    }
}

/// Hashes the fixed, proof-independent portion of a Wcash header.
fn wcash_block_id(header: &Header) -> [u8; 32] {
    let mut bytes = Vec::with_capacity(crate::work::equihash::Solution::INPUT_LENGTH + 32);
    bytes
        .write_u32::<LittleEndian>(header.version)
        .expect("writing to a Vec is infallible");
    header
        .previous_block_hash
        .zcash_serialize(&mut bytes)
        .expect("writing to a Vec is infallible");
    bytes.extend_from_slice(&header.merkle_root.0);
    bytes.extend_from_slice(&header.commitment_bytes[..]);
    bytes
        .write_u32::<LittleEndian>(
            header
                .time
                .timestamp()
                .try_into()
                .expect("deserialized and generated timestamps are u32 values"),
        )
        .expect("writing to a Vec is infallible");
    bytes
        .write_u32::<LittleEndian>(header.difficulty_threshold.0)
        .expect("writing to a Vec is infallible");
    bytes.extend_from_slice(&header.nonce[..]);

    let digest = blake2b_simd::Params::new()
        .hash_length(32)
        .personal(WCASH_BLOCK_ID_PERSONALIZATION)
        .hash(&bytes);
    digest
        .as_bytes()
        .try_into()
        .expect("the requested BLAKE2b digest length is 32 bytes")
}

impl From<Header> for Hash {
    // The borrow is actually needed to use From<&Header>
    #[allow(clippy::needless_borrow)]
    fn from(block_header: Header) -> Self {
        (&block_header).into()
    }
}

impl From<&Arc<Header>> for Hash {
    // The borrow is actually needed to use From<&Header>
    #[allow(clippy::needless_borrow)]
    fn from(block_header: &Arc<Header>) -> Self {
        block_header.as_ref().into()
    }
}

impl From<Arc<Header>> for Hash {
    // The borrow is actually needed to use From<&Header>
    #[allow(clippy::needless_borrow)]
    fn from(block_header: Arc<Header>) -> Self {
        block_header.as_ref().into()
    }
}

impl ZcashSerialize for Hash {
    fn zcash_serialize<W: io::Write>(&self, mut writer: W) -> Result<(), io::Error> {
        writer.write_all(&self.0)?;
        Ok(())
    }
}

impl ZcashDeserialize for Hash {
    fn zcash_deserialize<R: io::Read>(mut reader: R) -> Result<Self, SerializationError> {
        Ok(Hash(reader.read_32_bytes()?))
    }
}

impl std::str::FromStr for Hash {
    type Err = SerializationError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::from_hex(s)?)
    }
}

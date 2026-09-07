//! Minimal bounded reader and canonical CompactSize codec.

use crate::AuxPowError;

pub(crate) struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    pub(crate) const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    pub(crate) fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.position)
    }

    pub(crate) fn read_u8(&mut self, field: &'static str) -> Result<u8, AuxPowError> {
        Ok(self.read_array::<1>(field)?[0])
    }

    pub(crate) fn read_u32_le(&mut self, field: &'static str) -> Result<u32, AuxPowError> {
        Ok(u32::from_le_bytes(self.read_array(field)?))
    }

    pub(crate) fn read_array<const N: usize>(
        &mut self,
        field: &'static str,
    ) -> Result<[u8; N], AuxPowError> {
        let mut array = [0; N];
        array.copy_from_slice(self.read_slice(N, field)?);
        Ok(array)
    }

    pub(crate) fn read_slice(
        &mut self,
        length: usize,
        field: &'static str,
    ) -> Result<&'a [u8], AuxPowError> {
        let remaining = self.remaining();
        if length > remaining {
            return Err(AuxPowError::UnexpectedEnd {
                field,
                needed: length,
                remaining,
            });
        }
        let end = self
            .position
            .checked_add(length)
            .ok_or(AuxPowError::EncodingLengthOverflow)?;
        let slice = &self.bytes[self.position..end];
        self.position = end;
        Ok(slice)
    }

    pub(crate) fn read_compact_size(&mut self) -> Result<usize, AuxPowError> {
        let prefix = self.read_u8("CompactSize prefix")?;
        let value = match prefix {
            0..=0xfc => u64::from(prefix),
            0xfd => {
                let value = u16::from_le_bytes(self.read_array("CompactSize u16")?);
                if value < 0xfd {
                    return Err(AuxPowError::NonCanonicalCompactSize);
                }
                u64::from(value)
            }
            0xfe => {
                let value = self.read_u32_le("CompactSize u32")?;
                if u16::try_from(value).is_ok() {
                    return Err(AuxPowError::NonCanonicalCompactSize);
                }
                u64::from(value)
            }
            0xff => {
                let value = u64::from_le_bytes(self.read_array("CompactSize u64")?);
                if u32::try_from(value).is_ok() {
                    return Err(AuxPowError::NonCanonicalCompactSize);
                }
                value
            }
        };

        usize::try_from(value).map_err(|_| AuxPowError::CompactSizeOverflow(value))
    }
}

pub(crate) fn encode_compact_size(value: usize, output: &mut Vec<u8>) {
    if value <= 0xfc {
        // Safe because this branch bounds `value` to u8.
        output.push(value as u8);
    } else if u16::try_from(value).is_ok() {
        output.push(0xfd);
        // Safe because the conversion was checked above.
        output.extend_from_slice(&(value as u16).to_le_bytes());
    } else if u32::try_from(value).is_ok() {
        output.push(0xfe);
        // Safe because the conversion was checked above.
        output.extend_from_slice(&(value as u32).to_le_bytes());
    } else {
        output.push(0xff);
        // `usize` is no wider than u64 on supported Rust targets.
        output.extend_from_slice(&(value as u64).to_le_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_size_round_trips_only_minimal_forms() {
        for value in [0, 0xfc, 0xfd, usize::from(u16::MAX), 65_536] {
            let mut bytes = Vec::new();
            encode_compact_size(value, &mut bytes);
            let mut reader = Reader::new(&bytes);
            assert_eq!(reader.read_compact_size(), Ok(value));
            assert_eq!(reader.remaining(), 0);
        }

        for bytes in [
            &[0xfd, 0xfc, 0][..],
            &[0xfe, 0xff, 0xff, 0, 0][..],
            &[0xff, 0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0][..],
        ] {
            assert_eq!(
                Reader::new(bytes).read_compact_size(),
                Err(AuxPowError::NonCanonicalCompactSize)
            );
        }
    }
}

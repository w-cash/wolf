//! Authenticated Wcash proof-of-work target.

use crate::AuxPowError;

/// A nonzero 256-bit target in little-endian numeric byte order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Target([u8; 32]);

impl Target {
    /// Easiest representable target, useful for isolated tests.
    pub const MAX: Self = Self([u8::MAX; 32]);

    /// Creates a nonzero target from little-endian numeric bytes.
    pub fn from_le_bytes(bytes: [u8; 32]) -> Result<Self, AuxPowError> {
        if bytes == [0; 32] {
            return Err(AuxPowError::ZeroTarget);
        }
        Ok(Self(bytes))
    }

    /// Returns this target's little-endian numeric bytes.
    pub const fn to_le_bytes(self) -> [u8; 32] {
        self.0
    }

    /// Returns true when `hash_le` is numerically less than or equal to this target.
    pub fn is_met_by_le_hash(self, hash_le: [u8; 32]) -> bool {
        for index in (0..32).rev() {
            match hash_le[index].cmp(&self.0[index]) {
                std::cmp::Ordering::Less => return true,
                std::cmp::Ordering::Greater => return false,
                std::cmp::Ordering::Equal => {}
            }
        }
        true
    }

    /// Returns true when this target is numerically easier than or equal to
    /// `other`, so every hash meeting `other` also meets this target.
    pub fn includes(self, other: Self) -> bool {
        self.is_met_by_le_hash(other.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn little_endian_target_comparison_is_inclusive() {
        let mut bytes = [0; 32];
        bytes[1] = 1;
        let target = Target::from_le_bytes(bytes).expect("target is nonzero");
        assert!(target.is_met_by_le_hash(bytes));

        let mut lower = bytes;
        lower[0] = u8::MAX;
        lower[1] = 0;
        assert!(target.is_met_by_le_hash(lower));

        let mut higher = bytes;
        higher[2] = 1;
        assert!(!target.is_met_by_le_hash(higher));
        assert!(Target::MAX.includes(target));
        assert!(target.includes(target));
        assert!(!target.includes(Target::MAX));
        assert_eq!(Target::from_le_bytes([0; 32]), Err(AuxPowError::ZeroTarget));
    }
}

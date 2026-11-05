//! Explicit parent transaction domains, independent of parser capabilities.

#[cfg(any(feature = "zebra", test))]
use crate::AuxPowError;

/// Consensus rules for the parent coinbase carried by a Wcash AuxPoW proof.
///
/// Parsing support does not authorize a new parent domain. Callers must select
/// the rules for the candidate Wcash block independently from the external
/// parent-chain tip. Adding a variant requires a separately reviewed Wcash
/// activation policy and proof vectors.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum ParentCoinbaseRules {
    /// The production parent-domain set accepted before NU7 compatibility.
    ///
    /// This includes the existing Wcash transaction domains because the pinned
    /// parent parser already recognizes them. Retaining them preserves existing
    /// proof acceptance; it does not assert Zcash canonical-chain inclusion.
    #[default]
    Legacy,
}

impl ParentCoinbaseRules {
    /// Check the exact branch embedded in a canonical v5 or v6 coinbase.
    ///
    /// These literal domains deliberately do not follow an upstream parser's
    /// enum or activation schedule: a dependency update must not silently
    /// expand Wcash consensus. Experimental placeholder domains are excluded.
    #[cfg(any(feature = "zebra", test))]
    pub(crate) fn check_branch(self, branch_id: u32) -> Result<(), AuxPowError> {
        let allowed = match self {
            Self::Legacy => matches!(
                branch_id,
                0x0000_0000
                    | 0x5ba8_1b19
                    | 0x76b8_09bb
                    | 0x2bb4_0e60
                    | 0xf5b9_230b
                    | 0xe9ff_75a6
                    | 0xc2d6_d0b4
                    | 0xc8e7_1055
                    | 0x4dec_4df0
                    | 0x5437_f330
                    | 0x37a5_165b
                    | 0xb3cf_d27e
                    | 0x54ba_2bfb
                    | 0xd9c6_a7ee
                    | 0xc3a6_678a
            ),
        };

        if allowed {
            Ok(())
        } else {
            Err(AuxPowError::UnsupportedParentCoinbaseBranch(branch_id))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_rules_retain_every_production_parser_domain() {
        // The frozen production parser's complete set, including the four
        // Wcash domains. This vector must not be derived from that dependency.
        let legacy_domains = [
            0x0000_0000,
            0x5ba8_1b19,
            0x76b8_09bb,
            0x2bb4_0e60,
            0xf5b9_230b,
            0xe9ff_75a6,
            0xc2d6_d0b4,
            0xc8e7_1055,
            0x4dec_4df0,
            0x5437_f330,
            0x37a5_165b,
            0xb3cf_d27e,
            0x54ba_2bfb,
            0xd9c6_a7ee,
            0xc3a6_678a,
        ];

        for branch_id in legacy_domains {
            assert_eq!(ParentCoinbaseRules::Legacy.check_branch(branch_id), Ok(()));
        }
        assert_eq!(ParentCoinbaseRules::default(), ParentCoinbaseRules::Legacy);
    }

    #[test]
    fn legacy_rules_reject_nu7_unknown_and_experimental_domains() {
        for branch_id in [0x7719_0ad9, 0xdead_beef, 0xffff_ffff, 0xffff_fffe] {
            assert_eq!(
                ParentCoinbaseRules::Legacy.check_branch(branch_id),
                Err(AuxPowError::UnsupportedParentCoinbaseBranch(branch_id))
            );
        }
    }
}

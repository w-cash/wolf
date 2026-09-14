//! Wcash-only key derivation.

use secrecy::{ExposeSecret, SecretVec};
use thiserror::Error;
use zcash_keys::keys::{DerivationError, UnifiedSpendingKey};
use zip32::AccountId;

use crate::WalletNetwork;

const SEED_PERSONALIZATION: &[u8; 16] = b"WcashSeedV1_____";
const DERIVATION_LABEL: &[u8] = b"Wcash wallet seed derivation version 1";

/// Persistent identifier for the exact seed KDF implemented by this module.
///
/// This value must be incremented whenever the personalization, derivation
/// label, input framing, network domain, or genesis binding changes.
pub(crate) const WALLET_SEED_KDF_VERSION: u32 = 1;

/// Errors returned while deriving Wcash wallet keys.
#[derive(Debug, Error)]
pub enum WalletKeyError {
    /// ZIP 32 requires at least 32 bytes of seed entropy.
    #[error("wallet seed must contain between 32 and 252 bytes")]
    InvalidSeedLength,
    /// The requested ZIP 32 account number is invalid.
    #[error("account index must be below 2^31")]
    InvalidAccount,
    /// A component spending key could not be derived.
    #[error("could not derive unified spending key: {0}")]
    Derivation(#[from] DerivationError),
}

/// Derives a Wcash-only ZIP 32 seed from caller-owned entropy.
///
/// The KDF binds the result to Wcash, the selected network, and its frozen
/// genesis identifier before any standard ZIP 32 derivation occurs. Supplying
/// the same master entropy to a Zcash wallet therefore does not reuse keys.
/// The returned secret must not be logged or persisted as plaintext.
pub fn derive_wallet_seed(
    master_seed: &SecretVec<u8>,
    network: WalletNetwork,
) -> Result<SecretVec<u8>, WalletKeyError> {
    let master_seed = master_seed.expose_secret();
    if !(32..=252).contains(&master_seed.len()) {
        return Err(WalletKeyError::InvalidSeedLength);
    }

    let mut state = blake2b_simd::Params::new()
        .hash_length(64)
        .personal(SEED_PERSONALIZATION)
        .to_state();
    state.update(DERIVATION_LABEL);
    let seed_len = u16::try_from(master_seed.len()).expect("validated seed length fits u16");
    state.update(&seed_len.to_le_bytes());
    state.update(master_seed);
    state.update(&[network.domain_byte()]);
    state.update(&network.genesis_hash());

    Ok(SecretVec::new(state.finalize().as_bytes().to_vec()))
}

/// Derives one Wcash unified spending key from caller-owned entropy.
pub fn derive_wallet_spending_key(
    master_seed: &SecretVec<u8>,
    network: WalletNetwork,
    account: u32,
) -> Result<UnifiedSpendingKey, WalletKeyError> {
    let account = AccountId::try_from(account).map_err(|_| WalletKeyError::InvalidAccount)?;
    let seed = derive_wallet_seed(master_seed, network)?;
    UnifiedSpendingKey::from_seed(&network.parameters(), seed.expose_secret(), account)
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seed(byte: u8) -> SecretVec<u8> {
        SecretVec::new(vec![byte; 32])
    }

    #[test]
    fn seed_domain_is_deterministic_and_network_bound() {
        let master = seed(7);
        let first = derive_wallet_seed(&master, WalletNetwork::Testnet).unwrap();
        let second = derive_wallet_seed(&master, WalletNetwork::Testnet).unwrap();
        let regtest = derive_wallet_seed(&master, WalletNetwork::Regtest).unwrap();

        assert_eq!(first.expose_secret(), second.expose_secret());
        assert_ne!(first.expose_secret(), regtest.expose_secret());
        assert_ne!(first.expose_secret(), master.expose_secret());
        assert_eq!(first.expose_secret().len(), 64);
    }

    #[test]
    fn testnet_v5_seed_domain_has_a_stable_vector() {
        let master = SecretVec::new((0u8..32).collect());
        let derived = derive_wallet_seed(&master, WalletNetwork::Testnet).unwrap();

        assert_eq!(
            hex::encode(derived.expose_secret()),
            "92dc36b870456da70d67656db7730a849c4921e77d80cf525f8f82001d0c43b547cc6b5c96813175927bd0985bd21e5f3f2bc0e3bac790d046e9dc5062e6e158"
        );
    }

    #[test]
    fn seed_length_is_checked_before_kdf() {
        assert!(matches!(
            derive_wallet_seed(&SecretVec::new(vec![0; 31]), WalletNetwork::Testnet),
            Err(WalletKeyError::InvalidSeedLength)
        ));
        assert!(matches!(
            derive_wallet_seed(&SecretVec::new(vec![0; 253]), WalletNetwork::Testnet),
            Err(WalletKeyError::InvalidSeedLength)
        ));
    }

    #[test]
    fn account_derivation_is_deterministic_and_separated() {
        let master = seed(42);
        let a = derive_wallet_spending_key(&master, WalletNetwork::Testnet, 0).unwrap();
        let b = derive_wallet_spending_key(&master, WalletNetwork::Testnet, 0).unwrap();
        let other = derive_wallet_spending_key(&master, WalletNetwork::Testnet, 1).unwrap();

        assert_eq!(a.orchard().to_bytes(), b.orchard().to_bytes());
        assert_ne!(a.orchard().to_bytes(), other.orchard().to_bytes());
    }
}

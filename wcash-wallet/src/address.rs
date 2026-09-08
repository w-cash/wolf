//! Wcash address conversion at the wallet boundary.

use thiserror::Error;
use zcash_keys::{
    address::UnifiedAddress,
    keys::{UnifiedAddressRequest, UnifiedFullViewingKey},
};
use zebra_chain::primitives::{WcashAddress, WcashAddressKind, WcashAddressParseError};

use crate::WalletNetwork;

/// Errors returned while encoding or decoding wallet addresses.
#[derive(Debug, Error)]
pub enum WalletAddressError {
    /// The wallet could not derive an Orchard-only receiver.
    #[error("could not derive an Orchard-only address: {0}")]
    Derivation(String),
    /// The encoded value is not a canonical Wcash address.
    #[error("invalid Wcash address: {0}")]
    Parse(#[from] WcashAddressParseError),
    /// The address belongs to another Wcash network.
    #[error("address is for a different Wcash network")]
    WrongNetwork,
    /// Only a Wcash Unified Address with an Orchard receiver can receive Ironwood funds.
    #[error("recipient must be a Wcash Unified Address with an Orchard receiver")]
    MissingOrchardReceiver,
    /// The Unified Address receiver payload is malformed.
    #[error("invalid Unified Address receiver payload: {0}")]
    InvalidUnified(&'static str),
}

/// Encodes the default Orchard-only receiver of a wallet as a Wcash address.
pub fn encode_orchard_receiver(
    ufvk: &UnifiedFullViewingKey,
    network: WalletNetwork,
) -> Result<String, WalletAddressError> {
    let (address, _) = ufvk
        .default_address(UnifiedAddressRequest::ORCHARD)
        .map_err(|error| WalletAddressError::Derivation(error.to_string()))?;
    let container = address
        .to_zcash_address(network.address_network())
        .convert::<WcashAddress>()
        .map_err(|_| WalletAddressError::InvalidUnified("receiver conversion failed"))?;

    Ok(container.encode())
}

/// Decodes a Wcash recipient into the receiver type expected by librustzcash.
///
/// Zcash textual addresses are rejected. Extra receivers in a Wcash UA are
/// retained, but an Orchard receiver is mandatory because this wallet creates
/// transfers only in the private Ironwood pool.
pub fn decode_recipient(
    encoded: &str,
    network: WalletNetwork,
) -> Result<UnifiedAddress, WalletAddressError> {
    let address = WcashAddress::try_from_encoded(encoded)?;
    if address.network() != network.address_network() {
        return Err(WalletAddressError::WrongNetwork);
    }

    let WcashAddressKind::Unified(container) = address.kind() else {
        return Err(WalletAddressError::MissingOrchardReceiver);
    };
    let address =
        UnifiedAddress::try_from(container.clone()).map_err(WalletAddressError::InvalidUnified)?;
    if address.orchard().is_none() {
        return Err(WalletAddressError::MissingOrchardReceiver);
    }

    Ok(address)
}

#[cfg(test)]
mod tests {
    use secrecy::SecretVec;

    use super::*;
    use crate::derive_wallet_spending_key;

    fn test_ufvk(network: WalletNetwork) -> UnifiedFullViewingKey {
        derive_wallet_spending_key(&SecretVec::new(vec![19; 32]), network, 0)
            .unwrap()
            .to_unified_full_viewing_key()
    }

    #[test]
    fn wallet_addresses_are_wcash_orchard_only() {
        let encoded =
            encode_orchard_receiver(&test_ufvk(WalletNetwork::Testnet), WalletNetwork::Testnet)
                .unwrap();
        assert!(encoded.starts_with("wutest1"));

        let decoded = decode_recipient(&encoded, WalletNetwork::Testnet).unwrap();
        assert!(decoded.orchard().is_some());
        assert!(decoded.sapling().is_none());
        assert!(decoded.transparent().is_none());
    }

    #[test]
    fn cross_network_and_zcash_addresses_are_rejected() {
        let ufvk = test_ufvk(WalletNetwork::Testnet);
        let encoded = encode_orchard_receiver(&ufvk, WalletNetwork::Testnet).unwrap();
        assert!(matches!(
            decode_recipient(&encoded, WalletNetwork::Regtest),
            Err(WalletAddressError::WrongNetwork)
        ));

        let (ua, _) = ufvk
            .default_address(UnifiedAddressRequest::ORCHARD)
            .unwrap();
        let zcash = ua.encode(&WalletNetwork::Testnet.parameters());
        assert!(matches!(
            decode_recipient(&zcash, WalletNetwork::Testnet),
            Err(WalletAddressError::Parse(WcashAddressParseError::NotWcash))
        ));
    }

    #[test]
    fn fixed_master_seed_has_stable_wcash_addresses() {
        let master_seed = SecretVec::new((0u8..32).collect());
        let testnet = derive_wallet_spending_key(&master_seed, WalletNetwork::Testnet, 0)
            .unwrap()
            .to_unified_full_viewing_key();
        let regtest = derive_wallet_spending_key(&master_seed, WalletNetwork::Regtest, 0)
            .unwrap()
            .to_unified_full_viewing_key();

        assert_eq!(
            encode_orchard_receiver(&testnet, WalletNetwork::Testnet).unwrap(),
            "wutest1qpdhnp6kuttafgxvkyz23a0g0ezcl2v0a6y76dst0xn8mahxkfwrxw7e08ty9du42ur2e44u3nam6s7hv0f8zkea83t5r7f4lsfhdzjd"
        );
        assert_eq!(
            encode_orchard_receiver(&regtest, WalletNetwork::Regtest).unwrap(),
            "wuregtest1m6qlf78t724tks6lxvpy7dylmuae5df0xrwaacykakred0jv8tez5v4lqhwhwvrpg9wp4qyf5ty5a9z9ultvqf9h3yd6rgdh6vvdnemk"
        );
    }
}

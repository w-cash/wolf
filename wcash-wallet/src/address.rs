//! Wcash address conversion at the wallet boundary.

use thiserror::Error;
use zcash_keys::{
    address::UnifiedAddress,
    keys::{UnifiedAddressRequest, UnifiedFullViewingKey},
};
use zcash_transparent::{address::TransparentAddress, keys::IncomingViewingKey};
use zebra_chain::primitives::{WcashAddress, WcashAddressKind, WcashAddressParseError};

use crate::WalletNetwork;

/// Errors returned while encoding or decoding wallet addresses.
#[derive(Debug, Error)]
pub enum WalletAddressError {
    /// The wallet could not derive an Orchard-only receiver.
    #[error("could not derive an Orchard-only address: {0}")]
    Derivation(String),
    /// The wallet full viewing key has no usable transparent component.
    #[error("could not derive the default transparent coinbase address: {0}")]
    TransparentDerivation(String),
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

/// Encodes the wallet's default external P2PKH receiver in the Wcash namespace.
///
/// This receiver is derived from the transparent component of the same
/// domain-separated unified spending key that owns the wallet's private
/// Ironwood receiver. It is intended only for receiving coinbase payouts that
/// will later be shielded by this wallet.
pub fn encode_transparent_coinbase_receiver(
    ufvk: &UnifiedFullViewingKey,
    network: WalletNetwork,
) -> Result<String, WalletAddressError> {
    default_transparent_receiver(ufvk)
        .map(|receiver| encode_wcash_transparent_receiver(receiver, network))
}

pub(crate) fn default_transparent_receiver(
    ufvk: &UnifiedFullViewingKey,
) -> Result<TransparentAddress, WalletAddressError> {
    let account_key = ufvk.transparent().ok_or_else(|| {
        WalletAddressError::TransparentDerivation(
            "unified full viewing key has no transparent component".to_owned(),
        )
    })?;
    let incoming = account_key
        .derive_external_ivk()
        .map_err(|error| WalletAddressError::TransparentDerivation(error.to_string()))?;
    Ok(incoming.default_address().0)
}

pub(crate) fn encode_wcash_transparent_receiver(
    receiver: TransparentAddress,
    network: WalletNetwork,
) -> String {
    match receiver {
        TransparentAddress::PublicKeyHash(bytes) => {
            WcashAddress::from_transparent_p2pkh(network.address_network(), bytes).encode()
        }
        TransparentAddress::ScriptHash(bytes) => {
            WcashAddress::from_transparent_p2sh(network.address_network(), bytes).encode()
        }
    }
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
    fn wallet_exposes_a_disjoint_default_transparent_coinbase_receiver() {
        let ufvk = test_ufvk(WalletNetwork::Testnet);
        let encoded = encode_transparent_coinbase_receiver(&ufvk, WalletNetwork::Testnet).unwrap();
        assert!(encoded.starts_with("WT"));
        assert!(WcashAddress::try_from_encoded(&encoded).is_ok());

        let zcash = zcash_keys::encoding::encode_transparent_address_p(
            &WalletNetwork::Testnet.parameters(),
            &default_transparent_receiver(&ufvk).unwrap(),
        );
        assert!(zcash.starts_with("tm"));
        assert_ne!(encoded, zcash);
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
            "wutest1y8vqv8yds0dsh0fd8q8nm37q84fkjf33vy8e4cwyx29yl55avhq4udaevvtygde7ae5pvav8y02yqrggm36gnmphazpaqerxj5ja8grr"
        );
        assert_eq!(
            encode_orchard_receiver(&regtest, WalletNetwork::Regtest).unwrap(),
            "wuregtest1xryxj7ddyajw4mv7jpelftnfhkwu3v5w03smp88kk6fkmfvlewpzrs26pxqs4wycul43485lg0h9ry8zzxkj9q8gvh7dmg0uh5e2t28k"
        );
        assert_eq!(
            encode_transparent_coinbase_receiver(&testnet, WalletNetwork::Testnet).unwrap(),
            "WTES2x4gFzRan36RGCoc2ZNb9SwFMQf2Pbg"
        );
        assert_eq!(
            encode_transparent_coinbase_receiver(&regtest, WalletNetwork::Regtest).unwrap(),
            "WRHUq9CTLZFa52NmAyHH5usN21Q8jZVskN1"
        );
    }
}

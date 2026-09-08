//! Response type for the `validateaddress` RPC.

use derive_getters::Getters;
use derive_new::new;
use jsonrpsee::core::RpcResult;
use zebra_chain::{
    parameters::{Network, NetworkKind},
    primitives,
};

use crate::methods::types::validate_address;

/// `validateaddress` response
#[derive(
    Clone, Default, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, Getters, new,
)]
pub struct ValidateAddressResponse {
    /// Whether the address is valid.
    ///
    /// If not, this is the only property returned.
    #[serde(rename = "isvalid")]
    pub(crate) is_valid: bool,

    /// The zcash address that has been validated.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) address: Option<String>,

    /// If the key is a script.
    #[serde(rename = "isscript")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[getter(copy)]
    pub(crate) is_script: Option<bool>,
}

impl ValidateAddressResponse {
    /// Creates an empty response with `isvalid` of false.
    pub fn invalid() -> Self {
        Self::default()
    }
}

/// Checks if a zcash transparent address of type P2PKH, P2SH or TEX is valid.
/// Returns information about the given address if valid.
pub fn validate_address(
    network: Network,
    raw_address: String,
) -> RpcResult<ValidateAddressResponse> {
    let uses_wcash_consensus = network.uses_wcash_consensus();
    let address = if uses_wcash_consensus {
        let Ok(address) = raw_address.parse::<primitives::WcashAddress>() else {
            return Ok(ValidateAddressResponse::invalid());
        };
        address.convert::<primitives::Address>()
    } else {
        let Ok(address) = raw_address.parse::<zcash_address::ZcashAddress>() else {
            return Ok(ValidateAddressResponse::invalid());
        };
        address.convert::<primitives::Address>()
    };

    let address = match address {
        Ok(address) => address,
        Err(err) => {
            tracing::debug!(?err, "address conversion error");
            return Ok(ValidateAddressResponse::invalid());
        }
    };

    // We want to match zcashd's behaviour
    if !address.is_transparent() {
        return Ok(ValidateAddressResponse::invalid());
    }

    // Wcash has distinct Testnet and Regtest namespaces. Zcash transparent
    // Testnet and Regtest addresses historically share an encoding, so preserve
    // the existing mainnet/non-mainnet comparison for Zcash consensus.
    let address_matches_network = if uses_wcash_consensus {
        address.network() == network.kind()
    } else {
        let addr_is_mainnet = matches!(address.network(), NetworkKind::Mainnet);
        let net_is_mainnet = network.kind() == NetworkKind::Mainnet;
        addr_is_mainnet == net_is_mainnet
    };

    if !address_matches_network {
        tracing::info!(
            ?network,
            address_network = ?address.network(),
            "invalid address in validateaddress RPC: Zebra's configured network must match address network"
        );
        return Ok(validate_address::ValidateAddressResponse::invalid());
    }

    Ok(validate_address::ValidateAddressResponse {
        address: Some(raw_address),
        is_valid: true,
        is_script: Some(address.is_script_hash()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use zcash_protocol::consensus::NetworkType;

    #[test]
    fn wcash_consensus_accepts_only_matching_transparent_addresses() {
        for (network, expected, other) in [
            (
                Network::new_wcash_testnet(),
                NetworkType::Test,
                NetworkType::Regtest,
            ),
            (
                Network::new_wcash_regtest(),
                NetworkType::Regtest,
                NetworkType::Test,
            ),
        ] {
            let matching =
                primitives::WcashAddress::from_transparent_p2pkh(expected, [0; 20]).encode();
            assert_eq!(
                validate_address(network.clone(), matching.clone()).unwrap(),
                ValidateAddressResponse {
                    is_valid: true,
                    address: Some(matching),
                    is_script: Some(false),
                }
            );

            let wrong_network =
                primitives::WcashAddress::from_transparent_p2pkh(other, [0; 20]).encode();
            assert_eq!(
                validate_address(network.clone(), wrong_network).unwrap(),
                ValidateAddressResponse::invalid(),
            );

            assert_eq!(
                validate_address(network, "tmVqEASZxBNKFTbmASZikGa5fPLkd68iJyx".to_string(),)
                    .unwrap(),
                ValidateAddressResponse::invalid(),
            );
        }
    }
}

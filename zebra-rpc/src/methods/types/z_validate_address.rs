//! Response type for the `z_validateaddress` RPC.

use derive_getters::Getters;
use derive_new::new;
use jsonrpsee::core::RpcResult;

use zebra_chain::{
    parameters::{Network, NetworkKind},
    primitives::{Address, WcashAddress},
};

/// `z_validateaddress` response
#[derive(
    Clone, Default, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, Getters, new,
)]
pub struct ZValidateAddressResponse {
    /// Whether the address is valid.
    ///
    /// If not, this is the only property returned.
    #[serde(rename = "isvalid")]
    pub(crate) is_valid: bool,

    /// The zcash address that has been validated.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) address: Option<String>,

    /// The type of the address.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[getter(copy)]
    pub(crate) address_type: Option<ZValidateAddressType>,

    /// Whether the address is yours or not.
    ///
    /// Always false for now since Zebra doesn't have a wallet yet.
    #[serde(rename = "ismine")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[getter(copy)]
    pub(crate) is_mine: Option<bool>,
}

impl ZValidateAddressResponse {
    /// Creates an empty response with `isvalid` of false.
    pub fn invalid() -> Self {
        Self::default()
    }
}

/// Address types supported by the `z_validateaddress` RPC according to
/// <https://zcash.github.io/rpc/z_validateaddress.html>.
#[derive(Copy, Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ZValidateAddressType {
    /// The `p2pkh` address type.
    P2pkh,
    /// The `p2sh` address type.
    P2sh,
    /// The `sapling` address type.
    Sapling,
    /// The `unified` address type.
    Unified,
}

impl From<&Address> for ZValidateAddressType {
    fn from(address: &Address) -> Self {
        match address {
            Address::Transparent(_) => {
                if address.is_script_hash() {
                    Self::P2sh
                } else {
                    Self::P2pkh
                }
            }
            Address::Sapling { .. } => Self::Sapling,
            Address::Unified { .. } => Self::Unified,
        }
    }
}

/// Checks if a zcash address of type P2PKH, P2SH, TEX, SAPLING or UNIFIED is valid.
/// Returns information about the given address if valid.
pub fn z_validate_address(
    network: Network,
    raw_address: String,
) -> RpcResult<ZValidateAddressResponse> {
    let uses_wcash_consensus = network.uses_wcash_consensus();
    let address = if uses_wcash_consensus {
        let Ok(address) = raw_address.parse::<WcashAddress>() else {
            return Ok(ZValidateAddressResponse::invalid());
        };
        address.convert::<Address>()
    } else {
        let Ok(address) = raw_address.parse::<zcash_address::ZcashAddress>() else {
            return Ok(ZValidateAddressResponse::invalid());
        };
        address.convert::<Address>()
    };

    let address = match address {
        Ok(address) => address,
        Err(err) => {
            tracing::debug!(?err, "address conversion error");
            return Ok(ZValidateAddressResponse::invalid());
        }
    };

    let is_transparent = matches!(address, Address::Transparent(_));

    // Wcash gives Regtest its own namespace. Preserve Zcash's historical
    // transparent Testnet/Regtest namespace sharing on Zcash consensus.
    let expected_kind = match (uses_wcash_consensus, is_transparent, network.kind()) {
        (true, _, _) => NetworkKind::Regtest,
        (false, true, NetworkKind::Regtest) => NetworkKind::Testnet,
        (false, _, network_kind) => network_kind,
    };

    if address.network() == expected_kind {
        Ok(ZValidateAddressResponse {
            is_valid: true,
            address: Some(raw_address),
            address_type: Some(ZValidateAddressType::from(&address)),
            is_mine: Some(false),
        })
    } else {
        tracing::info!(
            ?network,
            address_network = ?address.network(),
            "invalid address network in z_validateaddress RPC: address is for {:?} but Zebra is on {:?}",
            address.network(),
            network
        );

        Ok(ZValidateAddressResponse::invalid())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zcash_protocol::consensus::NetworkType;

    #[test]
    fn wcash_consensus_accepts_only_wcash_regtest_addresses() {
        let network = Network::new_wcash_regtest();
        let wcash_regtest =
            WcashAddress::from_transparent_p2sh(NetworkType::Regtest, [0; 20]).encode();

        assert_eq!(
            z_validate_address(network.clone(), wcash_regtest.clone()).unwrap(),
            ZValidateAddressResponse {
                is_valid: true,
                address: Some(wcash_regtest),
                address_type: Some(ZValidateAddressType::P2sh),
                is_mine: Some(false),
            }
        );

        let wcash_testnet =
            WcashAddress::from_transparent_p2sh(NetworkType::Test, [0; 20]).encode();
        assert_eq!(
            z_validate_address(network.clone(), wcash_testnet).unwrap(),
            ZValidateAddressResponse::invalid(),
        );

        assert_eq!(
            z_validate_address(
                network,
                "zregtestsapling1jalqhycwumq3unfxlzyzcktq3n478n82k2wacvl8gwfxk6ahshkxmtp2034qj28n7gl92ka5wca"
                    .to_string(),
            )
            .unwrap(),
            ZValidateAddressResponse::invalid(),
        );
    }
}

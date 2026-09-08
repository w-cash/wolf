//! Wcash wallet network selection.

use serde::{Deserialize, Serialize};
use zcash_protocol::consensus::NetworkType;
use zebra_chain::parameters::Network;

/// A public Wcash network supported by the testnet wallet.
///
/// Mainnet is intentionally absent until its genesis and transaction domain are
/// frozen and independently reviewed.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WalletNetwork {
    /// Public Wcash Testnet.
    Testnet,
    /// Local Wcash Regtest.
    Regtest,
}

impl WalletNetwork {
    /// Returns Zebra's consensus parameters for this network.
    pub fn parameters(self) -> Network {
        match self {
            Self::Testnet => Network::new_wcash_testnet(),
            Self::Regtest => Network::new_wcash_regtest(),
        }
    }

    /// Returns the address-network discriminator used by Wcash encodings.
    pub fn address_network(self) -> NetworkType {
        match self {
            Self::Testnet => NetworkType::Test,
            Self::Regtest => NetworkType::Regtest,
        }
    }

    /// Returns the frozen genesis block identifier in internal/serialized byte order.
    pub fn genesis_hash(self) -> [u8; 32] {
        self.parameters().genesis_hash().0
    }

    /// Returns a stable domain byte used by the wallet seed KDF.
    pub(crate) const fn domain_byte(self) -> u8 {
        match self {
            Self::Testnet => 1,
            Self::Regtest => 2,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zcash_protocol::consensus::{BlockHeight, NetworkUpgrade, Parameters};

    #[test]
    fn supported_networks_have_disjoint_consensus_identity() {
        assert_ne!(
            WalletNetwork::Testnet.genesis_hash(),
            WalletNetwork::Regtest.genesis_hash()
        );
        assert_ne!(
            WalletNetwork::Testnet.address_network(),
            WalletNetwork::Regtest.address_network()
        );
    }

    #[test]
    fn wcash_launch_parameters_expose_cumulative_shielded_activations() {
        for network in [WalletNetwork::Testnet, WalletNetwork::Regtest] {
            let parameters = network.parameters();
            let launch = Some(BlockHeight::from_u32(1));
            assert_eq!(
                parameters.activation_height(NetworkUpgrade::Sapling),
                launch
            );
            assert_eq!(parameters.activation_height(NetworkUpgrade::Nu5), launch);
            assert_eq!(parameters.activation_height(NetworkUpgrade::Nu6_3), launch);
            assert_eq!(
                parameters.branch_id_for_upgrade(NetworkUpgrade::Nu6_3),
                zcash_protocol::consensus::BranchId::WcashTestnetV1,
            );
        }
    }
}

//! Wcash wallet network selection.

use serde::{Deserialize, Serialize};
use zcash_protocol::consensus::{BranchId, NetworkType};
use zebra_chain::parameters::Network;

/// A Wcash network with a frozen genesis and transaction domain.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WalletNetwork {
    /// Public Wcash Mainnet.
    Mainnet,
    /// Public Wcash Testnet.
    Testnet,
    /// Local Wcash Regtest.
    Regtest,
}

impl WalletNetwork {
    /// Returns Zebra's consensus parameters for this network.
    pub fn parameters(self) -> Network {
        match self {
            Self::Mainnet => Network::try_new_wcash_mainnet()
                .expect("frozen Wcash Mainnet parameters must be available"),
            Self::Testnet => Network::new_wcash_testnet(),
            Self::Regtest => Network::new_wcash_regtest(),
        }
    }

    /// Returns the address-network discriminator used by Wcash encodings.
    pub fn address_network(self) -> NetworkType {
        match self {
            Self::Mainnet => NetworkType::Main,
            Self::Testnet => NetworkType::Test,
            Self::Regtest => NetworkType::Regtest,
        }
    }

    /// Returns this network's transaction signature and transaction-ID domain.
    pub const fn branch_id(self) -> BranchId {
        match self {
            Self::Mainnet => BranchId::WcashMainnetV1,
            Self::Testnet => BranchId::WcashTestnetV3,
            Self::Regtest => BranchId::WcashRegtestV1,
        }
    }

    /// Returns this network's consensus branch ID in display byte order.
    pub fn branch_id_hex(self) -> String {
        format!("{:08x}", u32::from(self.branch_id()))
    }

    /// Returns the frozen genesis block identifier in internal/serialized byte order.
    pub fn genesis_hash(self) -> [u8; 32] {
        self.parameters().genesis_hash().0
    }

    /// Returns a stable domain byte used by the wallet seed KDF.
    pub(crate) const fn domain_byte(self) -> u8 {
        match self {
            Self::Mainnet => 3,
            Self::Testnet => 1,
            Self::Regtest => 2,
        }
    }

    /// Returns the stable network identifier persisted in wallet databases.
    pub(crate) const fn database_identity_name(self) -> &'static str {
        match self {
            Self::Mainnet => "wcash-mainnet",
            Self::Testnet => "wcash-testnet",
            Self::Regtest => "wcash-regtest",
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
            WalletNetwork::Mainnet.genesis_hash(),
            WalletNetwork::Testnet.genesis_hash()
        );
        assert_ne!(
            WalletNetwork::Mainnet.address_network(),
            WalletNetwork::Testnet.address_network()
        );
        assert_ne!(
            WalletNetwork::Mainnet.branch_id(),
            WalletNetwork::Testnet.branch_id()
        );
        assert_ne!(
            WalletNetwork::Testnet.genesis_hash(),
            WalletNetwork::Regtest.genesis_hash()
        );
        assert_ne!(
            WalletNetwork::Testnet.address_network(),
            WalletNetwork::Regtest.address_network()
        );
        assert_ne!(
            WalletNetwork::Testnet.branch_id(),
            WalletNetwork::Regtest.branch_id()
        );
    }

    #[test]
    fn wcash_launch_parameters_expose_cumulative_shielded_activations() {
        for network in [
            WalletNetwork::Mainnet,
            WalletNetwork::Testnet,
            WalletNetwork::Regtest,
        ] {
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
                network.branch_id(),
            );
        }
    }
}

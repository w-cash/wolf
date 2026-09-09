//! Initialization and arbitrary data generation functions for zebra-state.

use std::sync::Arc;

use proptest::prelude::*;

use zebra_chain::{
    block::{self, Block, ChainHistoryBlockTxAuthCommitmentHash, Height},
    history_tree::HistoryTree,
    parallel::tree::NoteCommitmentTrees,
    parameters::{
        Network::{self, *},
        NetworkUpgrade,
    },
    serialization::ZcashDeserializeInto,
    transaction::{LockTime, Transaction},
    transparent,
};

use crate::{
    service::{
        check, finalized_state::FinalizedState, non_finalized_state::NonFinalizedState, read,
    },
    tests::FakeChainHelper,
    CheckpointVerifiedBlock, Config,
};

/// Returns a built-in network compatible with the consensus profile selected
/// for this test binary.
///
/// State tests must not accidentally exercise Zcash fixtures through a Wcash
/// state service (or vice versa), because production initialization rejects
/// that profile mismatch before opening the database.
pub(crate) fn test_network() -> Network {
    if Network::Mainnet.is_compatible_with_compiled_consensus() {
        Network::Mainnet
    } else {
        Network::new_wcash_regtest()
    }
}

/// Returns the built-in Zcash networks only when this binary uses Zcash
/// consensus. This is for frozen historical-vector tests that have no Wcash
/// equivalent, not for generic state tests.
pub(crate) fn zcash_test_networks() -> impl Iterator<Item = Network> {
    Network::iter().filter(Network::is_compatible_with_compiled_consensus)
}

/// Returns true when this test binary was compiled for Wcash consensus.
pub(crate) fn uses_wcash_consensus() -> bool {
    !Network::Mainnet.is_compatible_with_compiled_consensus()
}

/// Returns the genesis block belonging to `network`.
pub(crate) fn test_genesis(network: &Network) -> Arc<Block> {
    if network.is_wcash_testnet() {
        block::genesis::wcash_testnet_genesis_block()
    } else if network.is_wcash_regtest() {
        block::genesis::wcash_regtest_genesis_block()
    } else if *network == Network::Mainnet {
        zebra_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
            .zcash_deserialize_into()
            .expect("mainnet genesis block should deserialize")
    } else {
        zebra_test::vectors::BLOCK_TESTNET_GENESIS_BYTES
            .zcash_deserialize_into()
            .expect("testnet genesis block should deserialize")
    }
}

/// Builds transparent-only fake children with valid Wcash block commitments.
///
/// Wcash activates Ironwood at height 1, so inherited pre-Heartwood fixtures
/// cannot bypass chain-history checks. This helper exercises the real Wcash
/// history schedule starting from its built-in genesis block.
pub(crate) fn wcash_fake_children(network: &Network, len: usize) -> Vec<Arc<Block>> {
    wcash_fake_children_with_transactions(network, vec![Vec::new(); len])
}

/// Builds Wcash fake children with the supplied non-coinbase transactions.
///
/// Each outer vector entry belongs to one successive child. Unlike
/// [`wcash_fake_children`], this helper also updates the pool note-commitment
/// trees, so Ironwood transactions get the same history roots as state code.
pub(crate) fn wcash_fake_children_with_transactions(
    network: &Network,
    extra_transactions: Vec<Vec<Arc<Transaction>>>,
) -> Vec<Arc<Block>> {
    use zebra_chain::{
        block::CHAIN_HISTORY_ACTIVATION_RESERVED,
        primitives::zcash_history::BlockCommitmentTreeRoots,
    };

    assert!(network.uses_wcash_consensus());

    let mut previous = test_genesis(network);
    let mut history_tree = HistoryTree::default();
    let mut note_commitment_trees = NoteCommitmentTrees::default();
    let mut children = Vec::with_capacity(extra_transactions.len());

    for transactions in extra_transactions {
        let previous_height = previous
            .coinbase_height()
            .expect("fake Wcash parent has a coinbase height");
        let previous_hash = previous.hash();
        let mut child = previous.as_ref().clone();
        let coinbase = Arc::try_unwrap(child.transactions.remove(0))
            .unwrap_or_else(|transaction| transaction.as_ref().clone());
        let coinbase = coinbase.with_transparent_inputs(vec![transparent::Input::Coinbase {
            height: Height(previous_height.0 + 1),
            data: b"Wcash state test".to_vec(),
            sequence: u32::MAX,
        }]);
        child.transactions.insert(0, Arc::new(coinbase));
        child.transactions.extend(transactions);
        Arc::make_mut(&mut child.header).previous_block_hash = previous_hash;
        let mut child = Arc::new(child);
        let height = child
            .coinbase_height()
            .expect("fake Wcash child has a coinbase height");
        let history_root = history_tree
            .hash()
            .or_else(|| {
                (NetworkUpgrade::Heartwood.activation_height(network) == Some(height))
                    .then_some(CHAIN_HISTORY_ACTIVATION_RESERVED.into())
            })
            .expect("Wcash history exists after its activation block");
        let commitment = ChainHistoryBlockTxAuthCommitmentHash::from_commitments(
            &history_root,
            &child.auth_data_root(),
        );
        child = child.set_block_commitment(commitment.into());

        note_commitment_trees
            .update_trees_parallel(&child)
            .expect("fake Wcash child updates its note-commitment trees");
        history_tree
            .push(
                network,
                child.clone(),
                BlockCommitmentTreeRoots {
                    sapling: &note_commitment_trees.sapling.root(),
                    orchard: &note_commitment_trees.orchard.root(),
                    ironwood: &note_commitment_trees.ironwood.root(),
                },
            )
            .expect("fake Wcash child updates its history tree");

        previous = child.clone();
        children.push(child);
    }

    children
}

/// Generate a chain that allows us to make tests for the legacy chain rules.
///
/// Arguments:
/// - `transaction_version_override`: See `LedgerState::height_strategy` for details.
/// - `transaction_has_valid_network_upgrade`: See `LedgerState::height_strategy` for details.
///   Note: `false` allows zero or more invalid network upgrades.
/// - `blocks_after_nu_activation`: The number of blocks the strategy will generate
///   after the provided `network_upgrade`.
/// - `network_upgrade` - The network upgrade that we are using to simulate from where the
///   legacy chain checks should start to apply.
///
/// Returns:
/// A generated arbitrary strategy for the provided arguments.
pub(crate) fn partial_nu5_chain_strategy(
    transaction_version_override: u32,
    transaction_has_valid_network_upgrade: bool,
    blocks_after_nu_activation: u32,
    // TODO: This argument can be removed and just use Nu5 after we have an activation height #1841
    network_upgrade: NetworkUpgrade,
) -> impl Strategy<
    Value = (
        Network,
        Height,
        zebra_chain::fmt::SummaryDebug<Vec<Arc<Block>>>,
    ),
> {
    (
        any::<Network>(),
        NetworkUpgrade::reduced_branch_id_strategy(),
    )
        .prop_flat_map(move |(network, random_nu)| {
            // TODO: update this to Nu5 after we have a height #1841
            let mut nu = network_upgrade;
            let nu_activation = nu.activation_height(&network).unwrap();
            let height = Height(nu_activation.0 + blocks_after_nu_activation);

            // The `network_upgrade_override` will not be enough as when it is `None`,
            // current network upgrade will be used (`NetworkUpgrade::Canopy`) which will be valid.
            if !transaction_has_valid_network_upgrade {
                nu = random_nu;
            }

            zebra_chain::block::LedgerState::height_strategy(
                height,
                Some(nu),
                Some(transaction_version_override),
                transaction_has_valid_network_upgrade,
            )
            .prop_flat_map(move |init| {
                Block::partial_chain_strategy(
                    init,
                    blocks_after_nu_activation as usize,
                    check::utxo::transparent_coinbase_spend,
                    false,
                )
            })
            .prop_map(move |partial_chain| {
                let network_clone = network.clone();
                (network_clone, nu_activation, partial_chain)
            })
        })
}

/// Return a new `StateService` containing the mainnet genesis block.
/// Also returns the finalized genesis block itself.
pub(crate) fn new_state_with_mainnet_genesis(
) -> (FinalizedState, NonFinalizedState, CheckpointVerifiedBlock) {
    let genesis = zebra_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
        .zcash_deserialize_into::<Arc<Block>>()
        .expect("block should deserialize");

    let config = Config::ephemeral();
    let network = Mainnet;

    let mut finalized_state = FinalizedState::new_with_debug(
        &config,
        &network,
        // The tests that use this setup function also commit invalid blocks to the state.
        true,
        #[cfg(feature = "elasticsearch")]
        false,
        false,
    )
    .expect("opening an ephemeral database should succeed");
    let non_finalized_state = NonFinalizedState::new(&network);

    assert_eq!(
        None,
        read::best_tip(&non_finalized_state, &finalized_state.db)
    );

    let genesis = CheckpointVerifiedBlock::from(genesis);
    finalized_state
        .commit_finalized_direct(genesis.clone().into(), None, "test")
        .expect("unexpected invalid genesis block test vector");

    assert_eq!(
        Some((Height(0), genesis.hash)),
        read::best_tip(&non_finalized_state, &finalized_state.db)
    );

    (finalized_state, non_finalized_state, genesis)
}

/// Return a `Transaction::V4` with the coinbase data from `coinbase`.
///
/// Used to convert a coinbase transaction to a version that the non-finalized state will accept.
pub(crate) fn transaction_v4_from_coinbase(coinbase: &Transaction) -> Transaction {
    assert!(
        !coinbase.has_sapling_shielded_data(),
        "conversion assumes sapling shielded data is None"
    );

    Transaction::test_v4(
        coinbase.inputs().to_vec(),
        coinbase.outputs().to_vec(),
        coinbase.lock_time().unwrap_or_else(LockTime::unlocked),
        // `Height(0)` means that the expiry height is ignored
        coinbase.expiry_height().unwrap_or(Height(0)),
    )
}

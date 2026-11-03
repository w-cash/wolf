use std::{
    net::{SocketAddr, TcpStream},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use color_eyre::eyre::{Result, WrapErr};
use tower::service_fn;

use zebra_chain::{
    block,
    chain_tip::NoChainTip,
    parameters::Network::{self, *},
    serialization::ZcashDeserializeInto,
};
use zebra_network::{InventoryResponse, Request, Response};

use crate::common::sync::{
    sync_until, sync_until_with_genesis_peer, MempoolBehavior, STOP_AT_HEIGHT_REGEX,
    STOP_ON_LOAD_TIMEOUT, TINY_CHECKPOINT_TEST_HEIGHT, TINY_CHECKPOINT_TIMEOUT,
};

/// Test if `zebrad` can sync the first checkpoint on mainnet.
///
/// The first checkpoint contains a single genesis block.
#[test]
fn sync_one_checkpoint_mainnet() -> Result<()> {
    let _init_guard = zebra_test::init();
    let peer = MainnetGenesisPeer::new()?;

    sync_until_with_genesis_peer(
        TINY_CHECKPOINT_TEST_HEIGHT,
        &Mainnet,
        STOP_AT_HEIGHT_REGEX,
        TINY_CHECKPOINT_TIMEOUT,
        None,
        MempoolBehavior::ShouldNotActivate,
        // checkpoint sync is irrelevant here - all tested checkpoints are mandatory
        true,
        true,
        peer.address,
    )?;

    assert!(
        peer.requests() > 0,
        "the node must download the genesis block"
    );
    peer.assert_shutdown();
    Ok(())
}

/// Test if `zebrad` can sync the first checkpoint on testnet.
///
/// The first checkpoint contains a single genesis block.
// TODO: disabled because testnet is not currently reliable
// #[test]
#[allow(dead_code)]
fn sync_one_checkpoint_testnet() -> Result<()> {
    sync_until(
        TINY_CHECKPOINT_TEST_HEIGHT,
        &Network::new_default_testnet(),
        STOP_AT_HEIGHT_REGEX,
        TINY_CHECKPOINT_TIMEOUT,
        None,
        MempoolBehavior::ShouldNotActivate,
        // checkpoint sync is irrelevant here - all tested checkpoints are mandatory
        true,
        true,
    )
    .map(|_tempdir| ())
}

/// Test if `zebrad` can sync the first checkpoint, restart, and stop on load.
#[test]
fn restart_stop_at_height() -> Result<()> {
    let _init_guard = zebra_test::init();

    restart_stop_at_height_for_network(Network::Mainnet, TINY_CHECKPOINT_TEST_HEIGHT)?;
    // TODO: disabled because testnet is not currently reliable
    // restart_stop_at_height_for_network(Network::Testnet, TINY_CHECKPOINT_TEST_HEIGHT)?;

    Ok(())
}

fn restart_stop_at_height_for_network(network: Network, height: block::Height) -> Result<()> {
    assert_eq!(network, Mainnet, "the fixture serves the Mainnet genesis");
    assert_eq!(height, TINY_CHECKPOINT_TEST_HEIGHT);
    let peer = MainnetGenesisPeer::new()?;

    let reuse_tempdir = sync_until_with_genesis_peer(
        height,
        &network,
        STOP_AT_HEIGHT_REGEX,
        TINY_CHECKPOINT_TIMEOUT,
        None,
        MempoolBehavior::ShouldNotActivate,
        // checkpoint sync is irrelevant here - all tested checkpoints are mandatory
        true,
        true,
        peer.address,
    )?;
    let genesis_requests = peer.requests();
    assert!(
        genesis_requests > 0,
        "the node must download the genesis block"
    );
    // if stopping corrupts the rocksdb database, zebrad might hang or crash here
    // if stopping does not write the rocksdb database to disk, Zebra will
    // sync, rather than stopping immediately at the configured height
    sync_until_with_genesis_peer(
        height,
        &network,
        "state is already at the configured height",
        STOP_ON_LOAD_TIMEOUT,
        reuse_tempdir,
        MempoolBehavior::ShouldNotActivate,
        // checkpoint sync is irrelevant here - all tested checkpoints are mandatory
        true,
        false,
        peer.address,
    )?;

    assert_eq!(
        peer.requests(),
        genesis_requests,
        "reloading the persisted genesis must not download it again"
    );
    peer.assert_shutdown();

    Ok(())
}

/// A real loopback Mainnet peer serving only the frozen genesis block.
struct MainnetGenesisPeer {
    address: SocketAddr,
    genesis_requests: Arc<AtomicUsize>,
    shutdown: Option<Box<dyn FnOnce()>>,
}

impl MainnetGenesisPeer {
    fn new() -> Result<Self> {
        let genesis = Arc::new(
            zebra_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
                .as_slice()
                .zcash_deserialize_into::<block::Block>()?,
        );
        let genesis_hash = genesis.hash();
        assert_eq!(genesis_hash, Mainnet.genesis_hash());

        let genesis_requests = Arc::new(AtomicUsize::new(0));
        let recorded_requests = Arc::clone(&genesis_requests);
        let inbound = service_fn(move |request| {
            let genesis = Arc::clone(&genesis);
            let recorded_requests = Arc::clone(&recorded_requests);
            async move {
                Ok::<_, zebra_network::BoxError>(match request {
                    Request::Peers => Response::Peers(Vec::new()),
                    Request::BlocksByHash(hashes) => Response::Blocks(
                        hashes
                            .into_iter()
                            .map(|hash| {
                                if hash == genesis_hash {
                                    recorded_requests.fetch_add(1, Ordering::SeqCst);
                                    InventoryResponse::Available((Arc::clone(&genesis), None))
                                } else {
                                    InventoryResponse::Missing(hash)
                                }
                            })
                            .collect(),
                    ),
                    Request::FindBlocks { .. } => Response::BlockHashes(Vec::new()),
                    Request::FindHeaders { .. } => Response::BlockHeaders(Vec::new()),
                    _ => Response::Nil,
                })
            }
        });
        let config = zebra_network::Config {
            network: Mainnet,
            listen_addr: "127.0.0.1:0".parse()?,
            initial_mainnet_peers: Default::default(),
            initial_testnet_peers: Default::default(),
            cache_dir: zebra_network::CacheDir::disabled(),
            peerset_initial_target_size: 1,
            ..zebra_network::Config::default()
        };
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()?;
        let initialized = runtime.block_on(async {
            tokio::time::timeout(
                STOP_ON_LOAD_TIMEOUT,
                zebra_network::init(config, inbound, NoChainTip, "/genesis-test/".to_owned()),
            )
            .await
        });
        let network = match initialized {
            Ok(network) => network,
            Err(error) => {
                runtime.shutdown_timeout(Duration::from_secs(5));
                return Err(error).wrap_err("the loopback genesis peer did not initialize");
            }
        };
        let address = network
            .1
            .lock()
            .expect("genesis peer initialization keeps its address book valid")
            .local_listener_socket_addr();
        assert!(address.ip().is_loopback());
        assert_ne!(address.port(), 0);

        Ok(Self {
            address,
            genesis_requests,
            shutdown: Some(Box::new(move || {
                drop(network);
                runtime.shutdown_timeout(Duration::from_secs(5));
            })),
        })
    }

    fn requests(&self) -> usize {
        self.genesis_requests.load(Ordering::SeqCst)
    }

    fn assert_shutdown(mut self) {
        self.close();
        assert!(
            TcpStream::connect_timeout(&self.address, Duration::from_millis(100)).is_err(),
            "the owned genesis peer listener must close"
        );
    }

    fn close(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            shutdown();
        }
    }
}

impl Drop for MainnetGenesisPeer {
    fn drop(&mut self) {
        self.close();
    }
}

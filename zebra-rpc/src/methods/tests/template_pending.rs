//! A pending proof must return before any auxiliary candidate capability exists.

#![cfg(feature = "wcash-consensus")]

use std::{sync::mpsc, thread, time::Duration};

use tokio::{sync::oneshot, time::timeout};
use tower::buffer::Buffer;
use zcash_protocol::consensus::NetworkType;
use zebra_chain::{
    amount::Amount,
    block::{genesis::wcash_regtest_genesis_block, Height},
    chain_sync_status::MockSyncStatus,
    chain_tip::mock::MockChainTip,
    parameters::Network,
    primitives::WcashAddress,
    serialization::DateTime32,
};
use zebra_network::address_book_peers::MockAddressBookPeers;
use zebra_node_services::{mempool, BoxError};
use zebra_state::{GetBlockTemplateChainInfo, ReadRequest, ReadResponse};
use zebra_test::mock_service::MockService;

use crate::{
    config::mining,
    methods::{
        types::{get_block_template::MinerParams, transaction::TransactionTemplate},
        RpcImpl, RpcServer, WCASH_AUX_TIP_CHANGED_ERROR_CODE,
    },
};

const TEST_LIMIT: Duration = Duration::from_secs(8);

#[tokio::test(flavor = "current_thread")]
async fn template_build_pending_createauxblock_issues_no_candidate_or_retirement_token() {
    let network = Network::new_wcash_regtest();
    let configured = WcashAddress::from_transparent_p2pkh(NetworkType::Regtest, [1; 20]);
    let alternate = WcashAddress::from_transparent_p2pkh(NetworkType::Regtest, [2; 20]);
    let params = MinerParams::new(
        &network,
        mining::Config {
            miner_address: Some(alternate.clone().into()),
            ..Default::default()
        },
    )
    .unwrap();
    let height = Height(1);
    let fee = Amount::zero();
    let expected = TransactionTemplate::new_coinbase(&network, height, &params, fee)
        .expect("the transparent fixture requires no private proof");

    let mut mempool: MockService<_, _, _, BoxError> = MockService::build().for_unit_tests();
    let state: MockService<_, _, _, BoxError> = MockService::build().for_unit_tests();
    let mut read_state: MockService<_, _, _, BoxError> = MockService::build().for_unit_tests();
    let mut verifier: MockService<_, _, _, BoxError> = MockService::build().for_unit_tests();
    let (tip, tip_sender) = MockChainTip::new();
    let tip_hash = network.genesis_hash();
    tip_sender.send_best_tip_hash(tip_hash);
    tip_sender.send_best_tip_height(Height(0));
    tip_sender.send_estimated_distance_to_network_chain_tip(Some(0));
    let mut synced = MockSyncStatus::default();
    synced.set_is_close_to_tip(true);
    let (_log_tx, log_rx) = tokio::sync::watch::channel(None);
    let (rpc, _queue) = RpcImpl::new(
        network,
        mining::Config {
            miner_address: Some(configured.into()),
            ..Default::default()
        },
        Default::default(),
        "0.0.1",
        "pending template fixture",
        Buffer::new(mempool.clone(), 1),
        state,
        Buffer::new(read_state.clone(), 1),
        verifier.clone(),
        synced,
        tip,
        MockAddressBookPeers::default(),
        log_rx,
        None,
    );
    assert_eq!(rpc.wcash_aux_blocks.allocation_counts(), (0, 0, 0));

    let mut alternate_handler = rpc.gbt.clone();
    alternate_handler.set_auxiliary_miner_params(params);
    let cache = alternate_handler.coinbase_cache();
    let warming_cache = cache.clone();
    let warming_coinbase = expected.clone();
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let warmer = thread::spawn(move || {
        warming_cache.try_get_or_build(height, fee, || {
            let _ = entered_tx.send(());
            release_rx
                .recv_timeout(TEST_LIMIT)
                .expect("the pending RPC test releases the held proof");
            Ok::<_, &'static str>(warming_coinbase)
        })
    });
    timeout(TEST_LIMIT, entered_rx).await.unwrap().unwrap();

    let request_rpc = rpc.clone();
    let request_address = alternate.to_string();
    let retry_address = request_address.clone();
    let request = tokio::spawn(async move { request_rpc.create_aux_block(request_address).await });
    let now = DateTime32::now();
    read_state
        .expect_request(ReadRequest::ChainInfo)
        .await
        .respond(ReadResponse::ChainInfo(GetBlockTemplateChainInfo {
            tip_hash,
            tip_height: Height(0),
            expected_difficulty: wcash_regtest_genesis_block().header.difficulty_threshold,
            cur_time: now,
            min_time: now,
            max_time: now,
            chain_history_root: None,
        }));
    mempool
        .expect_request(mempool::Request::FullTransactions)
        .await
        .respond(mempool::Response::FullTransactions {
            transactions: vec![],
            transaction_dependencies: Default::default(),
            last_seen_tip_hash: tip_hash,
        });
    let error = timeout(TEST_LIMIT, request)
        .await
        .expect("the server answers before the held proof completes")
        .unwrap()
        .expect_err("a pending template cannot issue an auxiliary candidate");
    assert_eq!(error.code(), WCASH_AUX_TIP_CHANGED_ERROR_CODE);
    assert!(error.message().contains("construction is pending"));
    assert_eq!(rpc.wcash_aux_blocks.allocation_counts(), (0, 0, 0));
    verifier.expect_no_requests().await;

    release_tx.send(()).unwrap();
    assert_eq!(warmer.join().unwrap().unwrap(), expected);
    let reused = alternate_handler
        .build_template(move || {
            Ok(cache.get_or_build(height, fee, || {
                panic!("the completed exact proof must be reused")
            }))
        })
        .await
        .expect("a retry consumes the completed proof without rebuilding it");
    assert_eq!(reused, expected);
    assert_eq!(rpc.wcash_aux_blocks.allocation_counts(), (0, 0, 0));
    verifier.expect_no_requests().await;

    let retry_rpc = rpc.clone();
    let retry = tokio::spawn(async move { retry_rpc.create_aux_block(retry_address).await });
    read_state
        .expect_request(ReadRequest::ChainInfo)
        .await
        .respond(ReadResponse::ChainInfo(GetBlockTemplateChainInfo {
            tip_hash,
            tip_height: Height(0),
            expected_difficulty: wcash_regtest_genesis_block().header.difficulty_threshold,
            cur_time: now,
            min_time: now,
            max_time: now,
            chain_history_root: None,
        }));
    mempool
        .expect_request(mempool::Request::FullTransactions)
        .await
        .respond(mempool::Response::FullTransactions {
            transactions: vec![],
            transaction_dependencies: Default::default(),
            last_seen_tip_hash: tip_hash,
        });
    verifier
        .expect_request_that(|request| {
            matches!(request, zebra_consensus::Request::CheckProposal(_))
        })
        .await
        .respond_with(|request| match request {
            zebra_consensus::Request::CheckProposal(block) => block.hash(),
            _ => unreachable!("only candidate proposal validation is expected"),
        });
    let response = timeout(TEST_LIMIT, retry)
        .await
        .expect("the ordinary retry uses the completed proof promptly")
        .unwrap()
        .expect("the exact warmed proof supports a validated candidate");
    let response = serde_json::to_value(response).unwrap();
    assert!(response["hash"]
        .as_str()
        .is_some_and(|hash| hash.len() == 64));
    assert!(response["retiretoken"]
        .as_str()
        .is_some_and(|token| token.len() == 64));
    assert_eq!(rpc.wcash_aux_blocks.allocation_counts(), (1, 1, 0));
}

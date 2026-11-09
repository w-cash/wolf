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
    let request =
        tokio::spawn(async move { request_rpc.create_aux_block(request_address, None).await });
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
    let retry = tokio::spawn(async move { retry_rpc.create_aux_block(retry_address, None).await });
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

#[tokio::test(flavor = "current_thread")]
async fn speculative_createauxblock_deadline_returns_pending_without_candidate() {
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
        network.clone(),
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
    alternate_handler.set_auxiliary_miner_params(params.clone());
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

    let module = rpc.clone().into_rpc();
    let request = serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "createauxblock",
        "params": [alternate.to_string(), {"mode": "speculative"}]
    })
    .to_string();
    let now = DateTime32::now();
    let responses = async {
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
    };
    // Drive both local futures without requiring raw_json_request's borrowed
    // dispatch future to be Send. This bound is below the legacy two seconds.
    let response = timeout(Duration::from_millis(750), async {
        let (response, ()) = tokio::join!(module.raw_json_request(&request, 1), responses);
        response.unwrap().0
    })
    .await
    .ok();
    let counts_before_release = rpc.wcash_aux_blocks.allocation_counts();
    if response.is_some() {
        // The obsolete exact key is still blocked, while the new height's key
        // was independently warmed. Demand keeps the other existing slot.
        let next_height = Height(2);
        let next_coinbase =
            TransactionTemplate::new_coinbase(&network, next_height, &params, fee).unwrap();
        cache.get_or_build(next_height, fee, || next_coinbase);
        let next_tip = zebra_chain::block::Hash([0x42; 32]);
        tip_sender.send_best_tip_hash(next_tip);
        tip_sender.send_best_tip_height(Height(1));
        let demand_rpc = rpc.clone();
        let demand_address = alternate.to_string();
        let demand =
            tokio::spawn(async move { demand_rpc.create_aux_block(demand_address, None).await });
        read_state
            .expect_request(ReadRequest::ChainInfo)
            .await
            .respond(ReadResponse::ChainInfo(GetBlockTemplateChainInfo {
                tip_hash: next_tip,
                tip_height: Height(1),
                expected_difficulty: wcash_regtest_genesis_block().header.difficulty_threshold,
                cur_time: now,
                min_time: now,
                max_time: now,
                // This mocked post-activation state must supply a history root.
                // Proposal validity remains the independent mock's boundary.
                chain_history_root: Some([0x44; 32].into()),
            }));
        mempool
            .expect_request(mempool::Request::FullTransactions)
            .await
            .respond(mempool::Response::FullTransactions {
                transactions: vec![],
                transaction_dependencies: Default::default(),
                last_seen_tip_hash: next_tip,
            });
        verifier
            .expect_request_that(|request| {
                matches!(request, zebra_consensus::Request::CheckProposal(_))
            })
            .await
            .respond_with(|request| match request {
                zebra_consensus::Request::CheckProposal(block) => block.hash(),
                _ => unreachable!("the candidate must pass proposal validation"),
            });
        let demanded = timeout(Duration::from_millis(750), demand)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(
            !warmer.is_finished(),
            "the obsolete proof has not been released"
        );
        let demanded = serde_json::to_value(demanded).unwrap();
        assert_eq!(demanded["height"], 2);
        assert_eq!(demanded["previousblockhash"], hex::encode(next_tip.0));
        assert!(demanded["retiretoken"]
            .as_str()
            .is_some_and(|token| token.len() == 64));
        assert_eq!(rpc.wcash_aux_blocks.allocation_counts(), (1, 1, 0));
        for options in [
            serde_json::json!({}),
            serde_json::json!({"mode": "unknown"}),
            serde_json::json!({"mode": null}),
            serde_json::json!({"mode": "speculative", "wait_ms": 1}),
            serde_json::json!(true),
        ] {
            let malformed = serde_json::json!({
                "jsonrpc": "2.0", "id": 2, "method": "createauxblock",
                "params": [alternate.to_string(), options]
            })
            .to_string();
            let (invalid, _) = timeout(TEST_LIMIT, module.raw_json_request(&malformed, 1))
                .await
                .unwrap()
                .unwrap();
            let invalid: serde_json::Value = serde_json::from_str(&invalid).unwrap();
            assert_eq!(invalid["error"]["code"], -32602);
        }
        // A same-height reorg after the independent proposal response must
        // still fail the publication fence without allocating another lease.
        let reorg_rpc = rpc.clone();
        let reorg_address = alternate.to_string();
        let reorg =
            tokio::spawn(async move { reorg_rpc.create_aux_block(reorg_address, None).await });
        read_state
            .expect_request(ReadRequest::ChainInfo)
            .await
            .respond(ReadResponse::ChainInfo(GetBlockTemplateChainInfo {
                tip_hash: next_tip,
                tip_height: Height(1),
                expected_difficulty: wcash_regtest_genesis_block().header.difficulty_threshold,
                cur_time: now,
                min_time: now,
                max_time: now,
                // This mocked post-activation state must supply a history root.
                // Proposal validity remains the independent mock's boundary.
                chain_history_root: Some([0x44; 32].into()),
            }));
        mempool
            .expect_request(mempool::Request::FullTransactions)
            .await
            .respond(mempool::Response::FullTransactions {
                transactions: vec![],
                transaction_dependencies: Default::default(),
                last_seen_tip_hash: next_tip,
            });
        verifier
            .expect_request_that(|request| {
                matches!(request, zebra_consensus::Request::CheckProposal(_))
            })
            .await
            .respond_with(|request| match request {
                zebra_consensus::Request::CheckProposal(block) => {
                    tip_sender.send_best_tip_hash(zebra_chain::block::Hash([0x43; 32]));
                    block.hash()
                }
                _ => unreachable!("only proposal validation is expected"),
            });
        let reorg = timeout(Duration::from_millis(750), reorg)
            .await
            .unwrap()
            .unwrap()
            .expect_err("the same-height changed parent must reject publication");
        assert_eq!(reorg.code(), WCASH_AUX_TIP_CHANGED_ERROR_CODE);
        assert_eq!(rpc.wcash_aux_blocks.allocation_counts(), (1, 1, 0));
    }
    // Clean up the owned blocking proof before asserting the baseline failure.
    release_tx.send(()).unwrap();
    assert_eq!(warmer.join().unwrap().unwrap(), expected);
    assert_eq!(counts_before_release, (0, 0, 0));
    verifier.expect_no_requests().await;
    let response: serde_json::Value = serde_json::from_str(
        &response.expect("speculative node waiter must return before the legacy two-second budget"),
    )
    .unwrap();
    assert_eq!(response["error"]["code"], WCASH_AUX_TIP_CHANGED_ERROR_CODE);
    assert!(response["error"]["message"]
        .as_str()
        .unwrap()
        .contains("construction is pending"));
    assert!(response.get("result").is_none());
    // Only the new demanded candidate was published; completion of the old
    // detached template cannot allocate or publish a late candidate.
    assert_eq!(rpc.wcash_aux_blocks.allocation_counts(), (1, 1, 0));
}

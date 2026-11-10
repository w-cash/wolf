//! Public-RPC regression for ready auxiliary work behind two obsolete workers.
//! Proof timing and proposal/state authority are gated local fixtures; this
//! does not claim an independently mined or consensus-verified winning block.

#![cfg(feature = "wcash-consensus")]

use crate::{
    config::mining,
    methods::{
        types::{get_block_template::MinerParams, transaction::TransactionTemplate},
        RpcImpl, RpcServer,
    },
};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc, Arc,
    },
    time::Duration,
};
use tokio::{sync::oneshot, time::timeout};
use tower::buffer::Buffer;
use zcash_protocol::consensus::NetworkType;
use zebra_chain::{
    amount::{Amount, NonNegative},
    block::{genesis::wcash_regtest_genesis_block, Hash, Height},
    chain_sync_status::MockSyncStatus,
    chain_tip::mock::MockChainTip,
    parameters::Network,
    primitives::WcashAddress,
    serialization::{DateTime32, ZcashDeserializeInto},
    transaction::{self, Transaction, UnminedTx, VerifiedUnminedTx},
    transparent,
};
use zebra_network::address_book_peers::MockAddressBookPeers;
use zebra_node_services::{mempool, BoxError};
use zebra_state::{GetBlockTemplateChainInfo, ReadRequest, ReadResponse};
use zebra_test::mock_service::MockService;

const WORKER_LIMIT: Duration = Duration::from_secs(6);
const CLEANUP_LIMIT: Duration = Duration::from_secs(2);

struct HeldWorkers {
    release: Option<mpsc::Sender<()>>,
    completed: Vec<mpsc::Receiver<()>>,
}
impl HeldWorkers {
    fn release_and_wait(&mut self) -> bool {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
        let mut all_completed = true;
        for done in self.completed.drain(..) {
            // Attempt every cleanup even if an earlier worker missed its bound.
            all_completed &= done.recv_timeout(CLEANUP_LIMIT).is_ok();
        }
        all_completed
    }
}
impl Drop for HeldWorkers {
    fn drop(&mut self) {
        let _ = self.release_and_wait();
    }
}

fn fee_paying_transfer(network: &Network, height: Height) -> VerifiedUnminedTx {
    let mut p2sh_op_true = vec![0xa9, 0x14];
    p2sh_op_true.extend_from_slice(&[
        0xda, 0x17, 0x45, 0xe9, 0xb5, 0x49, 0xbd, 0x0b, 0xfa, 0x1a, 0x56, 0x99, 0x71, 0xc7, 0x7e,
        0xba, 0x30, 0xcd, 0x5a, 0x4b,
    ]);
    p2sh_op_true.push(0x87);
    let spent_output = transparent::Output {
        value: Amount::try_from(30_000).unwrap(),
        lock_script: transparent::Script::new(&p2sh_op_true),
    };
    let tx = Arc::new(Transaction::test_v6_for_network(
        network,
        height,
        vec![transparent::Input::PrevOut {
            outpoint: transparent::OutPoint {
                hash: transaction::Hash([0x42; 32]),
                index: 0,
            },
            unlock_script: transparent::Script::new(&[0x01, 0x51]),
            sequence: u32::MAX,
        }],
        vec![transparent::Output {
            value: Amount::try_from(15_000).unwrap(),
            lock_script: transparent::Script::new(&p2sh_op_true),
        }],
        transaction::LockTime::unlocked(),
        Height(height.0 + 1),
    ));
    VerifiedUnminedTx::new(
        UnminedTx::from(tx),
        Amount::try_from(15_000).unwrap(),
        0,
        0,
        Arc::new(vec![spent_output]),
    )
    .unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn ready_auxiliary_candidate_with_real_fees_survives_two_held_template_workers() {
    exercise_ready_public_rpc(false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn reserve_ready_auxiliary_publication_preserves_the_next_current_request() {
    exercise_ready_public_rpc(true).await;
}

async fn exercise_ready_public_rpc(from_reserve: bool) {
    let network = Network::new_wcash_regtest();
    let configured = WcashAddress::from_transparent_p2pkh(NetworkType::Regtest, [1; 20]);
    let recipient = WcashAddress::from_transparent_p2pkh(NetworkType::Regtest, [2; 20]);
    let params = MinerParams::new(
        &network,
        mining::Config {
            miner_address: Some(recipient.clone().into()),
            ..Default::default()
        },
    )
    .unwrap();
    let old_height = Height(1);
    let current_height = Height(2);
    let fee = Amount::<NonNegative>::try_from(15_000).unwrap();
    let old_coinbase =
        TransactionTemplate::new_coinbase(&network, old_height, &params, fee).unwrap();
    let sizing =
        TransactionTemplate::new_coinbase(&network, current_height, &params, Amount::zero())
            .unwrap();
    let selected =
        TransactionTemplate::new_coinbase(&network, current_height, &params, fee).unwrap();
    let transfer = fee_paying_transfer(&network, current_height);
    assert!(transfer.pays_conventional_fee());
    let transfer_hash = transfer.transaction.id.mined_id();
    let mut mempool: MockService<_, _, _, BoxError> = MockService::build().for_unit_tests();
    let state: MockService<_, _, _, BoxError> = MockService::build().for_unit_tests();
    let mut read_state: MockService<_, _, _, BoxError> = MockService::build().for_unit_tests();
    let mut verifier: MockService<_, _, _, BoxError> = MockService::build().for_unit_tests();
    let (tip, tip_sender) = MockChainTip::new();
    let current_tip = Hash([0x52; 32]);
    tip_sender.send_best_tip_hash(current_tip);
    tip_sender.send_best_tip_height(Height(1));
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
        "ready lane regression",
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
    let mut handler = rpc.gbt.clone();
    handler.set_auxiliary_miner_params(params);
    let cache = handler.coinbase_cache();
    if !from_reserve {
        cache.store(current_height, Amount::zero(), sizing);
        cache.store(current_height, fee, selected.clone());
    }
    let constructors = Arc::new(AtomicUsize::new(0));
    let (release_tx, release_rx) = mpsc::channel();
    let (first_entered_tx, first_entered_rx) = oneshot::channel();
    let (first_done_tx, first_done_rx) = mpsc::channel();
    let first_handler = handler.clone();
    let first_cache = cache.clone();
    let first_coinbase = old_coinbase.clone();
    let first_constructors = Arc::clone(&constructors);
    let first = tokio::spawn(async move {
        first_handler
            .build_template(move || {
                let value = first_cache.get_or_build(old_height, fee, || {
                    first_constructors.fetch_add(1, Ordering::SeqCst);
                    let _ = first_entered_tx.send(());
                    release_rx
                        .recv_timeout(WORKER_LIMIT)
                        .expect("test releases its owned constructor");
                    first_coinbase
                });
                let _ = first_done_tx.send(());
                Ok(value)
            })
            .await
    });
    let mut held = HeldWorkers {
        release: Some(release_tx),
        completed: vec![first_done_rx],
    };
    timeout(CLEANUP_LIMIT, first_entered_rx)
        .await
        .unwrap()
        .unwrap();
    let (second_entered_tx, second_entered_rx) = oneshot::channel();
    let (second_done_tx, second_done_rx) = mpsc::channel();
    let second_handler = handler.clone();
    let second_cache = cache.clone();
    let second = tokio::spawn(async move {
        second_handler
            .build_template(move || {
                let _ = second_entered_tx.send(());
                let value = second_cache.get_or_build(old_height, fee, || {
                    panic!("the second old worker must join the original singleflight constructor")
                });
                let _ = second_done_tx.send(());
                Ok(value)
            })
            .await
    });
    held.completed.push(second_done_rx);
    timeout(CLEANUP_LIMIT, second_entered_rx)
        .await
        .unwrap()
        .unwrap();
    if from_reserve {
        // Only the existing optional worker claims these future entries. No
        // normal foreground lookup or store promotes the current keys here.
        handler.prewarm_auxiliary_coinbase(&network, old_height, fee);
    }
    macro_rules! attempt {
        () => {{
    let transfer=transfer.clone();
    let(rpc_done_tx,rpc_done_rx)=oneshot::channel();
    let request=async {
        let result=rpc.create_aux_block(recipient.to_string(),None).await;
        let _=rpc_done_tx.send(());result
    };
    let now=DateTime32::now();
    let responses=async {
        read_state.expect_request(ReadRequest::ChainInfo).await.respond(
            ReadResponse::ChainInfo(GetBlockTemplateChainInfo {
                tip_hash:current_tip,tip_height:Height(1),
                expected_difficulty:wcash_regtest_genesis_block().header.difficulty_threshold,
                cur_time:now,min_time:now,max_time:now,chain_history_root:Some([0x44;32].into()),
            }));
        mempool.expect_request(mempool::Request::FullTransactions).await.respond(
            mempool::Response::FullTransactions { transactions:vec![transfer],
                transaction_dependencies:Default::default(),last_seen_tip_hash:current_tip });
        tokio::select! {
            proposal=verifier.expect_request_that(|request| matches!(request,zebra_consensus::Request::CheckProposal(_))) => {
                proposal.respond_with(|request| match request {
                    zebra_consensus::Request::CheckProposal(block) => {
                        assert_eq!(block.header.previous_block_hash,current_tip);
                        assert_eq!(block.coinbase_height(),Some(current_height));
                        assert_eq!(block.transactions.len(),2);
                        assert_eq!(block.transactions[1].hash(),transfer_hash);
                        block.hash()
                    }
                    _=>unreachable!("only independent proposal validation is expected"),
                });true
            }
            _=rpc_done_rx=>false,
        }
    };
    timeout(Duration::from_millis(900),async {tokio::join!(request,responses)}).await

        }};
    }
    let mut response = attempt!();
    if from_reserve {
        // Observe actual readiness through the public RPC, rather than assume
        // a sleep made the existing optional worker finish. Only explicit
        // pending can retry; malformed/authority errors fail below.
        let began = tokio::time::Instant::now();
        while began.elapsed()<Duration::from_secs(1) && response.as_ref().is_ok_and(|(reply,_)| {
            matches!(reply,Err(error) if error.code()==crate::methods::WCASH_AUX_TIP_CHANGED_ERROR_CODE &&
                error.message()=="Wcash candidate construction is pending; retry createauxblock")
        }) {
            tokio::time::sleep(Duration::from_millis(10)).await;
            response=attempt!();
        }
    }
    // The first successful RPC itself publishes and advances the reserve.
    // The second must still have both current proof entries, while old work
    // keeps the original two foreground slots occupied.
    let second_response = if from_reserve && response.as_ref().is_ok_and(|(reply, _)| reply.is_ok())
    {
        let mut second = attempt!();
        let began = tokio::time::Instant::now();
        // Background registration can briefly hold the metadata mutex; only
        // its explicit fail-closed pending response may retry in this bound.
        while began.elapsed()<Duration::from_secs(1) && second.as_ref().is_ok_and(|(reply,_)| {
            matches!(reply,Err(error) if error.code()==crate::methods::WCASH_AUX_TIP_CHANGED_ERROR_CODE &&
                error.message()=="Wcash candidate construction is pending; retry createauxblock")
        }) {
            tokio::time::sleep(Duration::from_millis(10)).await;
            second=attempt!();
        }
        Some(second)
    } else {
        None
    };
    let allocated_before_cleanup = rpc.wcash_aux_blocks.allocation_counts();
    let old_workers_still_held = constructors.load(Ordering::SeqCst) == 1
        && held
            .completed
            .iter()
            .all(|done| matches!(done.try_recv(), Err(mpsc::TryRecvError::Empty)));
    // Every ordinary/failing assertion below occurs only after owned cleanup.
    let completed = held.release_and_wait();
    let first = timeout(CLEANUP_LIMIT, first).await;
    let second = timeout(CLEANUP_LIMIT, second).await;
    assert!(
        completed,
        "both owned old workers complete after explicit release"
    );
    for result in [first, second] {
        match result.unwrap().unwrap() {
            Ok(coinbase) => assert_eq!(coinbase, old_coinbase),
            Err(error) => assert_eq!(
                error.message(),
                "block template construction is pending; retry"
            ),
        }
    }
    assert_eq!(constructors.load(Ordering::SeqCst), 1);
    let (response, proposal_checked) =
        response.expect("the ready current RPC is bounded independently of old workers");
    let response = response.expect(
        "ready exact fee15000 auxiliary work must not be rejected by two obsolete template workers",
    );
    assert!(
        old_workers_still_held,
        "candidate completed before either old worker was released"
    );
    assert!(
        proposal_checked,
        "the candidate passes the independent proposal boundary"
    );
    let candidate: zebra_chain::block::Block =
        response.data().as_slice().zcash_deserialize_into().unwrap();
    let response = serde_json::to_value(response).unwrap();
    assert_eq!(response["height"], current_height.0);
    assert_eq!(response["previousblockhash"], hex::encode(current_tip.0));
    assert!(response["retiretoken"]
        .as_str()
        .is_some_and(|token| token.len() == 64));
    if from_reserve {
        let (second, second_proposal) = second_response
            .expect("the successful reserve publication is followed by another RPC")
            .expect("the second ready request stays bounded");
        let second = serde_json::to_value(
            second.expect("publication must retain the consumed reserve proof entries"),
        )
        .unwrap();
        assert!(second_proposal);
        assert_eq!(second["height"], response["height"]);
        assert_eq!(second["previousblockhash"], response["previousblockhash"]);
        assert_eq!(second["coinbasevalue"], response["coinbasevalue"]);
        assert_ne!(second["hash"], response["hash"]);
        assert_ne!(second["retiretoken"], response["retiretoken"]);
        assert_eq!(allocated_before_cleanup, (2, 2, 0));
        assert_eq!(
            rpc.wcash_aux_blocks.allocation_counts(),
            (2, 2, 0),
            "both issued candidates and leases remain active"
        );
    } else {
        assert_eq!(allocated_before_cleanup, (1, 1, 0));
    }
    let cached = cache.get_or_build(current_height, fee, || {
        panic!("selected proof remains an exact ready cache hit")
    });
    assert_eq!(cached.fee().zatoshis(), -15_000);
    assert_eq!(candidate.transactions[0].hash(), cached.hash());
    if !from_reserve {
        assert_eq!(cached, selected);
    }
}

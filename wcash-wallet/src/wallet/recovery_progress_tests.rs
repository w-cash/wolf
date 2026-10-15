use super::*;

use std::{
    sync::Mutex,
    task::{Context, Poll},
};

use futures_util::stream;
use tonic::{
    codegen::{http, Body, BoxFuture, BoxStream, Service, StdError},
    transport::{Endpoint, Server},
    Request, Response, Status,
};
use zcash_client_backend::proto::{
    compact_formats::CompactBlock,
    service::{
        BlockId, ChainSpec, GetAddressUtxosArg, GetAddressUtxosReply, RawTransaction,
        TransparentAddressBlockFilter, TxFilter,
    },
};

const TIP: u32 = 130;
const SERVICE: &str = "cash.z.wallet.sdk.rpc.CompactTxStreamer";

#[derive(Default, Debug)]
struct Calls {
    raw: BTreeMap<Vec<u8>, usize>,
    history: Vec<(u32, u32)>,
    blocks: Vec<u32>,
    delayed_managed_history: bool,
}

#[derive(Default)]
struct RecoveryFixture {
    utxos: Vec<GetAddressUtxosReply>,
    raw: BTreeMap<Vec<u8>, RawTransaction>,
    history: Vec<RawTransaction>,
    raw_delay: Duration,
    candidate_delay: Duration,
    delay_first_managed_history: bool,
    canonical_overrides: Mutex<BTreeMap<u32, [u8; 32]>>,
    calls: Mutex<Calls>,
}

impl RecoveryFixture {
    async fn latest(&self, _request: ChainSpec) -> Result<BlockId, Status> {
        Ok(BlockId {
            height: u64::from(TIP),
            hash: vec![0xaa; 32],
        })
    }

    async fn block(&self, request: BlockId) -> Result<CompactBlock, Status> {
        let height = u32::try_from(request.height)
            .map_err(|_| Status::invalid_argument("fixture height exceeds u32"))?;
        self.calls.lock().unwrap().blocks.push(height);
        let byte = if height == TIP {
            0xaa
        } else {
            u8::try_from(height & 0xff).unwrap()
        };
        Ok(CompactBlock {
            height: request.height,
            hash: self
                .canonical_overrides
                .lock()
                .unwrap()
                .get(&height)
                .copied()
                .unwrap_or([byte; 32])
                .to_vec(),
            ..Default::default()
        })
    }

    async fn transaction(&self, request: TxFilter) -> Result<RawTransaction, Status> {
        *self
            .calls
            .lock()
            .unwrap()
            .raw
            .entry(request.hash.clone())
            .or_default() += 1;
        tokio::time::sleep(self.raw_delay).await;
        self.raw
            .get(&request.hash)
            .cloned()
            .ok_or_else(|| Status::not_found("fixture creator is unknown"))
    }

    async fn utxos(
        &self,
        request: GetAddressUtxosArg,
    ) -> Result<Vec<GetAddressUtxosReply>, Status> {
        Ok(self
            .utxos
            .iter()
            .filter(|output| {
                output.height >= request.start_height && request.addresses.contains(&output.address)
            })
            .take(if request.max_entries == 0 {
                usize::MAX
            } else {
                usize::try_from(request.max_entries).unwrap()
            })
            .cloned()
            .collect())
    }

    async fn history(
        &self,
        request: TransparentAddressBlockFilter,
    ) -> Result<Vec<RawTransaction>, Status> {
        let range = request
            .range
            .ok_or_else(|| Status::invalid_argument("missing range"))?;
        let start = u32::try_from(range.start.unwrap().height).unwrap();
        let end = u32::try_from(range.end.unwrap().height).unwrap();
        let delay_managed = {
            let mut calls = self.calls.lock().unwrap();
            calls.history.push((start, end));
            let delay = self.delay_first_managed_history
                && start == 1
                && end == TIP
                && !calls.delayed_managed_history;
            calls.delayed_managed_history |= delay;
            delay
        };
        // Only exact candidate-height queries are delayed in the second case.
        // Ordinary history draining remains fast, isolating candidate-prefix
        // replay from unrelated address-history request pagination.
        if delay_managed {
            // This occurs only after the partial-spend pass has succeeded.
            // Its one interrupted response proves that the next attempt can
            // reuse completed proof progress as well as unfinished prefixes.
            tokio::time::sleep(Duration::from_secs(3)).await;
        } else if start == end && (110..118).contains(&start) {
            tokio::time::sleep(self.candidate_delay).await;
        }
        Ok(self
            .history
            .iter()
            .filter(|raw| (u64::from(start)..=u64::from(end)).contains(&raw.height))
            .cloned()
            .collect())
    }
}

macro_rules! unary_handler {
    ($name:ident, $request:ty, $response:ty, $method:ident) => {
        struct $name(Arc<RecoveryFixture>);
        impl tonic::server::UnaryService<$request> for $name {
            type Response = $response;
            type Future = BoxFuture<Response<Self::Response>, Status>;

            fn call(&mut self, request: Request<$request>) -> Self::Future {
                let fixture = Arc::clone(&self.0);
                Box::pin(async move {
                    fixture
                        .$method(request.into_inner())
                        .await
                        .map(Response::new)
                })
            }
        }
    };
}

macro_rules! streaming_handler {
    ($name:ident, $request:ty, $response:ty, $method:ident) => {
        struct $name(Arc<RecoveryFixture>);
        impl tonic::server::ServerStreamingService<$request> for $name {
            type Response = $response;
            type ResponseStream = BoxStream<Self::Response>;
            type Future = BoxFuture<Response<Self::ResponseStream>, Status>;

            fn call(&mut self, request: Request<$request>) -> Self::Future {
                let fixture = Arc::clone(&self.0);
                Box::pin(async move {
                    let outputs = fixture.$method(request.into_inner()).await?;
                    let stream: Self::ResponseStream =
                        Box::pin(stream::iter(outputs.into_iter().map(Ok)));
                    Ok(Response::new(stream))
                })
            }
        }
    };
}

unary_handler!(Latest, ChainSpec, BlockId, latest);
unary_handler!(Block, BlockId, CompactBlock, block);
unary_handler!(TransactionRpc, TxFilter, RawTransaction, transaction);
streaming_handler!(Utxos, GetAddressUtxosArg, GetAddressUtxosReply, utxos);
streaming_handler!(
    History,
    TransparentAddressBlockFilter,
    RawTransaction,
    history
);

#[derive(Clone)]
struct FixtureService(Arc<RecoveryFixture>);

impl tonic::server::NamedService for FixtureService {
    const NAME: &'static str = SERVICE;
}

impl<B> Service<http::Request<B>> for FixtureService
where
    B: Body + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let fixture = Arc::clone(&self.0);
        Box::pin(async move {
            let response = match request.uri().path() {
                "/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLatestBlock" => {
                    tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                        .unary(Latest(fixture), request)
                        .await
                }
                "/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetBlock" => {
                    tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                        .unary(Block(fixture), request)
                        .await
                }
                "/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetTransaction" => {
                    tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                        .unary(TransactionRpc(fixture), request)
                        .await
                }
                "/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetAddressUtxosStream" => {
                    tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                        .server_streaming(Utxos(fixture), request)
                        .await
                }
                "/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetTaddressTransactions" => {
                    tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                        .server_streaming(History(fixture), request)
                        .await
                }
                _ => http::Response::builder()
                    .status(200)
                    .header("content-type", "application/grpc")
                    .header("grpc-status", "12")
                    .body(tonic::body::Body::empty())
                    .unwrap(),
            };
            Ok(response)
        })
    }
}

fn raw_transaction(transaction: &Transaction, height: u32) -> RawTransaction {
    let mut data = Vec::new();
    transaction.write(&mut data).unwrap();
    RawTransaction {
        data,
        height: u64::from(height),
    }
}

fn creator_at_height(receiver: TransparentAddress, height: u32) -> Transaction {
    use zcash_transparent::{
        address::Script,
        bundle::{Authorized, Bundle, TxIn, TxOut},
    };
    zcash_primitives::transaction::TransactionData::from_parts_v6(
        WalletNetwork::Regtest.branch_id(),
        0,
        BlockHeight::from_u32(height),
        Some(Bundle {
            vin: vec![TxIn::<Authorized>::from_parts(
                OutPoint::NULL,
                Script::default(),
                u32::MAX,
            )],
            vout: vec![
                TxOut::new(
                    Zatoshis::const_from_u64(437_500_000),
                    receiver.script().into(),
                ),
                TxOut::new(
                    Zatoshis::const_from_u64(187_500_000),
                    receiver.script().into(),
                ),
            ],
            authorization: Authorized,
        }),
        None,
        None,
        None,
    )
    .freeze()
    .unwrap()
}

async fn deadline_retry_case(preload_spends: bool) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("wallet.sqlite");
    let network = WalletNetwork::Regtest;
    create_wallet_accounts(&path, network, 1);
    let path = fs::canonicalize(path).unwrap();
    let tip = set_synchronized_test_tip(&path, network, TIP, 0xaa);
    let _lock = acquire_wallet_operation_lock(&path, WalletOperationLockMode::Exclusive).unwrap();
    let mut wallet = open_wallet_database(&path, network).unwrap();
    let account_id = wallet.get_account_ids().unwrap()[0];
    let account = wallet.get_account(account_id).unwrap().unwrap();
    let receivers =
        managed_coinbase_receivers(&wallet, account_id, account.ufvk().unwrap(), network).unwrap();
    let receivers = &receivers[..1];
    let managed = &receivers[0];
    let parameters = network.parameters();
    let count = if preload_spends { 8u32 } else { 3u32 };
    let mut fixture = RecoveryFixture {
        utxos: Vec::new(),
        raw: BTreeMap::new(),
        history: Vec::new(),
        raw_delay: if preload_spends {
            Duration::ZERO
        } else {
            Duration::from_millis(1_100)
        },
        candidate_delay: if preload_spends {
            Duration::from_millis(800)
        } else {
            Duration::ZERO
        },
        delay_first_managed_history: preload_spends,
        canonical_overrides: Mutex::default(),
        calls: Mutex::new(Calls::default()),
    };
    let mut creator_ids = Vec::new();
    for offset in 0..count {
        let height = 10 + offset;
        let creator = creator_at_height(managed.receiver, height);
        let spend_height = height + 100;
        // Parser-valid bookkeeping inputs, as in the focused database tests;
        // this transport test exercises recovery, not Ironwood proof validity.
        let spender = transparent_spend_bookkeeping_fixture(
            vec![OutPoint::new(creator.txid().into(), 0)],
            200,
        );
        let raw = raw_transaction(&creator, height);
        creator_ids.push(creator.txid());
        fixture
            .raw
            .insert(creator.txid().as_ref().to_vec(), raw.clone());
        fixture.history.push(raw);
        fixture
            .history
            .push(raw_transaction(&spender, spend_height));
        fixture.utxos.push(GetAddressUtxosReply {
            address: managed.address.clone(),
            txid: creator.txid().as_ref().to_vec(),
            index: 1,
            script: zcash_transparent::address::Script::from(managed.receiver.script())
                .0
                 .0,
            value_zat: 187_500_000,
            height: u64::from(height),
        });
        if preload_spends {
            for (transaction, mined_height) in [(&creator, height), (&spender, spend_height)] {
                decrypt_and_store_transaction(
                    &parameters,
                    &mut wallet,
                    transaction,
                    Some(BlockHeight::from_u32(mined_height)),
                )
                .unwrap();
            }
        }
    }
    if preload_spends {
        // This case starts from a wallet that already knows its complete mined
        // history. Advance the backend's real address-history requests through
        // the pinned tip, rather than spending its short retry budget on an
        // unrelated initial discovery pass after candidate verification.
        for round in 0..32 {
            let groups = group_transparent_history_requests(
                wallet.transaction_data_requests().unwrap(),
                receivers,
                TIP + 1,
            )
            .unwrap();
            if groups.is_empty() {
                break;
            }
            assert!(round < 31, "cached history frontier did not converge");
            for ((_, _, end), requests) in groups {
                for request in requests {
                    wallet
                        .notify_address_checked(request, BlockHeight::from_u32(end - 1))
                        .unwrap();
                }
            }
        }
    }
    fixture.history.sort_by_key(|raw| raw.height);
    drop(wallet);
    let fixture = Arc::new(fixture);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let incoming = stream::unfold(listener, |listener| async {
        let next = listener.accept().await.map(|(stream, _)| stream);
        Some((next, listener))
    });
    let (shutdown, stopped) = tokio::sync::oneshot::channel();
    let service = FixtureService(Arc::clone(&fixture));
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming_shutdown(incoming, async {
                let _ = stopped.await;
            })
            .await
    });
    let channel = Endpoint::from_shared(endpoint)
        .unwrap()
        .connect_timeout(Duration::from_secs(2))
        .connect()
        .await
        .unwrap();
    let mut client = AttestedWcashClient::from_test_channel(network, channel);
    let mut frozen_raw_counts = BTreeMap::new();
    let mut interruptions = 0;
    let budget = Duration::from_secs(2);
    let mut complete = false;
    let mut attempts = 0;
    for attempt in 1..=12 {
        attempts = attempt;
        let mut wallet = open_wallet_database(&path, network).unwrap();
        let session = begin_transparent_recovery(&mut wallet).unwrap();
        let cancellation = WalletSyncCancellation::new();
        let recovery = TransparentRecoveryContext {
            parameters: &parameters,
            receivers,
            expected_tip: tip,
            deadline: tokio::time::Instant::now() + budget,
            cancellation: &cancellation,
        };
        match recover_transparent_coinbase(&mut client, &mut wallet, &recovery).await {
            Ok(()) => {
                complete_transparent_recovery(&mut wallet, session, tip).unwrap();
                let summary =
                    wallet_balance_summary(&wallet, public_confirmation_policy()).unwrap();
                assert_eq!(
                    summary.accounts[0].transparent_coinbase_total_zat,
                    u64::from(count) * 187_500_000
                );
                assert_eq!(summary.accounts[0].transparent_regular_total_zat, 0);
                complete = true;
            }
            Err(WalletServiceError::TransparentRecoveryDeadline) => {
                interruptions += 1;
                assert!(matches!(
                    require_transparent_recovery_complete(&mut wallet),
                    Err(WalletServiceError::TransparentRecoveryIncomplete)
                ));
            }
            Err(error) => panic!("controlled recovery failed unexpectedly: {error}"),
        }
        {
            let connection = open_wallet_connection(&path).unwrap();
            let calls = fixture.calls.lock().unwrap();
            for txid in &creator_ids {
                let stored: bool = connection
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM transactions WHERE txid = ?1 AND raw IS NOT NULL)",
                    [txid.as_ref()],
                    |row| row.get(0),
                )
                .unwrap();
                let observed = calls.raw.get(&txid.as_ref()[..]).copied().unwrap_or(0);
                if let Some(previous) = frozen_raw_counts.get(&txid.as_ref()[..]) {
                    assert_eq!(
                        *previous, observed,
                        "cached creator was fetched again: {txid}"
                    );
                } else if stored {
                    frozen_raw_counts.insert(txid.as_ref().to_vec(), observed);
                }
            }
        }
        if complete {
            break;
        }
    }
    let _ = shutdown.send(());
    tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        complete,
        "deadline retries did not complete; calls: {:?}",
        fixture.calls.lock().unwrap()
    );
    assert!(
        interruptions > 0,
        "fixture must interrupt at least one real recovery attempt"
    );
    let calls = fixture.calls.lock().unwrap();
    println!(
        "controlled recovery {}: attempts={attempts}, interruptions={interruptions}, raw_rpcs={}",
        if preload_spends {
            "cached-candidates"
        } else {
            "creator-prefix"
        },
        calls.raw.values().sum::<usize>()
    );
    if preload_spends {
        assert!(calls.delayed_managed_history);
        assert!(
            calls
                .history
                .iter()
                .filter(|range| **range == (1, TIP))
                .count()
                >= 2
        );
        assert!(
            calls.raw.is_empty(),
            "cached creators must not invoke GetTransaction"
        );
        for height in 110..118 {
            assert!(
                calls.history.contains(&(height, height)),
                "missing spend was never reattested: {height}"
            );
        }
        assert_eq!(
            calls
                .history
                .iter()
                .filter(|range| **range == (110, 110))
                .count(),
            1,
            "already verified candidate prefix was replayed after deadline"
        );
    } else {
        assert_eq!(
            calls.history.first().map(|range| range.0),
            Some(10),
            "cached partial creators must still register their omitted outputs"
        );
        // Creators are visited in txid order, which need not match height.
        // The per-attempt frozen counts above enforce the invariant for each
        // creator; at least the first committed creator is fetched only once.
        assert!(calls.raw.values().any(|count| *count == 1));
    }
}

#[tokio::test]
async fn actual_recovery_retries_cached_partial_creators_without_raw_prefix_replay() {
    tokio::time::timeout(Duration::from_secs(60), deadline_retry_case(false))
        .await
        .unwrap();
}

#[tokio::test]
async fn actual_recovery_retries_distinct_spend_heights_without_candidate_prefix_replay() {
    tokio::time::timeout(Duration::from_secs(60), deadline_retry_case(true))
        .await
        .unwrap();
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProofMutation {
    None,
    CanonicalAnchor,
    MissingRaw,
    TamperedRaw,
    ChangedOutputs,
}

async fn restore_proof_case(mutation: ProofMutation) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("wallet.sqlite");
    let network = WalletNetwork::Regtest;
    create_wallet_accounts(&path, network, 1);
    let path = fs::canonicalize(path).unwrap();
    let tip = set_synchronized_test_tip(&path, network, TIP, 0xaa);
    let _lock = acquire_wallet_operation_lock(&path, WalletOperationLockMode::Exclusive).unwrap();
    let mut wallet = open_wallet_database(&path, network).unwrap();
    let account_id = wallet.get_account_ids().unwrap()[0];
    let account = wallet.get_account(account_id).unwrap().unwrap();
    let receivers =
        managed_coinbase_receivers(&wallet, account_id, account.ufvk().unwrap(), network).unwrap();
    let receiver = receivers[0].receiver;
    let parameters = network.parameters();
    let cancellation = WalletSyncCancellation::new();
    let recovery = TransparentRecoveryContext {
        parameters: &parameters,
        receivers: &receivers[..1],
        expected_tip: tip,
        deadline: tokio::time::Instant::now() + Duration::from_secs(10),
        cancellation: &cancellation,
    };
    begin_transparent_recovery(&mut wallet).unwrap();
    let creator_transaction = same_address_creator_fixture(receiver);
    let creator = partial_creator_fixture(&creator_transaction, 1);
    let mut original = PartialTransparentSpendReconciliation::default();
    original.track(&creator, &receiver).unwrap();
    let digest = original.checkpoint_digest();
    decrypt_and_store_transaction(
        &parameters,
        &mut wallet,
        &creator_transaction,
        Some(BlockHeight::from_u32(10)),
    )
    .unwrap();
    let consumed = OutPoint::new(creator_transaction.txid().into(), 0);
    let spender = transparent_spend_bookkeeping_fixture(vec![consumed.clone()], 200);
    let spender_txid = spender.txid();
    let receipts = import_partial_spend_history(
        &mut wallet,
        &recovery,
        &mut original,
        vec![crate::rpc::MinedTransparentTransaction {
            transaction: spender,
            height: 110,
        }],
    )
    .unwrap();
    assert_eq!(receipts.len(), 1);
    original.finish().unwrap();
    let anchor = BlockRef {
        height: 120,
        hash: [120; 32],
    };
    crate::partial_recovery::store_witnesses(&mut wallet, 0, digest, anchor, &receipts, &[110])
        .unwrap();
    crate::partial_recovery::store(&mut wallet, 0, digest, anchor).unwrap();
    drop(wallet);

    match mutation {
        ProofMutation::MissingRaw => {
            open_wallet_connection(&path)
                .unwrap()
                .execute(
                    "UPDATE transactions SET raw = NULL WHERE txid = ?1",
                    [spender_txid.as_ref()],
                )
                .unwrap();
        }
        ProofMutation::TamperedRaw => {
            // The replacement still parses and spends the same outpoint;
            // exact persisted transaction identity and digest must bind it.
            let replacement = transparent_spend_bookkeeping_fixture(vec![consumed], 201);
            open_wallet_connection(&path)
                .unwrap()
                .execute(
                    "UPDATE transactions SET raw = ?1 WHERE txid = ?2",
                    params![
                        raw_transaction(&replacement, 110).data,
                        spender_txid.as_ref()
                    ],
                )
                .unwrap();
        }
        _ => {}
    }

    let fixture = Arc::new(RecoveryFixture::default());
    if mutation == ProofMutation::CanonicalAnchor {
        fixture
            .canonical_overrides
            .lock()
            .unwrap()
            .insert(anchor.height, [0x99; 32]);
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let incoming = stream::unfold(listener, |listener| async {
        let next = listener.accept().await.map(|(stream, _)| stream);
        Some((next, listener))
    });
    let (shutdown, stopped) = tokio::sync::oneshot::channel();
    let service = FixtureService(Arc::clone(&fixture));
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming_shutdown(incoming, async {
                let _ = stopped.await;
            })
            .await
    });
    let channel = Endpoint::from_shared(endpoint)
        .unwrap()
        .connect_timeout(Duration::from_secs(2))
        .connect()
        .await
        .unwrap();
    let mut client = AttestedWcashClient::from_test_channel(network, channel);
    assert_eq!(client.latest_block().await.unwrap(), tip);
    let mut wallet = open_wallet_database(&path, network).unwrap();
    let live_index = usize::from(mutation != ProofMutation::ChangedOutputs);
    let current = partial_creator_fixture(&creator_transaction, live_index);
    let mut restored = PartialTransparentSpendReconciliation::default();
    restored.track(&current, &receiver).unwrap();
    let current_digest = restored.checkpoint_digest();
    assert!(restored.finish().is_err());
    restore_verified_partial_spends(
        &mut client,
        &mut wallet,
        0,
        current_digest,
        &recovery,
        &mut restored,
    )
    .await
    .unwrap();
    assert!(matches!(
        require_transparent_recovery_complete(&mut wallet),
        Err(WalletServiceError::TransparentRecoveryIncomplete)
    ));
    if mutation == ProofMutation::None {
        restored.finish().unwrap();
        assert_eq!(
            crate::partial_recovery::load_witnesses(&mut wallet, 0, digest)
                .unwrap()
                .unwrap(),
            (anchor, receipts)
        );
        assert_eq!(
            crate::partial_recovery::load(&mut wallet, 0, digest).unwrap(),
            Some(anchor)
        );
    } else {
        assert!(restored.finish().is_err(), "{mutation:?}");
        assert_eq!(
            crate::partial_recovery::load_witnesses(&mut wallet, 0, digest).unwrap(),
            None,
            "{mutation:?}"
        );
        assert_eq!(
            crate::partial_recovery::load(&mut wallet, 0, digest).unwrap(),
            None,
            "{mutation:?}"
        );
    }
    assert_eq!(client.latest_block().await.unwrap(), tip);
    let _ = shutdown.send(());
    tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let calls = fixture.calls.lock().unwrap();
    assert!(calls.raw.is_empty());
    assert!(calls.history.is_empty());
    if mutation != ProofMutation::ChangedOutputs {
        assert!(calls.blocks.contains(&anchor.height));
    }
    println!("actual proof restore {mutation:?}: matched_expected_boundary");
}

#[tokio::test]
async fn actual_partial_proof_restore_reopens_exact_witness_and_rejects_invalid_generations() {
    tokio::time::timeout(Duration::from_secs(60), async {
        for mutation in [
            ProofMutation::None,
            ProofMutation::CanonicalAnchor,
            ProofMutation::MissingRaw,
            ProofMutation::TamperedRaw,
            ProofMutation::ChangedOutputs,
        ] {
            restore_proof_case(mutation).await;
        }
    })
    .await
    .unwrap();
}

//! Full native preparation fixtures with controlled independent RPC delays.
//!
//! These exercise the production supervisor, canonical candidate/parent decode,
//! exact proposal RPC, final tip fences and synced native-generation journal.
//! Fake nodes attest proposals; they do not emulate consensus fork choice,
//! solve proof of work, deliver a frontend notify, or prove ASIC installation.

use std::sync::mpsc::{self, Receiver, Sender};

use hex::ToHex;
use serde_json::Value;
use zebra_chain::{
    block::{merkle::AuthDataRoot, ChainHistoryBlockTxAuthCommitmentHash, ChainHistoryMmrRootHash},
    parameters::testnet::{ConfiguredActivationHeights, RegtestParameters},
};

use super::*;

const WAIT: Duration = Duration::from_secs(3);
const PARENT_PAYOUT: &str = "tmJymvcUCn1ctbghvTJpXBwHiMEB8P6wxNV";

struct RpcGate {
    armed: AtomicBool,
    entered: Sender<()>,
    release: Mutex<Receiver<()>>,
    next_delay_ms: AtomicU64,
    response_sent: AtomicBool,
}

struct GateControl {
    gate: Arc<RpcGate>,
    entered: Receiver<()>,
    release: Sender<()>,
}

impl GateControl {
    fn new() -> Self {
        let (entered_tx, entered) = mpsc::channel();
        let (release, release_rx) = mpsc::channel();
        Self {
            gate: Arc::new(RpcGate {
                armed: AtomicBool::new(false),
                entered: entered_tx,
                release: Mutex::new(release_rx),
                next_delay_ms: AtomicU64::new(0),
                response_sent: AtomicBool::new(false),
            }),
            entered,
            release,
        }
    }

    fn arm(&self) {
        self.gate.armed.store(true, Ordering::Release);
    }

    fn release(&self) {
        let _ = self.release.send(());
    }
}

impl Drop for GateControl {
    fn drop(&mut self) {
        self.release();
    }
}

impl RpcGate {
    fn visit(&self) {
        if self.armed.swap(false, Ordering::AcqRel) {
            let _ = self.entered.send(());
            self.release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(8))
                .expect("the fixture releases every held RPC before asserting");
        }
        let delay = self.next_delay_ms.swap(0, Ordering::AcqRel);
        if delay != 0 {
            thread::sleep(Duration::from_millis(delay));
        }
    }
}

#[derive(Clone, Copy)]
enum Role {
    Child,
    Template,
    Validator,
    Observer,
}

#[derive(Clone, Copy, Default)]
enum Reply {
    #[default]
    Valid,
    RpcError(i64),
    WrongChildChain,
    RejectProposal,
    MalformedTip,
}

struct Model {
    parent_tip: [u8; 32],
    parent_height: u32,
    child_reply: Reply,
    catchup_reply: Reply,
    proposal_reply: Reply,
    created: Vec<(String, String)>,
    retired: Vec<(String, String)>,
    template_requests: usize,
    proposals: Vec<Vec<u8>>,
}

impl Model {
    fn new() -> Self {
        Self {
            parent_tip: Network::new_regtest(Default::default()).genesis_hash().0,
            parent_height: 0,
            child_reply: Reply::Valid,
            catchup_reply: Reply::Valid,
            proposal_reply: Reply::Valid,
            created: Vec::new(),
            retired: Vec::new(),
            template_requests: 0,
            proposals: Vec::new(),
        }
    }

    fn dispatch(&mut self, role: Role, request: &Value) -> Result<Value, Value> {
        let method = request["method"].as_str().unwrap();
        let child = matches!(role, Role::Child);
        match method {
            "getblockhash" => {
                assert_eq!(request["params"], json!([0]));
                Ok(json!(if child {
                    Network::new_wcash_regtest().genesis_hash().to_string()
                } else {
                    Network::new_regtest(Default::default())
                        .genesis_hash()
                        .to_string()
                }))
            }
            "getblockstatus" if matches!(role, Role::Template | Role::Validator) => {
                // Work nodes authenticate the required status capability against
                // their identity-bound canonical genesis before preparation.
                let genesis = Network::new_regtest(Default::default())
                    .genesis_hash()
                    .to_string();
                assert_eq!(request["params"], json!([genesis]));
                Ok(json!({
                    "state":"best_chain","hash":genesis,"height":0,
                    "confirmations":self.parent_height + 1,
                }))
            }
            "getblockchaininfo" => {
                if matches!(role, Role::Observer) {
                    if let Reply::RpcError(code) = self.catchup_reply {
                        return Err(json!({"code":code,"message":"fixture catchup response"}));
                    }
                    if matches!(self.catchup_reply, Reply::MalformedTip) {
                        return Ok(json!({"blocks":0,"bestblockhash":"invalid"}));
                    }
                }
                Ok(
                    json!({"blocks":self.parent_height,"bestblockhash":display_hash(self.parent_tip)}),
                )
            }
            "getblockcount" if child => Ok(json!(0)),
            "getbestblockhash" if child => Ok(json!(Network::new_wcash_regtest()
                .genesis_hash()
                .to_string())),
            "createauxblock" if child => {
                if let Reply::RpcError(code) = self.child_reply {
                    return Err(json!({"code":code,"message":"fixture child response"}));
                }
                let (mut candidate, payout) = transparent_wcash_candidate([0x71; 20], 625_000_000);
                assert_eq!(request["params"], json!([payout.encode()]));
                let root = candidate.transactions.iter().collect();
                let header = Arc::make_mut(&mut candidate.header);
                header.previous_block_hash = Network::new_wcash_regtest().genesis_hash();
                header.merkle_root = root;
                header.solution = Solution::for_wcash(Vec::new()).unwrap();
                header.nonce = [u8::try_from(self.created.len() + 1).unwrap(); 32].into();
                let hash = display_hash(candidate.hash().0);
                let token = format!("{:064x}", self.created.len() + 1);
                self.created.push((hash.clone(), token.clone()));
                Ok(json!({
                    "hash":hash,"retiretoken":token,
                    "data":hex::encode(candidate.zcash_serialize_to_vec().unwrap()),
                    "chainid":if matches!(self.child_reply, Reply::WrongChildChain) { 1 } else { WCASH_AUXILIARY_CHAIN_ID },
                    "target":candidate.header.difficulty_threshold.to_expanded().unwrap().encode_hex::<String>(),
                    "height":1,"previousblockhash":candidate.header.previous_block_hash.to_string(),
                    "bits":candidate.header.difficulty_threshold.encode_hex::<String>(),
                    "coinbasevalue":625_000_000,
                }))
            }
            "retireauxblock" if child => {
                let lease = (
                    request["params"][0].as_str().unwrap().to_string(),
                    request["params"][1].as_str().unwrap().to_string(),
                );
                assert!(
                    self.created.contains(&lease),
                    "retire only an issued exact capability"
                );
                assert!(
                    !self.retired.contains(&lease),
                    "the fixture expects one retirement per lease"
                );
                self.retired.push(lease);
                Ok(json!({"state":"retired"}))
            }
            "getblocktemplate" if matches!(role, Role::Template) => {
                assert_eq!(request["params"][0]["mode"], "template");
                self.template_requests += 1;
                let hash = parse_display_hash(
                    request["params"][0]["wcashaux"]["blockhash"]
                        .as_str()
                        .unwrap(),
                    "fixture child hash",
                )
                .unwrap();
                let nonce =
                    u32::try_from(request["params"][0]["wcashaux"]["nonce"].as_u64().unwrap())
                        .unwrap();
                Ok(parent_template(
                    hash,
                    nonce,
                    self.parent_tip,
                    self.parent_height + 1,
                ))
            }
            "getblocktemplate" if matches!(role, Role::Validator) => {
                assert_eq!(request["params"][0]["mode"], "proposal");
                let bytes = hex::decode(request["params"][0]["data"].as_str().unwrap()).unwrap();
                let block: Block = bytes.as_slice().zcash_deserialize_into().unwrap();
                assert_eq!(block.zcash_serialize_to_vec().unwrap(), bytes);
                assert_eq!(block.header.previous_block_hash.0, self.parent_tip);
                assert_eq!(block.coinbase_height().unwrap().0, self.parent_height + 1);
                assert_eq!(
                    block.header.merkle_root,
                    block.transactions.iter().collect()
                );
                self.proposals.push(bytes);
                if matches!(self.proposal_reply, Reply::RejectProposal) {
                    Ok(json!("fixture rejects the exact proposal"))
                } else {
                    Ok(Value::Null)
                }
            }
            _ => panic!("unexpected turnover fixture RPC {method}"),
        }
    }
}

fn parent_template(child: [u8; 32], nonce: u32, previous: [u8; 32], height: u32) -> Value {
    let network = Network::new_regtest(RegtestParameters {
        activation_heights: ConfiguredActivationHeights {
            nu6_3: Some(1),
            ..Default::default()
        },
        ..Default::default()
    });
    let subsidy = block_subsidy(Height(height), &network).unwrap();
    let reward = u64::try_from(
        miner_subsidy(Height(height), &network, subsidy)
            .unwrap()
            .zatoshis(),
    )
    .unwrap();
    let payout: transparent::Address = PARENT_PAYOUT.parse().unwrap();
    let job = crate::PreparedJob::new(
        child,
        Target::MAX,
        crate::JobConfig {
            parent_height: height,
            previous_block_hash: previous,
            timestamp: 1_800_000_000 + height,
            auxiliary_nonce: nonce,
            parent_outputs: vec![crate::ParentOutput {
                value_zatoshis: reward,
                script_pubkey: payout.script().as_raw_bytes().to_vec(),
            }],
            ..Default::default()
        },
    )
    .unwrap();
    let coinbase: Transaction = job.coinbase_bytes().zcash_deserialize_into().unwrap();
    let transactions = [Arc::new(coinbase)];
    let merkle: zebra_chain::block::merkle::Root = transactions.iter().collect();
    let auth: AuthDataRoot = transactions.iter().collect();
    let history = ChainHistoryMmrRootHash::default();
    let commitments = ChainHistoryBlockTxAuthCommitmentHash::from_commitments(&history, &auth);
    let bits = CompactDifficulty::from_hex("207fffff").unwrap();
    json!({
        "version":4,"previousblockhash":display_hash(previous),
        "blockcommitmentshash":commitments.encode_hex::<String>(),
        "defaultroots":{"merkleroot":merkle.encode_hex::<String>(),"chainhistoryroot":history.encode_hex::<String>(),"authdataroot":auth.encode_hex::<String>(),"blockcommitmentshash":commitments.encode_hex::<String>()},
        "transactions":[],
        "coinbasetxn":{"data":hex::encode(job.coinbase_bytes()),"hash":transactions[0].hash().to_string(),"authdigest":transactions[0].auth_digest().unwrap().to_string()},
        "target":bits.to_expanded().unwrap().encode_hex::<String>(),"curtime":1_800_000_000 + height,"bits":bits.encode_hex::<String>(),"height":height,
        "wcashparentpayoutcommitment":hex::encode(wcash_zcash_aux::parent_payout_address_commitment(PARENT_PAYOUT)),
    })
}

struct FixtureServer {
    stop: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
}

impl Drop for FixtureServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let joined = worker.join();
            if !thread::panicking() {
                joined.expect("bounded turnover server exits");
            }
        }
    }
}

fn server(
    role: Role,
    model: Arc<Mutex<Model>>,
    gate: Option<Arc<RpcGate>>,
) -> (RpcEndpoint, FixtureServer) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let endpoint = RpcEndpoint::new(
        format!("http://{}/", listener.local_addr().unwrap()),
        None,
        None,
    )
    .unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let stopping = Arc::clone(&stop);
    let worker = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut count = 0;
        while !stopping.load(Ordering::Acquire) {
            assert!(
                Instant::now() < deadline,
                "turnover fixture server is bounded"
            );
            let mut stream = match listener.accept() {
                Ok((stream, _)) => stream,
                Err(error) if error.kind() == ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(1));
                    continue;
                }
                Err(error) => panic!("turnover accept failed: {error}"),
            };
            let request = read_test_rpc_request(&mut stream);
            count += 1;
            assert!(count <= 128, "turnover fixture RPC count is bounded");
            let gated = matches!(role, Role::Observer) && request["method"] == "getblockchaininfo"
                || matches!(role, Role::Child) && request["method"] == "createauxblock"
                || matches!(role, Role::Validator) && request["method"] == "getblocktemplate";
            if gated {
                if let Some(gate) = gate.as_ref() {
                    gate.visit();
                }
            }
            // No shared model lock is held while either independent RPC waits.
            let reply = model.lock().unwrap().dispatch(role, &request);
            let response = serde_json::to_vec(&match reply {
                Ok(result) => json!({"jsonrpc":"2.0","id":request["id"],"result":result}),
                Err(error) => json!({"jsonrpc":"2.0","id":request["id"],"error":error}),
            })
            .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", response.len()).unwrap();
            stream.write_all(&response).unwrap();
            if matches!(role, Role::Child) && request["method"] == "createauxblock" {
                gate.as_ref()
                    .unwrap()
                    .response_sent
                    .store(true, Ordering::Release);
            }
        }
    });
    (
        endpoint,
        FixtureServer {
            stop,
            worker: Some(worker),
        },
    )
}

struct Fixture {
    // Release controls precede server owners so panic cleanup cannot strand a held request.
    catchup: GateControl,
    child: GateControl,
    proposal: GateControl,
    _servers: Vec<FixtureServer>,
    model: Arc<Mutex<Model>>,
    _directory: tempfile::TempDir,
    supervisor: Arc<NativeMiningSupervisor>,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempdir().unwrap();
        let catchup = GateControl::new();
        let child = GateControl::new();
        let proposal = GateControl::new();
        let model = Arc::new(Mutex::new(Model::new()));
        let (wcash, a) = server(
            Role::Child,
            Arc::clone(&model),
            Some(Arc::clone(&child.gate)),
        );
        let (template, b) = server(Role::Template, Arc::clone(&model), None);
        let (validator, c) = server(
            Role::Validator,
            Arc::clone(&model),
            Some(Arc::clone(&proposal.gate)),
        );
        let (observer, d) = server(
            Role::Observer,
            Arc::clone(&model),
            Some(Arc::clone(&catchup.gate)),
        );
        let supervisor = Arc::new(test_pool_supervisor(
            directory.path(),
            wcash,
            template,
            validator,
            vec![],
            vec![observer],
        ));
        Self {
            catchup,
            child,
            proposal,
            _servers: vec![a, b, c, d],
            model,
            _directory: directory,
            supervisor,
        }
    }

    fn prepare(
        &self,
        execution: CandidatePreparationExecution,
        budget: Duration,
    ) -> Result<NativeMiningCoordinator, MinerError> {
        self.supervisor.prepare_generation_timed_with_execution(
            &mut NativePreparationTiming::new(),
            execution,
            budget,
        )
    }

    fn turn_parent_tip(&self) {
        let mut model = self.model.lock().unwrap();
        model.parent_tip = [0x41; 32];
        model.parent_height = 1;
    }

    fn activations(&self) -> usize {
        fs::read_to_string(self._directory.path().join("native-share-journal.jsonl"))
            .unwrap()
            .lines()
            .filter(|line| {
                serde_json::from_str::<Value>(line).unwrap()["record"] == "job_activated"
            })
            .count()
    }
}

struct PrefixRun {
    catchup_entered: bool,
    child_entered: bool,
    parent_templates_while_held: usize,
    returned_while_held: bool,
    result: Result<NativeMiningCoordinator, MinerError>,
}

fn held_prefix(
    fixture: &Fixture,
    execution: CandidatePreparationExecution,
    budget: Duration,
) -> PrefixRun {
    fixture.catchup.arm();
    fixture.child.arm();
    let preparing = Arc::clone(&fixture.supervisor);
    let worker = thread::spawn(move || {
        preparing.prepare_generation_timed_with_execution(
            &mut NativePreparationTiming::new(),
            execution,
            budget,
        )
    });
    let deadline = Instant::now() + WAIT;
    let catchup_entered = fixture
        .catchup
        .entered
        .recv_timeout(deadline.saturating_duration_since(Instant::now()))
        .is_ok();
    let child_entered = fixture
        .child
        .entered
        .recv_timeout(deadline.saturating_duration_since(Instant::now()))
        .is_ok();
    let parent_templates_while_held = fixture.model.lock().unwrap().template_requests;
    let returned_while_held = worker.is_finished();
    // Release both independent owners before any assertion or join.
    fixture.catchup.release();
    fixture.child.release();
    let result = worker.join().expect("preparation fixture worker exits");
    PrefixRun {
        catchup_entered,
        child_entered,
        parent_templates_while_held,
        returned_while_held,
        result,
    }
}

#[test]
fn unheld_candidate_prepares_a_real_exact_parent_job() {
    let fixture = Fixture::new();
    let mut coordinator = fixture
        .prepare(
            CandidatePreparationExecution::Parallel,
            Duration::from_secs(45),
        )
        .unwrap();
    let descriptor = coordinator.generation_descriptor();
    assert_eq!(
        descriptor.zcash_previous_hash_le(),
        fixture.model.lock().unwrap().parent_tip
    );
    assert_eq!(
        descriptor.wcash_previous_hash_le(),
        Network::new_wcash_regtest().genesis_hash().0
    );
    crate::pool_backend::job_descriptor_from_native(descriptor)
        .unwrap()
        .validate()
        .unwrap();
    assert_eq!(fixture.activations(), 1);
    assert_eq!(fixture.model.lock().unwrap().proposals.len(), 1);
    assert!(coordinator.remaining_job_lifetime() <= Duration::from_secs(45));
    assert_eq!(
        coordinator.retire_generation().unwrap(),
        GenerationRetirement::Retired
    );
}

#[test]
fn zec_tip_turnover_enters_catchup_and_child_before_either_is_released() {
    let fixture = Fixture::new();
    let mut previous = fixture
        .prepare(
            CandidatePreparationExecution::Parallel,
            Duration::from_secs(45),
        )
        .unwrap();
    let old_id = previous.job().job().job_id().to_string();
    fixture.turn_parent_tip();
    let run = held_prefix(
        &fixture,
        CandidatePreparationExecution::Parallel,
        Duration::from_secs(45),
    );
    let mut next = run.result.unwrap();
    let cleanup = next.retire_generation();
    let previous_cleanup = previous.retire_generation();
    assert!(
        run.catchup_entered && run.child_entered,
        "independent prefix RPCs must enter before either response is released"
    );
    assert!(!run.returned_while_held);
    assert_eq!(
        run.parent_templates_while_held, 1,
        "no second child-bound ZEC GBT before the held child is available"
    );
    assert_ne!(next.job().job().job_id(), old_id);
    assert_eq!(
        next.generation_descriptor().zcash_previous_hash_le(),
        [0x41; 32]
    );
    assert_eq!(fixture.activations(), 2);
    assert_eq!(fixture.model.lock().unwrap().proposals.len(), 2);
    assert!(cleanup.is_ok() && previous_cleanup.is_ok());
}

#[test]
fn serial_reference_enters_child_only_after_catchup_is_released() {
    let fixture = Fixture::new();
    let run = held_prefix(
        &fixture,
        CandidatePreparationExecution::Serial,
        Duration::from_secs(45),
    );
    let mut job = run.result.unwrap();
    let retired = job.retire_generation();
    assert!(run.catchup_entered);
    assert!(
        !run.child_entered,
        "the baseline serial ordering must not overlap the held RPCs"
    );
    assert!(!run.returned_while_held);
    assert_eq!(run.parent_templates_while_held, 0);
    assert_eq!(fixture.activations(), 1);
    assert!(retired.is_ok());
}

#[test]
fn failed_prefix_never_activates_and_retires_only_an_exact_successful_child() {
    for (catchup, child, expected_code, retires) in [
        (Reply::RpcError(-28), Reply::Valid, -28, 1),
        (Reply::Valid, Reply::RpcError(-9), -9, 0),
        (Reply::RpcError(-28), Reply::RpcError(-9), -28, 0),
        (Reply::RpcError(-28), Reply::RpcError(-12346), -12346, 0),
        (Reply::RpcError(-12345), Reply::RpcError(-9), -12345, 0),
    ] {
        let fixture = Fixture::new();
        {
            let mut model = fixture.model.lock().unwrap();
            model.catchup_reply = catchup;
            model.child_reply = child;
        }
        let run = held_prefix(
            &fixture,
            CandidatePreparationExecution::Parallel,
            Duration::from_secs(45),
        );
        let error = run
            .result
            .err()
            .expect("one failed prefix cannot create a job");
        assert!(run.catchup_entered && run.child_entered);
        assert!(
            matches!(error, MinerError::RpcError { code: Some(code), .. } if code == expected_code)
        );
        let model = fixture.model.lock().unwrap();
        assert_eq!(model.retired.len(), retires);
        assert_eq!(model.template_requests, 0);
        assert!(model.proposals.is_empty());
        assert_eq!(fixture.activations(), 0);
        assert!(fixture
            .supervisor
            .pending_candidate_retirement
            .lock()
            .unwrap()
            .is_none());
        assert_eq!(
            fixture.supervisor.initial_outbox_recovery_complete.load(Ordering::Acquire),
            matches!(catchup, Reply::Valid),
            "successful catchup keeps its one-time recovery boundary even when child preparation fails"
        );
    }
}

#[test]
fn malformed_child_and_rejected_proposal_keep_the_validation_and_retirement_fences() {
    for proposal_rejected in [false, true] {
        let fixture = Fixture::new();
        {
            let mut model = fixture.model.lock().unwrap();
            if proposal_rejected {
                model.proposal_reply = Reply::RejectProposal;
            } else {
                model.child_reply = Reply::WrongChildChain;
            }
        }
        let result = fixture.prepare(
            CandidatePreparationExecution::Parallel,
            Duration::from_secs(45),
        );
        assert!(result.is_err());
        let model = fixture.model.lock().unwrap();
        assert_eq!(model.created.len(), 1);
        assert_eq!(model.retired.len(), 1);
        assert_eq!(model.proposals.len(), usize::from(proposal_rejected));
        assert_eq!(model.template_requests, usize::from(proposal_rejected));
        assert_eq!(fixture.activations(), 0);
    }
}

#[test]
fn fatal_local_validation_is_not_masked_by_the_other_retryable_prefix() {
    for fatal_child in [false, true] {
        let fixture = Fixture::new();
        {
            let mut model = fixture.model.lock().unwrap();
            if fatal_child {
                model.catchup_reply = Reply::RpcError(-28);
                model.child_reply = Reply::WrongChildChain;
            } else {
                model.catchup_reply = Reply::MalformedTip;
                model.child_reply = Reply::RpcError(-9);
            }
        }
        let run = held_prefix(
            &fixture,
            CandidatePreparationExecution::Parallel,
            Duration::from_secs(45),
        );
        let error = run
            .result
            .err()
            .expect("fatal preparation cannot create a job");
        assert!(run.catchup_entered && run.child_entered);
        assert!(
            !error.is_retryable_preparation(),
            "a fatal local invariant must win over a retryable peer error"
        );
        assert_eq!(fixture.activations(), 0);
        let model = fixture.model.lock().unwrap();
        assert_eq!(model.retired.len(), usize::from(fatal_child));
        assert_eq!(model.template_requests, 0);
    }
}

#[test]
fn catchup_unwind_joins_and_retires_an_already_returned_exact_child() {
    let fixture = Fixture::new();
    let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        fixture.supervisor.prepare_child_with_catchup(
            &mut NativePreparationTiming::new(),
            CandidatePreparationExecution::Parallel,
            || -> Result<(), MinerError> {
                let deadline = Instant::now() + WAIT;
                while !fixture.child.gate.response_sent.load(Ordering::Acquire) {
                    assert!(
                        Instant::now() < deadline,
                        "exact child response must be sent before the injected unwind"
                    );
                    thread::sleep(Duration::from_millis(1));
                }
                panic!("fixture catchup unwind after exact child RPC response");
            },
        )
    }));
    assert!(unwind.is_err());
    let model = fixture.model.lock().unwrap();
    assert_eq!(model.created.len(), 1);
    assert_eq!(model.retired.len(), 1);
    assert_eq!(model.template_requests, 0);
    assert_eq!(fixture.activations(), 0);
    assert!(fixture
        .supervisor
        .pending_candidate_retirement
        .lock()
        .unwrap()
        .is_none());
}

#[test]
fn an_overlapped_result_outside_its_budget_is_retired_without_publication() {
    let fixture = Fixture::new();
    let run = held_prefix(
        &fixture,
        CandidatePreparationExecution::Parallel,
        Duration::ZERO,
    );
    assert!(run.catchup_entered && run.child_entered);
    assert!(run.result.is_err());
    assert_eq!(fixture.activations(), 0);
    let model = fixture.model.lock().unwrap();
    assert_eq!(model.created.len(), 1);
    assert_eq!(model.retired.len(), 1);
}

#[test]
fn proposal_delay_expires_an_overlapped_child_before_journal_activation() {
    let fixture = Fixture::new();
    let budget = Duration::from_secs(3);
    fixture.proposal.arm();
    let preparing = Arc::clone(&fixture.supervisor);
    let worker = thread::spawn(move || {
        preparing.prepare_generation_timed_with_execution(
            &mut NativePreparationTiming::new(),
            CandidatePreparationExecution::Parallel,
            budget,
        )
    });
    let proposal_entered = fixture.proposal.entered.recv_timeout(WAIT).is_ok();
    // Reaching the exact proposal proves the first post-join budget check passed.
    // Hold for a full, ample budget from this later point, then release before assertions.
    if proposal_entered {
        thread::sleep(budget);
    }
    fixture.proposal.release();
    let result = worker.join().expect("expired proposal worker exits");
    assert!(proposal_entered);
    assert!(matches!(result, Err(MinerError::StaleNativeJob(_))));
    assert_eq!(fixture.activations(), 0);
    let model = fixture.model.lock().unwrap();
    assert_eq!(model.template_requests, 1);
    assert_eq!(model.proposals.len(), 1);
    assert_eq!(model.created.len(), 1);
    assert_eq!(model.retired.len(), 1);
}

#[test]
fn failed_worker_spawn_preserves_the_serial_validated_fallback() {
    let fixture = Fixture::new();
    let mut job = fixture
        .prepare(
            CandidatePreparationExecution::SpawnFailure,
            Duration::from_secs(45),
        )
        .unwrap();
    assert_eq!(fixture.activations(), 1);
    assert_eq!(fixture.model.lock().unwrap().proposals.len(), 1);
    assert_eq!(
        job.retire_generation().unwrap(),
        GenerationRetirement::Retired
    );
}

#[test]
#[ignore = "controlled local latency characterization; execute serially off production"]
fn controlled_rpc_readiness_delays_report_real_supervisor_turnover_latency() {
    // The delays model readiness, not actual coinbase proof duration or a production percentile.
    // Serial and parallel modes use the same actual native supervisor/validation/journal path.
    for (label, delay_ms) in [("zero_delay", 0), ("controlled_delay", 250)] {
        // One unmeasured turnover per mode warms fixture/runtime paths, not a proof cache.
        // Five ABBA rounds then give ten measured samples per mode and case.
        let modes = [true, false]
            .into_iter()
            .map(|serial| (serial, None))
            .chain((0..5).flat_map(|round| {
                [true, false, false, true]
                    .into_iter()
                    .enumerate()
                    .map(move |(slot, serial)| (serial, Some((round, slot))))
            }));
        for (serial, sample) in modes {
            let fixture = Fixture::new();
            let mut previous = fixture
                .prepare(
                    CandidatePreparationExecution::Parallel,
                    Duration::from_secs(45),
                )
                .unwrap();
            fixture.turn_parent_tip();
            fixture
                .catchup
                .gate
                .next_delay_ms
                .store(delay_ms, Ordering::Release);
            fixture
                .child
                .gate
                .next_delay_ms
                .store(delay_ms, Ordering::Release);
            let started = Instant::now();
            let execution = if serial {
                CandidatePreparationExecution::Serial
            } else {
                CandidatePreparationExecution::Parallel
            };
            let mut job = fixture.prepare(execution, Duration::from_secs(45)).unwrap();
            let ready_us = started.elapsed().as_micros();
            assert_eq!(fixture.activations(), 2);
            assert_eq!(fixture.model.lock().unwrap().proposals.len(), 2);
            assert_eq!(
                job.generation_descriptor().zcash_previous_hash_le(),
                [0x41; 32]
            );
            assert_eq!(
                job.retire_generation().unwrap(),
                GenerationRetirement::Retired
            );
            assert_eq!(
                previous.retire_generation().unwrap(),
                GenerationRetirement::Retired
            );
            if let Some((round, slot)) = sample {
                eprintln!(
                    "turnover_readiness_sample {}",
                    json!({"case":label,"serial":serial,"round":round,"abba_slot":slot,"ready_us":ready_us,"catchup_delay_ms":delay_ms,"child_delay_ms":delay_ms,"boundary":"supervisor_return_after_native_journal_sync","frontend_notify_measured":false})
                );
            }
        }
    }
}

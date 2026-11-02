// Real captured proofs exercise actor admission/journaling; RPC authority is
// modeled separately. These fixtures do not prove Mainnet canonical acceptance.

use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

use crate::zcash_recovery::{PoolBackendZcashRecoveryAuthority, PoolBackendZcashWorkObservation};

use super::*;

struct Authority {
    genesis: Hex32,
    observation: AtomicU8,
    parent: Mutex<Option<(Hex32, u32)>>,
    reads: AtomicUsize,
}

impl Authority {
    fn new() -> Self {
        Self {
            genesis: config().zcash_genesis,
            observation: AtomicU8::new(2),
            parent: Mutex::new(None),
            reads: AtomicUsize::new(0),
        }
    }

    fn bind(&self, descriptor: &JobDescriptor) {
        *self.parent.lock().unwrap() = Some((
            descriptor.zcash_previous_hash_le.clone(),
            descriptor.zcash_height,
        ));
        self.set(0);
    }

    fn set(&self, category: u8) {
        self.observation.store(category, Ordering::Release);
    }
}

impl PoolBackendZcashRecoveryAuthority for Authority {
    fn expected_genesis(&self) -> Hex32 {
        self.genesis.clone()
    }

    fn observe(&self, descriptor: &JobDescriptor) -> PoolBackendZcashWorkObservation {
        self.reads.fetch_add(1, Ordering::AcqRel);
        match self.observation.load(Ordering::Acquire) {
            0 => {
                let Ok(parent) = self.parent.try_lock() else {
                    return PoolBackendZcashWorkObservation::Unknown;
                };
                if parent.as_ref().is_some_and(|(hash, height)| {
                    hash == &descriptor.zcash_previous_hash_le && *height == descriptor.zcash_height
                }) {
                    PoolBackendZcashWorkObservation::Current
                } else {
                    PoolBackendZcashWorkObservation::Stale
                }
            }
            1 => PoolBackendZcashWorkObservation::Stale,
            _ => PoolBackendZcashWorkObservation::Unknown,
        }
    }
}

fn submit(
    actor: &PoolBackendActor,
    request: BackendRequest,
) -> Result<Vec<BackendMessage>, PoolBackendHandlerError> {
    actor.dispatch(uuid(20), Some(0), BackendRequestKind::SubmitShare, request)
}

fn health(actor: &PoolBackendActor, cursor: Option<u64>) -> serde_json::Value {
    let response = actor
        .dispatch(
            uuid(20),
            cursor,
            BackendRequestKind::Health,
            BackendRequest::Health {
                version: BACKEND_PROTOCOL_VERSION,
                id: 90,
            },
        )
        .unwrap();
    serde_json::to_value(response.last().unwrap()).unwrap()
}

fn stats(actor: &PoolBackendActor) -> ([u64; 3], [[u8; 32]; 3], u64, u64, bool) {
    let stats = actor
        .try_zcash_work_observation()
        .expect("uncontended statistics");
    (
        stats.shares,
        stats.target_work_le,
        stats.first_event_seq,
        stats.last_event_seq,
        stats.overflowed,
    )
}

#[test]
fn fresh_authority_recovers_real_zec_after_ordinary_expiry_without_any_replacement() {
    for detach_first in [false, true] {
        let clock = Arc::new(TestClock::new());
        let authority = Arc::new(Authority::new());
        let (fixture, descriptor, request, retained) =
            independent_submission::late_authority_fixture(clock.clone(), Some(authority.clone()));
        authority.bind(&descriptor);
        fixture
            .actor
            .activate_job(retained, Duration::ZERO)
            .unwrap();
        clock.set(Duration::from_millis(45_001));
        if detach_first {
            health(&fixture.actor, Some(0));
            assert!(fixture.actor.lock_state().unwrap().current.is_none());
        }
        let response = submit(&fixture.actor, request.clone()).unwrap();
        let receipt = match response.last().unwrap() {
            BackendMessage::ShareCommitted {
                receipt,
                replayed: false,
                ..
            } => receipt,
            _ => panic!("recovered proof must have one fresh durable receipt"),
        };
        assert!(matches!(
            receipt.winners.as_slice(),
            [WinnerDescriptor {
                chain: MergedChain::Zcash,
                ..
            }]
        ));
        assert_eq!(
            PoolBackendLiveWinnerScheduler::for_chain(MergedChain::Zcash)
                .next(&fixture.actor)
                .unwrap()
                .unwrap()
                .block_bytes(),
            fixture.zcash_bytes,
        );
        assert!(
            PoolBackendLiveWinnerScheduler::for_chain(MergedChain::Wcash)
                .next(&fixture.actor)
                .unwrap()
                .is_none()
        );
        let before_replay = stats(&fixture.actor);
        assert_eq!(before_replay.0, [1, 0, 0]);
        assert_eq!(before_replay.2, receipt.event_seq);
        assert_eq!(before_replay.3, receipt.event_seq);
        let replay = submit(&fixture.actor, request).unwrap();
        assert!(matches!(
            replay.last(),
            Some(BackendMessage::ShareCommitted { replayed: true, .. })
        ));
        assert_eq!(stats(&fixture.actor), before_replay);
        let state = fixture.actor.lock_state().unwrap();
        assert_eq!(state.shares.len(), 1);
        assert!(state
            .current
            .as_ref()
            .is_none_or(|id| { acceptable_job(&state.live_jobs[id], clock.now()).is_none() }));
    }
}

#[test]
fn legacy_context_does_not_gain_authority_after_ordinary_expiry() {
    let clock = Arc::new(TestClock::new());
    let (fixture, _, request, retained) =
        independent_submission::late_authority_fixture(clock.clone(), None);
    fixture
        .actor
        .activate_job(retained, Duration::ZERO)
        .unwrap();
    clock.set(Duration::from_millis(45_001));
    assert_eq!(submit(&fixture.actor, request).unwrap_err(), stale_job());
    assert!(fixture.actor.lock_state().unwrap().shares.is_empty());
}

#[test]
fn recovery_preserves_original_tail_deadline_before_and_after_detachment() {
    for shortened in [false, true] {
        for at_boundary in [false, true] {
            let clock = Arc::new(TestClock::new());
            let authority = Arc::new(Authority::new());
            let (fixture, mut descriptor, mut request, _) =
                independent_submission::late_authority_fixture(
                    clock.clone(),
                    Some(authority.clone()),
                );
            let remaining = if shortened { 1_234 } else { 45_000 };
            descriptor.max_age_ms = remaining;
            let original = crate::native::NativeZcashWinnerSnapshot::from_verified_test_block(
                &fixture.zcash_bytes,
            );
            let retained = Arc::new(SnapshotJob {
                metadata: TestRetainedJob::new(descriptor.clone()),
                snapshot: Arc::new(original),
            });
            // Only admission lifetime changes; exact proof/header/target metadata stays frozen.
            if let BackendRequest::SubmitShare { job_id, .. } = &mut request {
                *job_id = descriptor.job_id.clone();
            }
            authority.bind(&descriptor);
            fixture
                .actor
                .activate_job(retained, Duration::ZERO)
                .unwrap();
            clock.set(Duration::from_millis(u64::from(remaining) + 1));
            health(&fixture.actor, Some(0));
            let deadline = Duration::from_millis(u64::from(remaining) + 60_000);
            assert_eq!(
                fixture.actor.lock_state().unwrap().zcash_winner_snapshots[&descriptor.job_id]
                    .deadline,
                deadline
            );
            clock.set(deadline - Duration::from_millis(u64::from(!at_boundary)));
            let response = submit(&fixture.actor, request);
            assert_eq!(response.is_ok(), !at_boundary);
            if at_boundary {
                assert!(fixture.actor.lock_state().unwrap().shares.is_empty());
            }
        }
    }
}

struct SnapshotJob {
    metadata: TestRetainedJob,
    snapshot: Arc<dyn PoolBackendZcashWinnerSnapshot>,
}

impl PoolBackendRetainedJob for SnapshotJob {
    fn zcash_winner_snapshot(&self) -> Option<Arc<dyn PoolBackendZcashWinnerSnapshot>> {
        Some(self.snapshot.clone())
    }
    fn descriptor(&self) -> JobDescriptor {
        self.metadata.descriptor()
    }
    fn wcash_payout_commitment(&self) -> Hex32 {
        self.metadata.wcash_payout_commitment()
    }
    fn zcash_payout_commitment(&self) -> Hex32 {
        self.metadata.zcash_payout_commitment()
    }
    fn remaining_lifetime(&self) -> Option<Duration> {
        self.metadata.remaining_lifetime()
    }
    fn is_healthy(&self) -> bool {
        self.metadata.is_healthy()
    }
    fn is_zcash_current(&self) -> bool {
        self.metadata.is_zcash_current()
    }
    fn validate_share(
        &self,
        share: PoolBackendShareRequest<'_>,
    ) -> Result<PoolBackendValidatedShare, PoolBackendShareValidationError> {
        self.metadata.validate_share(share)
    }
}

#[test]
fn recovery_requires_current_exact_parent_and_never_commits_unknown_or_stale_work() {
    for defect in ["stale", "unknown", "hash", "height", "no_sample"] {
        let clock = Arc::new(TestClock::new());
        let authority = Arc::new(Authority::new());
        let (fixture, descriptor, request, retained) =
            independent_submission::late_authority_fixture(clock.clone(), Some(authority.clone()));
        if defect != "no_sample" {
            authority.bind(&descriptor);
        }
        match defect {
            "stale" => authority.set(1),
            "unknown" => authority.set(2),
            "hash" => authority.parent.lock().unwrap().as_mut().unwrap().0 = Hex32::new([0xfa; 32]),
            "height" => authority.parent.lock().unwrap().as_mut().unwrap().1 += 1,
            _ => (),
        }
        fixture
            .actor
            .activate_job(retained, Duration::ZERO)
            .unwrap();
        clock.set(Duration::from_millis(45_001));
        let error = submit(&fixture.actor, request).unwrap_err();
        assert_eq!(
            error,
            if matches!(defect, "unknown" | "no_sample") {
                recovery_deferred()
            } else {
                stale_job()
            }
        );
        assert!(fixture.actor.lock_state().unwrap().shares.is_empty());
        assert_eq!(stats(&fixture.actor).0, [0; 3]);
    }
}

#[test]
fn recovery_health_is_explicit_cursor_bound_and_never_ordinary_healthy() {
    let clock = Arc::new(TestClock::new());
    let authority = Arc::new(Authority::new());
    let (fixture, descriptor, _, retained) =
        independent_submission::late_authority_fixture(clock.clone(), Some(authority.clone()));
    authority.bind(&descriptor);
    fixture
        .actor
        .activate_job(retained, Duration::ZERO)
        .unwrap();
    let ordinary = health(&fixture.actor, Some(0));
    assert_eq!(ordinary["healthy"], true);
    assert!(ordinary.get("recovery_only").is_none());
    clock.set(Duration::from_millis(45_001));
    let recovery = health(&fixture.actor, Some(0));
    assert_eq!(recovery["healthy"], false);
    assert_eq!(recovery["recovery_only"], true);
    assert!(fixture.actor.lock_state().unwrap().current.is_none());
    let read_only = health(&fixture.actor, None);
    assert_eq!(read_only["healthy"], false);
    assert!(read_only.get("recovery_only").is_none());
    authority.set(2);
    assert_eq!(health(&fixture.actor, Some(0))["recovery_only"], true);
    authority.set(1);
    assert!(health(&fixture.actor, Some(0))
        .get("recovery_only")
        .is_none());
}

#[test]
fn unknown_authority_defers_without_credit_then_same_exact_proof_commits_once() {
    let clock = Arc::new(TestClock::new());
    let authority = Arc::new(Authority::new());
    let (fixture, descriptor, request, retained) =
        independent_submission::late_authority_fixture(clock.clone(), Some(authority.clone()));
    fixture
        .actor
        .activate_job(retained, Duration::ZERO)
        .unwrap();
    clock.set(Duration::from_millis(45_001));
    assert_eq!(
        submit(&fixture.actor, request.clone()).unwrap_err(),
        recovery_deferred()
    );
    assert!(fixture.actor.lock_state().unwrap().shares.is_empty());
    assert_eq!(stats(&fixture.actor).0, [0; 3]);
    authority.bind(&descriptor);
    let response = submit(&fixture.actor, request.clone()).unwrap();
    assert!(matches!(
        response.last(),
        Some(BackendMessage::ShareCommitted {
            replayed: false,
            ..
        })
    ));
    assert_eq!(stats(&fixture.actor).0, [1, 0, 0]);
    submit(&fixture.actor, request).unwrap();
    assert_eq!(stats(&fixture.actor).0, [1, 0, 0]);
}

#[test]
fn authority_genesis_is_bound_before_actor_use() {
    let directory = private_temp_dir();
    let configuration = config();
    let clock = Arc::new(TestClock::new());
    let mut authority = Authority::new();
    authority.genesis = Hex32::new([0x99; 32]);
    assert!(actor(
        &directory.path().join("wrong-genesis.jsonl"),
        &configuration,
        clock
    )
    .with_zcash_recovery_authority(Arc::new(authority))
    .is_err());
}

#[test]
fn ordinary_work_statistics_are_partitioned_exact_and_idempotent() {
    // The ordinary-share validator is a local test double: this test covers
    // accounting of already validated durable receipts, not consensus proof validity.
    let directory = private_temp_dir();
    let configuration = config();
    let clock = Arc::new(TestClock::new());
    let authority = Arc::new(Authority::new());
    let actor = actor(
        &directory.path().join("work-observation.jsonl"),
        &configuration,
        clock,
    )
    .with_zcash_recovery_authority(authority.clone())
    .unwrap();
    let mut descriptor = job(0x83);
    descriptor.wcash_target_le = TargetLe::new([1; 32]);
    descriptor.zcash_target_le = TargetLe::new([1; 32]);
    authority.bind(&descriptor);
    actor
        .activate_job(
            Arc::new(TestRetainedJob::new(descriptor.clone())),
            Duration::ZERO,
        )
        .unwrap();
    let mut receipts = Vec::new();
    let mut last_request = None;
    for (ordinal, category, leading) in [(0, 0, 0xff), (1, 1, 0x7f), (2, 2, 0x3f), (3, 0, 0xff)] {
        authority.set(category);
        let mut request = submit_request(ordinal + 1, &descriptor);
        if let BackendRequest::SubmitShare {
            nonce, target_le, ..
        } = &mut request
        {
            *nonce = Hex32::new([ordinal as u8 + 1; 32]);
            let mut target = [0xff; 32];
            target[31] = leading;
            *target_le = TargetLe::new(target);
        }
        let response = submit(&actor, request.clone()).unwrap();
        let Some(BackendMessage::ShareCommitted { receipt, .. }) = response.last() else {
            panic!("ordinary commit")
        };
        receipts.push(receipt.event_seq);
        last_request = Some(request);
    }
    let observed = stats(&actor);
    assert_eq!(observed.0, [2, 1, 1]);
    for (category, expected) in [2u8, 2, 4].into_iter().enumerate() {
        let mut work = [0u8; 32];
        work[0] = expected;
        assert_eq!(observed.1[category], work);
    }
    assert_eq!(observed.2, receipts[0]);
    assert_eq!(observed.3, *receipts.last().unwrap());
    assert!(!observed.4);
    submit(&actor, last_request.unwrap()).unwrap();
    assert_eq!(stats(&actor), observed);
    let mut rejected = submit_request(100, &descriptor);
    if let BackendRequest::SubmitShare { time, .. } = &mut rejected {
        *time = Hex4::new([9; 4]);
    }
    assert!(submit(&actor, rejected).is_err());
    assert_eq!(stats(&actor), observed);
}

#[test]
fn authority_never_turns_nonwinning_or_invalid_proof_into_recovered_work() {
    for defect in [
        "nonwinner",
        "equihash",
        "time",
        "unknown_job",
        "unsafe_target",
    ] {
        let clock = Arc::new(TestClock::new());
        let authority = Arc::new(Authority::new());
        let (fixture, descriptor, mut request, retained) =
            independent_submission::late_authority_fixture(clock.clone(), Some(authority.clone()));
        authority.bind(&descriptor);
        fixture
            .actor
            .activate_job(retained, Duration::ZERO)
            .unwrap();
        clock.set(Duration::from_millis(45_001));
        let BackendRequest::SubmitShare {
            time,
            nonce,
            solution,
            job_id,
            target_le,
            ..
        } = &mut request
        else {
            unreachable!()
        };
        match defect {
            "time" => *time = Hex4::new([1, 2, 3, 4]),
            "unknown_job" => *job_id = Hex32::new([0xee; 32]),
            "unsafe_target" => *target_le = TargetLe::new([1; 32]),
            _ => {
                let target =
                    wcash_zcash_aux::Target::from_le_bytes(*descriptor.zcash_target_le.as_bytes())
                        .unwrap();
                let original = *solution.as_bytes();
                let mut selected = false;
                for changed in 1..=u8::MAX {
                    let mut bytes = original;
                    bytes[0] ^= changed;
                    let candidate = Hex1344::new(bytes);
                    let hash = canonical_parent_header_hash_le(
                        &descriptor.header_input,
                        nonce,
                        &candidate,
                    );
                    if target.is_met_by_le_hash(*hash.as_bytes()) == (defect == "equihash") {
                        *solution = Box::new(candidate);
                        selected = true;
                        break;
                    }
                }
                assert!(
                    selected,
                    "bounded mutation chooses the intended network-target side"
                );
            }
        }
        authority.reads.store(0, Ordering::Release);
        let error = submit(&fixture.actor, request).unwrap_err();
        if defect == "equihash" {
            assert_eq!(
                error,
                PoolBackendShareValidationError::InvalidEquihash.handler_error()
            );
        }
        if defect == "nonwinner" {
            assert_eq!(
                authority.reads.load(Ordering::Acquire),
                0,
                "non-ZEC hashes do not consult recovery authority"
            );
        }
        assert!(fixture.actor.lock_state().unwrap().shares.is_empty());
        assert_eq!(stats(&fixture.actor).0, [0; 3]);
    }
}

struct UnreachableSnapshot;
impl PoolBackendZcashWinnerSnapshot for UnreachableSnapshot {
    fn retained_bytes(&self) -> usize {
        1
    }
    fn validate_zcash_winner(
        &self,
        _: PoolBackendShareRequest<'_>,
    ) -> Result<Vec<u8>, PoolBackendShareValidationError> {
        panic!("a real WEC-only proof must fail original ZEC target prefilter");
    }
}

#[test]
fn real_wec_only_proof_is_not_admitted_through_zec_recovery() {
    use sha2::Digest as _;
    use zebra_chain::{
        serialization::ZcashSerialize, transaction::Transaction, transparent::Input,
        work::difficulty::CompactDifficulty,
    };
    let directory = private_temp_dir();
    let clock = Arc::new(TestClock::new());
    let authority = Arc::new(Authority::new());
    let actor = actor(
        &directory.path().join("wec-only.jsonl"),
        &config(),
        clock.clone(),
    )
    .with_zcash_recovery_authority(authority.clone())
    .unwrap();
    // Preserve the public block's actual header, targets, proof and chain
    // metadata. Authority/genesis/payout configuration is still modeled here;
    // this actor regression is not a current Mainnet admission or payout test.
    let bytes = hex::decode(include_str!("../vectors/wcash-mainnet-000048509-wec-only.txt").trim())
        .unwrap();
    assert_eq!(
        hex::encode(sha2::Sha256::digest(&bytes)),
        "28079d3d286480baedc4dffd230a184fd730b04ea4879eb1b0275877f49ceacd"
    );
    let child: Block = bytes.as_slice().zcash_deserialize_into().unwrap();
    assert_eq!(child.zcash_serialize_to_vec().unwrap(), bytes);
    assert_eq!(
        child.hash().to_string(),
        "3c36883dd11261485149b5943b3d388a46a6263bf4395a6045c8a803f0c899aa"
    );
    assert_eq!(child.coinbase_height().unwrap().0, 48_509);
    let proof =
        wcash_zcash_aux::AuxPowProof::decode(child.header.solution.as_wcash().unwrap().as_bytes())
            .unwrap();
    let parent_coinbase: Transaction = proof.coinbase_bytes().zcash_deserialize_into().unwrap();
    let parent_height = match parent_coinbase.inputs().as_slice() {
        [Input::Coinbase { height, .. }] => height.0,
        _ => panic!("captured proof must contain one canonical parent coinbase input"),
    };
    assert_eq!(parent_height, 3_539_592);
    let child_target: U256 = child
        .header
        .difficulty_threshold
        .to_expanded()
        .unwrap()
        .into();
    let child_target =
        wcash_zcash_aux::Target::from_le_bytes(child_target.to_little_endian()).unwrap();
    proof
        .validate(child.hash().0, child_target)
        .expect("captured proof is valid WEC AuxPoW");
    assert_eq!(proof.parent_header().advertised_n_bits(), 0x1c00_8620);
    let bits = CompactDifficulty::from_bytes_in_display_order(
        &proof.parent_header().advertised_n_bits().to_be_bytes(),
    )
    .unwrap();
    let parent_target: U256 = bits.to_expanded().unwrap().into();
    let parent_target =
        wcash_zcash_aux::Target::from_le_bytes(parent_target.to_little_endian()).unwrap();
    assert!(
        !parent_target.is_met_by_le_hash(*proof.parent_header().block_hash().as_le_bytes()),
        "fixture is a real WEC winner that misses its original ZEC target"
    );
    let header = proof.parent_header().as_bytes();
    let mut descriptor = job(0x85);
    descriptor.max_age_ms = 45_000;
    descriptor.header_input = Hex108::new(header[..108].try_into().unwrap());
    descriptor.wcash_candidate_hash_le = Hex32::new(child.hash().0);
    descriptor.wcash_previous_hash_le = Hex32::new(child.header.previous_block_hash.0);
    descriptor.wcash_coinbase_txid_le = Hex32::new(child.transactions[0].hash().0);
    descriptor.wcash_height = child.coinbase_height().unwrap().0;
    descriptor.wcash_target_le = TargetLe::new(child_target.to_le_bytes());
    descriptor.zcash_previous_hash_le = Hex32::new(header[4..36].try_into().unwrap());
    descriptor.zcash_coinbase_txid_le = Hex32::new(parent_coinbase.hash().0);
    descriptor.zcash_height = parent_height;
    descriptor.zcash_target_le = TargetLe::new(parent_target.to_le_bytes());
    descriptor
        .validate()
        .expect("exact captured proof metadata forms a strict protocol job");
    authority.bind(&descriptor);
    let request = BackendRequest::SubmitShare {
        version: BACKEND_PROTOCOL_VERSION,
        id: 1,
        job_id: descriptor.job_id.clone(),
        identity: identity(),
        target_le: TargetLe::new([0xff; 32]),
        time: Hex4::new(header[100..104].try_into().unwrap()),
        nonce: Hex32::new(header[108..140].try_into().unwrap()),
        solution: Box::new(Hex1344::new(header[143..1487].try_into().unwrap())),
    };
    actor
        .activate_job(
            Arc::new(SnapshotJob {
                metadata: TestRetainedJob::new(descriptor),
                snapshot: Arc::new(UnreachableSnapshot),
            }),
            Duration::ZERO,
        )
        .unwrap();
    clock.set(Duration::from_millis(45_001));
    authority.reads.store(0, Ordering::Release);
    assert_eq!(submit(&actor, request).unwrap_err(), stale_job());
    assert_eq!(authority.reads.load(Ordering::Acquire), 0);
    assert!(actor.lock_state().unwrap().shares.is_empty());
    assert_eq!(stats(&actor).0, [0; 3]);
}

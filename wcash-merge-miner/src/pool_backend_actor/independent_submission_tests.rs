// Included in the actor test module so fixtures exercise the real journal and
// authority-bound snapshots without adding a production injection API.
pub(crate) mod independent_submission {
    use super::*;

    /// Exact two-chain proof from the existing AuxPoW vector. Its parent meets
    /// its own encoded target as well as the child target. This validates proof
    /// framing, metadata, and PoW, not a claim of canonical chain acceptance.
    pub(crate) struct IndependentWinnerFixture {
        _directory: TempDir,
        pub(crate) actor: Arc<PoolBackendActor>,
        descriptor: JobDescriptor,
        parent_hash: Hex32,
        pub(crate) wcash_bytes: Vec<u8>,
        pub(crate) zcash_bytes: Vec<u8>,
    }

    impl IndependentWinnerFixture {
        pub(crate) fn empty() -> Self {
            Self::new(false)
        }

        pub(crate) fn retained_dual() -> Self {
            Self::new(true)
        }

        fn new(retained: bool) -> Self {
            Self::with_clock(retained, Arc::new(TestClock::new()))
        }

        fn with_clock(retained: bool, clock: Arc<TestClock>) -> Self {
            let directory = private_temp_dir();
            let path = directory.path().join("independent-winners.jsonl");
            let configuration = config();
            let wcash_bytes = hex::decode(
                include_str!("../vectors/wcash-valid-dual-auxpow-nonzero-parent.txt").trim(),
            )
            .unwrap();
            let child: Block = wcash_bytes.as_slice().zcash_deserialize_into().unwrap();
            let proof = wcash_zcash_aux::AuxPowProof::decode(
                child.header.solution.as_wcash().unwrap().as_bytes(),
            )
            .unwrap();
            assert!(proof.parent_merkle_branch().is_empty());
            assert!(proof.auth_data_merkle_branch().is_empty());
            let mut zcash_bytes = proof.parent_header().as_bytes().to_vec();
            zcash_bytes.push(1); // Canonical CompactSize for the sole coinbase.
            zcash_bytes.extend_from_slice(proof.coinbase_bytes());
            let parent: Block = zcash_bytes.as_slice().zcash_deserialize_into().unwrap();
            let mut parent_display_hash = parent.hash().0;
            parent_display_hash.reverse();
            let parent_height = parent.coinbase_height().unwrap().0;
            let parent_hash = Hex32::new(
                validate_persisted_winner_block(
                    &zcash_bytes,
                    &hex::encode(parent_display_hash),
                    parent_height,
                    PersistedWinnerBlockKind::Zcash,
                )
                .expect("fixture parent has real valid Equihash and meets its encoded target"),
            );
            let child_target: U256 = child
                .header
                .difficulty_threshold
                .to_expanded()
                .unwrap()
                .into();
            let parent_target: U256 = parent
                .header
                .difficulty_threshold
                .to_expanded()
                .unwrap()
                .into();
            let mut descriptor = job(0x78);
            descriptor.wcash_candidate_hash_le = Hex32::new(child.hash().0);
            descriptor.wcash_previous_hash_le = Hex32::new(child.header.previous_block_hash.0);
            descriptor.zcash_previous_hash_le = Hex32::new(parent.header.previous_block_hash.0);
            descriptor.wcash_coinbase_txid_le = Hex32::new(child.transactions[0].hash().0);
            descriptor.zcash_coinbase_txid_le = Hex32::new(parent.transactions[0].hash().0);
            descriptor.wcash_height = child.coinbase_height().unwrap().0;
            descriptor.zcash_height = parent_height;
            descriptor.wcash_target_le = TargetLe::new(child_target.to_little_endian());
            descriptor.zcash_target_le = TargetLe::new(parent_target.to_little_endian());
            descriptor.wcash_maturity_confirmations = 2;
            descriptor.zcash_maturity_confirmations = 2;
            descriptor.header_input =
                Hex108::new(proof.parent_header().as_bytes()[..108].try_into().unwrap());
            descriptor
                .validate()
                .expect("strict synthetic dual vector forms a valid exact protocol job");

            let journal = create_journal(&path, &configuration);
            if retained {
                journal
                    .append_event(BackendEvent::JobActivated {
                        event_seq: 0,
                        job: descriptor.clone(),
                    })
                    .unwrap();
                append_exact(
                    &journal,
                    &descriptor,
                    &parent_hash,
                    &wcash_bytes,
                    &zcash_bytes,
                    1,
                    true,
                    true,
                );
                drop(journal);
            } else {
                drop(journal);
            }
            // Reopening validates exact persisted bytes; the actor also closes
            // the abandoned generation exactly as normal restart recovery does.
            let actor = Arc::new(actor_from_journal(
                open_journal(&path, &configuration),
                clock,
            ));
            Self {
                _directory: directory,
                actor,
                descriptor,
                parent_hash,
                wcash_bytes,
                zcash_bytes,
            }
        }

        pub(crate) fn append(&self, ordinal: u32, wcash: bool, zcash: bool) -> Hex32 {
            if self.actor.lock_state().unwrap().current.is_none() {
                let mut descriptor = self.descriptor.clone();
                descriptor.job_id = Hex32::new([0x79; 32]);
                self.actor
                    .activate_job(Arc::new(TestRetainedJob::new(descriptor)), Duration::ZERO)
                    .unwrap();
            }
            let share_id = {
                let state = self.actor.lock_state().unwrap();
                let descriptor = &state
                    .live_jobs
                    .get(state.current.as_ref().unwrap())
                    .unwrap()
                    .descriptor;
                append_exact(
                    &state.journal,
                    descriptor,
                    &self.parent_hash,
                    &self.wcash_bytes,
                    &self.zcash_bytes,
                    ordinal,
                    wcash,
                    zcash,
                )
            };
            self.actor.wake_winner_worker().unwrap();
            share_id
        }
    }

    struct ProofCheckingRetainedJob {
        metadata: TestRetainedJob,
        wcash_bytes: Vec<u8>,
        zcash_bytes: Vec<u8>,
        snapshot: Arc<dyn PoolBackendZcashWinnerSnapshot>,
    }

    impl PoolBackendRetainedJob for ProofCheckingRetainedJob {
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
            self.metadata.validations.fetch_add(1, Ordering::Acquire);
            let mut bytes = self.metadata.descriptor.header_input.as_bytes().to_vec();
            bytes.extend_from_slice(share.nonce().as_bytes());
            bytes.extend_from_slice(&[0xfd, 0x40, 0x05]);
            bytes.extend_from_slice(share.solution().as_bytes());
            let header = wcash_zcash_aux::ParentHeader::decode(&bytes)
                .map_err(|_| PoolBackendShareValidationError::InvalidEquihash)?;
            let share_target =
                wcash_zcash_aux::Target::from_le_bytes(*share.target_le().as_bytes())
                    .map_err(|_| PoolBackendShareValidationError::BackendUnavailable)?;
            let work = header
                .validate_work(share_target)
                .map_err(|error| match error {
                    wcash_zcash_aux::AuxPowError::InsufficientParentWork { .. } => {
                        PoolBackendShareValidationError::LowDifficulty
                    }
                    _ => PoolBackendShareValidationError::InvalidEquihash,
                })?;
            // The harness only materializes the one frozen, real proof. It
            // cannot return preconfigured winner bytes for another submission.
            if bytes.as_slice() != &self.zcash_bytes[..1487] {
                return Err(PoolBackendShareValidationError::BackendUnavailable);
            }
            let child: Block = self
                .wcash_bytes
                .as_slice()
                .zcash_deserialize_into()
                .unwrap();
            let proof = wcash_zcash_aux::AuxPowProof::decode(
                child.header.solution.as_wcash().unwrap().as_bytes(),
            )
            .unwrap();
            let wcash_target = wcash_zcash_aux::Target::from_le_bytes(
                *self.metadata.descriptor.wcash_target_le.as_bytes(),
            )
            .unwrap();
            proof
                .validate(child.hash().0, wcash_target)
                .expect("the submitted exact proof has real Equihash and bound WEC commitment");
            let zcash_target = wcash_zcash_aux::Target::from_le_bytes(
                *self.metadata.descriptor.zcash_target_le.as_bytes(),
            )
            .unwrap();
            let hash = work.block_hash().into_le_bytes();
            Ok(PoolBackendValidatedShare::with_winners(
                wcash_target
                    .is_met_by_le_hash(hash)
                    .then(|| self.wcash_bytes.clone()),
                zcash_target
                    .is_met_by_le_hash(hash)
                    .then(|| self.zcash_bytes.clone()),
            ))
        }
    }

    /// This vector has real Equihash and authenticated merged-mining bindings.
    /// Its synthetic chain context is not a current consensus-valid Mainnet block.
    fn verified_dual_submission(
        fixture: &IndependentWinnerFixture,
    ) -> (JobDescriptor, BackendRequest, Arc<ProofCheckingRetainedJob>) {
        let child: Block = fixture
            .wcash_bytes
            .as_slice()
            .zcash_deserialize_into()
            .unwrap();
        let proof = wcash_zcash_aux::AuxPowProof::decode(
            child.header.solution.as_wcash().unwrap().as_bytes(),
        )
        .unwrap();
        let child_target =
            wcash_zcash_aux::Target::from_le_bytes(*fixture.descriptor.wcash_target_le.as_bytes())
                .unwrap();
        let parent_target =
            wcash_zcash_aux::Target::from_le_bytes(*fixture.descriptor.zcash_target_le.as_bytes())
                .unwrap();
        proof
            .validate(child.hash().0, child_target)
            .expect("real Equihash and both coinbase commitment bindings validate");
        let parent_work = proof
            .parent_header()
            .validate_work(parent_target)
            .expect("real Equihash parent meets its unchanged encoded Zcash target");
        assert_eq!(
            parent_work.block_hash().as_le_bytes(),
            fixture.parent_hash.as_bytes()
        );

        let header = proof.parent_header().as_bytes();
        let mut descriptor = fixture.descriptor.clone();
        descriptor.max_age_ms = 45_000;
        assert_eq!(
            descriptor.wcash_target_le,
            fixture.descriptor.wcash_target_le
        );
        assert_eq!(
            descriptor.zcash_target_le,
            fixture.descriptor.zcash_target_le
        );
        let request = BackendRequest::SubmitShare {
            version: BACKEND_PROTOCOL_VERSION,
            id: 2,
            job_id: descriptor.job_id.clone(),
            identity: identity(),
            target_le: TargetLe::new([0xff; 32]),
            time: Hex4::new(header[100..104].try_into().unwrap()),
            nonce: Hex32::new(header[108..140].try_into().unwrap()),
            solution: Box::new(Hex1344::new(header[143..1487].try_into().unwrap())),
        };
        let retained = Arc::new(ProofCheckingRetainedJob {
            metadata: TestRetainedJob::new(descriptor.clone()),
            wcash_bytes: fixture.wcash_bytes.clone(),
            zcash_bytes: fixture.zcash_bytes.clone(),
            snapshot: Arc::new(
                crate::native::NativeZcashWinnerSnapshot::from_verified_test_block(
                    &fixture.zcash_bytes,
                ),
            ),
        });
        (descriptor, request, retained)
    }

    fn replace_wec_tip_only(
        fixture: &IndependentWinnerFixture,
        previous: &JobDescriptor,
    ) -> Arc<TestRetainedJob> {
        // Only transition metadata is modeled for this replacement. Its bytes
        // are never presented as a proposal-validated replacement mining job.
        let mut replacement = previous.clone();
        replacement.job_id = Hex32::new([0x7a; 32]);
        replacement.wcash_previous_hash_le = Hex32::new([0xa1; 32]);
        replacement.wcash_candidate_hash_le = Hex32::new([0xa2; 32]);
        replacement.wcash_height = replacement.wcash_height.checked_add(1).unwrap();
        assert_ne!(
            replacement.wcash_previous_hash_le,
            previous.wcash_previous_hash_le
        );
        assert_eq!(
            replacement.zcash_previous_hash_le,
            previous.zcash_previous_hash_le
        );
        assert_eq!(replacement.zcash_height, previous.zcash_height);
        assert_eq!(replacement.header_input, previous.header_input);
        assert_eq!(replacement.zcash_target_le, previous.zcash_target_le);
        assert_eq!(replacement.wcash_target_le, previous.wcash_target_le);
        let retained = Arc::new(TestRetainedJob::new(replacement));
        fixture
            .actor
            .activate_job(retained.clone(), Duration::from_millis(2_000))
            .unwrap();
        retained
    }

    #[test]
    fn late_winner_reproduction_real_dual_proof_commits_while_admissible() {
        // Every case owns a distinct actor and journal: an earlier commit must
        // never make the later rejection case pass as an idempotent replay.
        for after_wec_rollover in [false, true] {
            let clock = Arc::new(TestClock::new());
            let fixture = IndependentWinnerFixture::with_clock(false, Arc::clone(&clock));
            let (descriptor, request, retained) = verified_dual_submission(&fixture);
            fixture
                .actor
                .activate_job(retained.clone(), Duration::ZERO)
                .unwrap();
            if after_wec_rollover {
                replace_wec_tip_only(&fixture, &descriptor);
                clock.advance(Duration::from_millis(1_999));
            }
            let response = fixture
                .actor
                .dispatch(uuid(20), Some(0), BackendRequestKind::SubmitShare, request)
                .expect("the exact real proof is admitted before its policy deadline");
            assert_eq!(retained.metadata.validations.load(Ordering::Acquire), 1);
            assert!(matches!(
                response.last(),
                Some(BackendMessage::ShareCommitted {
                    receipt: ShareReceipt { winners, .. },
                    replayed: false,
                    ..
                }) if winners.len() == 2
                    && winners.iter().any(|winner| winner.chain == MergedChain::Zcash)
            ));
            let parent = PoolBackendLiveWinnerScheduler::for_chain(MergedChain::Zcash)
                .next(&fixture.actor)
                .unwrap()
                .unwrap();
            assert_eq!(parent.block_bytes(), fixture.zcash_bytes);
        }
    }

    #[test]
    fn late_winner_recovery_wec_rollover_commits_only_zcash_after_ordinary_grace() {
        let clock = Arc::new(TestClock::new());
        let fixture = IndependentWinnerFixture::with_clock(false, Arc::clone(&clock));
        let (descriptor, request, retained) = verified_dual_submission(&fixture);
        fixture
            .actor
            .activate_job(retained.clone(), Duration::ZERO)
            .unwrap();
        replace_wec_tip_only(&fixture, &descriptor);
        clock.advance(Duration::from_millis(2_001));

        assert_eq!(Arc::strong_count(&retained), 2);
        assert!(fixture
            .actor
            .lock_state()
            .unwrap()
            .zcash_winner_snapshots
            .is_empty());
        let response = fixture
            .actor
            .dispatch(uuid(20), Some(0), BackendRequestKind::SubmitShare, request)
            .expect("the exact Zcash winner survives grace expiry before maintenance");
        assert_eq!(retained.metadata.validations.load(Ordering::Acquire), 0);
        assert_eq!(
            Arc::strong_count(&retained),
            2,
            "submission does not run ordinary retirement ahead of its winner"
        );
        {
            let state = fixture.actor.lock_state().unwrap();
            assert!(state.live_jobs.contains_key(&descriptor.job_id));
            assert!(state.zcash_winner_snapshots.is_empty());
        }
        assert!(
            matches!(response.last(), Some(BackendMessage::ShareCommitted {
            receipt: ShareReceipt { winners, .. }, replayed: false, ..
        }) if matches!(winners.as_slice(), [WinnerDescriptor { chain: MergedChain::Zcash, .. }]))
        );
        assert_eq!(
            PoolBackendLiveWinnerScheduler::for_chain(MergedChain::Zcash)
                .next(&fixture.actor)
                .unwrap()
                .unwrap()
                .block_bytes(),
            fixture.zcash_bytes
        );
        assert!(
            PoolBackendLiveWinnerScheduler::for_chain(MergedChain::Wcash)
                .next(&fixture.actor)
                .unwrap()
                .is_none()
        );
        health_maintenance(&fixture.actor);
        assert_eq!(
            Arc::strong_count(&retained),
            1,
            "maintenance releases ordinary coordinator ownership after submission"
        );
        let current = fixture.actor.lock_state().unwrap();
        assert!(current
            .zcash_winner_snapshots
            .contains_key(&descriptor.job_id));
        let replacement = current
            .live_jobs
            .get(current.current.as_ref().unwrap())
            .unwrap();
        assert_eq!(
            replacement.descriptor.zcash_previous_hash_le,
            descriptor.zcash_previous_hash_le
        );
        assert_eq!(replacement.descriptor.zcash_height, descriptor.zcash_height);
        // No refresh/currentness callback ran during the simulated preparation
        // gap: recovery consumes the existing nonblocking chain-specific sample.
    }

    fn health_maintenance(actor: &PoolBackendActor) {
        actor
            .dispatch(
                uuid(20),
                None,
                BackendRequestKind::Health,
                BackendRequest::Health {
                    version: BACKEND_PROTOCOL_VERSION,
                    id: 3,
                },
            )
            .expect("health performs bounded lifecycle maintenance");
    }

    #[test]
    fn late_winner_recovery_submit_defers_expired_context_closures_to_health() {
        let clock = Arc::new(TestClock::new());
        let fixture = IndependentWinnerFixture::with_clock(false, Arc::clone(&clock));
        let (descriptor, old_request, retained) = verified_dual_submission(&fixture);
        fixture
            .actor
            .activate_job(retained, Duration::ZERO)
            .unwrap();
        replace_wec_tip_only(&fixture, &descriptor);
        clock.advance(Duration::from_millis(2_001));
        health_maintenance(&fixture.actor);

        // Activate a new real-proof control before the old recovery deadline.
        // Advancing the clock then leaves an expired detached context pending
        // maintenance beside a fresh ordinary generation.
        clock.set(Duration::from_secs(100));
        let (mut fresh_descriptor, mut fresh_request, template) =
            verified_dual_submission(&fixture);
        fresh_descriptor.job_id = Hex32::new([0x7f; 32]);
        let BackendRequest::SubmitShare { job_id, .. } = &mut fresh_request else {
            unreachable!()
        };
        *job_id = fresh_descriptor.job_id.clone();
        fixture
            .actor
            .activate_job(
                Arc::new(ProofCheckingRetainedJob {
                    metadata: TestRetainedJob::new(fresh_descriptor),
                    wcash_bytes: template.wcash_bytes.clone(),
                    zcash_bytes: template.zcash_bytes.clone(),
                    snapshot: template.snapshot.clone(),
                }),
                Duration::ZERO,
            )
            .unwrap();
        clock.set(Duration::from_secs(106));
        let before = {
            let state = fixture.actor.lock_state().unwrap();
            assert!(state
                .zcash_winner_snapshots
                .contains_key(&descriptor.job_id));
            state.journal.current_event_seq().unwrap()
        };
        assert_eq!(
            fixture
                .actor
                .dispatch(
                    uuid(20),
                    Some(0),
                    BackendRequestKind::SubmitShare,
                    old_request
                )
                .unwrap_err(),
            stale_job(),
            "the original recovery deadline applies before maintenance too"
        );
        let response = fixture
            .actor
            .dispatch(
                uuid(20),
                Some(0),
                BackendRequestKind::SubmitShare,
                fresh_request,
            )
            .expect("fresh real winner is committed without unrelated expiry writes");
        assert!(
            matches!(response.last(), Some(BackendMessage::ShareCommitted {
            receipt: ShareReceipt { winners, .. }, replayed: false, ..
        }) if winners.len() == 2)
        );
        {
            let state = fixture.actor.lock_state().unwrap();
            assert_eq!(state.journal.current_event_seq().unwrap(), before + 1);
            assert!(state
                .zcash_winner_snapshots
                .contains_key(&descriptor.job_id));
            assert_eq!(
                state.durable_jobs[&descriptor.job_id].phase,
                DurableJobPhase::Invalidated
            );
        }
        health_maintenance(&fixture.actor);
        let state = fixture.actor.lock_state().unwrap();
        assert!(!state
            .zcash_winner_snapshots
            .contains_key(&descriptor.job_id));
        assert_eq!(
            state.durable_jobs[&descriptor.job_id].phase,
            DurableJobPhase::Closed
        );
    }

    #[test]
    fn late_winner_recovery_original_lease_uses_new_active_same_zcash_parent() {
        let clock = Arc::new(TestClock::new());
        let fixture = IndependentWinnerFixture::with_clock(false, Arc::clone(&clock));
        let (descriptor, request, retained) = verified_dual_submission(&fixture);
        fixture
            .actor
            .activate_job(retained.clone(), Duration::ZERO)
            .unwrap();
        clock.advance(Duration::from_secs(40));
        replace_wec_tip_only(&fixture, &descriptor);
        clock.advance(Duration::from_millis(5_001));

        let response = fixture
            .actor
            .dispatch(uuid(20), Some(0), BackendRequestKind::SubmitShare, request)
            .expect("a newer active same-parent job permits recovery after original lease");
        assert!(
            matches!(response.last(), Some(BackendMessage::ShareCommitted {
            receipt: ShareReceipt { winners, .. }, ..
        }) if matches!(winners.as_slice(), [WinnerDescriptor { chain: MergedChain::Zcash, .. }]))
        );
        assert_eq!(retained.metadata.validations.load(Ordering::Acquire), 0);
        let state = fixture.actor.lock_state().unwrap();
        assert_eq!(state.shares.len(), 1);
        assert_eq!(
            state.durable_jobs[&descriptor.job_id].phase,
            DurableJobPhase::Invalidated
        );
    }

    #[test]
    fn late_winner_reproduction_mutated_solution_cannot_pass_real_validator() {
        let clock = Arc::new(TestClock::new());
        let fixture = IndependentWinnerFixture::with_clock(false, clock);
        let (_, mut request, retained) = verified_dual_submission(&fixture);
        fixture
            .actor
            .activate_job(retained.clone(), Duration::ZERO)
            .unwrap();
        let BackendRequest::SubmitShare { solution, .. } = &mut request else {
            unreachable!();
        };
        let mut corrupted = *solution.as_bytes();
        corrupted[0] ^= 1;
        *solution = Box::new(Hex1344::new(corrupted));

        let rejection = fixture
            .actor
            .dispatch(uuid(20), Some(0), BackendRequestKind::SubmitShare, request)
            .expect_err("a mutated proof must not pass the real Equihash verifier");
        assert_eq!(
            rejection,
            PoolBackendShareValidationError::InvalidEquihash.handler_error()
        );
        assert_eq!(retained.metadata.validations.load(Ordering::Acquire), 1);
        assert!(fixture.actor.lock_state().unwrap().shares.is_empty());
    }

    fn late_winner_setup() -> (
        IndependentWinnerFixture,
        Arc<TestClock>,
        JobDescriptor,
        BackendRequest,
        Arc<TestRetainedJob>,
    ) {
        let clock = Arc::new(TestClock::new());
        let fixture = IndependentWinnerFixture::with_clock(false, Arc::clone(&clock));
        let (descriptor, request, retained) = verified_dual_submission(&fixture);
        fixture
            .actor
            .activate_job(retained, Duration::ZERO)
            .unwrap();
        let current = replace_wec_tip_only(&fixture, &descriptor);
        clock.advance(Duration::from_millis(2_001));
        (fixture, clock, descriptor, request, current)
    }

    #[test]
    fn late_winner_recovery_rejects_nonwinner_bad_equihash_time_and_unknown_job() {
        for case in ["ordinary", "bad_equihash", "time", "unknown"] {
            let (fixture, _, descriptor, mut request, _) = late_winner_setup();
            let BackendRequest::SubmitShare {
                nonce,
                solution,
                time,
                job_id,
                ..
            } = &mut request
            else {
                unreachable!()
            };
            let target =
                wcash_zcash_aux::Target::from_le_bytes(*descriptor.zcash_target_le.as_bytes())
                    .unwrap();
            match case {
                "time" => *time = Hex4::new([1, 2, 3, 4]),
                "unknown" => *job_id = Hex32::new([0xee; 32]),
                "ordinary" | "bad_equihash" => {
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
                        let winning = target.is_met_by_le_hash(*hash.as_bytes());
                        if winning == (case == "bad_equihash") {
                            *solution = Box::new(candidate);
                            selected = true;
                            break;
                        }
                    }
                    assert!(
                        selected,
                        "the bounded test search finds the required target side"
                    );
                }
                _ => unreachable!(),
            }
            let error = fixture
                .actor
                .dispatch(uuid(20), Some(0), BackendRequestKind::SubmitShare, request)
                .unwrap_err();
            if case == "bad_equihash" {
                assert_eq!(
                    error,
                    PoolBackendShareValidationError::InvalidEquihash.handler_error()
                );
            } else {
                assert_eq!(error, stale_job());
            }
            assert!(fixture.actor.lock_state().unwrap().shares.is_empty());
        }
    }

    #[test]
    fn late_winner_recovery_requires_active_zcash_current_parent_and_safe_target() {
        for case in [
            "zec_unavailable",
            "unhealthy",
            "no_active",
            "new_parent",
            "target",
            "empty_snapshot",
            "oversized_snapshot",
        ] {
            let (fixture, clock, descriptor, mut request, current) = late_winner_setup();
            match case {
                "zec_unavailable" => current.zcash_current.store(false, Ordering::Release),
                "unhealthy" => current.healthy.store(false, Ordering::Release),
                "no_active" => clock.advance(Duration::from_secs(46)),
                "new_parent" => {
                    let mut next = descriptor.clone();
                    next.job_id = Hex32::new([0x7b; 32]);
                    next.zcash_previous_hash_le = Hex32::new([0xb1; 32]);
                    let mut header = *next.header_input.as_bytes();
                    header[4..36].copy_from_slice(next.zcash_previous_hash_le.as_bytes());
                    next.header_input = Hex108::new(header);
                    fixture
                        .actor
                        .activate_job(Arc::new(TestRetainedJob::new(next)), Duration::from_secs(2))
                        .unwrap();
                }
                "target" => {
                    let BackendRequest::SubmitShare { target_le, .. } = &mut request else {
                        unreachable!()
                    };
                    *target_le = TargetLe::new([1; 32]);
                }
                "empty_snapshot" | "oversized_snapshot" => {
                    let mut state = fixture.actor.lock_state().unwrap();
                    let original = state.live_jobs.get_mut(&descriptor.job_id).unwrap();
                    original.zcash_snapshot = Some(Arc::new(ChargedSnapshot {
                        inner: original.zcash_snapshot.take().unwrap(),
                        bytes: if case == "empty_snapshot" {
                            0
                        } else {
                            MAX_ZCASH_WINNER_SNAPSHOT_BYTES + 1
                        },
                    }));
                }
                _ => unreachable!(),
            }
            assert!(fixture
                .actor
                .dispatch(uuid(20), Some(0), BackendRequestKind::SubmitShare, request)
                .is_err());
            assert!(fixture.actor.lock_state().unwrap().shares.is_empty());
        }
    }

    #[test]
    fn late_winner_recovery_commits_once_and_restart_recovers_exact_zcash_bytes() {
        let (fixture, _, _, request, _) = late_winner_setup();
        let first = fixture
            .actor
            .dispatch(
                uuid(20),
                Some(0),
                BackendRequestKind::SubmitShare,
                request.clone(),
            )
            .unwrap();
        let Some(BackendMessage::ShareCommitted {
            receipt,
            replayed: false,
            ..
        }) = first.last()
        else {
            panic!("first late winner was not durable")
        };
        let expected = receipt.clone();
        let mut crossed = request.clone();
        let BackendRequest::SubmitShare { identity, .. } = &mut crossed else {
            unreachable!()
        };
        identity.label.push('x');
        assert_eq!(
            fixture
                .actor
                .dispatch(uuid(20), Some(0), BackendRequestKind::SubmitShare, crossed)
                .unwrap_err(),
            attribution_conflict()
        );
        let replay = fixture
            .actor
            .dispatch(uuid(20), Some(0), BackendRequestKind::SubmitShare, request)
            .unwrap();
        assert!(
            matches!(replay.last(), Some(BackendMessage::ShareCommitted { receipt, replayed: true, .. }) if receipt == &expected)
        );
        assert_eq!(fixture.actor.lock_state().unwrap().shares.len(), 1);
        let exact_parent = fixture.zcash_bytes.clone();
        let IndependentWinnerFixture {
            _directory, actor, ..
        } = fixture;
        drop(actor);
        let path = _directory.path().join("independent-winners.jsonl");
        let recovered =
            actor_from_journal(open_journal(&path, &config()), Arc::new(TestClock::new()));
        let winner = PoolBackendLiveWinnerScheduler::for_chain(MergedChain::Zcash)
            .next(&recovered)
            .unwrap()
            .unwrap();
        assert_eq!(winner.block_bytes(), exact_parent);
        assert_eq!(winner.share_id(), &expected.share_id);
        assert!(
            PoolBackendLiveWinnerScheduler::for_chain(MergedChain::Wcash)
                .next(&recovered)
                .unwrap()
                .is_none()
        );
        assert!(recovered
            .lock_state()
            .unwrap()
            .zcash_winner_snapshots
            .is_empty());
    }

    struct ChargedSnapshot {
        inner: Arc<dyn PoolBackendZcashWinnerSnapshot>,
        bytes: usize,
    }

    impl PoolBackendZcashWinnerSnapshot for ChargedSnapshot {
        fn retained_bytes(&self) -> usize {
            self.bytes
        }
        fn validate_zcash_winner(
            &self,
            share: PoolBackendShareRequest<'_>,
        ) -> Result<Vec<u8>, PoolBackendShareValidationError> {
            self.inner.validate_zcash_winner(share)
        }
    }

    #[test]
    fn late_winner_recovery_caps_bytes_count_and_releases_ordinary_ownership() {
        for charged_bytes in [None, Some(40 * 1024 * 1024)] {
            let clock = Arc::new(TestClock::new());
            let fixture = IndependentWinnerFixture::with_clock(false, Arc::clone(&clock));
            let (original, _, template) = verified_dual_submission(&fixture);
            let mut weak = Vec::new();
            for index in 1..=35u8 {
                let mut descriptor = original.clone();
                descriptor.job_id = Hex32::new([index; 32]);
                let winner_snapshot: Arc<dyn PoolBackendZcashWinnerSnapshot> = match charged_bytes {
                    Some(bytes) => Arc::new(ChargedSnapshot {
                        inner: template.snapshot.clone(),
                        bytes,
                    }),
                    None => template.snapshot.clone(),
                };
                let retained = Arc::new(ProofCheckingRetainedJob {
                    metadata: TestRetainedJob::new(descriptor),
                    wcash_bytes: fixture.wcash_bytes.clone(),
                    zcash_bytes: fixture.zcash_bytes.clone(),
                    snapshot: winner_snapshot,
                });
                weak.push(Arc::downgrade(&retained));
                fixture
                    .actor
                    .activate_job(retained, Duration::from_millis(2))
                    .unwrap();
                clock.advance(Duration::from_millis(3));
                let _ = snapshot(&fixture.actor, 0);
            }
            assert!(
                weak[..34].iter().all(|owner| owner.upgrade().is_none()),
                "rescue snapshots cannot retain ordinary coordinator ownership"
            );
            let state = fixture.actor.lock_state().unwrap();
            assert_eq!(
                state.zcash_winner_snapshots.len(),
                if charged_bytes.is_some() { 1 } else { 32 }
            );
            assert!(state.zcash_winner_snapshots.len() <= MAX_ZCASH_WINNER_SNAPSHOTS);
            assert!(state.zcash_snapshot_bytes <= MAX_ZCASH_WINNER_SNAPSHOT_BYTES);
            assert!(!state
                .zcash_winner_snapshots
                .contains_key(&Hex32::new([1; 32])));
            drop(state);
            fixture.actor.close_zcash_winner_snapshots().unwrap();
            assert!(fixture
                .actor
                .lock_state()
                .unwrap()
                .zcash_winner_snapshots
                .is_empty());
            assert!(
                weak[34].upgrade().is_some(),
                "shutdown cache cleanup does not retire the active ordinary owner"
            );
        }
    }

    #[test]
    fn late_winner_recovery_tail_does_not_reset_original_native_deadline() {
        let clock = Arc::new(TestClock::new());
        let fixture = IndependentWinnerFixture::with_clock(false, Arc::clone(&clock));
        let (descriptor, _, retained) = verified_dual_submission(&fixture);
        retained
            .metadata
            .set_remaining_lifetime(Some(Duration::from_millis(1_234)));
        fixture
            .actor
            .activate_job(retained, Duration::ZERO)
            .unwrap();
        replace_wec_tip_only(&fixture, &descriptor);
        clock.advance(Duration::from_millis(2_001));
        let _ = snapshot(&fixture.actor, 0);
        assert_eq!(
            fixture.actor.lock_state().unwrap().zcash_winner_snapshots[&descriptor.job_id].deadline,
            Duration::from_millis(61_234)
        );
        clock.set(Duration::from_millis(61_235));
        let _ = snapshot(&fixture.actor, 0);
        let state = fixture.actor.lock_state().unwrap();
        assert!(!state
            .zcash_winner_snapshots
            .contains_key(&descriptor.job_id));
        assert_eq!(
            state.durable_jobs[&descriptor.job_id].phase,
            DurableJobPhase::Closed
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn append_exact(
        journal: &PoolBackendJournal,
        descriptor: &JobDescriptor,
        parent_hash: &Hex32,
        wcash_bytes: &[u8],
        zcash_bytes: &[u8],
        ordinal: u32,
        wcash: bool,
        zcash: bool,
    ) -> Hex32 {
        let mut id = [0x90; 32];
        id[..4].copy_from_slice(&ordinal.to_le_bytes());
        let share_id = Hex32::new(id);
        journal
            .append_share_committed(
                descriptor.job_id.clone(),
                share_id.clone(),
                parent_hash.clone(),
                winner_descriptors(descriptor, parent_hash, wcash, zcash),
                identity(),
                TargetLe::new([0xff; 32]),
                JournalWinnerBlocks {
                    wcash: wcash.then(|| wcash_bytes.to_vec()),
                    zcash: zcash.then(|| zcash_bytes.to_vec()),
                },
            )
            .expect("commit exact same-proof chain winners through the real durable journal");
        share_id
    }

    #[test]
    fn dual_winners_keep_exact_proofs_and_chain_local_audit_fences() {
        let fixture = IndependentWinnerFixture::empty();
        let mut wcash = PoolBackendLiveWinnerScheduler::for_chain(MergedChain::Wcash);
        let mut zcash = PoolBackendLiveWinnerScheduler::for_chain(MergedChain::Zcash);
        assert!(wcash.next(&fixture.actor).unwrap().is_none());
        assert!(zcash.next(&fixture.actor).unwrap().is_none());
        let zcash_old_bound = zcash.audit_before();
        let share = fixture.append(2, true, true);
        let child = wcash.next(&fixture.actor).unwrap().unwrap();
        assert_eq!(child.share_id(), &share);
        assert_eq!(child.winner().chain, MergedChain::Wcash);
        assert_eq!(child.block_bytes(), fixture.wcash_bytes);
        assert_eq!(child.winner_ordinal, 0);
        let mut zcash_audit = PoolBackendWinnerAuditScheduler::for_chain(MergedChain::Zcash);
        for _ in 0..3 {
            assert!(zcash_audit
                .next(&fixture.actor, zcash_old_bound)
                .unwrap()
                .snapshot()
                .is_none());
        }
        // Even the other chain's advanced bound excludes the unclaimed parent.
        assert!(zcash_audit
            .next(&fixture.actor, wcash.audit_before())
            .unwrap()
            .snapshot()
            .is_none());
        let parent = zcash.next(&fixture.actor).unwrap().unwrap();
        assert_eq!(parent.winner().chain, MergedChain::Zcash);
        assert_eq!(parent.block_bytes(), fixture.zcash_bytes);
        assert_eq!(parent.winner_ordinal, 1);
        assert_eq!(parent.parent_hash_le(), child.parent_hash_le());
        let work = PoolBackendWinnerAuditScheduler::for_chain(MergedChain::Zcash)
            .next(&fixture.actor, zcash.audit_before())
            .unwrap();
        assert_eq!(work.snapshot().unwrap().key(), parent.key());
        let work = PoolBackendWinnerAuditScheduler::for_chain(MergedChain::Wcash)
            .next(&fixture.actor, wcash.audit_before())
            .unwrap();
        assert_eq!(work.snapshot().unwrap().key(), child.key());
    }

    #[test]
    fn chain_filter_continues_past_256_other_chain_winners_without_new_wakeup() {
        let fixture = IndependentWinnerFixture::empty();
        let mut zcash = PoolBackendLiveWinnerScheduler::for_chain(MergedChain::Zcash);
        assert!(zcash.next(&fixture.actor).unwrap().is_none());
        for ordinal in 1..=257 {
            fixture.append(ordinal, true, false);
        }
        let expected = fixture.append(258, false, true);
        assert!(zcash.next(&fixture.actor).unwrap().is_none());
        assert!(
            zcash.has_pending_scan(),
            "a slice boundary must not be confused with the durable tail"
        );
        let parent = zcash.next(&fixture.actor).unwrap().unwrap();
        assert_eq!(parent.share_id(), &expected);
        assert_eq!(parent.block_bytes(), fixture.zcash_bytes);
        assert!(!zcash.has_pending_scan());
        assert!(zcash.next(&fixture.actor).unwrap().is_none());
        assert!(!zcash.has_pending_scan());
    }

    #[test]
    fn restart_recovers_each_chain_without_reordering_or_discarding_exact_proofs() {
        let fixture = IndependentWinnerFixture::retained_dual();
        for (chain, expected) in [
            (MergedChain::Wcash, &fixture.wcash_bytes),
            (MergedChain::Zcash, &fixture.zcash_bytes),
        ] {
            let mut live = PoolBackendLiveWinnerScheduler::for_chain(chain);
            let recovered = live.next(&fixture.actor).unwrap().unwrap();
            assert_eq!(recovered.winner().chain, chain);
            assert_eq!(recovered.block_bytes(), expected);
            assert_eq!(recovered.lifecycle(), &JournalWinnerLifecycle::Pending);
            assert!(live.next(&fixture.actor).unwrap().is_none());
            let audit = PoolBackendWinnerAuditScheduler::for_chain(chain)
                .next(&fixture.actor, live.audit_before())
                .unwrap();
            assert_eq!(audit.snapshot().unwrap().key(), recovered.key());
        }
    }

    #[test]
    fn chain_filtered_audits_retain_observed_and_matured_lifecycle_tiers() {
        let fixture = IndependentWinnerFixture::retained_dual();
        let mut child_live = PoolBackendLiveWinnerScheduler::for_chain(MergedChain::Wcash);
        let mut parent_live = PoolBackendLiveWinnerScheduler::for_chain(MergedChain::Zcash);
        let child = child_live.next(&fixture.actor).unwrap().unwrap();
        let parent = parent_live.next(&fixture.actor).unwrap().unwrap();
        for snapshot in [&child, &parent] {
            fixture
                .actor
                .compare_and_apply_winner_transition(
                    snapshot,
                    PoolBackendWinnerTransition::Observed {
                        tip: ChainTip {
                            block_hash_le: snapshot.winner().block_hash_le.clone(),
                            height: snapshot.winner().height,
                        },
                        confirmations: 1,
                    },
                )
                .unwrap();
        }
        let observed_child = fixture
            .actor
            .winner_snapshot(&child.key())
            .unwrap()
            .unwrap();
        fixture
            .actor
            .compare_and_apply_winner_transition(
                &observed_child,
                PoolBackendWinnerTransition::Matured {
                    tip: ChainTip {
                        block_hash_le: Hex32::new([0xa1; 32]),
                        height: child.winner().height + 1,
                    },
                    confirmations: 2,
                },
            )
            .unwrap();
        for (chain, key, bound) in [
            (MergedChain::Wcash, child.key(), child_live.audit_before()),
            (MergedChain::Zcash, parent.key(), parent_live.audit_before()),
        ] {
            let mut audit = PoolBackendWinnerAuditScheduler::for_chain(chain);
            let mut observed = 0;
            for _ in 0..6 {
                if let Some(snapshot) = audit.next(&fixture.actor, bound).unwrap().snapshot() {
                    assert_eq!(snapshot.key(), key);
                    assert_eq!(snapshot.winner().chain, chain);
                    assert_eq!(
                        matches!(snapshot.lifecycle(), JournalWinnerLifecycle::Matured { .. }),
                        chain == MergedChain::Wcash
                    );
                    observed += 1;
                }
            }
            assert!(
                observed > 0,
                "filtering must not erase retained lifecycle audit work"
            );
        }
    }
}

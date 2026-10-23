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
                Arc::new(TestClock::new()),
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

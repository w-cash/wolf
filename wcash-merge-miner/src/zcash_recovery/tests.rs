//! Deterministic cache/clock tests; these never issue RPC or start a worker.

use wcash_pool_protocol::Hex108;
use zebra_chain::parameters::Network;

use super::*;

fn authority() -> NativePoolZcashRecoveryAuthority {
    let template = crate::rpc::RpcEndpoint::new("http://127.0.0.1:1/", None, None).unwrap();
    let validator = crate::rpc::RpcEndpoint::new("http://127.0.0.1:2/", None, None).unwrap();
    let config = NativeZcashConfig::new(
        template,
        vec![validator],
        crate::NativeZcashNetwork::Regtest,
        Network::new_regtest(Default::default())
            .genesis_hash()
            .to_string(),
        "tmJymvcUCn1ctbghvTJpXBwHiMEB8P6wxNV".parse().unwrap(),
    )
    .unwrap();
    NativePoolZcashRecoveryAuthority::new(&config).unwrap()
}

fn job() -> JobDescriptor {
    let mut header = [0u8; 108];
    header[..4].copy_from_slice(&4u32.to_le_bytes());
    header[4..36].copy_from_slice(&[0x31; 32]);
    header[100..104].copy_from_slice(&1u32.to_le_bytes());
    JobDescriptor {
        job_id: Hex32::new([0x21; 32]),
        wcash_candidate_hash_le: Hex32::new([0x22; 32]),
        header_input: Hex108::new(header),
        wcash_previous_hash_le: Hex32::new([0x23; 32]),
        zcash_previous_hash_le: Hex32::new([0x31; 32]),
        wcash_coinbase_txid_le: Hex32::new([0x24; 32]),
        zcash_coinbase_txid_le: Hex32::new([0x32; 32]),
        wcash_target_le: TargetLe::new([0xff; 32]),
        zcash_target_le: TargetLe::new([0xfe; 32]),
        wcash_height: 1,
        zcash_height: 2,
        wcash_reward_zat: 625_000_000,
        zcash_reward_zat: 312_500_000,
        wcash_maturity_confirmations: 100,
        zcash_maturity_confirmations: 100,
        max_age_ms: 45_000,
    }
}

fn sample(attempt: u64, started: Instant, completed: Instant) -> RecoverySample {
    RecoverySample {
        attempt,
        started,
        completed,
        tip: Some(NativeChainTip {
            block_hash_le: [0x31; 32],
            height: 1,
        }),
    }
}

#[test]
fn no_initial_sample_and_request_start_ttl_fail_closed_at_exact_boundary() {
    use PoolBackendZcashWorkObservation::{Current, Unknown};
    let authority = authority();
    let now = Instant::now();
    let job = job();
    assert_eq!(authority.observe_at(&job, now), Unknown);
    authority.publish(sample(1, now, now));
    assert_eq!(authority.observe_at(&job, now), Current);
    assert_eq!(
        authority.observe_at(&job, now + AUTHORITY_MAX_AGE - Duration::from_nanos(1)),
        Current
    );
    assert_eq!(authority.observe_at(&job, now + AUTHORITY_MAX_AGE), Unknown);
    assert_eq!(
        authority.observe_at(&job, now + AUTHORITY_MAX_AGE + Duration::from_nanos(1)),
        Unknown
    );
}

#[test]
fn future_reversed_and_slow_completed_samples_cannot_refresh_old_evidence() {
    use PoolBackendZcashWorkObservation::{Current, Unknown};
    for defect in ["future_complete", "future_start", "reversed", "slow"] {
        let authority = authority();
        let now = Instant::now();
        let job = job();
        authority.publish(sample(1, now, now));
        assert_eq!(authority.observe_at(&job, now), Current);
        let invalid = match defect {
            "future_complete" => sample(2, now, now + Duration::from_nanos(1)),
            "future_start" => sample(
                2,
                now + Duration::from_nanos(1),
                now + Duration::from_nanos(1),
            ),
            "reversed" => sample(2, now, now - Duration::from_nanos(1)),
            "slow" => sample(2, now - AUTHORITY_MAX_AGE, now),
            _ => unreachable!(),
        };
        authority.publish(invalid);
        assert_eq!(authority.observe_at(&job, now), Unknown, "{defect}");
    }
}

#[test]
fn newer_failure_or_mismatch_supersedes_positive_and_old_completion_cannot_reopen_it() {
    use PoolBackendZcashWorkObservation::{Current, Stale, Unknown};
    for failure in [false, true] {
        let authority = authority();
        let now = Instant::now();
        let job = job();
        authority.publish(sample(1, now, now));
        assert_eq!(authority.observe_at(&job, now), Current);
        let mut newer = sample(3, now, now);
        if failure {
            newer.tip = None;
        } else {
            newer.tip.as_mut().unwrap().block_hash_le = [0xa1; 32];
        }
        authority.publish(newer);
        let expected = if failure { Unknown } else { Stale };
        assert_eq!(authority.observe_at(&job, now), expected);
        authority.publish(sample(2, now, now));
        assert_eq!(authority.observe_at(&job, now), expected);
        authority.publish(sample(3, now, now));
        assert_eq!(
            authority.observe_at(&job, now),
            expected,
            "duplicate attempt cannot replace evidence"
        );
    }
}

#[test]
fn authority_matches_both_predecessor_hash_and_height_and_rejects_zero_height() {
    use PoolBackendZcashWorkObservation::{Current, Stale};
    let authority = authority();
    let now = Instant::now();
    let mut job = job();
    authority.publish(sample(1, now, now));
    assert_eq!(authority.observe_at(&job, now), Current);
    job.zcash_previous_hash_le = Hex32::new([0xa2; 32]);
    assert_eq!(authority.observe_at(&job, now), Stale);
    job.zcash_previous_hash_le = Hex32::new([0x31; 32]);
    job.zcash_height = 3;
    assert_eq!(authority.observe_at(&job, now), Stale);
    job.zcash_height = 0;
    assert_eq!(authority.observe_at(&job, now), Stale);
}

#[test]
fn contention_is_unknown_without_waiting_and_close_is_shared_and_terminal() {
    use PoolBackendZcashWorkObservation::{Current, Unknown};
    let authority = authority();
    let sibling = authority.clone();
    let now = Instant::now();
    let job = job();
    authority.publish(sample(1, now, now));
    let held = authority.state.sample.lock().unwrap();
    assert_eq!(sibling.observe_at(&job, now), Unknown);
    drop(held);
    assert_eq!(sibling.observe_at(&job, now), Current);
    sibling.close();
    authority.publish(sample(2, now, now));
    assert_eq!(authority.observe_at(&job, now), Unknown);
    assert_eq!(sibling.observe_at(&job, now), Unknown);
}

#[test]
fn attempt_counter_overflow_closes_before_any_network_operation() {
    let authority = authority();
    let now = Instant::now();
    authority.publish(sample(1, now, now));
    authority
        .state
        .next_attempt
        .store(u64::MAX, Ordering::Release);
    // If overflow incorrectly tries metadata RPC, the unserved endpoint would
    // be contacted. The production branch closes before invoking its provider.
    authority.refresh();
    assert!(authority.state.closed.load(Ordering::Acquire));
    assert_eq!(
        authority.observe_at(&job(), now),
        PoolBackendZcashWorkObservation::Unknown
    );
}

#[test]
fn target_work_is_exact_integer_floor_across_full_width_boundaries() {
    let mut half = [0xff; 32];
    half[31] = 0x7f;
    let mut near_max = [0xff; 32];
    near_max[0] -= 1;
    let mut one = [0u8; 32];
    one[0] = 1;
    let mut two = [0u8; 32];
    two[0] = 2;
    let mut work_one = [0u8; 32];
    work_one[0] = 1;
    let mut work_two = [0u8; 32];
    work_two[0] = 2;
    let mut work_half_range = [0u8; 32];
    work_half_range[31] = 0x80;
    for (target, expected) in [
        ([0xff; 32], work_one),
        (near_max, work_one),
        (half, work_two),
        (one, work_half_range),
        (two, [0x55; 32]),
    ] {
        let mut stats = PoolBackendZcashWorkStats::default();
        stats.record(
            PoolBackendZcashWorkObservation::Current,
            &TargetLe::new(target),
            7,
        );
        assert_eq!(stats.shares, [1, 0, 0]);
        assert_eq!(stats.target_work_le[0], expected);
        assert_eq!(stats.first_event_seq, 7);
        assert_eq!(stats.last_event_seq, 7);
        assert!(!stats.overflowed);
    }
}

#[test]
fn work_counter_saturation_is_explicit_and_does_not_wrap_or_reclassify() {
    let mut stats = PoolBackendZcashWorkStats::default();
    stats.shares[0] = u64::MAX;
    stats.target_work_le[0] = [0xff; 32];
    stats.record(
        PoolBackendZcashWorkObservation::Current,
        &TargetLe::new([0xff; 32]),
        8,
    );
    assert_eq!(stats.shares, [u64::MAX, 0, 0]);
    assert_eq!(stats.target_work_le[0], [0xff; 32]);
    assert!(stats.overflowed);
    stats.record(
        PoolBackendZcashWorkObservation::Unknown,
        &TargetLe::new([0xff; 32]),
        9,
    );
    assert_eq!(stats.shares, [u64::MAX, 0, 1]);
    assert_eq!(stats.target_work_le[0], [0xff; 32]);
    assert_eq!(stats.last_event_seq, 9);
    let mut invalid = PoolBackendZcashWorkStats::default();
    invalid.record(
        PoolBackendZcashWorkObservation::Current,
        &TargetLe::new([0; 32]),
        1,
    );
    assert!(invalid.overflowed);
    assert_eq!(invalid.shares, [0; 3]);
}

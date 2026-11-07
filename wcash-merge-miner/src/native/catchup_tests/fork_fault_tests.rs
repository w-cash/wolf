//! Faults invoke the production fork relay over bounded disposable HTTP peers.

use super::*;

fn bounded_relay(
    provider: &NativeZcashProvider,
    observer: &FixtureRpc,
    node: &FixtureRpc,
    budget: &mut ObserverRelayBudget,
) -> Result<(), MinerError> {
    let tip = observer.state.lock().unwrap().tip;
    provider.relay_observer_blocks_to_node_with_budget(&observer.client, tip, &node.client, budget)
}

fn diverged_height_two_state() -> FixtureState {
    let first = competing_fixture_block(false);
    let mut second = public_block(2);
    Arc::make_mut(&mut second.header).previous_block_hash = first.hash();
    let mut state = FixtureState::new(second, 2);
    let first_hash = display_hex(first.hash().0);
    state.canonical.insert(1, first_hash.clone());
    state.blocks.insert(first_hash, (1, first));
    state.relay_chain = (1..=3)
        .map(|height| (height, public_block(height)))
        .collect();
    state.adopt_relay_at_height = Some(3);
    state
}

#[test]
fn fork_catchup_exact_node_producer_ancestor_races_remain_retryable() {
    for observer_fails in [false, true] {
        for (code, message) in [
            (-8, "Block not found"),
            (-32602, "Provided index is greater than the current tip"),
        ] {
            let mut observer_state = fork_observer_state();
            let mut node_state = diverged_height_two_state();
            let target = if observer_fails {
                &mut observer_state
            } else {
                &mut node_state
            };
            target.lookup_error_at = Some((1, json!({"code":code,"message":message})));
            let observer = FixtureRpc::new(observer_state);
            let node = FixtureRpc::new(node_state);
            let error = relay(&fixture_provider(), &observer, &node).unwrap_err();
            assert!(
                matches!(error, MinerError::StaleNativeJob(_) | MinerError::ParentTipMismatch { .. }),
                "only this canonical ancestor lookup's exact producer reply is a retryable race: observer={observer_fails}, code={code}, error={error:?}"
            );
            assert_eq!(node.state.lock().unwrap().submissions(), 0);
        }
    }
}

#[test]
fn fork_catchup_unrelated_invalid_params_are_not_reclassified_as_races() {
    for observer_fails in [false, true] {
        for code in [-8, -32602] {
            let mut observer_state = fork_observer_state();
            let mut node_state = diverged_height_two_state();
            let target = if observer_fails {
                &mut observer_state
            } else {
                &mut node_state
            };
            target.lookup_error_at = Some((
                1,
                json!({"code":code,"message":"fixture malformed parameter"}),
            ));
            let observer = FixtureRpc::new(observer_state);
            let node = FixtureRpc::new(node_state);
            assert!(matches!(
                relay(&fixture_provider(), &observer, &node),
                Err(MinerError::RpcError { code: Some(actual), .. }) if actual == code
            ));
            assert_eq!(node.state.lock().unwrap().submissions(), 0);
        }
    }
}

#[test]
fn fork_catchup_attempt_deadline_clones_do_not_poison_original_or_extend_nested() {
    let node = FixtureRpc::new(fork_observer_state());
    let expired = node.client.with_deadline(Instant::now());
    let extended = expired.with_deadline(Instant::now() + Duration::from_secs(1));
    for client in [&expired, &extended] {
        assert!(matches!(
            client.call_value("getblockchaininfo", json!([])),
            Err(MinerError::StaleNativeJob(_))
        ));
    }
    assert!(node.state.lock().unwrap().requests.is_empty());
    node.client
        .call_value("getblockchaininfo", json!([]))
        .expect("the attempt deadline must not mutate the original shared client");
    node.client
        .with_deadline(Instant::now() + Duration::from_secs(1))
        .call_value("getblockchaininfo", json!([]))
        .expect("a fresh clone retains the original client's independent deadline");
    let state = node.state.lock().unwrap();
    assert_eq!(state.requests.len(), 2);
    let first = state.requests[0]["id"].as_u64().unwrap();
    let second = state.requests[1]["id"].as_u64().unwrap();
    assert!(
        second > first,
        "clones retain one monotone request-ID source"
    );
}

#[test]
fn fork_catchup_nonconvergence_does_not_force_tip_or_repeat_writes() {
    let observer = FixtureRpc::new(fork_observer_state());
    let mut state = diverged_fork_state(false);
    state.adopt_relay_at_height = None;
    let node = FixtureRpc::new(state);
    let provider = fixture_provider();
    assert!(matches!(
        relay(&provider, &observer, &node),
        Err(MinerError::ParentTipMismatch { .. })
    ));
    assert_eq!(node.state.lock().unwrap().submissions(), 3);
    assert!(matches!(
        relay(&provider, &observer, &node),
        Err(MinerError::ParentTipMismatch { .. })
    ));
    assert_eq!(node.state.lock().unwrap().submissions(), 3);
    assert_ne!(
        node.state.lock().unwrap().tip,
        observer.state.lock().unwrap().tip
    );
}

#[test]
fn fork_catchup_ambiguous_write_is_paced_without_same_attempt_resend() {
    let observer = FixtureRpc::new(fork_observer_state());
    let mut state = diverged_fork_state(false);
    state.fork_submission_error =
        Some(json!({"code":-32603,"message":"fixture ambiguous submission"}));
    let node = FixtureRpc::new(state);
    let provider = fixture_provider();
    assert!(relay(&provider, &observer, &node).is_err());
    assert_eq!(node.state.lock().unwrap().submissions(), 1);
    assert!(relay(&provider, &observer, &node).is_err());
    assert_eq!(node.state.lock().unwrap().submissions(), 1);
}

#[test]
fn fork_catchup_consensus_rejection_keeps_original_error_and_stops_descendants() {
    let observer = FixtureRpc::new(fork_observer_state());
    let mut state = diverged_fork_state(false);
    state.fork_submission_error = Some(json!({"code":-26,"message":"fixture consensus rejection"}));
    let node = FixtureRpc::new(state);
    assert!(matches!(
        relay(&fixture_provider(), &observer, &node),
        Err(MinerError::RpcError {
            code: Some(-26),
            ..
        })
    ));
    assert_eq!(node.state.lock().unwrap().submissions(), 1);
}

#[test]
fn fork_catchup_observer_moves_after_first_write_stops_remaining_descendants() {
    let observer = FixtureRpc::new(fork_observer_state());
    let mut state = diverged_fork_state(false);
    state.observer_change_on_submission = Some((
        Arc::clone(&observer.state),
        NativeChainTip {
            height: 2,
            block_hash_le: public_block(2).hash().0,
        },
    ));
    let node = FixtureRpc::new(state);
    assert!(matches!(
        relay(&fixture_provider(), &observer, &node),
        Err(MinerError::ParentTipMismatch { .. })
    ));
    assert_eq!(node.state.lock().unwrap().submissions(), 1);
}

#[test]
fn fork_catchup_wrong_raw_block_is_rejected_before_any_submission() {
    let mut state = fork_observer_state();
    state.raw_override = Some(hex::encode(
        public_block(3).zcash_serialize_to_vec().unwrap(),
    ));
    let observer = FixtureRpc::new(state);
    let node = FixtureRpc::new(diverged_fork_state(false));
    assert!(matches!(
        relay(&fixture_provider(), &observer, &node),
        Err(MinerError::RpcProtocol(_))
    ));
    assert_eq!(node.state.lock().unwrap().submissions(), 0);
}

#[test]
fn fork_catchup_wrong_link_is_rejected_before_any_submission() {
    let mut state = fork_observer_state();
    let mut wrong = public_block(2);
    Arc::make_mut(&mut wrong.header).previous_block_hash = block::Hash([0x55; 32]);
    let hash = display_hex(wrong.hash().0);
    state.canonical.insert(2, hash.clone());
    state.blocks.insert(hash, (2, wrong));
    let observer = FixtureRpc::new(state);
    let node = FixtureRpc::new(diverged_fork_state(false));
    assert!(matches!(
        relay(&fixture_provider(), &observer, &node),
        Err(MinerError::RpcProtocol(_))
    ));
    assert_eq!(node.state.lock().unwrap().submissions(), 0);
}

#[test]
fn fork_catchup_wrong_coinbase_height_is_rejected_before_any_submission() {
    let mut state = fork_observer_state();
    state
        .canonical
        .insert(2, display_hex(public_block(3).hash().0));
    let observer = FixtureRpc::new(state);
    let node = FixtureRpc::new(diverged_fork_state(false));
    assert!(matches!(
        relay(&fixture_provider(), &observer, &node),
        Err(MinerError::RpcProtocol(_))
    ));
    assert_eq!(node.state.lock().unwrap().submissions(), 0);
}

#[test]
fn fork_catchup_total_decoded_byte_budget_stops_before_any_write() {
    let observer = FixtureRpc::new(fork_observer_state());
    let node = FixtureRpc::new(diverged_fork_state(false));
    let mut budget = ObserverRelayBudget::new();
    budget.remaining_bytes = public_block(1).zcash_serialize_to_vec().unwrap().len()
        + public_block(2).zcash_serialize_to_vec().unwrap().len()
        - 1;
    assert!(matches!(
        bounded_relay(&fixture_provider(), &observer, &node, &mut budget),
        Err(MinerError::StaleNativeJob(_))
    ));
    assert_eq!(observer.state.lock().unwrap().raw_block_reads(), 2);
    assert_eq!(node.state.lock().unwrap().submissions(), 0);
}

#[test]
fn fork_catchup_total_decoded_block_budget_stops_before_any_write() {
    let observer = FixtureRpc::new(fork_observer_state());
    let node = FixtureRpc::new(diverged_fork_state(false));
    let mut budget = ObserverRelayBudget::new();
    budget.remaining_blocks = 2;
    assert!(matches!(
        bounded_relay(&fixture_provider(), &observer, &node, &mut budget),
        Err(MinerError::StaleNativeJob(_) | MinerError::ParentTipMismatch { .. })
    ));
    assert_eq!(node.state.lock().unwrap().submissions(), 0);
}

#[test]
fn fork_catchup_block_budget_is_shared_across_recipients() {
    let observer = FixtureRpc::new(fork_observer_state());
    let first = FixtureRpc::new(diverged_fork_state(false));
    let second = FixtureRpc::new(diverged_fork_state(false));
    let provider = fixture_provider();
    let mut budget = ObserverRelayBudget::new();
    budget.remaining_blocks = 3;
    bounded_relay(&provider, &observer, &first, &mut budget).unwrap();
    assert_eq!(first.state.lock().unwrap().submissions(), 3);
    assert!(bounded_relay(&provider, &observer, &second, &mut budget).is_err());
    assert_eq!(second.state.lock().unwrap().submissions(), 0);
}

#[test]
fn fork_catchup_expired_attempt_has_no_rpc_or_write() {
    let observer = FixtureRpc::new(fork_observer_state());
    let node = FixtureRpc::new(diverged_fork_state(false));
    let mut budget = ObserverRelayBudget::new();
    budget.deadline = Instant::now();
    assert!(matches!(
        bounded_relay(&fixture_provider(), &observer, &node, &mut budget),
        Err(MinerError::StaleNativeJob(_))
    ));
    assert!(observer.state.lock().unwrap().requests.is_empty());
    assert!(node.state.lock().unwrap().requests.is_empty());
}

#[test]
fn fork_catchup_absolute_deadline_bounds_an_inflight_raw_block_rpc() {
    let mut state = fork_observer_state();
    state.raw_delay = Duration::from_millis(1200);
    let observer = FixtureRpc::new(state);
    let node = FixtureRpc::new(diverged_fork_state(false));
    let mut budget = ObserverRelayBudget::new();
    budget.deadline = Instant::now() + Duration::from_millis(75);
    let started = Instant::now();
    assert!(matches!(
        bounded_relay(&fixture_provider(), &observer, &node, &mut budget),
        Err(MinerError::RpcTransport(_) | MinerError::StaleNativeJob(_))
    ));
    assert!(
        started.elapsed() < Duration::from_millis(800),
        "the absolute relay deadline must stop the RPC before its 1.2s fixture reply"
    );
    assert_eq!(node.state.lock().unwrap().submissions(), 0);
}

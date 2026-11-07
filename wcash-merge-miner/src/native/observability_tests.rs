use super::*;

#[test]
fn preparation_failure_context_maps_only_unique_configured_roles() {
    let mut provider = super::catchup_tests::fixture_provider();
    let expected = format!("{} at height 3548858", "ab".repeat(32));
    let actual = format!("{} at height 3548856", "cd".repeat(32));
    let error = MinerError::ParentTipMismatch {
        expected,
        endpoint: provider.proposal_validators[0].label().to_string(),
        actual,
    };
    let context = provider.preparation_failure_context(&error);
    assert_eq!(context.role.map(|role| role.name()), Some("validator"));
    assert_eq!(context.index, Some(0));
    assert_eq!(context.expected_height, Some(3548858));
    assert_eq!(context.actual_height, Some(3548856));
    assert_eq!(context.expected_hash, Some([0xab; 32]));
    assert_eq!(context.actual_hash, Some([0xcd; 32]));
    // Broadcast recipients do not participate in observer catch-up. Reusing a
    // validator there does not make this phase's recipient ambiguous.
    provider
        .broadcast_nodes
        .push(provider.proposal_validators[0].clone());
    assert_eq!(
        provider
            .preparation_failure_context(&error)
            .role
            .map(|role| role.name()),
        Some("validator")
    );
    assert_eq!(provider.preparation_failure_context(&error).index, Some(0));
    let (_, rows) = crate::diagnostics::capture_preparation_timings(|| {
        let mut timing = NativePreparationTiming::new();
        timing.enter(PreparationPhase::ObserverCatchup);
        timing.set_failure_context(provider.preparation_failure_context(&error));
        timing.finish_error(&error);
    });
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0]["preparation"]["parent_failure"]["node_role"],
        "validator"
    );
    assert_eq!(
        rows[0]["preparation"]["parent_failure"]["actual_tip_height"],
        3548856
    );

    provider
        .observer_nodes
        .push(provider.proposal_validators[0].clone());
    let ambiguous = provider.preparation_failure_context(&error);
    assert!(ambiguous.role.is_none());
    assert!(ambiguous.index.is_none());
    assert_eq!(ambiguous.expected_height, Some(3548858));
}

#[test]
fn preparation_failure_context_discards_unknown_endpoints_and_private_tip_suffixes() {
    let provider = super::catchup_tests::fixture_provider();
    let error = MinerError::ParentTipMismatch {
        expected: format!("{} at height 7 PRIVATE_PAYLOAD", "ab".repeat(32)),
        endpoint: "https://PRIVATE_ENDPOINT.invalid/PRIVATE_COOKIE".to_string(),
        actual: "PRIVATE_RESPONSE".to_string(),
    };
    let context = provider.preparation_failure_context(&error);
    assert!(context.role.is_none());
    assert!(context.index.is_none());
    assert!(context.expected_hash.is_none());
    assert!(context.actual_hash.is_none());
    assert!(context.expected_height.is_none());
    assert!(context.actual_height.is_none());
    let (_, rows) = crate::diagnostics::capture_preparation_timings(|| {
        let mut timing = NativePreparationTiming::new();
        timing.set_failure_context(context);
        timing.finish_error(&error);
    });
    assert_eq!(rows.len(), 1);
    assert!(!serde_json::to_string(&rows).unwrap().contains("PRIVATE"));
}

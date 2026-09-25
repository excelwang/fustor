use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use data_view::{
    ActionExecutionState, ActionResultSubmission, BoundedSourceAction, ContinuationPolicy,
    CreateStreamingSubscription, DATA_VIEW_WIRE_VERSION, DataViewCommitStatus,
    DataViewContractError, DataViewEngine, DataViewPersistence, DataViewStore, DerivationEvidence,
    DiscoveryOperation, FailureMode, FileDataViewPersistence, FreshnessState,
    MemoryDataViewPersistence, NativeAnchor, ObservationPath, PayloadEncoding, ProducerKind,
    ProjectionMode, SchemaVersionRef, SideEffectLevel, SingleResultNormalizer, SourceAuthorityRef,
    StreamingRefreshEntry, StreamingRefreshOperation, StreamingRefreshSubmission,
    SystemIdGenerator,
};

fn schema() -> SchemaVersionRef {
    SchemaVersionRef {
        schema_ref: "fustor.test.persistence".into(),
        schema_version: "1".into(),
    }
}

#[test]
fn read_only_file_persistence_never_initializes_or_writes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("missing.json");
    let missing = Arc::new(FileDataViewPersistence::new_read_only(&path));
    assert!(missing.load().is_err());
    assert!(missing.persist(b"not-a-snapshot").is_err());
    assert!(DataViewEngine::with_persistence("test", Arc::new(SystemIdGenerator), missing).is_err());
    assert!(!path.exists());
    assert!(!dir.path().join("missing.json.lock").exists());
    let writer = Arc::new(FileDataViewPersistence::new_exclusive(&path).unwrap());
    let engine = DataViewEngine::with_persistence("test", Arc::new(SystemIdGenerator), writer.clone()).unwrap();
    let before = std::fs::read(&path).unwrap();
    let reader = Arc::new(FileDataViewPersistence::new_read_only(&path));
    let inspected = DataViewEngine::with_persistence("test", Arc::new(SystemIdGenerator), reader.clone()).unwrap();
    assert_eq!(inspected.head_ref(), engine.head_ref());
    assert!(!reader.holds_exclusive_lock());
    assert!(reader.persist(b"cannot-overwrite").is_err());
    assert_eq!(std::fs::read(&path).unwrap(), before);
}

fn action() -> BoundedSourceAction {
    BoundedSourceAction {
        wire_version: DATA_VIEW_WIRE_VERSION,
        action_id: "action:persist".into(),
        source_authority_ref: SourceAuthorityRef("source:test".into()),
        observation_path: ObservationPath::Direct,
        operation: DiscoveryOperation::ObjectHead,
        permission_scope: "tenant-a:test".into(),
        freshness_precondition: FreshnessState::Fresh,
        max_rows: 1,
        max_bytes: 1024,
        max_calls: 1,
        max_fanout: 1,
        max_latency_ms: 1000,
        max_cost_units: 2,
        side_effect_level: SideEffectLevel::ReadOnly,
        continuation_policy: ContinuationPolicy::None,
        failure_modes: vec![FailureMode::NotDisclosed],
        expansion_rules: vec![],
        expected_information_gain: 50,
        explanation: "persist one result".into(),
    }
}

fn prepare_engine(
    persistence: Arc<dyn DataViewPersistence>,
) -> (DataViewEngine, data_view::ActionExecutionContext) {
    let mut engine =
        DataViewEngine::with_persistence("global", Arc::new(SystemIdGenerator), persistence)
            .expect("engine");
    engine
        .register_normalizer(Arc::new(SingleResultNormalizer::new(schema(), "object")))
        .expect("normalizer");
    engine.register_action(action()).expect("action");
    let context = engine
        .issue_action_context(
            "action:persist",
            engine.head_ref(),
            ProjectionMode::CurrentActionable,
            "policy-a",
            4,
            100,
            200,
        )
        .expect("context");
    (engine, context)
}

fn submission(context: data_view::ActionExecutionContext) -> ActionResultSubmission {
    let payload = br#"{"value":1}"#.to_vec();
    ActionResultSubmission {
        wire_version: DATA_VIEW_WIRE_VERSION,
        submission_id: "submission:persist".into(),
        execution: context,
        source_authority_ref: SourceAuthorityRef("source:test".into()),
        authority_object_ref: "source:test".into(),
        producer_id: "source:test".into(),
        producer_kind: ProducerKind::Source,
        projection_evidence: None,
        schema: schema(),
        encoding: PayloadEncoding::Json,
        actual_bytes: payload.len() as u64,
        payload,
        native_anchors: vec![NativeAnchor {
            anchor_kind: "object".into(),
            native_ref: "object:1".into(),
            native_pointer: None,
            native_version: Some("rev-1".into()),
        }],
        source_revision: "rev-1".into(),
        evidence_refs: vec!["evidence:1".into()],
        observed_at_ms: 120,
        policy_labels: BTreeSet::from(["tenant-a".into()]),
        grant_epoch: 4,
        freshness: FreshnessState::Fresh,
        derivation: DerivationEvidence {
            derivation_kind: "observed".into(),
            rule_ref: None,
            dependency_refs: vec![],
        },
        continuation: None,
        actual_rows: 1,
        actual_calls: 1,
        actual_fanout: 1,
        actual_latency_ms: 10,
        actual_cost_units: 1,
    }
}

fn reopen_engine(persistence: Arc<dyn DataViewPersistence>) -> DataViewEngine {
    let mut engine =
        DataViewEngine::with_persistence("global", Arc::new(SystemIdGenerator), persistence)
            .expect("reopen engine");
    engine
        .register_normalizer(Arc::new(SingleResultNormalizer::new(schema(), "object")))
        .expect("normalizer");
    engine
}

#[test]
fn file_persistence_reopens_the_same_head_and_complete_payload() {
    let temp = tempfile::tempdir().expect("tempdir");
    let path = temp.path().join("data-view.json");
    let persistence = Arc::new(FileDataViewPersistence::new(&path));
    let (mut engine, context) = prepare_engine(persistence.clone());
    let outcome = engine.submit_action_result(submission(context), 120, "policy-a");
    assert_eq!(outcome.status, DataViewCommitStatus::Accepted);
    let expected_head = outcome.data_view.clone();
    drop(engine);

    let reopened =
        DataViewStore::with_persistence("global", Arc::new(SystemIdGenerator), persistence)
            .expect("reopen store");
    assert_eq!(reopened.head_ref(), expected_head);
    let snapshot = reopened.snapshot(&expected_head).expect("snapshot");
    let version_ref = snapshot
        .selected_payload_versions
        .values()
        .next()
        .expect("selected payload");
    let version = reopened
        .payload_version(version_ref)
        .expect("payload version");
    assert_eq!(
        reopened
            .attachment(&version.attachment_ref)
            .expect("attachment")
            .payload,
        br#"{"value":1}"#
    );
    assert!(!temp.path().join(".data-view.json.tmp").exists());
}

#[test]
fn exclusive_file_persistence_rejects_a_second_writer() {
    let temp = tempfile::tempdir().expect("tempdir");
    let path = temp.path().join("data-view.json");
    let first = FileDataViewPersistence::new_exclusive(&path).expect("first writer");
    assert!(first.holds_exclusive_lock());
    assert!(FileDataViewPersistence::new_exclusive(&path).is_err());
    drop(first);
    assert!(FileDataViewPersistence::new_exclusive(&path).is_ok());
}

#[test]
fn issued_execution_and_commit_outcome_survive_restart_idempotently() {
    let persistence = Arc::new(MemoryDataViewPersistence::default());
    let (engine, context) = prepare_engine(persistence.clone());
    let request = submission(context.clone());
    let issued_head = engine.head_ref();
    drop(engine);

    let mut reopened = reopen_engine(persistence.clone());
    assert_eq!(
        reopened
            .action_registry()
            .execution_state(&context.execution_id),
        Some(&ActionExecutionState::Issued)
    );
    let accepted = reopened.submit_action_result(request.clone(), 120, "policy-a");
    assert_eq!(accepted.status, DataViewCommitStatus::Accepted);
    assert_ne!(accepted.data_view, issued_head);
    let metrics = reopened.store().metrics();
    drop(reopened);

    let mut reopened = reopen_engine(persistence);
    let duplicate = reopened.submit_action_result(request, 121, "policy-a");
    assert_eq!(duplicate, accepted);
    assert_eq!(reopened.store().metrics(), metrics);
    assert!(matches!(
        reopened
            .action_registry()
            .execution_state(&context.execution_id),
        Some(ActionExecutionState::Committed { .. })
    ));
}

#[test]
fn active_subscription_watermark_and_processed_batch_survive_restart() {
    let persistence = Arc::new(MemoryDataViewPersistence::default());
    let (mut engine, context) = prepare_engine(persistence.clone());
    let materialized = engine.submit_action_result(submission(context), 120, "policy-a");
    assert_eq!(materialized.status, DataViewCommitStatus::Accepted);
    let tracked_result = engine
        .store()
        .snapshot(&materialized.data_view)
        .expect("materialized snapshot")
        .selected_payload_versions
        .keys()
        .next()
        .cloned()
        .expect("tracked result");
    let subscription = engine
        .create_streaming_subscription(CreateStreamingSubscription {
            wire_version: DATA_VIEW_WIRE_VERSION,
            tracked_result_refs: BTreeSet::from([tracked_result.clone()]),
            source_authority_ref: SourceAuthorityRef("source:test".into()),
            observation_path: ObservationPath::Direct,
            allowed_schema_versions: BTreeSet::from([schema()]),
            source_watermark: "wm-0".into(),
            max_events_per_batch: 4,
            max_bytes_per_batch: 1024,
            max_batches_per_window: 4,
            batch_window_ms: 100,
            max_in_flight: 1,
            lease_issued_at_ms: 120,
            lease_expires_at_ms: 1000,
            policy_context_digest: "policy-a".into(),
            grant_epoch: 4,
        })
        .expect("subscription");
    let payload = br#"{"value":2}"#.to_vec();
    let refresh = StreamingRefreshSubmission {
        wire_version: DATA_VIEW_WIRE_VERSION,
        submission_id: "submission:stream:1".into(),
        subscription_id: subscription.subscription_id.clone(),
        batch_id: "batch:1".into(),
        sequence: 1,
        previous_watermark: "wm-0".into(),
        resulting_watermark: "wm-1".into(),
        schema: schema(),
        encoding: PayloadEncoding::Json,
        refreshes: vec![StreamingRefreshEntry {
            tracked_result_ref: tracked_result,
            operation: StreamingRefreshOperation::Refresh,
            payload: Some(payload.clone()),
            native_anchors: vec![NativeAnchor {
                anchor_kind: "object".into(),
                native_ref: "object:1".into(),
                native_pointer: None,
                native_version: Some("rev-2".into()),
            }],
            source_revision: "rev-2".into(),
            evidence_refs: vec!["evidence:2".into()],
            observed_at_ms: 130,
        }],
        projection_evidence: None,
        actual_bytes: payload.len() as u64,
        actual_events: 1,
    };
    drop(engine);

    let mut reopened = reopen_engine(persistence.clone());
    let accepted = reopened.submit_stream_refresh(refresh.clone(), 130, "policy-a", 4);
    assert_eq!(accepted.status, DataViewCommitStatus::Accepted);
    let metrics = reopened.store().metrics();
    drop(reopened);

    let mut reopened = reopen_engine(persistence);
    let restored = reopened
        .streaming_registry()
        .subscription(&subscription.subscription_id)
        .expect("restored subscription");
    assert_eq!(restored.next_sequence, 2);
    assert_eq!(restored.source_watermark, "wm-1");
    let duplicate = reopened.submit_stream_refresh(refresh, 131, "policy-a", 4);
    assert_eq!(duplicate, accepted);
    assert_eq!(reopened.store().metrics(), metrics);
}

#[derive(Debug, Default)]
struct FailablePersistence {
    fail_writes: AtomicBool,
    state: Mutex<Option<Vec<u8>>>,
}

impl FailablePersistence {
    fn set_fail_writes(&self, fail: bool) {
        self.fail_writes.store(fail, Ordering::SeqCst);
    }
}

impl DataViewPersistence for FailablePersistence {
    fn load(&self) -> Result<Option<Vec<u8>>, DataViewContractError> {
        self.state
            .lock()
            .map(|state| state.clone())
            .map_err(|_| DataViewContractError::PersistenceFailed)
    }

    fn persist(&self, state: &[u8]) -> Result<(), DataViewContractError> {
        if self.fail_writes.load(Ordering::SeqCst) {
            return Err(DataViewContractError::PersistenceFailed);
        }
        *self
            .state
            .lock()
            .map_err(|_| DataViewContractError::PersistenceFailed)? = Some(state.to_vec());
        Ok(())
    }
}

#[test]
fn persistence_failure_keeps_head_history_and_execution_uncommitted() {
    let persistence = Arc::new(FailablePersistence::default());
    let (mut engine, context) = prepare_engine(persistence.clone());
    let execution_id = context.execution_id.clone();
    let head_before = engine.head_ref();
    let metrics_before = engine.store().metrics();
    persistence.set_fail_writes(true);

    let outcome = engine.submit_action_result(submission(context), 120, "policy-a");
    assert_eq!(outcome.status, DataViewCommitStatus::Rejected);
    assert_eq!(engine.head_ref(), head_before);
    assert_eq!(engine.store().metrics(), metrics_before);
    assert_eq!(
        engine.action_registry().execution_state(&execution_id),
        Some(&ActionExecutionState::Issued)
    );

    persistence.set_fail_writes(false);
    let reopened =
        DataViewEngine::with_persistence("global", Arc::new(SystemIdGenerator), persistence)
            .expect("reopen engine");
    assert_eq!(reopened.head_ref(), head_before);
    assert_eq!(reopened.store().metrics(), metrics_before);
    assert_eq!(
        reopened.action_registry().execution_state(&execution_id),
        Some(&ActionExecutionState::Issued)
    );
}

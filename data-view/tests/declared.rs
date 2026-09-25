// ASSERTION INTENT (Black-box tests — public traits and APIs only). Do not introduce white-box testing logic or internal workarounds.
use data_view::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

fn policy(schema: &str) -> DeclaredResultPolicy {
    DeclaredResultPolicy {
        profile_id: schema.into(),
        producer_id: "test-producer".into(),
        result_authority_ref: "declared:test".into(),
        schema: SchemaVersionRef {
            schema_ref: schema.into(),
            schema_version: "1".into(),
        },
        allowed_external_ref_prefix: "file:///candidate/".into(),
        max_manifest_bytes: 16384,
        max_external_size_bytes: 1_000_000,
        policy_context_digest: "candidate-policy".into(),
        grant_epoch: 1,
        required_policy_labels: BTreeSet::from(["candidate-only".into()]),
        max_ttl_ms: 60000,
    }
}
fn configured(persistence: Arc<dyn DataViewPersistence>) -> DataViewEngine {
    let mut engine = DataViewEngine::with_persistence(
        "native-candidate",
        Arc::new(SystemIdGenerator),
        persistence,
    )
    .unwrap();
    for schema in ["manifest:table", "manifest:graph"] {
        let policy = policy(schema);
        engine
            .register_normalizer(Arc::new(DeclaredManifestNormalizer::new(
                policy.schema.clone(),
            )))
            .unwrap();
        engine.register_declared_policy(policy).unwrap();
    }
    engine
}
fn submission(engine: &mut DataViewEngine, schema: &str, key: &str) -> DeclaredResultSubmission {
    DeclaredResultSubmission {
        submission_id: key.into(),
        admission: engine
            .issue_declared_admission(schema, key, 1000, 60000)
            .unwrap(),
        manifest: ExternalResultManifest {
            external_ref: format!("file:///candidate/{key}.json"),
            external_revision: format!("external:{key}:1"),
            digest_algorithm: "sha256".into(),
            digest: "a".repeat(64),
            size_bytes: 999999,
            schema: policy(schema).schema,
            media_type: "application/json".into(),
            producer_id: "test-producer".into(),
            producer_version: "1".into(),
            lineage_refs: vec!["evidence:input:1".into()],
            evidence_refs: vec!["evidence:output:1".into()],
            validation_refs: vec!["evidence:validated:1".into()],
            observed_or_produced_at_ms: 1001,
            policy_labels: BTreeSet::from(["candidate-only".into()]),
            freshness: FreshnessState::Fresh,
            retention: BTreeMap::from([("mode".into(), "candidate-cleanup".into())]),
        },
        derivation: DerivationEvidence {
            derivation_kind: "external-workflow".into(),
            rule_ref: None,
            dependency_refs: vec![],
        },
    }
}
// @verify_spec("CONTRACTS.DATA_VIEW.DECLARED_EXTERNAL_RESULT_ADMISSION", mode="system")
#[test]
fn native_manifest_admission_is_atomic_idempotent_persisted_and_not_source_ingest() {
    let persistence = Arc::new(MemoryDataViewPersistence::default());
    let mut engine = configured(persistence.clone());
    let request = submission(&mut engine, "manifest:table", "first");
    let before = engine.head_ref();
    let mut malformed = request.clone();
    malformed.manifest.validation_refs.clear();
    assert_eq!(
        engine
            .submit_declared_result(malformed, 1002, "candidate-policy", 1)
            .status,
        DataViewCommitStatus::Rejected
    );
    assert_eq!(engine.head_ref(), before);
    let accepted = engine.submit_declared_result(request.clone(), 1002, "candidate-policy", 1);
    assert_eq!(accepted.status, DataViewCommitStatus::Accepted);
    assert_ne!(engine.head_ref(), before);
    let snapshot = persistence.snapshot().unwrap();
    assert!(snapshot.len() < 40000); // The 999999 referenced product bytes were not fetched.
    drop(engine);
    let mut engine = configured(persistence);
    assert_eq!(
        engine.submit_declared_result(request.clone(), 100_000, "candidate-policy", 1),
        accepted
    );
    let mut conflict = request.clone();
    conflict.manifest.digest = "b".repeat(64);
    assert_eq!(
        engine
            .submit_declared_result(conflict, 1002, "candidate-policy", 1)
            .status,
        DataViewCommitStatus::Rejected
    );
    assert_eq!(engine.head_ref(), accepted.data_view);
    let graph = submission(&mut engine, "manifest:graph", "second");
    assert_eq!(
        engine
            .submit_declared_result(graph, 1003, "candidate-policy", 1)
            .status,
        DataViewCommitStatus::Accepted
    );
}

// @verify_spec("CONTRACTS.DATA_VIEW.DECLARED_EXTERNAL_RESULT_ADMISSION", mode="system")
#[test]
fn stale_scope_digest_grant_and_expiry_never_move_head() {
    let mut engine = configured(Arc::new(MemoryDataViewPersistence::default()));
    let request = submission(&mut engine, "manifest:table", "first");
    let before = engine.head_ref();
    for reference in [
        "file:///production/file",
        "file:///candidate/../secret",
        "file:///candidate/%2e%2e/secret",
    ] {
        let mut bad = request.clone();
        bad.manifest.external_ref = reference.into();
        assert_eq!(
            engine
                .submit_declared_result(bad, 1002, "candidate-policy", 1)
                .status,
            DataViewCommitStatus::Rejected
        );
    }
    assert_eq!(
        engine
            .submit_declared_result(request.clone(), 1002, "candidate-policy", 2)
            .status,
        DataViewCommitStatus::RequiresRevalidation
    );
    assert_eq!(
        engine
            .submit_declared_result(request.clone(), 100_000, "candidate-policy", 1)
            .status,
        DataViewCommitStatus::RequiresRevalidation
    );
    assert_eq!(engine.head_ref(), before);
    let other = submission(&mut engine, "manifest:graph", "other");
    engine.submit_declared_result(other, 1002, "candidate-policy", 1);
    let head = engine.head_ref();
    assert_eq!(
        engine
            .submit_declared_result(request, 1002, "candidate-policy", 1)
            .status,
        DataViewCommitStatus::RequiresRevalidation
    );
    assert_eq!(engine.head_ref(), head);
}

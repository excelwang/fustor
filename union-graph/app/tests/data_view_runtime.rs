use std::collections::{BTreeMap, BTreeSet};

use capanix_app_sdk::Event;
use capanix_app_sdk::runtime::{EventMetadata, NodeId};
use data_view::{
    DerivationEvidence, DiscoveryOperation, FreshnessState, ProjectionMode, SchemaVersionRef,
    SourceAuthorityRef,
};
use data_view_normalizers::{
    BUILTIN_SCHEMA_VERSION, S3_SCHEMA_REF, SourceActionSubmissionContext,
    map_source_action_submission,
};
use private_discovery::{
    DataViewRevisionSelector, DisclosureBudget, DiscoveryBudget, DiscoveryIntent, DiscoveryScope,
    PolicyContext, PrivateDataDiscoveryRequest, ScopedDataViewProjectionOutcome,
    ScopedDisclosureBudget, ScopedProjectionAccess, SourceCapability, SourceFragmentObservation,
    SourceInstanceKey,
};
use source_kit::{
    SourceExecutionContext, SourceExecutionEnvelope, SourceExecutionLimits, decode_msgpack,
    decode_source_request, encode_msgpack, encode_source_request, encode_source_response,
};
use union_graph::{NodeKind, SourceKind};
use union_graph_runtime::{
    BuildScopedDataViewRequest, DataViewRuntimePayload, DataViewRuntimeRequest,
    DataViewRuntimeService,
};

fn build_request() -> BuildScopedDataViewRequest {
    let authority = SourceAuthorityRef("authority:s3:corp-bucket:reports".into());
    BuildScopedDataViewRequest {
        revision_selector: DataViewRevisionSelector::Latest,
        access: ScopedProjectionAccess {
            subject_scope_id: "tenant-a:user-a".into(),
            tenant_id: "tenant-a".into(),
            principal_id: "user-a".into(),
            purpose: "research".into(),
            permitted_source_authority_refs: BTreeSet::from([authority]),
            visible_policy_labels: BTreeSet::from(["tenant-a".into()]),
            current_grant_epoch: 4,
            allow_stale_but_plannable: false,
            discovery_budget: DiscoveryBudget::for_time_budget(5 * 60 * 1000),
            disclosure_budget: ScopedDisclosureBudget {
                max_results: 20,
                max_payload_bytes: 1024 * 1024,
                max_relationships: 20,
                include_payloads: true,
                include_evidence_refs: true,
            },
            coverage_evidence: data_view::CoverageState::CompleteForScope,
        },
        discovery_request: PrivateDataDiscoveryRequest {
            discovery_intent: DiscoveryIntent {
                task: "find finance reports".into(),
                priorities: vec![],
            },
            policy_context: PolicyContext {
                user_id: "user-a".into(),
                tenant_id: "tenant-a".into(),
                task_purpose: "research".into(),
                allowed_source_instances: BTreeSet::from([SourceInstanceKey::new(
                    SourceKind::S3,
                    "corp-bucket",
                )]),
                grant_epoch: Some(4),
                request_context: BTreeMap::new(),
            },
            discovery_scope: DiscoveryScope {
                source_instances: BTreeSet::new(),
                max_fragments: 20,
            },
            time_budget_ms: 5 * 60 * 1000,
            disclosure_budget: DisclosureBudget {
                max_fragments: 20,
                max_names: 20,
                max_relation_hints: 20,
            },
            candidate_fragments: vec![SourceFragmentObservation {
                source_kind: SourceKind::S3,
                source_instance: "corp-bucket".into(),
                local_id: "reports".into(),
                node_kind: NodeKind::Collection,
                native_ref: "s3://corp-bucket/reports/".into(),
                display_name: "reports".into(),
                native_pointer: "s3://corp-bucket/reports/".into(),
                fingerprint: Some("rev-1".into()),
                observed_at_ms: 100,
                grant_epoch: Some(4),
            }],
            scoped_data_view: None,
            source_capabilities: vec![SourceCapability {
                source_kind: SourceKind::S3,
                source_instance: "corp-bucket".into(),
                operations: vec![DiscoveryOperation::ObjectList],
                max_rows: 10,
                max_bytes: 4096,
                max_calls: 1,
                max_fanout: 1,
                max_latency_ms: 1000,
            }],
        },
        now_ms: 120,
    }
}

fn call(
    service: &DataViewRuntimeService,
    request: DataViewRuntimeRequest,
) -> DataViewRuntimePayload {
    let event = Event::new(
        EventMetadata {
            origin_id: NodeId("caller".into()),
            timestamp_us: 1,
            logical_ts: None,
            correlation_id: Some(7),
            ingress_auth: None,
            trace: None,
        },
        encode_msgpack(&request).expect("encode request"),
    );
    let reply = service.handle_event(event);
    assert_eq!(reply.metadata().correlation_id, Some(7));
    decode_msgpack(reply.payload_bytes()).expect("decode reply")
}

#[test]
fn runtime_builds_scoped_view_issues_visible_context_and_materializes_generic_result() {
    let service = DataViewRuntimeService::new("global", NodeId("data-view-runtime-test".into()))
        .expect("service");
    let build = build_request();
    let first = call(
        &service,
        DataViewRuntimeRequest::BuildScopedDataView {
            request: build.clone(),
        },
    );
    let DataViewRuntimePayload::ScopedDataView(ScopedDataViewProjectionOutcome::Projected(view)) =
        first
    else {
        panic!("expected scoped view");
    };
    assert_eq!(view.projection_mode, ProjectionMode::CurrentActionable);
    let action = view
        .discovery_frontier
        .ranked_actions
        .first()
        .expect("visible action")
        .action
        .clone();

    let context_payload = call(
        &service,
        DataViewRuntimeRequest::IssueVisibleActionContext {
            request: build.clone(),
            action_id: action.action_id,
            policy_context_digest: "policy-a".into(),
            grant_epoch: 4,
            issued_at_ms: 120,
            expires_at_ms: 220,
        },
    );
    let DataViewRuntimePayload::ActionExecutionContext(context) = context_payload else {
        panic!("expected action context");
    };

    let source_request = s3_source::S3SourceRequest {
        object_ref: "corp-bucket".into(),
        bucket: Some("corp-bucket".into()),
        prefix: Some("reports/".into()),
        operation: s3_source::S3SourceOperation::SnapshotPage,
        limit: Some(10),
        cursor: None,
    };
    let encoded_source_request = encode_source_request(&SourceExecutionEnvelope::new(
        SourceExecutionContext {
            request_id: context.execution_id.clone(),
            grant_epoch: context.grant_epoch,
            issued_at_ms: context.issued_at_ms,
            deadline_ms: context.expires_at_ms,
            limits: SourceExecutionLimits {
                max_rows: 10,
                max_bytes: 1024 * 1024,
                max_calls: 1,
                max_fanout: 1,
                max_latency_ms: 100,
                max_cost_units: 1,
            },
        },
        source_request,
    ))
    .expect("encode source request");
    let decoded_source_request =
        decode_source_request::<s3_source::S3SourceRequest>(&encoded_source_request, 125)
            .expect("decode source request");
    let source_payload = s3_source::S3SourcePayload::Objects(s3_source::S3ObjectBatch {
        object_ref: "corp-bucket".into(),
        bucket: "corp-bucket".into(),
        prefix: "reports/".into(),
        source_revision: "s3-list-observation:reports".into(),
        objects: vec![s3_source::S3ObjectEvent {
            key: "reports/q1.csv".into(),
            size: 128,
            last_modified_unix_ms: 100,
            etag: Some("etag-1".into()),
            storage_class: Some("STANDARD".into()),
            owner_id: None,
            owner_display_name: None,
            metadata: serde_json::json!({"team": "finance"}),
        }],
        next_cursor: None,
        has_more: false,
        grant_epoch: Some(4),
        diagnostics: None,
    });
    let encoded_source_response =
        encode_source_response(&decoded_source_request, source_payload, 10, 130)
            .expect("encode source response");
    let submission = map_source_action_submission::<s3_source::S3SourcePayload>(
        SourceActionSubmissionContext {
            submission_id: "submission:s3:reports".into(),
            execution: context,
            source_authority_ref: SourceAuthorityRef("authority:s3:corp-bucket:reports".into()),
            producer_id: "s3-source:corp-bucket".into(),
            schema: SchemaVersionRef {
                schema_ref: S3_SCHEMA_REF.into(),
                schema_version: BUILTIN_SCHEMA_VERSION.into(),
            },
            policy_labels: BTreeSet::from(["tenant-a".into()]),
            freshness: FreshnessState::Fresh,
            derivation: DerivationEvidence {
                derivation_kind: "observed".into(),
                rule_ref: None,
                dependency_refs: vec![],
            },
        },
        "s3",
        encoded_source_response,
    )
    .expect("map source response");
    let commit = call(
        &service,
        DataViewRuntimeRequest::SubmitActionResult {
            submission,
            now_ms: 130,
            current_policy_context_digest: "policy-a".into(),
        },
    );
    let DataViewRuntimePayload::Commit(outcome) = commit else {
        panic!("expected commit outcome");
    };
    assert!(matches!(
        outcome.status,
        data_view::DataViewCommitStatus::Accepted | data_view::DataViewCommitStatus::SafeMerged
    ));

    let second = call(
        &service,
        DataViewRuntimeRequest::BuildScopedDataView { request: build },
    );
    let DataViewRuntimePayload::ScopedDataView(ScopedDataViewProjectionOutcome::Projected(view)) =
        second
    else {
        panic!("expected updated scoped view");
    };
    assert_eq!(view.visible_results.len(), 2);
    assert_eq!(view.visible_relationships.len(), 1);
}

// @verify_spec("CONTRACTS.SOURCE.BOUNDED_SOURCE_ACTIONS", mode="system")
#[test]
fn runtime_reprojection_versions_changed_action_contracts_without_collision() {
    let service = DataViewRuntimeService::new("global", NodeId("data-view-runtime-test".into()))
        .expect("service");
    let first_request = build_request();
    let first = call(
        &service,
        DataViewRuntimeRequest::BuildScopedDataView {
            request: first_request.clone(),
        },
    );
    let DataViewRuntimePayload::ScopedDataView(ScopedDataViewProjectionOutcome::Projected(first)) =
        first
    else {
        panic!("expected first scoped view");
    };
    let first_action = first.discovery_frontier.ranked_actions[0].action.clone();

    let mut stale_request = first_request;
    stale_request.now_ms = 10_000_000;
    let stale = call(
        &service,
        DataViewRuntimeRequest::BuildScopedDataView {
            request: stale_request.clone(),
        },
    );
    let DataViewRuntimePayload::ScopedDataView(ScopedDataViewProjectionOutcome::Projected(stale)) =
        stale
    else {
        panic!("expected stale scoped view");
    };
    let stale_action = stale.discovery_frontier.ranked_actions[0].action.clone();
    assert_eq!(first_action.freshness_precondition, FreshnessState::Fresh);
    assert_eq!(
        stale_action.freshness_precondition,
        FreshnessState::RequiresRevalidation
    );
    assert_ne!(first_action.action_id, stale_action.action_id);

    let repeated = call(
        &service,
        DataViewRuntimeRequest::BuildScopedDataView {
            request: stale_request,
        },
    );
    let DataViewRuntimePayload::ScopedDataView(ScopedDataViewProjectionOutcome::Projected(
        repeated,
    )) = repeated
    else {
        panic!("expected repeated stale scoped view");
    };
    assert_eq!(
        repeated.discovery_frontier.ranked_actions[0]
            .action
            .action_id,
        stale_action.action_id
    );
}

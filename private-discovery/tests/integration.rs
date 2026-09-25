use std::collections::{BTreeMap, BTreeSet};

use pg_source::{PgFieldCatalog, PgFieldDescriptor};
use private_discovery::{
    DisclosureBudget, DisclosureClass, DiscoveryIntent, DiscoveryScope, FailureMode,
    FreshnessState, PolicyContext, PrivateDataDiscoveryRequest, PrivateDiscoveryOrchestrator,
    ProbeOutcome, ProbeOutcomeKind, SourceInstanceKey,
};
use s3_source::{S3ObjectBatch, S3ObjectEvent};
use union_graph::{SourceKind, UnionGraphStore, skeleton_from_pg_fields, skeleton_from_s3_batch};

fn pg_catalog() -> PgFieldCatalog {
    PgFieldCatalog {
        object_ref: "warehouse".into(),
        fields: vec![
            PgFieldDescriptor {
                schema: "public".into(),
                table: "orders".into(),
                column: "order_id".into(),
                ordinal_position: Some(1),
                column_type: Some("int8".into()),
            },
            PgFieldDescriptor {
                schema: "public".into(),
                table: "orders".into(),
                column: "risk_score".into(),
                ordinal_position: Some(2),
                column_type: Some("numeric".into()),
            },
        ],
    }
}

fn s3_batch(grant_epoch: Option<u64>) -> S3ObjectBatch {
    S3ObjectBatch {
        object_ref: "corp-bucket".into(),
        bucket: "corp-bucket".into(),
        prefix: "finance/2026/".into(),
        source_revision: "s3-list-observation:risk".into(),
        objects: vec![S3ObjectEvent {
            key: "finance/2026/risk.csv".into(),
            size: 4_096,
            last_modified_unix_ms: 1_000,
            etag: Some("etag-risk".into()),
            storage_class: Some("STANDARD".into()),
            owner_id: None,
            owner_display_name: None,
            metadata: serde_json::Value::Null,
        }],
        next_cursor: None,
        has_more: false,
        grant_epoch,
        diagnostics: None,
    }
}

fn ingest_source_payloads(store: &mut UnionGraphStore) -> usize {
    let pg = store
        .ingest(skeleton_from_pg_fields(&pg_catalog()))
        .expect("ingest pg source payload");
    let s3 = store
        .ingest(skeleton_from_s3_batch(&s3_batch(Some(12))))
        .expect("ingest s3 source payload");
    pg.nodes_upserted + s3.nodes_upserted
}

fn policy(grant_epoch: Option<u64>) -> PolicyContext {
    PolicyContext {
        user_id: "alice".into(),
        tenant_id: "tenant-a".into(),
        task_purpose: "risk-research".into(),
        allowed_source_instances: BTreeSet::from([
            SourceInstanceKey::new(SourceKind::Pg, "warehouse"),
            SourceInstanceKey::new(SourceKind::S3, "corp-bucket"),
        ]),
        grant_epoch,
        request_context: BTreeMap::from([
            ("trace_id".into(), "trace-1".into()),
            ("deadline_ms".into(), "300000".into()),
        ]),
    }
}

fn request_from_union_graph(
    store: &UnionGraphStore,
    time_budget_ms: u64,
    grant_epoch: Option<u64>,
) -> PrivateDataDiscoveryRequest {
    PrivateDataDiscoveryRequest {
        discovery_intent: DiscoveryIntent {
            task: "find cross-source risk evidence".into(),
            priorities: Vec::new(),
        },
        policy_context: policy(grant_epoch),
        discovery_scope: DiscoveryScope {
            source_instances: BTreeSet::new(),
            max_fragments: 10,
        },
        time_budget_ms,
        disclosure_budget: DisclosureBudget {
            max_fragments: 10,
            max_names: 10,
            max_relation_hints: 0,
        },
        candidate_fragments: store
            .list_nodes(None, None, None)
            .into_iter()
            .map(|node| private_discovery::SourceFragmentObservation {
                source_kind: node.source_kind,
                source_instance: node.source_instance,
                local_id: node.local_id,
                node_kind: node.node_kind,
                native_ref: node.native_ref,
                display_name: node.display_name,
                native_pointer: node.native_pointer,
                fingerprint: node.fingerprint,
                observed_at_ms: node.observed_at,
                grant_epoch: node.grant_epoch,
            })
            .collect(),
        scoped_data_view: None,
        source_capabilities: Vec::new(),
    }
}

#[test]
fn private_discovery_builds_action_map_from_source_payloads_without_exposing_raw_graph() {
    let mut store = UnionGraphStore::default();
    let nodes_upserted = ingest_source_payloads(&mut store);
    assert!(nodes_upserted >= 6);
    assert!(store.coverage().evidence_count >= 2);

    let response = PrivateDiscoveryOrchestrator::new().discover(
        request_from_union_graph(&store, 5 * 60 * 1000, Some(12)),
        1_100,
    );

    assert!(response.visible_fragments.len() >= 6);
    assert_eq!(
        response.scoped_discovery_graph.resource_nodes.len(),
        response.visible_fragments.len()
    );
    assert!(
        response
            .discovery_frontier
            .ranked_actions
            .iter()
            .any(|ranked| ranked.action.permission_scope == "pg:warehouse")
    );
    assert!(
        response
            .discovery_frontier
            .ranked_actions
            .iter()
            .any(|ranked| ranked.action.permission_scope == "s3:corp-bucket")
    );
    assert!(response.source_authority_records.iter().all(|record| {
        record.native_anchor.native_pointer.starts_with("pg://")
            || record.native_anchor.native_pointer.starts_with("s3://")
    }));
    assert!(
        response
            .execution_trace
            .steps
            .iter()
            .any(|step| step == "scheduled BFS frontier")
    );
}

#[test]
fn grant_epoch_change_invalidates_union_graph_seeded_fragments_before_execution() {
    let mut store = UnionGraphStore::default();
    store
        .ingest(skeleton_from_s3_batch(&s3_batch(Some(12))))
        .expect("ingest s3 source payload");

    let response = PrivateDiscoveryOrchestrator::new().discover(
        request_from_union_graph(&store, 30 * 60 * 1000, Some(99)),
        1_100,
    );

    assert!(
        response
            .scoped_discovery_graph
            .freshness_constraints
            .iter()
            .all(|guard| guard.state == FreshnessState::Invalidated)
    );
    assert!(
        response
            .discovery_frontier
            .ranked_actions
            .iter()
            .any(|ranked| {
                ranked
                    .action
                    .failure_modes
                    .iter()
                    .any(|mode| mode == &FailureMode::NeedsRevalidation)
            })
    );
}

#[test]
fn unauthorized_existing_and_nonexistent_have_same_caller_visible_class() {
    let orchestrator = PrivateDiscoveryOrchestrator::new();
    let unauthorized = orchestrator.normalize_probe(ProbeOutcome {
        kind: ProbeOutcomeKind::UnauthorizedExisting,
        visible_fragment: None,
        internal_diagnostic: Some("pg://warehouse.secret_table".into()),
    });
    let nonexistent = orchestrator.normalize_probe(ProbeOutcome {
        kind: ProbeOutcomeKind::Nonexistent,
        visible_fragment: None,
        internal_diagnostic: Some("pg://warehouse.missing_table".into()),
    });

    assert_eq!(unauthorized.class, DisclosureClass::NotDisclosed);
    assert_eq!(unauthorized.class, nonexistent.class);
    assert_eq!(unauthorized.external_message, nonexistent.external_message);
    assert!(!unauthorized.external_message.contains("secret_table"));
    assert!(!nonexistent.external_message.contains("missing_table"));
}

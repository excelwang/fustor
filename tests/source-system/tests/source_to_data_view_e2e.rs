#![forbid(unsafe_code)]

use std::collections::BTreeSet;
use std::fs;
use std::sync::Arc;
use std::time::Duration;

use capanix_app_sdk::runtime::{NodeId, RouteKey};
use capanix_app_sdk::{CnxError, Event, Result};
use data_view::{
    BoundedSourceAction, ContinuationPolicy, DATA_VIEW_WIRE_VERSION, DataViewCommitStatus,
    DataViewEngine, DerivationEvidence, DiscoveryOperation, FailureMode, FreshnessState,
    ObservationPath, ProjectionMode, SchemaVersionRef, SideEffectLevel, SourceAuthorityRef,
};
use data_view_normalizers::{
    BUILTIN_SCHEMA_VERSION, CSV_SCHEMA_REF, ES_SCHEMA_REF, JSONL_SCHEMA_REF, LOCAL_FS_SCHEMA_REF,
    MYSQL_SCHEMA_REF, PG_SCHEMA_REF, S3_SCHEMA_REF, SourceActionSubmissionContext,
    map_source_action_submission, register_builtin_normalizers,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use source_kit::{
    SourceExecutionContext, SourceExecutionEnvelope, SourceExecutionLimits, SourcePayloadMetadata,
    encode_source_request, source_now_ms,
};
use source_test_kit::ask_bound_route;
use tempfile::tempdir;

// ASSERTION INTENT (Black-box tests - public traits and APIs only). Do not introduce white-box testing logic or internal workarounds.
// @verify_spec("CONTRACTS.SOURCE.SOURCE_EXECUTION_ENVELOPE", mode="system")
// @verify_spec("CONTRACTS.SOURCE.SOURCE_OBSERVATION_RECEIPT", mode="system")
// @verify_spec("CONTRACTS.SOURCE.SOURCE_CONNECTOR_PARITY", mode="system")
// @verify_spec("CONTRACTS.DATA_VIEW.DEMAND_MATERIALIZATION", mode="system")
// @verify_spec("CONTRACTS.DATA_VIEW.RESULT_NORMALIZATION_OWNERSHIP", mode="system")
// @verify_spec("CONTRACTS.DATA_VIEW.FULL_BOUNDED_RESULT_MATERIALIZATION", mode="system")
// @verify_spec("CONTRACTS.DATA_VIEW.EVIDENCE_GATED_ATOMIC_COMMIT", mode="system")

async fn commit_bound_source<P, R, F>(
    object_ref: &str,
    source_kind: &str,
    schema_ref: &str,
    route: RouteKey,
    request: R,
    handler: F,
) where
    P: DeserializeOwned + Serialize + SourcePayloadMetadata,
    R: Clone + Serialize,
    F: Clone + Fn(Vec<Event>) -> Vec<Event> + Send + Sync + 'static,
{
    let now_ms = source_now_ms();
    let authority = SourceAuthorityRef(object_ref.to_string());
    let action_id = format!("action:e2e:{source_kind}");
    let mut engine = DataViewEngine::new(format!("data-view:e2e:{source_kind}"))
        .expect("create DataView engine");
    register_builtin_normalizers(engine.normalizer_registry_mut()).expect("register normalizers");
    engine
        .register_action(BoundedSourceAction {
            wire_version: DATA_VIEW_WIRE_VERSION,
            action_id: action_id.clone(),
            source_authority_ref: authority.clone(),
            observation_path: ObservationPath::Direct,
            operation: DiscoveryOperation::FieldDiscovery,
            permission_scope: format!("source:{object_ref}:read"),
            freshness_precondition: FreshnessState::Fresh,
            max_rows: 1_000,
            max_bytes: 1024 * 1024,
            max_calls: 1,
            max_fanout: 1,
            max_latency_ms: 30_000,
            max_cost_units: 1,
            side_effect_level: SideEffectLevel::ReadOnly,
            continuation_policy: ContinuationPolicy::CursorBounded,
            failure_modes: vec![FailureMode::SourceUnavailable, FailureMode::BudgetExceeded],
            expansion_rules: Vec::new(),
            expected_information_gain: 80,
            explanation: "source connector system acceptance".into(),
        })
        .expect("register action");
    let context = engine
        .issue_action_context(
            &action_id,
            engine.head_ref(),
            ProjectionMode::CurrentActionable,
            "policy:e2e",
            1,
            now_ms,
            now_ms + 30_000,
        )
        .expect("issue action context");
    let submission_context = |submission_id: &str| SourceActionSubmissionContext {
        submission_id: submission_id.into(),
        execution: context.clone(),
        source_authority_ref: authority.clone(),
        producer_id: format!("{source_kind}-source:e2e"),
        schema: SchemaVersionRef {
            schema_ref: schema_ref.into(),
            schema_version: BUILTIN_SCHEMA_VERSION.into(),
        },
        policy_labels: BTreeSet::from(["e2e".into()]),
        freshness: FreshnessState::Fresh,
        derivation: DerivationEvidence {
            derivation_kind: "source-observation".into(),
            rule_ref: None,
            dependency_refs: Vec::new(),
        },
    };
    for (case, execution_context) in [
        (
            "stale-grant",
            SourceExecutionContext {
                request_id: context.execution_id.clone(),
                grant_epoch: 2,
                issued_at_ms: context.issued_at_ms,
                deadline_ms: context.expires_at_ms,
                limits: SourceExecutionLimits {
                    max_rows: 1_000,
                    max_bytes: 1024 * 1024,
                    max_calls: 1,
                    max_fanout: 1,
                    max_latency_ms: 30_000,
                    max_cost_units: 1,
                },
            },
        ),
        (
            "byte-budget",
            SourceExecutionContext {
                request_id: context.execution_id.clone(),
                grant_epoch: 1,
                issued_at_ms: context.issued_at_ms,
                deadline_ms: context.expires_at_ms,
                limits: SourceExecutionLimits {
                    max_rows: 1_000,
                    max_bytes: 128,
                    max_calls: 1,
                    max_fanout: 1,
                    max_latency_ms: 30_000,
                    max_cost_units: 1,
                },
            },
        ),
        (
            "expired",
            SourceExecutionContext {
                request_id: context.execution_id.clone(),
                grant_epoch: 1,
                issued_at_ms: now_ms.saturating_sub(2),
                deadline_ms: now_ms.saturating_sub(1),
                limits: SourceExecutionLimits {
                    max_rows: 1_000,
                    max_bytes: 1024 * 1024,
                    max_calls: 1,
                    max_fanout: 1,
                    max_latency_ms: 1,
                    max_cost_units: 1,
                },
            },
        ),
        (
            "latency-exhausted",
            SourceExecutionContext {
                request_id: context.execution_id.clone(),
                grant_epoch: 1,
                issued_at_ms: now_ms.saturating_sub(100),
                deadline_ms: context.expires_at_ms,
                limits: SourceExecutionLimits {
                    max_rows: 1_000,
                    max_bytes: 1024 * 1024,
                    max_calls: 1,
                    max_fanout: 1,
                    max_latency_ms: 10,
                    max_cost_units: 1,
                },
            },
        ),
    ] {
        let rejected_request = SourceExecutionEnvelope::new(execution_context, request.clone());
        let rejected = ask_bound_route(
            route.clone(),
            encode_source_request(&rejected_request)
                .expect("encode rejected source request")
                .into(),
            Duration::from_secs(30),
            handler.clone(),
        )
        .await
        .expect("rejected bound source request");
        assert!(
            map_source_action_submission::<P>(
                submission_context(&format!("submission:e2e:{source_kind}:{case}")),
                source_kind,
                rejected[0].payload_bytes().to_vec(),
            )
            .is_err(),
            "{source_kind} {case} response must not be materializable"
        );
    }
    let request = SourceExecutionEnvelope::new(
        SourceExecutionContext {
            request_id: context.execution_id.clone(),
            grant_epoch: context.grant_epoch,
            issued_at_ms: context.issued_at_ms,
            deadline_ms: context.expires_at_ms,
            limits: SourceExecutionLimits {
                max_rows: 1_000,
                max_bytes: 1024 * 1024,
                max_calls: 1,
                max_fanout: 1,
                max_latency_ms: 30_000,
                max_cost_units: 1,
            },
        },
        request,
    );
    let replies = ask_bound_route(
        route,
        encode_source_request(&request)
            .expect("encode source request")
            .into(),
        Duration::from_secs(30),
        handler,
    )
    .await
    .expect("bound source request");
    let encoded_response = replies[0].payload_bytes().to_vec();
    let submission = map_source_action_submission::<P>(
        submission_context(&format!("submission:e2e:{source_kind}")),
        source_kind,
        encoded_response,
    )
    .expect("map source response to DataView submission");
    assert_eq!(submission.authority_object_ref, object_ref);
    let head_before = engine.head_ref();
    let outcome = engine.submit_action_result(submission, source_now_ms(), "policy:e2e");
    assert_eq!(
        outcome.status,
        DataViewCommitStatus::Accepted,
        "source commit must be accepted: outcome={outcome:?}"
    );
    assert_ne!(outcome.data_view, head_before);
    assert!(!engine.store().tracked_result_refs().is_empty());
}

struct PgFieldsDriver;

impl pg_source_runtime::PgSourceDriver for PgFieldsDriver {
    fn probe(
        &self,
        endpoint: &pg_source::ResolvedPgEndpoint,
        _credential: &pg_source::ResolvedCredential,
    ) -> Result<pg_source::PgProbeStatus> {
        Ok(pg_source::PgProbeStatus {
            object_ref: endpoint.object_ref.clone(),
            endpoint_uri: endpoint.endpoint_uri.clone(),
            reachable: true,
            server_version: Some("e2e".into()),
            diagnostics: None,
        })
    }

    fn discover_fields(
        &self,
        endpoint: &pg_source::ResolvedPgEndpoint,
        _credential: &pg_source::ResolvedCredential,
    ) -> Result<pg_source::PgFieldCatalog> {
        Ok(pg_source::PgFieldCatalog {
            object_ref: endpoint.object_ref.clone(),
            fields: vec![pg_source::PgFieldDescriptor {
                schema: "public".into(),
                table: "items".into(),
                column: "id".into(),
                ordinal_position: Some(1),
                column_type: Some("bigint".into()),
            }],
        })
    }

    fn open_snapshot(
        &self,
        _endpoint: &pg_source::ResolvedPgEndpoint,
        _credential: &pg_source::ResolvedCredential,
        _schema: &str,
        _table: &str,
        _fields: &[String],
    ) -> Result<Box<dyn pg_source_runtime::PgSnapshotSession>> {
        Err(CnxError::NotSupported("not used by e2e".into()))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgresql_route_to_data_view_system_e2e() {
    let service = Arc::new(pg_source_runtime::PgSourceService::new(
        pg_source::PgSourceRuntimeConfig {
            product: pg_source::PgSourceConfig {
                endpoints: vec![pg_source::PgEndpointConfig {
                    object_ref: "pg-e2e".into(),
                    endpoint_uri: "postgresql://unused/e2e".into(),
                    credential_ref: None,
                    schema_scopes: vec!["public".into()],
                    table_scopes: vec!["items".into()],
                    primary_key: Some("id".into()),
                    active: true,
                }],
                ..pg_source::PgSourceConfig::default()
            },
            runtime: pg_source::PgSourceRuntimeInputs {
                endpoint_grants: vec![pg_source::GrantedPgEndpoint {
                    object_ref: "pg-e2e".into(),
                    endpoint_ref: None,
                    endpoint_uri: None,
                    credential_ref: None,
                    schema_scopes: Vec::new(),
                    table_scopes: Vec::new(),
                    interfaces: vec!["read".into()],
                    active: true,
                    grant_epoch: Some(1),
                }],
            },
        },
        Arc::new(PgFieldsDriver),
        NodeId("pg-e2e".into()),
    ));
    let request = pg_source::PgSourceRequest {
        object_ref: "pg-e2e".into(),
        schema: None,
        table: None,
        operation: pg_source::PgSourceOperation::DiscoverFields,
        limit: None,
        cursor: None,
        field_filter: Vec::new(),
    };
    commit_bound_source::<pg_source::PgSourcePayload, _, _>(
        "pg-e2e",
        "pg",
        PG_SCHEMA_REF,
        pg_source::product_model::routes::fetch_request_route_for("pg-e2e"),
        request,
        move |events| service.handle_events(events),
    )
    .await;
}

struct MysqlFieldsDriver;

impl mysql_source_runtime::MysqlSourceDriver for MysqlFieldsDriver {
    fn probe(
        &self,
        endpoint: &mysql_source::ResolvedMysqlEndpoint,
        _credential: &mysql_source::ResolvedCredential,
    ) -> Result<mysql_source::MysqlProbeStatus> {
        Ok(mysql_source::MysqlProbeStatus {
            object_ref: endpoint.object_ref.clone(),
            endpoint_uri: endpoint.endpoint_uri.clone(),
            reachable: true,
            server_version: Some("e2e".into()),
            diagnostics: None,
        })
    }

    fn discover_fields(
        &self,
        endpoint: &mysql_source::ResolvedMysqlEndpoint,
        _credential: &mysql_source::ResolvedCredential,
    ) -> Result<mysql_source::MysqlFieldCatalog> {
        Ok(mysql_source::MysqlFieldCatalog {
            object_ref: endpoint.object_ref.clone(),
            fields: vec![mysql_source::MysqlFieldDescriptor {
                schema: "app".into(),
                table: "items".into(),
                column: "id".into(),
                ordinal_position: Some(1),
                column_type: Some("bigint".into()),
            }],
        })
    }

    fn open_snapshot(
        &self,
        _endpoint: &mysql_source::ResolvedMysqlEndpoint,
        _credential: &mysql_source::ResolvedCredential,
        _schema: &str,
        _table: &str,
        _fields: &[String],
    ) -> Result<Box<dyn mysql_source_runtime::MysqlSnapshotSession>> {
        Err(CnxError::NotSupported("not used by e2e".into()))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_route_to_data_view_system_e2e() {
    let service = Arc::new(mysql_source_runtime::MysqlSourceService::new(
        mysql_source::MysqlSourceRuntimeConfig {
            product: mysql_source::MysqlSourceConfig {
                endpoints: vec![mysql_source::MysqlEndpointConfig {
                    tls: None,
                    object_ref: "mysql-e2e".into(),
                    endpoint_uri: "mysql://unused/e2e".into(),
                    credential_ref: None,
                    schema_scopes: vec!["app".into()],
                    table_scopes: vec!["items".into()],
                    primary_key: Some("id".into()),
                    active: true,
                }],
                ..mysql_source::MysqlSourceConfig::default()
            },
            runtime: mysql_source::MysqlSourceRuntimeInputs {
                endpoint_grants: vec![mysql_source::GrantedMysqlEndpoint {
                    object_ref: "mysql-e2e".into(),
                    endpoint_ref: None,
                    endpoint_uri: None,
                    credential_ref: None,
                    schema_scopes: Vec::new(),
                    table_scopes: Vec::new(),
                    interfaces: vec!["read".into()],
                    active: true,
                    grant_epoch: Some(1),
                }],
            },
        },
        Arc::new(MysqlFieldsDriver),
        NodeId("mysql-e2e".into()),
    ));
    let request = mysql_source::MysqlSourceRequest {
        object_ref: "mysql-e2e".into(),
        schema: None,
        table: None,
        operation: mysql_source::MysqlSourceOperation::DiscoverFields,
        limit: None,
        cursor: None,
        field_filter: Vec::new(),
    };
    commit_bound_source::<mysql_source::MysqlSourcePayload, _, _>(
        "mysql-e2e",
        "mysql",
        MYSQL_SCHEMA_REF,
        mysql_source::product_model::routes::fetch_request_route_for("mysql-e2e"),
        request,
        move |events| service.handle_events(events),
    )
    .await;
}

#[derive(Default)]
struct EsFieldsDriver;

impl es_source_runtime::EsSourceDriver for EsFieldsDriver {
    fn execute(
        &self,
        endpoint: &es_source::ResolvedEsEndpoint,
        _credential: &es_source::ResolvedCredential,
        request: &es_source::EsSourceRequest,
        _limit: usize,
    ) -> Result<es_source::EsSourcePayload> {
        Ok(es_source::EsSourcePayload::Fields(
            es_source::EsFieldCatalog {
                object_ref: endpoint.object_ref.clone(),
                index: request.index.clone(),
                fields: vec![es_source::EsFieldDescriptor {
                    name: "message".into(),
                    field_type: Some("text".into()),
                }],
            },
        ))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn elasticsearch_route_to_data_view_system_e2e() {
    let service = Arc::new(es_source_runtime::EsSourceService::new(
        es_source::EsSourceRuntimeConfig {
            product: es_source::EsSourceConfig {
                endpoints: vec![es_source::EsEndpointConfig {
                    object_ref: "es-e2e".into(),
                    endpoint_uri: "http://unused".into(),
                    credential_ref: None,
                    index_scopes: vec!["logs".into()],
                    timestamp_field: "@timestamp".into(),
                    tie_breaker_field: "tie".into(),
                    active: true,
                }],
                ..es_source::EsSourceConfig::default()
            },
            runtime: es_source::EsSourceRuntimeInputs {
                endpoint_grants: vec![es_source::GrantedEsEndpoint {
                    object_ref: "es-e2e".into(),
                    endpoint_ref: None,
                    endpoint_uri: None,
                    credential_ref: None,
                    index_scopes: Vec::new(),
                    interfaces: vec!["read".into()],
                    active: true,
                    grant_epoch: Some(1),
                }],
            },
        },
        Arc::new(EsFieldsDriver),
        NodeId("es-e2e".into()),
    ));
    let mut request = es_source::EsSourceRequest::snapshot_page("es-e2e", "logs");
    request.operation = es_source::EsSourceOperation::DiscoverFields;
    commit_bound_source::<es_source::EsSourcePayload, _, _>(
        "es-e2e",
        "elasticsearch",
        ES_SCHEMA_REF,
        es_source::product_model::routes::fetch_request_route_for("es-e2e"),
        request,
        move |events| service.handle_events(events),
    )
    .await;
}

struct S3FieldsDriver;

impl s3_source_runtime::S3SourceDriver for S3FieldsDriver {
    fn probe(
        &self,
        endpoint: &s3_source::ResolvedS3Endpoint,
        _credential: &s3_source::ResolvedCredential,
    ) -> Result<s3_source::S3ProbeStatus> {
        Ok(s3_source::S3ProbeStatus {
            object_ref: endpoint.object_ref.clone(),
            endpoint_uri: endpoint.endpoint_uri.clone(),
            bucket: endpoint.bucket.clone(),
            reachable: true,
            diagnostics: None,
        })
    }

    fn discover_fields(
        &self,
        endpoint: &s3_source::ResolvedS3Endpoint,
        _credential: &s3_source::ResolvedCredential,
    ) -> Result<s3_source::S3FieldCatalog> {
        Ok(s3_source::S3FieldCatalog {
            object_ref: endpoint.object_ref.clone(),
            fields: vec![s3_source::S3FieldDescriptor {
                name: "Key".into(),
                field_type: "string".into(),
            }],
        })
    }

    fn snapshot_page(
        &self,
        _endpoint: &s3_source::ResolvedS3Endpoint,
        _credential: &s3_source::ResolvedCredential,
        _limit: usize,
        _cursor: Option<&s3_source::S3SnapshotCursor>,
    ) -> Result<s3_source_runtime::S3ObjectPage> {
        Err(CnxError::NotSupported("not used by e2e".into()))
    }

    fn poll_since(
        &self,
        _endpoint: &s3_source::ResolvedS3Endpoint,
        _credential: &s3_source::ResolvedCredential,
        _limit: usize,
        _cursor: Option<&s3_source::S3PollCursor>,
    ) -> Result<s3_source_runtime::S3ObjectPage> {
        Err(CnxError::NotSupported("not used by e2e".into()))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_route_to_data_view_system_e2e() {
    let service = Arc::new(s3_source_runtime::S3SourceService::new(
        s3_source::S3SourceRuntimeConfig {
            product: s3_source::S3SourceConfig {
                endpoints: vec![s3_source::S3EndpointConfig {
                    object_ref: "s3-e2e".into(),
                    endpoint_uri: "http://unused".into(),
                    bucket: "bucket".into(),
                    region: Some("us-east-1".into()),
                    prefix: "reports/".into(),
                    credential_ref: None,
                    bucket_scopes: vec!["bucket".into()],
                    prefix_scopes: vec!["reports/*".into()],
                    active: true,
                }],
                ..s3_source::S3SourceConfig::default()
            },
            runtime: s3_source::S3SourceRuntimeInputs {
                endpoint_grants: vec![s3_source::GrantedS3Endpoint {
                    object_ref: "s3-e2e".into(),
                    endpoint_ref: None,
                    endpoint_uri: None,
                    bucket: None,
                    credential_ref: None,
                    bucket_scopes: Vec::new(),
                    prefix_scopes: Vec::new(),
                    interfaces: vec!["read".into()],
                    active: true,
                    grant_epoch: Some(1),
                }],
            },
        },
        Arc::new(S3FieldsDriver),
        NodeId("s3-e2e".into()),
    ));
    let mut request = s3_source::S3SourceRequest::snapshot_page("s3-e2e");
    request.operation = s3_source::S3SourceOperation::DiscoverFields;
    commit_bound_source::<s3_source::S3SourcePayload, _, _>(
        "s3-e2e",
        "s3",
        S3_SCHEMA_REF,
        s3_source::product_model::routes::fetch_request_route_for("s3-e2e"),
        request,
        move |events| service.handle_events(events),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_fs_route_to_data_view_system_e2e() {
    let root = tempdir().expect("tempdir");
    fs::write(root.path().join("a.txt"), b"a").expect("write file");
    let service = Arc::new(local_fs_source_runtime::LocalFsSourceService::new(
        local_fs_source::LocalFsSourceRuntimeConfig {
            product: local_fs_source::LocalFsSourceConfig {
                endpoints: vec![local_fs_source::LocalFsEndpointConfig {
                    object_ref: "local-e2e".into(),
                    root_path: root.path().to_string_lossy().to_string(),
                    path_scopes: vec!["*".into()],
                    active: true,
                }],
                default_page_size: 10,
                max_page_size: 100,
            },
            runtime: local_fs_source::LocalFsSourceRuntimeInputs {
                endpoint_grants: vec![local_fs_source::GrantedLocalFsEndpoint {
                    object_ref: "local-e2e".into(),
                    path_scopes: Vec::new(),
                    interfaces: vec!["read".into()],
                    active: true,
                    grant_epoch: Some(1),
                }],
            },
        },
        Arc::new(local_fs_source_runtime::NativeLocalFsSourceDriver),
        NodeId("local-e2e".into()),
    ));
    let request = local_fs_source::LocalFsSourceRequest::snapshot_page("local-e2e");
    commit_bound_source::<local_fs_source::LocalFsSourcePayload, _, _>(
        "local-e2e",
        "local-fs",
        LOCAL_FS_SCHEMA_REF,
        local_fs_source::product_model::routes::fetch_request_route_for("local-e2e"),
        request,
        move |events| service.handle_events(events),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn csv_route_to_data_view_system_e2e() {
    let root = tempdir().expect("tempdir");
    fs::write(root.path().join("data.csv"), b"id,name\n1,Alice\n").expect("write CSV");
    let service = Arc::new(csv_source_runtime::CsvSourceService::new(
        csv_source::CsvSourceRuntimeConfig {
            product: csv_source::CsvSourceConfig {
                endpoints: vec![csv_source::CsvEndpointConfig {
                    object_ref: "csv-e2e".into(),
                    root_path: root.path().to_string_lossy().to_string(),
                    path_scopes: vec!["*.csv".into()],
                    has_headers: true,
                    delimiter: b',',
                    active: true,
                }],
                default_page_size: 10,
                max_page_size: 100,
            },
            runtime: csv_source::CsvSourceRuntimeInputs {
                endpoint_grants: vec![csv_source::GrantedCsvEndpoint {
                    object_ref: "csv-e2e".into(),
                    path_scopes: Vec::new(),
                    interfaces: vec!["read".into()],
                    active: true,
                    grant_epoch: Some(1),
                }],
            },
        },
        Arc::new(csv_source_runtime::NativeCsvSourceDriver),
        NodeId("csv-e2e".into()),
    ));
    let request = csv_source::CsvSourceRequest::snapshot_page("csv-e2e", "data.csv");
    commit_bound_source::<csv_source::CsvSourcePayload, _, _>(
        "csv-e2e",
        "csv",
        CSV_SCHEMA_REF,
        csv_source::product_model::routes::fetch_request_route_for("csv-e2e"),
        request,
        move |events| service.handle_events(events),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jsonl_route_to_data_view_system_e2e() {
    let root = tempdir().expect("tempdir");
    fs::write(
        root.path().join("data.jsonl"),
        b"{\"id\":1,\"name\":\"Alice\"}\n",
    )
    .expect("write JSONL");
    let service = Arc::new(jsonl_source_runtime::JsonlSourceService::new(
        jsonl_source::JsonlSourceRuntimeConfig {
            product: jsonl_source::JsonlSourceConfig {
                endpoints: vec![jsonl_source::JsonlEndpointConfig {
                    object_ref: "jsonl-e2e".into(),
                    root_path: root.path().to_string_lossy().to_string(),
                    path_scopes: vec!["*.jsonl".into()],
                    active: true,
                }],
                default_page_size: 10,
                max_page_size: 100,
                field_sample_limit: 100,
            },
            runtime: jsonl_source::JsonlSourceRuntimeInputs {
                endpoint_grants: vec![jsonl_source::GrantedJsonlEndpoint {
                    object_ref: "jsonl-e2e".into(),
                    path_scopes: Vec::new(),
                    interfaces: vec!["read".into()],
                    active: true,
                    grant_epoch: Some(1),
                }],
            },
        },
        Arc::new(jsonl_source_runtime::NativeJsonlSourceDriver),
        NodeId("jsonl-e2e".into()),
    ));
    let request = jsonl_source::JsonlSourceRequest::snapshot_page("jsonl-e2e", "data.jsonl");
    commit_bound_source::<jsonl_source::JsonlSourcePayload, _, _>(
        "jsonl-e2e",
        "jsonl",
        JSONL_SCHEMA_REF,
        jsonl_source::product_model::routes::fetch_request_route_for("jsonl-e2e"),
        request,
        move |events| service.handle_events(events),
    )
    .await;
}

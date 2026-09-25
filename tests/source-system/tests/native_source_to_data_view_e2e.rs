#![forbid(unsafe_code)]

use std::collections::{BTreeSet, HashMap};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use aws_credential_types::Credentials;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::primitives::ByteStream;
use capanix_app_sdk::Event;
use capanix_app_sdk::runtime::{ConfigValue, NodeId, RouteKey};
use data_view::{
    BoundedSourceAction, ContinuationPolicy, DATA_VIEW_WIRE_VERSION, DataViewCommitStatus,
    DataViewEngine, DerivationEvidence, DiscoveryOperation, FailureMode, FreshnessState,
    ObservationPath, ProjectionMode, SchemaVersionRef, SideEffectLevel, SourceAuthorityRef,
};
use data_view_normalizers::{
    BUILTIN_SCHEMA_VERSION, ES_SCHEMA_REF, MYSQL_SCHEMA_REF, PG_SCHEMA_REF, S3_SCHEMA_REF,
    SourceActionSubmissionContext, map_source_action_submission, register_builtin_normalizers,
};
use mysql::prelude::Queryable;
use mysql::{Conn, Opts, OptsBuilder};
use postgres::{Config as PgConfig, NoTls};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use source_kit::{
    SourceExecutionContext, SourceExecutionEnvelope, SourceExecutionLimits, SourcePayloadMetadata,
    encode_source_request, source_now_ms,
};
use source_test_kit::ask_bound_route;
use uuid::Uuid;

// ASSERTION INTENT (Black-box tests - public traits and APIs only). Do not introduce white-box testing logic or internal workarounds.
// @verify_spec("CONTRACTS.SOURCE.SOURCE_CONNECTOR_PARITY", mode="system")
// @verify_spec("CONTRACTS.SOURCE.SOURCE_OBSERVATION_RECEIPT", mode="system")
// @verify_spec("CONTRACTS.DATA_VIEW.RESULT_NORMALIZATION_OWNERSHIP", mode="system")
// @verify_spec("CONTRACTS.DATA_VIEW.FULL_BOUNDED_RESULT_MATERIALIZATION", mode="system")
// @verify_spec("CONTRACTS.DATA_VIEW.EVIDENCE_GATED_ATOMIC_COMMIT", mode="system")

fn required_env(key: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| panic!("missing required native source-to-DataView test env {key}"))
}

fn basic_env_credential(
    credential_ref: &str,
    username_env: &str,
    password_env: &str,
) -> source_kit::SourceCredentialConfig {
    source_kit::parse_credential_config(&HashMap::from([
        (
            "credential_ref".to_string(),
            ConfigValue::String(credential_ref.into()),
        ),
        (
            "auth_type".to_string(),
            ConfigValue::String("basic_env".into()),
        ),
        (
            "username_env".to_string(),
            ConfigValue::String(username_env.into()),
        ),
        (
            "password_env".to_string(),
            ConfigValue::String(password_env.into()),
        ),
    ]))
    .expect("source credential config")
}

fn pg_fixture_batch_execute(endpoint_uri: &str, username: &str, password: &str, sql: &str) {
    std::thread::scope(|scope| {
        scope
            .spawn(move || {
                let mut pg_config =
                    PgConfig::from_str(endpoint_uri).expect("parse PostgreSQL endpoint");
                pg_config.user(username).password(password);
                let mut fixture = pg_config
                    .connect(NoTls)
                    .expect("connect PostgreSQL fixture");
                fixture
                    .batch_execute(sql)
                    .expect("execute PostgreSQL fixture SQL");
            })
            .join()
            .expect("PostgreSQL fixture thread");
    });
}

struct NativeCommitCase<'a> {
    object_ref: &'a str,
    source_kind: &'a str,
    schema_ref: &'a str,
    expected_revision_prefix: &'a str,
    operation: DiscoveryOperation,
    route: RouteKey,
}

async fn execute_normalize_and_commit<P, R, F>(case: NativeCommitCase<'_>, request: R, handler: F)
where
    P: DeserializeOwned + Serialize + SourcePayloadMetadata,
    R: Serialize,
    F: Fn(Vec<Event>) -> Vec<Event> + Send + Sync + 'static,
{
    let NativeCommitCase {
        object_ref,
        source_kind,
        schema_ref,
        expected_revision_prefix,
        operation,
        route,
    } = case;
    let now_ms = source_now_ms();
    let authority = SourceAuthorityRef(object_ref.to_string());
    let action_id = format!("action:native-source-to-view:{source_kind}");
    let mut engine = DataViewEngine::new(format!("data-view:native:{source_kind}"))
        .expect("create native DataView engine");
    register_builtin_normalizers(engine.normalizer_registry_mut()).expect("register normalizers");
    engine
        .register_action(BoundedSourceAction {
            wire_version: DATA_VIEW_WIRE_VERSION,
            action_id: action_id.clone(),
            source_authority_ref: authority.clone(),
            observation_path: ObservationPath::Direct,
            operation,
            permission_scope: format!("source:{object_ref}:read"),
            freshness_precondition: FreshnessState::Fresh,
            max_rows: 100,
            max_bytes: 1024 * 1024,
            max_calls: 3,
            max_fanout: 1,
            max_latency_ms: 20_000,
            max_cost_units: 3,
            side_effect_level: SideEffectLevel::ReadOnly,
            continuation_policy: ContinuationPolicy::None,
            failure_modes: vec![FailureMode::SourceUnavailable, FailureMode::BudgetExceeded],
            expansion_rules: Vec::new(),
            expected_information_gain: 90,
            explanation: "native source-to-DataView acceptance".into(),
        })
        .expect("register native source action");
    let context = engine
        .issue_action_context(
            &action_id,
            engine.head_ref(),
            ProjectionMode::CurrentActionable,
            "policy:native-source-acceptance",
            1,
            now_ms,
            now_ms + 20_000,
        )
        .expect("issue native source action context");
    let source_request = SourceExecutionEnvelope::new(
        SourceExecutionContext {
            request_id: context.execution_id.clone(),
            grant_epoch: context.grant_epoch,
            issued_at_ms: context.issued_at_ms,
            deadline_ms: context.expires_at_ms,
            limits: SourceExecutionLimits {
                max_rows: 100,
                max_bytes: 1024 * 1024,
                max_calls: 3,
                max_fanout: 1,
                max_latency_ms: 20_000,
                max_cost_units: 3,
            },
        },
        request,
    );
    let replies = ask_bound_route(
        route,
        encode_source_request(&source_request)
            .expect("encode native source request")
            .into(),
        Duration::from_secs(20),
        handler,
    )
    .await
    .expect("native source bound route");
    assert_eq!(replies.len(), 1);
    let submission = map_source_action_submission::<P>(
        SourceActionSubmissionContext {
            submission_id: format!("submission:native-source-to-view:{source_kind}"),
            execution: context,
            source_authority_ref: authority,
            producer_id: format!("{source_kind}-source:native"),
            schema: SchemaVersionRef {
                schema_ref: schema_ref.into(),
                schema_version: BUILTIN_SCHEMA_VERSION.into(),
            },
            policy_labels: BTreeSet::from(["native-acceptance".into()]),
            freshness: FreshnessState::Fresh,
            derivation: DerivationEvidence {
                derivation_kind: "source-observation".into(),
                rule_ref: None,
                dependency_refs: Vec::new(),
            },
        },
        source_kind,
        replies[0].payload_bytes().to_vec(),
    )
    .expect("normalize native source response");
    assert_eq!(submission.authority_object_ref, object_ref);
    assert!(
        submission
            .source_revision
            .starts_with(expected_revision_prefix),
        "unexpected {source_kind} revision {}",
        submission.source_revision
    );
    assert!(!submission.payload.is_empty());
    assert!(!submission.native_anchors.is_empty());
    assert!(!submission.evidence_refs.is_empty());
    assert!(submission.actual_calls > 0 && submission.actual_calls <= 3);

    let head_before = engine.head_ref();
    let outcome = engine.submit_action_result(
        submission,
        source_now_ms(),
        "policy:native-source-acceptance",
    );
    assert_eq!(outcome.status, DataViewCommitStatus::Accepted);
    assert_ne!(outcome.data_view, head_before);
    assert!(!engine.store().tracked_result_refs().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires disposable PostgreSQL through FUSTOR_PG_SOURCE_* env vars"]
async fn native_postgresql_bound_route_normalizes_and_commits_to_data_view() {
    let endpoint_uri = required_env("FUSTOR_PG_SOURCE_ENDPOINT_URI");
    let username = required_env("FUSTOR_PG_SOURCE_USERNAME");
    let password = required_env("FUSTOR_PG_SOURCE_PASSWORD");
    let schema = std::env::var("FUSTOR_PG_SOURCE_SCHEMA").unwrap_or_else(|_| "public".into());
    let table = format!("source_view_{}", Uuid::new_v4().simple());
    pg_fixture_batch_execute(
        &endpoint_uri,
        &username,
        &password,
        &format!(
            "CREATE TABLE \"{schema}\".\"{table}\" (id BIGINT PRIMARY KEY, payload TEXT NOT NULL); \
             INSERT INTO \"{schema}\".\"{table}\" (id, payload) VALUES (1, 'one'), (2, 'two')"
        ),
    );

    let credential_ref = "pg-native-view";
    let service = Arc::new(pg_source_runtime::PgSourceService::new(
        pg_source::PgSourceRuntimeConfig {
            product: pg_source::PgSourceConfig {
                endpoints: vec![pg_source::PgEndpointConfig {
                    object_ref: "pg-native-view".into(),
                    endpoint_uri: endpoint_uri.clone(),
                    credential_ref: Some(credential_ref.into()),
                    schema_scopes: vec![schema.clone()],
                    table_scopes: vec![table.clone()],
                    primary_key: Some("id".into()),
                    active: true,
                }],
                credentials: vec![basic_env_credential(
                    credential_ref,
                    "FUSTOR_PG_SOURCE_USERNAME",
                    "FUSTOR_PG_SOURCE_PASSWORD",
                )],
                default_page_size: 100,
                max_page_size: 100,
                ..pg_source::PgSourceConfig::default()
            },
            runtime: pg_source::PgSourceRuntimeInputs {
                endpoint_grants: vec![pg_source::GrantedPgEndpoint {
                    object_ref: "pg-native-view".into(),
                    endpoint_ref: None,
                    endpoint_uri: None,
                    credential_ref: Some(credential_ref.into()),
                    schema_scopes: Vec::new(),
                    table_scopes: Vec::new(),
                    interfaces: vec!["read".into()],
                    active: true,
                    grant_epoch: Some(1),
                }],
            },
        },
        Arc::new(pg_source_runtime::NativePgSourceDriver),
        NodeId("pg-native-view-node".into()),
    ));
    let mut request = pg_source::PgSourceRequest::snapshot_open("pg-native-view", &schema, &table);
    request.limit = Some(100);
    execute_normalize_and_commit::<pg_source::PgSourcePayload, _, _>(
        NativeCommitCase {
            object_ref: "pg-native-view",
            source_kind: "pg",
            schema_ref: PG_SCHEMA_REF,
            expected_revision_prefix: "pg-txid-snapshot:",
            operation: DiscoveryOperation::LimitedSample,
            route: pg_source::product_model::routes::fetch_request_route_for("pg-native-view-node"),
        },
        request,
        move |events| service.handle_events(events),
    )
    .await;

    pg_fixture_batch_execute(
        &endpoint_uri,
        &username,
        &password,
        &format!("DROP TABLE \"{schema}\".\"{table}\""),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires disposable MySQL through FUSTOR_MYSQL_SOURCE_* env vars"]
async fn native_mysql_bound_route_normalizes_and_commits_to_data_view() {
    let endpoint_uri = required_env("FUSTOR_MYSQL_SOURCE_ENDPOINT_URI");
    let username = required_env("FUSTOR_MYSQL_SOURCE_USERNAME");
    let password = required_env("FUSTOR_MYSQL_SOURCE_PASSWORD");
    let schema = required_env("FUSTOR_MYSQL_SOURCE_SCHEMA");
    let table = format!("source_view_{}", Uuid::new_v4().simple());
    let opts = Opts::from_url(&endpoint_uri).expect("parse MySQL endpoint");
    let mut fixture = Conn::new(
        OptsBuilder::from_opts(opts)
            .user(Some(username))
            .pass(Some(password)),
    )
    .expect("connect MySQL fixture");
    fixture
        .query_drop(format!(
            "CREATE TABLE `{schema}`.`{table}` (id BIGINT PRIMARY KEY, payload VARCHAR(64) NOT NULL)"
        ))
        .expect("create MySQL fixture");
    fixture
        .query_drop(format!(
            "INSERT INTO `{schema}`.`{table}` (id, payload) VALUES (1, 'one'), (2, 'two')"
        ))
        .expect("seed MySQL fixture");

    let credential_ref = "mysql-native-view";
    let service = Arc::new(mysql_source_runtime::MysqlSourceService::new(
        mysql_source::MysqlSourceRuntimeConfig {
            product: mysql_source::MysqlSourceConfig {
                endpoints: vec![mysql_source::MysqlEndpointConfig {
                    tls: None,
                    object_ref: "mysql-native-view".into(),
                    endpoint_uri,
                    credential_ref: Some(credential_ref.into()),
                    schema_scopes: vec![schema.clone()],
                    table_scopes: vec![table.clone()],
                    primary_key: Some("id".into()),
                    active: true,
                }],
                credentials: vec![basic_env_credential(
                    credential_ref,
                    "FUSTOR_MYSQL_SOURCE_USERNAME",
                    "FUSTOR_MYSQL_SOURCE_PASSWORD",
                )],
                default_page_size: 100,
                max_page_size: 100,
                ..mysql_source::MysqlSourceConfig::default()
            },
            runtime: mysql_source::MysqlSourceRuntimeInputs {
                endpoint_grants: vec![mysql_source::GrantedMysqlEndpoint {
                    object_ref: "mysql-native-view".into(),
                    endpoint_ref: None,
                    endpoint_uri: None,
                    credential_ref: Some(credential_ref.into()),
                    schema_scopes: Vec::new(),
                    table_scopes: Vec::new(),
                    interfaces: vec!["read".into()],
                    active: true,
                    grant_epoch: Some(1),
                }],
            },
        },
        Arc::new(mysql_source_runtime::NativeMysqlSourceDriver),
        NodeId("mysql-native-view-node".into()),
    ));
    let mut request =
        mysql_source::MysqlSourceRequest::snapshot_open("mysql-native-view", &schema, &table);
    request.limit = Some(100);
    execute_normalize_and_commit::<mysql_source::MysqlSourcePayload, _, _>(
        NativeCommitCase {
            object_ref: "mysql-native-view",
            source_kind: "mysql",
            schema_ref: MYSQL_SCHEMA_REF,
            expected_revision_prefix: "mysql-consistent-snapshot:",
            operation: DiscoveryOperation::LimitedSample,
            route: mysql_source::product_model::routes::fetch_request_route_for(
                "mysql-native-view-node",
            ),
        },
        request,
        move |events| service.handle_events(events),
    )
    .await;

    fixture
        .query_drop(format!("DROP TABLE `{schema}`.`{table}`"))
        .expect("drop MySQL fixture");
}

fn es_fixture_request(method: reqwest::Method, url: &str, body: Option<Value>) -> Value {
    std::thread::scope(|scope| {
        scope
            .spawn(move || {
                let client = reqwest::blocking::Client::builder()
                    .timeout(Duration::from_secs(20))
                    .build()
                    .expect("build Elasticsearch fixture client");
                let request = client.request(method, url);
                let response = match body {
                    Some(body) => request.json(&body),
                    None => request,
                }
                .send()
                .expect("Elasticsearch fixture request");
                let status = response.status();
                let text = response.text().expect("Elasticsearch fixture response");
                assert!(
                    status.is_success(),
                    "Elasticsearch status={status} body={text}"
                );
                if text.trim().is_empty() {
                    Value::Null
                } else {
                    serde_json::from_str(&text).expect("Elasticsearch fixture JSON")
                }
            })
            .join()
            .expect("Elasticsearch fixture thread")
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires disposable Elasticsearch through FUSTOR_ES_SOURCE_ENDPOINT_URI"]
async fn native_elasticsearch_bound_route_normalizes_and_commits_to_data_view() {
    let endpoint_uri = required_env("FUSTOR_ES_SOURCE_ENDPOINT_URI")
        .trim_end_matches('/')
        .to_string();
    let index = format!("source-view-{}", Uuid::new_v4().simple());
    es_fixture_request(
        reqwest::Method::PUT,
        &format!("{endpoint_uri}/{index}"),
        Some(json!({
            "mappings": {"properties": {
                "@timestamp": {"type": "date"},
                "tie": {"type": "keyword"},
                "message": {"type": "text"}
            }}
        })),
    );
    for (id, tie) in [("1", "a"), ("2", "b")] {
        es_fixture_request(
            reqwest::Method::PUT,
            &format!("{endpoint_uri}/{index}/_doc/{id}?refresh=true"),
            Some(json!({
                "@timestamp": "2026-07-18T00:00:00Z",
                "tie": tie,
                "message": id
            })),
        );
    }

    let service = Arc::new(es_source_runtime::EsSourceService::new(
        es_source::EsSourceRuntimeConfig {
            product: es_source::EsSourceConfig {
                endpoints: vec![es_source::EsEndpointConfig {
                    object_ref: "es-native-view".into(),
                    endpoint_uri: endpoint_uri.clone(),
                    credential_ref: None,
                    index_scopes: vec![index.clone()],
                    timestamp_field: "@timestamp".into(),
                    tie_breaker_field: "tie".into(),
                    active: true,
                }],
                default_page_size: 100,
                max_page_size: 100,
                ..es_source::EsSourceConfig::default()
            },
            runtime: es_source::EsSourceRuntimeInputs {
                endpoint_grants: vec![es_source::GrantedEsEndpoint {
                    object_ref: "es-native-view".into(),
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
        Arc::new(es_source_runtime::HttpEsSourceDriver::default()),
        NodeId("es-native-view-node".into()),
    ));
    let mut request = es_source::EsSourceRequest::snapshot_page("es-native-view", &index);
    request.limit = Some(100);
    execute_normalize_and_commit::<es_source::EsSourcePayload, _, _>(
        NativeCommitCase {
            object_ref: "es-native-view",
            source_kind: "elasticsearch",
            schema_ref: ES_SCHEMA_REF,
            expected_revision_prefix: "es-pit:",
            operation: DiscoveryOperation::LimitedSample,
            route: es_source::product_model::routes::fetch_request_route_for("es-native-view-node"),
        },
        request,
        move |events| service.handle_events(events),
    )
    .await;

    es_fixture_request(
        reqwest::Method::DELETE,
        &format!("{endpoint_uri}/{index}"),
        None,
    );
}

struct S3TestEnv {
    endpoint_uri: String,
    access_key: String,
    secret_key: String,
    bucket: String,
    region: String,
}

impl S3TestEnv {
    fn from_env() -> Self {
        Self {
            endpoint_uri: required_env("FUSTOR_S3_SOURCE_ENDPOINT_URI"),
            access_key: required_env("FUSTOR_S3_SOURCE_ACCESS_KEY"),
            secret_key: required_env("FUSTOR_S3_SOURCE_SECRET_KEY"),
            bucket: required_env("FUSTOR_S3_SOURCE_BUCKET"),
            region: std::env::var("FUSTOR_S3_SOURCE_REGION").unwrap_or_else(|_| "us-east-1".into()),
        }
    }

    fn client(&self) -> S3Client {
        S3Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
                .region(Region::new(self.region.clone()))
                .endpoint_url(self.endpoint_uri.clone())
                .force_path_style(true)
                .credentials_provider(Credentials::new(
                    self.access_key.clone(),
                    self.secret_key.clone(),
                    None,
                    None,
                    "native-source-to-data-view",
                ))
                .build(),
        )
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires disposable S3 through FUSTOR_S3_SOURCE_* env vars"]
async fn native_s3_bound_route_normalizes_and_commits_to_data_view() {
    let env = S3TestEnv::from_env();
    let client = env.client();
    let _ = client.create_bucket().bucket(&env.bucket).send().await;
    let prefix = format!("source-view-{}/", Uuid::new_v4().simple());
    let key = format!("{prefix}item.json");
    client
        .put_object()
        .bucket(&env.bucket)
        .key(&key)
        .body(ByteStream::from_static(b"{\"id\":1}"))
        .send()
        .await
        .expect("seed S3 fixture");

    let credential_ref = "s3-native-view";
    let service = Arc::new(s3_source_runtime::S3SourceService::new(
        s3_source::S3SourceRuntimeConfig {
            product: s3_source::S3SourceConfig {
                endpoints: vec![s3_source::S3EndpointConfig {
                    object_ref: "s3-native-view".into(),
                    endpoint_uri: env.endpoint_uri.clone(),
                    bucket: env.bucket.clone(),
                    region: Some(env.region.clone()),
                    prefix: prefix.clone(),
                    credential_ref: Some(credential_ref.into()),
                    bucket_scopes: vec![env.bucket.clone()],
                    prefix_scopes: vec![format!("{prefix}*")],
                    active: true,
                }],
                credentials: vec![basic_env_credential(
                    credential_ref,
                    "FUSTOR_S3_SOURCE_ACCESS_KEY",
                    "FUSTOR_S3_SOURCE_SECRET_KEY",
                )],
                default_page_size: 100,
                max_page_size: 100,
            },
            runtime: s3_source::S3SourceRuntimeInputs {
                endpoint_grants: vec![s3_source::GrantedS3Endpoint {
                    object_ref: "s3-native-view".into(),
                    endpoint_ref: None,
                    endpoint_uri: None,
                    bucket: None,
                    credential_ref: Some(credential_ref.into()),
                    bucket_scopes: Vec::new(),
                    prefix_scopes: Vec::new(),
                    interfaces: vec!["read".into()],
                    active: true,
                    grant_epoch: Some(1),
                }],
            },
        },
        Arc::new(s3_source_runtime::NativeS3SourceDriver),
        NodeId("s3-native-view-node".into()),
    ));
    let mut request = s3_source::S3SourceRequest::snapshot_page("s3-native-view");
    request.limit = Some(100);
    execute_normalize_and_commit::<s3_source::S3SourcePayload, _, _>(
        NativeCommitCase {
            object_ref: "s3-native-view",
            source_kind: "s3",
            schema_ref: S3_SCHEMA_REF,
            expected_revision_prefix: "s3-list-observation:",
            operation: DiscoveryOperation::ObjectList,
            route: s3_source::product_model::routes::fetch_request_route_for("s3-native-view-node"),
        },
        request,
        move |events| service.handle_events(events),
    )
    .await;

    client
        .delete_object()
        .bucket(&env.bucket)
        .key(key)
        .send()
        .await
        .expect("delete S3 fixture");
}

use std::env;
use std::sync::Arc;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use capanix_app_sdk::runtime::{ConfigValue, NodeId};
use mysql::prelude::Queryable;
use mysql::{Conn, Opts, OptsBuilder};
use mysql_source::product_model::routes::fetch_request_route_for;
use mysql_source::shared_types::{
    MysqlRowEvent, MysqlSnapshotCursor, MysqlSourceCursor, MysqlSourceOperation,
    MysqlSourcePayload, MysqlSourceRequest,
};
use mysql_source::{
    GrantedMysqlEndpoint, MysqlEndpointConfig, MysqlSourceConfig, MysqlSourceRuntimeConfig,
    MysqlSourceRuntimeInputs, ResolvedCredential, ResolvedMysqlEndpoint,
};
use mysql_source_runtime::{MysqlSourceDriver, MysqlSourceService, NativeMysqlSourceDriver};
use serde_json::json;
use source_kit::{
    DecodedSourceResponse, SourceExecutionContext, SourceExecutionEnvelope, SourceExecutionLimits,
    decode_source_response, encode_source_request, source_now_ms,
};
use source_test_kit::ask_bound_route;
use url::Url;

struct MysqlTestEnv {
    endpoint_uri: String,
    username: String,
    password: String,
    schema: String,
}

impl MysqlTestEnv {
    fn from_env() -> Self {
        Self {
            endpoint_uri: required_env("FUSTOR_MYSQL_SOURCE_ENDPOINT_URI"),
            username: required_env("FUSTOR_MYSQL_SOURCE_USERNAME"),
            password: required_env("FUSTOR_MYSQL_SOURCE_PASSWORD"),
            schema: env::var("FUSTOR_MYSQL_SOURCE_SCHEMA")
                .unwrap_or_else(|_| format!("fustor_source_it_{}", unique_suffix())),
        }
    }

    fn credential(&self) -> ResolvedCredential {
        ResolvedCredential::Basic {
            username: self.username.clone(),
            password: self.password.clone(),
        }
    }

    fn endpoint(&self, primary_key: Option<&str>) -> ResolvedMysqlEndpoint {
ResolvedMysqlEndpoint {
            tls: None,
            object_ref: "mysql-it".into(),
            endpoint_uri: self.endpoint_uri.clone(),
            credential_ref: None,
            schema_scopes: vec![self.schema.clone()],
            table_scopes: vec!["it_*".into()],
            primary_key: primary_key.map(str::to_string),
            grant_epoch: Some(1),
        }
    }
}

fn required_env(key: &str) -> String {
    env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| panic!("missing required native MySQL test env {key}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires FUSTOR_MYSQL_SOURCE_ENDPOINT_URI, FUSTOR_MYSQL_SOURCE_USERNAME, FUSTOR_MYSQL_SOURCE_PASSWORD"]
async fn native_mysql_bound_route_uses_one_consistent_snapshot_transaction() {
    let env = MysqlTestEnv::from_env();
    let mut conn = setup_database(&env);
    let table = format!("it_keyset_{}", unique_suffix());
    conn.query_drop(format!(
        "CREATE TABLE `{}`.`{}` (id BIGINT PRIMARY KEY, name VARCHAR(64) NOT NULL)",
        env.schema, table
    ))
    .expect("create table");
    conn.exec_drop(
        format!(
            "INSERT INTO `{}`.`{}` (id, name) VALUES (?, ?), (?, ?), (?, ?)",
            env.schema, table
        ),
        (1_i64, "one", 2_i64, "two", 3_i64, "three"),
    )
    .expect("insert rows");

    let service = Arc::new(native_service(&env, &table, None));
    let (payload, receipt) = ask_source(
        service.clone(),
        "mysql-native-probe",
        MysqlSourceRequest {
            object_ref: "mysql-it".into(),
            schema: None,
            table: None,
            operation: MysqlSourceOperation::ProbeConnection,
            limit: None,
            cursor: None,
            field_filter: Vec::new(),
        },
    )
    .await;
    assert_eq!(receipt.actual.calls, 1);
    assert!(matches!(payload, MysqlSourcePayload::Probe(status) if status.reachable));

    let (payload, receipt) = ask_source(
        service.clone(),
        "mysql-native-fields",
        MysqlSourceRequest {
            object_ref: "mysql-it".into(),
            schema: None,
            table: None,
            operation: MysqlSourceOperation::DiscoverFields,
            limit: None,
            cursor: None,
            field_filter: Vec::new(),
        },
    )
    .await;
    assert_eq!(receipt.actual.calls, 1);
    assert!(
        matches!(payload, MysqlSourcePayload::Fields(catalog) if catalog.fields.iter().any(|field| field.table == table && field.column == "id"))
    );

    let mut open = MysqlSourceRequest::snapshot_open("mysql-it", &env.schema, &table);
    open.limit = Some(2);
    let (payload, receipt) = ask_source(service.clone(), "mysql-native-open", open).await;
    assert_eq!(receipt.actual.rows, 2);
    assert_eq!(receipt.actual.calls, 2);
    assert_eq!(receipt.actual.cost_units, 2);
    assert!(
        receipt
            .source_revision
            .starts_with("mysql-consistent-snapshot:")
    );
    let snapshot_source_revision = receipt.source_revision.clone();
    let (mut ids, cursor) = match payload {
        MysqlSourcePayload::Rows(batch) => {
            assert_eq!(batch.source_revision, snapshot_source_revision);
            let ids = batch.rows.iter().map(row_id).collect::<Vec<_>>();
            let MysqlSourceCursor::Snapshot(cursor) = batch.next_cursor.expect("snapshot cursor");
            (ids, cursor)
        }
        other => panic!("expected MySQL rows, got {other:?}"),
    };
    conn.exec_drop(
        format!(
            "INSERT INTO `{}`.`{}` (id, name) VALUES (?, ?)",
            env.schema, table
        ),
        (4_i64, "late"),
    )
    .expect("insert row after snapshot opened");

    let page = MysqlSourceRequest {
        object_ref: "mysql-it".into(),
        schema: Some(env.schema.clone()),
        table: Some(table.clone()),
        operation: MysqlSourceOperation::SnapshotPage,
        limit: Some(2),
        cursor: Some(MysqlSourceCursor::Snapshot(MysqlSnapshotCursor {
            ..cursor
        })),
        field_filter: Vec::new(),
    };
    let (payload, receipt) = ask_source(service, "mysql-native-page", page).await;
    assert_eq!(receipt.actual.rows, 1);
    assert_eq!(receipt.actual.calls, 2);
    assert_eq!(receipt.actual.cost_units, 2);
    assert_eq!(receipt.source_revision, snapshot_source_revision);
    match payload {
        MysqlSourcePayload::Rows(batch) => {
            assert_eq!(batch.source_revision, snapshot_source_revision);
            ids.extend(batch.rows.iter().map(row_id));
            assert!(!batch.has_more);
            assert!(batch.next_cursor.is_none());
        }
        other => panic!("expected MySQL continuation rows, got {other:?}"),
    }
    assert_eq!(ids, vec![1, 2, 3]);
}

#[test]
#[ignore = "requires FUSTOR_MYSQL_SOURCE_ENDPOINT_URI, FUSTOR_MYSQL_SOURCE_USERNAME, FUSTOR_MYSQL_SOURCE_PASSWORD"]
fn native_mysql_snapshot_hides_internally_added_primary_key_from_filtered_output() {
    let env = MysqlTestEnv::from_env();
    let mut conn = setup_database(&env);
    let table = format!("it_filter_{}", unique_suffix());
    conn.query_drop(format!(
        "CREATE TABLE `{}`.`{}` (id BIGINT PRIMARY KEY, name VARCHAR(64) NOT NULL)",
        env.schema, table
    ))
    .expect("create table");
    conn.exec_drop(
        format!(
            "INSERT INTO `{}`.`{}` (id, name) VALUES (?, ?)",
            env.schema, table
        ),
        (7_i64, "seven"),
    )
    .expect("insert row");

    let driver = NativeMysqlSourceDriver;
    let mut session = driver
        .open_snapshot(
            &env.endpoint(Some("id")),
            &env.credential(),
            &env.schema,
            &table,
            &["name".into()],
        )
        .expect("open snapshot");
    let page = session.fetch_page(1, None).expect("page");

    assert_eq!(page.fields, vec!["name"]);
    assert_eq!(page.rows[0].values["name"], json!("seven"));
    assert!(page.rows[0].values.get("id").is_none());
    assert_eq!(page.last_key, Some(json!(7)));
    assert!(
        page.source_revision
            .starts_with("mysql-consistent-snapshot:")
    );
    session.close().expect("close snapshot");
}

#[test]
#[ignore = "requires FUSTOR_MYSQL_SOURCE_ENDPOINT_URI, FUSTOR_MYSQL_SOURCE_USERNAME, FUSTOR_MYSQL_SOURCE_PASSWORD"]
fn native_mysql_snapshot_requires_configured_or_single_column_primary_key() {
    let env = MysqlTestEnv::from_env();
    let mut conn = setup_database(&env);
    let no_pk = format!("it_no_pk_{}", unique_suffix());
    let composite_pk = format!("it_composite_pk_{}", unique_suffix());
    conn.query_drop(format!(
        "CREATE TABLE `{}`.`{}` (id BIGINT NOT NULL, name VARCHAR(64) NOT NULL)",
        env.schema, no_pk
    ))
    .expect("create no-pk table");
    conn.query_drop(format!(
        "CREATE TABLE `{}`.`{}` (id BIGINT NOT NULL, shard BIGINT NOT NULL, PRIMARY KEY (id, shard))",
        env.schema, composite_pk
    ))
    .expect("create composite-pk table");

    let driver = NativeMysqlSourceDriver;
    let no_pk_err = match driver.open_snapshot(
        &env.endpoint(None),
        &env.credential(),
        &env.schema,
        &no_pk,
        &[],
    ) {
        Ok(_) => panic!("no primary key should fail"),
        Err(err) => err,
    };
    assert!(no_pk_err.to_string().contains("primary_key"));

    let composite_err = match driver.open_snapshot(
        &env.endpoint(None),
        &env.credential(),
        &env.schema,
        &composite_pk,
        &[],
    ) {
        Ok(_) => panic!("composite primary key should fail"),
        Err(err) => err,
    };
    assert!(composite_err.to_string().contains("composite primary keys"));
}

fn native_service(
    env: &MysqlTestEnv,
    table: &str,
    primary_key: Option<&str>,
) -> MysqlSourceService {
    let credential_ref = "mysql-native".to_string();
    let credential = source_kit::parse_credential_config(&std::collections::HashMap::from([
        (
            "credential_ref".to_string(),
            ConfigValue::String(credential_ref.clone()),
        ),
        (
            "auth_type".to_string(),
            ConfigValue::String("basic_env".into()),
        ),
        (
            "username_env".to_string(),
            ConfigValue::String("FUSTOR_MYSQL_SOURCE_USERNAME".into()),
        ),
        (
            "password_env".to_string(),
            ConfigValue::String("FUSTOR_MYSQL_SOURCE_PASSWORD".into()),
        ),
    ]))
    .expect("MySQL credential config");
    MysqlSourceService::new(
        MysqlSourceRuntimeConfig {
            product: MysqlSourceConfig {
                endpoints: vec![MysqlEndpointConfig {
                    tls: None,
                    object_ref: "mysql-it".into(),
                    endpoint_uri: env.endpoint_uri.clone(),
                    credential_ref: Some(credential_ref.clone()),
                    schema_scopes: vec![env.schema.clone()],
                    table_scopes: vec![table.to_string()],
                    primary_key: primary_key.map(str::to_string),
                    active: true,
                }],
                credentials: vec![credential],
                default_page_size: 2,
                max_page_size: 100,
                ..MysqlSourceConfig::default()
            },
            runtime: MysqlSourceRuntimeInputs {
                endpoint_grants: vec![GrantedMysqlEndpoint {
                    object_ref: "mysql-it".into(),
                    endpoint_ref: None,
                    endpoint_uri: None,
                    credential_ref: Some(credential_ref),
                    schema_scopes: Vec::new(),
                    table_scopes: Vec::new(),
                    interfaces: vec!["read".into()],
                    active: true,
                    grant_epoch: Some(1),
                }],
            },
        },
        Arc::new(NativeMysqlSourceDriver),
        NodeId("mysql-source-native-test".into()),
    )
}

async fn ask_source(
    service: Arc<MysqlSourceService>,
    request_id: &str,
    request: MysqlSourceRequest,
) -> (MysqlSourcePayload, source_kit::SourceObservationReceipt) {
    let now_ms = source_now_ms();
    let envelope = SourceExecutionEnvelope::new(
        SourceExecutionContext {
            request_id: request_id.into(),
            grant_epoch: 1,
            issued_at_ms: now_ms,
            deadline_ms: now_ms + 10_000,
            limits: SourceExecutionLimits {
                max_rows: 100,
                max_bytes: 1024 * 1024,
                max_calls: 3,
                max_fanout: 1,
                max_latency_ms: 10_000,
                max_cost_units: 3,
            },
        },
        request,
    );
    let replies = ask_bound_route(
        fetch_request_route_for("mysql-native-node"),
        encode_source_request(&envelope)
            .expect("encode MySQL source request")
            .into(),
        Duration::from_secs(10),
        move |events| service.handle_events(events),
    )
    .await
    .expect("MySQL bound route");
    let DecodedSourceResponse::Envelope(response) =
        decode_source_response::<MysqlSourcePayload>(replies[0].payload_bytes())
            .expect("decode MySQL source response")
    else {
        panic!("native MySQL request returned a legacy response")
    };
    assert_eq!(response.request_id, request_id);
    let receipt = response.receipt.expect("MySQL source receipt");
    assert_eq!(receipt.source_kind, "mysql");
    assert_eq!(receipt.grant_epoch, 1);
    assert_eq!(
        receipt.actual.bytes,
        replies[0].payload_bytes().len() as u64
    );
    (response.payload, receipt)
}

fn setup_database(env: &MysqlTestEnv) -> Conn {
    let mut conn = open_admin_connection(env);
    conn.query_drop(format!("CREATE DATABASE IF NOT EXISTS `{}`", env.schema))
        .expect("create schema");
    conn
}

fn open_admin_connection(env: &MysqlTestEnv) -> Conn {
    let url = Url::parse(&env.endpoint_uri).expect("parse endpoint uri");
    let host = url.host_str().expect("endpoint host");
    let port = url.port().unwrap_or(3306);
    let builder = OptsBuilder::new()
        .ip_or_hostname(Some(host.to_string()))
        .tcp_port(port)
        .user(Some(env.username.clone()))
        .pass(Some(env.password.clone()));
    Conn::new(Opts::from(builder)).expect("connect mysql")
}

fn row_id(row: &MysqlRowEvent) -> i64 {
    row.values["id"].as_i64().expect("row id")
}

fn unique_suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_micros()
}

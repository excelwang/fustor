use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use capanix_app_sdk::runtime::ConfigValue;
use pg_source::product_model::routes::fetch_request_route_for;
use pg_source::shared_types::{
    PgSnapshotCursor, PgSourceCursor, PgSourceOperation, PgSourcePayload, PgSourceRequest,
};
use pg_source::{
    GrantedPgEndpoint, PgEndpointConfig, PgSourceConfig, PgSourceRuntimeConfig,
    PgSourceRuntimeInputs,
};
use pg_source_runtime::{NativePgSourceDriver, PgSourceService};
use postgres::{Client, Config, NoTls};
use source_kit::{
    DecodedSourceResponse, SourceExecutionContext, SourceExecutionEnvelope, SourceExecutionLimits,
    decode_source_response, encode_source_request, source_now_ms,
};
use source_test_kit::ask_bound_route;
use uuid::Uuid;

fn required_env(key: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| panic!("missing required native PostgreSQL test env {key}"))
}

struct FixtureClient {
    client: Option<Client>,
}

impl FixtureClient {
    fn batch_execute(&mut self, sql: &str) -> Result<(), postgres::Error> {
        let client = self.client.as_mut().expect("fixture client");
        run_fixture(|| client.batch_execute(sql))
    }

    fn execute(
        &mut self,
        sql: &str,
        params: &[&(dyn postgres::types::ToSql + Sync)],
    ) -> Result<u64, postgres::Error> {
        let client = self.client.as_mut().expect("fixture client");
        run_fixture(|| client.execute(sql, params))
    }
}

impl Drop for FixtureClient {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            run_fixture(|| drop(client));
        }
    }
}

fn direct_client(
    endpoint_uri: &str,
    username: Option<&str>,
    password: Option<&str>,
) -> FixtureClient {
    let normalized = endpoint_uri
        .strip_prefix("pg://")
        .map(|rest| format!("postgresql://{rest}"))
        .unwrap_or_else(|| endpoint_uri.to_string());
    let mut config = Config::from_str(&normalized).expect("parse PostgreSQL test endpoint");
    if let Some(username) = username {
        config.user(username);
    }
    if let Some(password) = password {
        config.password(password);
    }
    FixtureClient {
        client: Some(run_fixture(|| {
            config.connect(NoTls).expect("connect PostgreSQL fixture")
        })),
    }
}

fn run_fixture<T, F>(operation: F) -> T
where
    T: Send,
    F: FnOnce() -> T + Send,
{
    std::thread::scope(|scope| {
        scope
            .spawn(operation)
            .join()
            .expect("PostgreSQL fixture thread")
    })
}

async fn ask_source(
    service: Arc<PgSourceService>,
    request_id: &str,
    request: PgSourceRequest,
) -> (PgSourcePayload, source_kit::SourceObservationReceipt) {
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
    let route = fetch_request_route_for("pg-native-node");
    let replies = ask_bound_route(
        route,
        encode_source_request(&envelope)
            .expect("encode PostgreSQL source request")
            .into(),
        Duration::from_secs(10),
        move |events| service.handle_events(events),
    )
    .await
    .expect("PostgreSQL bound route");
    let DecodedSourceResponse::Envelope(response) =
        decode_source_response::<PgSourcePayload>(replies[0].payload_bytes())
            .expect("decode PostgreSQL source response")
    else {
        panic!("native PostgreSQL request returned a legacy response")
    };
    assert_eq!(response.request_id, request_id);
    let receipt = response.receipt.unwrap_or_else(|| {
        panic!(
            "PostgreSQL source response has no receipt: {:?}",
            response.payload
        )
    });
    assert_eq!(receipt.source_kind, "pg");
    assert_eq!(receipt.grant_epoch, 1);
    assert_eq!(
        receipt.actual.bytes,
        replies[0].payload_bytes().len() as u64
    );
    (response.payload, receipt)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a disposable PostgreSQL database configured through FUSTOR_PG_SOURCE_* env vars"]
async fn native_pg_bound_route_uses_one_repeatable_read_snapshot_transaction() {
    let endpoint_uri = required_env("FUSTOR_PG_SOURCE_ENDPOINT_URI");
    let username = std::env::var("FUSTOR_PG_SOURCE_USERNAME").ok();
    let password = std::env::var("FUSTOR_PG_SOURCE_PASSWORD").ok();
    let schema = std::env::var("FUSTOR_PG_SOURCE_SCHEMA").unwrap_or_else(|_| "public".into());
    let table = format!("source_acceptance_{}", Uuid::new_v4().simple());
    let mut fixture = direct_client(&endpoint_uri, username.as_deref(), password.as_deref());
    fixture
        .batch_execute(&format!(
            "CREATE TABLE \"{schema}\".\"{table}\" (id BIGINT PRIMARY KEY, payload TEXT NOT NULL); \
             INSERT INTO \"{schema}\".\"{table}\" (id, payload) VALUES \
             (1, 'one'), (2, 'two'), (3, 'three')"
        ))
        .expect("create PostgreSQL fixture");

    let credential_ref = username.as_ref().map(|_| "pg-native".to_string());
    let mut config = PgSourceRuntimeConfig {
        product: PgSourceConfig {
            endpoints: vec![PgEndpointConfig {
                object_ref: "pg-native".into(),
                endpoint_uri: endpoint_uri.clone(),
                credential_ref: credential_ref.clone(),
                schema_scopes: vec![schema.clone()],
                table_scopes: vec![table.clone()],
                primary_key: None,
                active: true,
            }],
            default_page_size: 2,
            max_page_size: 100,
            ..PgSourceConfig::default()
        },
        runtime: PgSourceRuntimeInputs {
            endpoint_grants: vec![GrantedPgEndpoint {
                object_ref: "pg-native".into(),
                endpoint_ref: None,
                endpoint_uri: None,
                credential_ref,
                schema_scopes: Vec::new(),
                table_scopes: Vec::new(),
                interfaces: vec!["read".into()],
                active: true,
                grant_epoch: Some(1),
            }],
        },
    };
    if config.product.endpoints[0].credential_ref.is_some() {
        config.product.credentials.push(
            source_kit::parse_credential_config(&std::collections::HashMap::from([
                (
                    "credential_ref".to_string(),
                    ConfigValue::String("pg-native".into()),
                ),
                (
                    "auth_type".to_string(),
                    ConfigValue::String("basic_env".into()),
                ),
                (
                    "username_env".to_string(),
                    ConfigValue::String("FUSTOR_PG_SOURCE_USERNAME".into()),
                ),
                (
                    "password_env".to_string(),
                    ConfigValue::String("FUSTOR_PG_SOURCE_PASSWORD".into()),
                ),
            ]))
            .expect("credential config"),
        );
    }
    let service = Arc::new(PgSourceService::new(
        config,
        Arc::new(NativePgSourceDriver),
        capanix_app_sdk::runtime::NodeId("pg-source-native-test".into()),
    ));

    let (payload, receipt) = ask_source(
        service.clone(),
        "pg-native-probe",
        PgSourceRequest {
            object_ref: "pg-native".into(),
            schema: None,
            table: None,
            operation: PgSourceOperation::ProbeConnection,
            limit: None,
            cursor: None,
            field_filter: Vec::new(),
        },
    )
    .await;
    assert_eq!(receipt.actual.calls, 1);
    assert!(matches!(payload, PgSourcePayload::Probe(status) if status.reachable));

    let (payload, receipt) = ask_source(
        service.clone(),
        "pg-native-fields",
        PgSourceRequest {
            object_ref: "pg-native".into(),
            schema: None,
            table: None,
            operation: PgSourceOperation::DiscoverFields,
            limit: None,
            cursor: None,
            field_filter: Vec::new(),
        },
    )
    .await;
    assert_eq!(receipt.actual.calls, 1);
    let fields = match payload {
        PgSourcePayload::Fields(catalog) => catalog.fields,
        other => panic!("expected PostgreSQL fields, got {other:?}"),
    };
    assert!(
        fields
            .iter()
            .any(|field| field.table == table && field.column == "id")
    );
    assert!(
        fields
            .iter()
            .any(|field| field.table == table && field.column == "payload")
    );

    let mut open = PgSourceRequest::snapshot_open("pg-native", &schema, &table);
    open.limit = Some(2);
    let (payload, receipt) = ask_source(service.clone(), "pg-native-open", open).await;
    assert_eq!(receipt.actual.rows, 2);
    assert_eq!(receipt.actual.calls, 2);
    assert_eq!(receipt.actual.cost_units, 2);
    assert!(receipt.source_revision.starts_with("pg-txid-snapshot:"));
    let snapshot_source_revision = receipt.source_revision.clone();
    let (mut ids, cursor) = match payload {
        PgSourcePayload::Rows(batch) => {
            assert_eq!(batch.source_revision, snapshot_source_revision);
            let ids = batch
                .rows
                .iter()
                .map(|row| row.values["id"].as_i64().expect("row id"))
                .collect::<Vec<_>>();
            let PgSourceCursor::Snapshot(cursor) = batch.next_cursor.expect("snapshot cursor");
            (ids, cursor)
        }
        other => panic!("expected PostgreSQL rows, got {other:?}"),
    };

    fixture
        .execute(
            &format!("INSERT INTO \"{schema}\".\"{table}\" (id, payload) VALUES (4, 'late')"),
            &[],
        )
        .expect("insert row after snapshot opened");

    let page = PgSourceRequest {
        object_ref: "pg-native".into(),
        schema: Some(schema.clone()),
        table: Some(table.clone()),
        operation: PgSourceOperation::SnapshotPage,
        limit: Some(2),
        cursor: Some(PgSourceCursor::Snapshot(PgSnapshotCursor { ..cursor })),
        field_filter: Vec::new(),
    };
    let (payload, receipt) = ask_source(service, "pg-native-page", page).await;
    assert_eq!(receipt.actual.rows, 1);
    assert_eq!(receipt.actual.calls, 2);
    assert_eq!(receipt.actual.cost_units, 2);
    assert_eq!(receipt.source_revision, snapshot_source_revision);
    match payload {
        PgSourcePayload::Rows(batch) => {
            assert_eq!(batch.source_revision, snapshot_source_revision);
            ids.extend(
                batch
                    .rows
                    .iter()
                    .map(|row| row.values["id"].as_i64().expect("row id")),
            );
            assert!(!batch.has_more);
            assert!(batch.next_cursor.is_none());
        }
        other => panic!("expected PostgreSQL continuation rows, got {other:?}"),
    }
    assert_eq!(ids, vec![1, 2, 3]);

    fixture
        .batch_execute(&format!("DROP TABLE \"{schema}\".\"{table}\""))
        .expect("drop PostgreSQL fixture");
}

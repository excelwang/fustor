use std::sync::Arc;
use std::time::Duration;

use capanix_app_sdk::runtime::NodeId;
use es_source::product_model::routes::fetch_request_route_for;
use es_source::shared_types::{
    EsSnapshotCursor, EsSourceCursor, EsSourceOperation, EsSourcePayload, EsSourceRequest,
};
use es_source::{
    EsEndpointConfig, EsSourceConfig, EsSourceRuntimeConfig, EsSourceRuntimeInputs,
    GrantedEsEndpoint,
};
use es_source_runtime::{EsSourceService, HttpEsSourceDriver};
use serde_json::{Value, json};
use source_kit::{
    DecodedSourceResponse, SourceExecutionContext, SourceExecutionEnvelope, SourceExecutionLimits,
    SourceResponseEnvelope, decode_source_response, encode_source_request, source_now_ms,
};
use source_test_kit::ask_bound_route;
use uuid::Uuid;

fn required_env(key: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| panic!("missing required native Elasticsearch test env {key}"))
}

async fn ask_source(
    service: Arc<EsSourceService>,
    request_id: &str,
    request: EsSourceRequest,
) -> SourceResponseEnvelope<EsSourcePayload> {
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
        fetch_request_route_for("es-native-node"),
        encode_source_request(&envelope)
            .expect("encode Elasticsearch source request")
            .into(),
        Duration::from_secs(10),
        move |events| service.handle_events(events),
    )
    .await
    .expect("Elasticsearch bound route");
    let DecodedSourceResponse::Envelope(response) =
        decode_source_response::<EsSourcePayload>(replies[0].payload_bytes())
            .expect("decode Elasticsearch source response")
    else {
        panic!("native Elasticsearch request returned a legacy response")
    };
    assert_eq!(response.request_id, request_id);
    if let Some(receipt) = response.receipt.as_ref() {
        assert_eq!(receipt.source_kind, "elasticsearch");
        assert_eq!(receipt.grant_epoch, 1);
        assert_eq!(
            receipt.actual.bytes,
            replies[0].payload_bytes().len() as u64
        );
    }
    *response
}

fn request_json(method: reqwest::Method, url: &str, body: Option<Value>) -> Value {
    std::thread::scope(|scope| {
        scope
            .spawn(move || {
                let client = reqwest::blocking::Client::builder()
                    .timeout(Duration::from_secs(10))
                    .build()
                    .expect("build Elasticsearch fixture client");
                let request = client.request(method, url);
                let request = match body {
                    Some(body) => request.json(&body),
                    None => request,
                };
                let response = request.send().expect("Elasticsearch fixture request");
                let status = response.status();
                let text = response.text().expect("Elasticsearch fixture response");
                assert!(
                    status.is_success(),
                    "Elasticsearch fixture status={status} body={text}"
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
#[ignore = "requires a disposable Elasticsearch service configured through FUSTOR_ES_SOURCE_ENDPOINT_URI"]
async fn native_es_bound_route_covers_mapping_pit_close_and_ordered_poll() {
    let endpoint_uri = required_env("FUSTOR_ES_SOURCE_ENDPOINT_URI")
        .trim_end_matches('/')
        .to_string();
    let index = format!("source-acceptance-{}", Uuid::new_v4().simple());
    request_json(
        reqwest::Method::PUT,
        &format!("{endpoint_uri}/{index}"),
        Some(json!({
            "mappings": {
                "properties": {
                    "@timestamp": {"type": "date"},
                    "tie": {"type": "keyword"},
                    "message": {"type": "text"}
                }
            }
        })),
    );
    for (id, timestamp, tie) in [
        ("1", "2026-07-16T00:00:00Z", "a"),
        ("2", "2026-07-16T00:00:00Z", "b"),
        ("3", "2026-07-16T00:01:00Z", "c"),
    ] {
        request_json(
            reqwest::Method::PUT,
            &format!("{endpoint_uri}/{index}/_doc/{id}?refresh=true"),
            Some(json!({"@timestamp": timestamp, "tie": tie, "message": id})),
        );
    }

    let service = Arc::new(EsSourceService::new(
        EsSourceRuntimeConfig {
            product: EsSourceConfig {
                endpoints: vec![EsEndpointConfig {
                    object_ref: "es-native".into(),
                    endpoint_uri: endpoint_uri.clone(),
                    credential_ref: None,
                    index_scopes: vec![index.clone()],
                    timestamp_field: "@timestamp".into(),
                    tie_breaker_field: "tie".into(),
                    active: true,
                }],
                default_page_size: 2,
                max_page_size: 100,
                ..EsSourceConfig::default()
            },
            runtime: EsSourceRuntimeInputs {
                endpoint_grants: vec![GrantedEsEndpoint {
                    object_ref: "es-native".into(),
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
        Arc::new(HttpEsSourceDriver::default()),
        NodeId("es-source-native-test".into()),
    ));

    let mut probe = EsSourceRequest::snapshot_page("es-native", &index);
    probe.operation = EsSourceOperation::ProbeConnection;
    let response = ask_source(service.clone(), "es-native-probe", probe).await;
    assert!(response.receipt.is_some());
    assert_eq!(response.receipt.as_ref().unwrap().actual.calls, 1);
    assert!(matches!(response.payload, EsSourcePayload::Probe(status) if status.reachable));

    let mut fields = EsSourceRequest::snapshot_page("es-native", &index);
    fields.operation = EsSourceOperation::DiscoverFields;
    let response = ask_source(service.clone(), "es-native-fields", fields).await;
    assert_eq!(response.receipt.as_ref().unwrap().actual.calls, 1);
    assert!(
        matches!(response.payload, EsSourcePayload::Fields(catalog) if catalog.fields.iter().any(|field| field.name == "message"))
    );

    let mut snapshot = EsSourceRequest::snapshot_page("es-native", &index);
    snapshot.limit = Some(2);
    let response = ask_source(service.clone(), "es-native-snapshot-open", snapshot).await;
    assert_eq!(response.receipt.as_ref().unwrap().actual.rows, 2);
    assert_eq!(response.receipt.as_ref().unwrap().actual.calls, 2);
    let snapshot_source_revision = response.receipt.as_ref().unwrap().source_revision.clone();
    assert!(snapshot_source_revision.starts_with("es-pit:"));
    let cursor = match response.payload {
        EsSourcePayload::Batch(batch) => {
            assert_eq!(batch.source_revision, snapshot_source_revision);
            assert!(
                batch
                    .documents
                    .iter()
                    .all(|document| document.seq_no.is_some() && document.primary_term.is_some())
            );
            match batch.next_cursor.expect("snapshot cursor") {
                EsSourceCursor::Snapshot(cursor) => cursor,
                other => panic!("expected snapshot cursor, got {other:?}"),
            }
        }
        other => panic!("expected snapshot batch, got {other:?}"),
    };
    let mut page = EsSourceRequest::snapshot_page("es-native", &index);
    page.limit = Some(2);
    page.cursor = Some(EsSourceCursor::Snapshot(cursor));
    let response = ask_source(service.clone(), "es-native-snapshot-page", page).await;
    assert_eq!(response.receipt.as_ref().unwrap().actual.calls, 2);
    assert_eq!(
        response.receipt.as_ref().unwrap().source_revision,
        snapshot_source_revision
    );
    match response.payload {
        EsSourcePayload::Batch(batch) => {
            assert_eq!(batch.source_revision, snapshot_source_revision);
            assert!(!batch.has_more);
            assert_eq!(batch.documents.len(), 1);
        }
        other => panic!("expected snapshot continuation batch, got {other:?}"),
    }

    let mut close_open = EsSourceRequest::snapshot_page("es-native", &index);
    close_open.limit = Some(1);
    let response = ask_source(service.clone(), "es-native-close-open", close_open).await;
    assert_eq!(response.receipt.as_ref().unwrap().actual.calls, 2);
    let close_source_revision = response.receipt.as_ref().unwrap().source_revision.clone();
    let close_cursor = match response.payload {
        EsSourcePayload::Batch(batch) => {
            assert_eq!(batch.source_revision, close_source_revision);
            match batch.next_cursor.expect("close cursor") {
                EsSourceCursor::Snapshot(cursor) => cursor,
                other => panic!("expected snapshot cursor, got {other:?}"),
            }
        }
        other => panic!("expected snapshot batch, got {other:?}"),
    };
    let close =
        EsSourceRequest::snapshot_close("es-native", &index, EsSnapshotCursor { ..close_cursor });
    let response = ask_source(service.clone(), "es-native-close", close).await;
    assert!(response.receipt.is_none());
    match response.payload {
        EsSourcePayload::Batch(batch) => {
            assert_eq!(batch.source_revision, close_source_revision);
            assert!(batch.documents.is_empty());
        }
        other => panic!("expected closed snapshot batch, got {other:?}"),
    }

    let mut poll = EsSourceRequest::poll_since("es-native", &index);
    poll.limit = Some(2);
    let response = ask_source(service.clone(), "es-native-poll-1", poll).await;
    assert_eq!(response.receipt.as_ref().unwrap().actual.calls, 1);
    let poll_revision = response.receipt.as_ref().unwrap().source_revision.clone();
    assert!(poll_revision.starts_with("es-poll-observation:"));
    let (mut ids, poll_cursor) = match response.payload {
        EsSourcePayload::Batch(batch) => {
            assert_eq!(batch.source_revision, poll_revision);
            (
                batch
                    .documents
                    .iter()
                    .map(|document| document.id.clone())
                    .collect::<Vec<_>>(),
                batch.next_cursor.expect("poll cursor"),
            )
        }
        other => panic!("expected poll batch, got {other:?}"),
    };
    let mut poll = EsSourceRequest::poll_since("es-native", &index);
    poll.limit = Some(2);
    poll.cursor = Some(poll_cursor);
    let response = ask_source(service, "es-native-poll-2", poll).await;
    assert_eq!(response.receipt.as_ref().unwrap().actual.calls, 1);
    let poll_revision = response.receipt.as_ref().unwrap().source_revision.clone();
    assert!(poll_revision.starts_with("es-poll-observation:"));
    match response.payload {
        EsSourcePayload::Batch(batch) => {
            assert_eq!(batch.source_revision, poll_revision);
            ids.extend(batch.documents.iter().map(|document| document.id.clone()))
        }
        other => panic!("expected poll continuation batch, got {other:?}"),
    }
    assert_eq!(ids, vec!["1", "2", "3"]);

    request_json(
        reqwest::Method::DELETE,
        &format!("{endpoint_uri}/{index}"),
        None,
    );
}

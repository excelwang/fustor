use std::collections::HashMap;
use std::collections::VecDeque;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use capanix_app_sdk::CnxError;
use es_source::shared_types::{
    EsDocumentEventKind, EsFieldCatalog, EsPollCursor, EsSnapshotCursor, EsSourceBatch,
    EsSourceCursor, EsSourceOperation, EsSourcePayload, EsSourceRequest,
};
use es_source::{ResolvedCredential, ResolvedEsEndpoint};
use es_source_runtime::{EsSourceDriver, HttpEsSourceDriver};
use serde_json::{Value, json};
use source_kit::{
    SourceExecutionContext, SourceExecutionLimits, SourceOperationBudget, source_now_ms,
};

struct MockResponse {
    method: &'static str,
    target: &'static str,
    status: u16,
    body: &'static str,
    delay: Duration,
}

impl MockResponse {
    fn ok(method: &'static str, target: &'static str, body: &'static str) -> Self {
        Self {
            method,
            target,
            status: 200,
            body,
            delay: Duration::ZERO,
        }
    }

    fn delayed_ok(
        method: &'static str,
        target: &'static str,
        body: &'static str,
        delay: Duration,
    ) -> Self {
        Self {
            method,
            target,
            status: 200,
            body,
            delay,
        }
    }

    fn status(method: &'static str, target: &'static str, status: u16, body: &'static str) -> Self {
        Self {
            method,
            target,
            status,
            body,
            delay: Duration::ZERO,
        }
    }
}

#[derive(Debug, Clone)]
struct RecordedRequest {
    method: String,
    target: String,
    headers: HashMap<String, String>,
    body: String,
    json_body: Option<Value>,
}

struct MockEsServer {
    base_url: String,
    recorded: Arc<Mutex<Vec<RecordedRequest>>>,
    handle: thread::JoinHandle<()>,
}

impl MockEsServer {
    fn start(responses: Vec<MockResponse>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock es server");
        let base_url = format!("http://{}", listener.local_addr().expect("local addr"));
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let thread_recorded = recorded.clone();
        let handle = thread::spawn(move || {
            let mut responses = VecDeque::from(responses);
            while let Some(expected) = responses.pop_front() {
                let (mut stream, _) = listener.accept().expect("accept mock request");
                let request = read_request(&mut stream);
                assert_eq!(request.method, expected.method);
                assert_eq!(request.target, expected.target);
                thread_recorded.lock().unwrap().push(request);
                thread::sleep(expected.delay);
                write_response(&mut stream, expected.status, expected.body);
            }
        });
        Self {
            base_url,
            recorded,
            handle,
        }
    }

    fn finish(self) -> Vec<RecordedRequest> {
        self.handle.join().expect("mock es server thread");
        Arc::try_unwrap(self.recorded)
            .expect("mock server recorded still shared")
            .into_inner()
            .expect("recorded lock")
    }
}

fn endpoint(base_url: String) -> ResolvedEsEndpoint {
    ResolvedEsEndpoint {
        object_ref: "es-prod".into(),
        endpoint_uri: base_url,
        credential_ref: None,
        index_scopes: vec!["logs-*".into()],
        timestamp_field: "@timestamp".into(),
        tie_breaker_field: "_id".into(),
        grant_epoch: Some(9),
    }
}

fn operation_budget(max_calls: u64) -> SourceOperationBudget {
    let now_ms = source_now_ms();
    SourceOperationBudget::from_context(
        Some(&SourceExecutionContext {
            request_id: format!("es-http-budget-{max_calls}"),
            grant_epoch: 9,
            issued_at_ms: now_ms,
            deadline_ms: now_ms + 5_000,
            limits: SourceExecutionLimits {
                max_rows: 100,
                max_bytes: 1_048_576,
                max_calls,
                max_fanout: 1,
                max_latency_ms: 5_000,
                max_cost_units: max_calls,
            },
        }),
        now_ms,
    )
    .expect("operation budget")
}

fn read_request(stream: &mut TcpStream) -> RecordedRequest {
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set read timeout");
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 1024];
    loop {
        let read = stream.read(&mut chunk).expect("read request");
        assert!(read > 0, "connection closed before headers");
        buffer.extend_from_slice(&chunk[..read]);
        if header_end(&buffer).is_some() {
            break;
        }
    }
    let header_end = header_end(&buffer).expect("header end");
    let headers = String::from_utf8_lossy(&buffer[..header_end]).to_string();
    let content_length = headers
        .lines()
        .find_map(|line| {
            line.split_once(':').and_then(|(name, value)| {
                if name.eq_ignore_ascii_case("content-length") {
                    value.trim().parse::<usize>().ok()
                } else {
                    None
                }
            })
        })
        .unwrap_or(0);
    let body_start = header_end + 4;
    while buffer.len() < body_start + content_length {
        let read = stream.read(&mut chunk).expect("read request body");
        assert!(read > 0, "connection closed before full body");
        buffer.extend_from_slice(&chunk[..read]);
    }
    let request_line = headers.lines().next().expect("request line");
    let mut parts = request_line.split_whitespace();
    let method = parts.next().expect("method").to_string();
    let target = parts.next().expect("target").to_string();
    let headers_map = headers
        .lines()
        .skip(1)
        .filter_map(|line| {
            let (name, value) = line.split_once(':')?;
            Some((name.trim().to_ascii_lowercase(), value.trim().to_string()))
        })
        .collect::<HashMap<_, _>>();
    let body_bytes = &buffer[body_start..body_start + content_length];
    let body = String::from_utf8_lossy(body_bytes).to_string();
    let json_body = if body.trim().is_empty() {
        None
    } else {
        Some(serde_json::from_str(&body).expect("json request body"))
    };
    RecordedRequest {
        method,
        target,
        headers: headers_map,
        body,
        json_body,
    }
}

fn header_end(buffer: &[u8]) -> Option<usize> {
    buffer.windows(4).position(|window| window == b"\r\n\r\n")
}

fn write_response(stream: &mut TcpStream, status: u16, body: &str) {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "Status",
    };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    let _ = stream.write_all(response.as_bytes());
}

#[test]
fn request_timeout_is_classified_as_timeout() {
    let server = MockEsServer::start(vec![MockResponse::delayed_ok(
        "GET",
        "/",
        r#"{"cluster_name":"slow"}"#,
        Duration::from_millis(100),
    )]);
    let driver = HttpEsSourceDriver::default();
    let endpoint = endpoint(server.base_url.clone());
    let mut request = EsSourceRequest::snapshot_page("es-prod", "logs-2026");
    request.operation = EsSourceOperation::ProbeConnection;

    let error = driver
        .execute_with_timeout(
            &endpoint,
            &ResolvedCredential::None,
            &request,
            1,
            Some(Duration::from_millis(10)),
        )
        .expect_err("delayed response must time out");

    assert!(matches!(error, CnxError::Timeout));
    assert_eq!(server.finish().len(), 1);
}

#[test]
fn snapshot_page_opens_pit_searches_and_closes_terminal_pit() {
    let server = MockEsServer::start(vec![
        MockResponse::ok("POST", "/logs-2026/_pit?keep_alive=1m", r#"{"id":"pit-1"}"#),
        MockResponse::ok(
            "POST",
            "/_search",
            r#"{"hits":{"hits":[{"_index":"logs-2026","_id":"doc-1","_seq_no":7,"_primary_term":1,"_source":{"@timestamp":"2026-05-09T00:00:00Z","message":"hello"},"sort":["2026-05-09T00:00:00Z",1]}]}}"#,
        ),
        MockResponse::ok("DELETE", "/_pit", r#"{"succeeded":true}"#),
    ]);
    let driver = HttpEsSourceDriver::default();
    let endpoint = endpoint(server.base_url.clone());
    let request = EsSourceRequest {
        object_ref: "es-prod".into(),
        index: "logs-2026".into(),
        operation: EsSourceOperation::SnapshotPage,
        limit: Some(2),
        cursor: None,
        timestamp_after: None,
        field_filter: vec!["message".into()],
        query_filter: Some(json!({"term": {"level": "info"}})),
    };

    let mut budget = operation_budget(3);
    let payload = driver
        .execute_with_budget(
            &endpoint,
            &ResolvedCredential::None,
            &request,
            2,
            &mut budget,
        )
        .expect("snapshot execute");
    assert_eq!(budget.accounting().calls, 3);
    assert_eq!(budget.accounting().cost_units, 3);

    let EsSourcePayload::Batch(EsSourceBatch {
        documents,
        next_cursor,
        has_more,
        grant_epoch,
        ..
    }) = payload
    else {
        panic!("expected batch payload");
    };
    assert_eq!(documents.len(), 1);
    assert_eq!(documents[0].event_kind, EsDocumentEventKind::SnapshotInsert);
    assert_eq!(documents[0].id, "doc-1");
    assert_eq!(documents[0].seq_no, Some(7));
    assert_eq!(documents[0].source["message"], json!("hello"));
    assert!(!has_more);
    assert!(next_cursor.is_none());
    assert_eq!(grant_epoch, Some(9));

    let recorded = server.finish();
    assert_eq!(recorded.len(), 3);
    let search_body = recorded[1].json_body.as_ref().expect("search json body");
    assert_eq!(search_body["size"], json!(2));
    assert_eq!(search_body["pit"]["id"], json!("pit-1"));
    assert_eq!(search_body["_source"]["includes"], json!(["message"]));
    assert_eq!(search_body["query"]["term"]["level"], json!("info"));
    assert_eq!(
        recorded[2].json_body.as_ref().expect("close json body")["id"],
        json!("pit-1")
    );
}

#[test]
fn snapshot_rejects_insufficient_call_budget_before_opening_pit() {
    let driver = HttpEsSourceDriver::default();
    let endpoint = endpoint("http://127.0.0.1:9".into());
    let request = EsSourceRequest::snapshot_page("es-prod", "logs-2026");
    let mut budget = operation_budget(2);

    let error = driver
        .execute_with_budget(
            &endpoint,
            &ResolvedCredential::None,
            &request,
            2,
            &mut budget,
        )
        .expect_err("new PIT snapshot requires open, search, and terminal-close capacity");

    assert!(matches!(error, CnxError::ResourceExhausted(_)));
    assert_eq!(budget.accounting().calls, 0);
}

#[test]
fn poll_since_sends_timestamp_filter_search_after_and_returns_next_cursor() {
    let server = MockEsServer::start(vec![MockResponse::ok(
        "POST",
        "/logs-2026/_search",
        r#"{"hits":{"hits":[{"_index":"logs-2026","_id":"doc-2","_source":{"@timestamp":"2026-05-09T00:01:00Z","message":"next"},"sort":["2026-05-09T00:01:00Z","doc-2"]}]}}"#,
    )]);
    let driver = HttpEsSourceDriver::default();
    let endpoint = endpoint(server.base_url.clone());
    let request = EsSourceRequest {
        object_ref: "es-prod".into(),
        index: "logs-2026".into(),
        operation: EsSourceOperation::PollSince,
        limit: Some(10),
        cursor: Some(EsSourceCursor::Poll(EsPollCursor {
            timestamp_after: Some(json!("2026-05-09T00:00:00Z")),
            search_after: vec![json!("2026-05-09T00:00:00Z"), json!("doc-1")],
        })),
        timestamp_after: None,
        field_filter: Vec::new(),
        query_filter: Some(json!({"term": {"level": "warn"}})),
    };

    let payload = driver
        .execute(&endpoint, &ResolvedCredential::None, &request, 10)
        .expect("poll execute");

    let EsSourcePayload::Batch(batch) = payload else {
        panic!("expected batch payload");
    };
    assert_eq!(batch.documents.len(), 1);
    assert_eq!(
        batch.documents[0].event_kind,
        EsDocumentEventKind::PollUpdate
    );
    assert_eq!(batch.documents[0].id, "doc-2");
    assert!(!batch.has_more);
    match batch.next_cursor.expect("next poll cursor") {
        EsSourceCursor::Poll(cursor) => {
            assert_eq!(cursor.timestamp_after, Some(json!("2026-05-09T00:01:00Z")));
            assert_eq!(
                cursor.search_after,
                vec![json!("2026-05-09T00:01:00Z"), json!("doc-2")]
            );
        }
        other => panic!("expected poll cursor, got {other:?}"),
    }

    let recorded = server.finish();
    assert_eq!(recorded.len(), 1);
    assert!(!recorded[0].body.is_empty());
    let body = recorded[0].json_body.as_ref().expect("poll json body");
    assert_eq!(body["size"], json!(10));
    assert_eq!(
        body["sort"],
        json!([
            {"@timestamp": {"order": "asc"}},
            {"_id": {"order": "asc"}}
        ])
    );
    assert_eq!(
        body["query"]["bool"]["filter"][1]["range"]["@timestamp"]["gte"],
        json!("2026-05-09T00:00:00Z")
    );
    assert_eq!(
        body["search_after"],
        json!(["2026-05-09T00:00:00Z", "doc-1"])
    );
}

#[test]
fn discover_fields_fetches_mapping_and_flattens_es_properties() {
    let server = MockEsServer::start(vec![MockResponse::ok(
        "GET",
        "/logs-2026/_mapping",
        r#"{"logs-2026":{"mappings":{"properties":{"@timestamp":{"type":"date"},"message":{"type":"text","fields":{"keyword":{"type":"keyword"}}},"user":{"properties":{"id":{"type":"keyword"}}}}}}}"#,
    )]);
    let driver = HttpEsSourceDriver::default();
    let endpoint = endpoint(server.base_url.clone());
    let request = EsSourceRequest {
        object_ref: "es-prod".into(),
        index: "logs-2026".into(),
        operation: EsSourceOperation::DiscoverFields,
        limit: None,
        cursor: None,
        timestamp_after: None,
        field_filter: Vec::new(),
        query_filter: None,
    };

    let payload = driver
        .execute(&endpoint, &ResolvedCredential::None, &request, 10)
        .expect("discover execute");

    let EsSourcePayload::Fields(EsFieldCatalog { fields, .. }) = payload else {
        panic!("expected fields payload");
    };
    let names = fields
        .iter()
        .map(|field| field.name.as_str())
        .collect::<Vec<_>>();
    for expected in [
        "@timestamp",
        "message",
        "message.keyword",
        "user.id",
        "_id",
        "_index",
    ] {
        assert!(
            names.contains(&expected),
            "missing field {expected}: {names:?}"
        );
    }

    let recorded = server.finish();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].body, "");
}

#[test]
fn auth_headers_are_sent_for_supported_credentials() {
    let server = MockEsServer::start(vec![
        MockResponse::ok("GET", "/", r#"{"cluster_name":"basic"}"#),
        MockResponse::ok("GET", "/", r#"{"cluster_name":"api-key"}"#),
        MockResponse::ok("GET", "/", r#"{"cluster_name":"bearer"}"#),
    ]);
    let driver = HttpEsSourceDriver::default();
    let endpoint = endpoint(server.base_url.clone());
    let request = EsSourceRequest {
        object_ref: "es-prod".into(),
        index: "logs-2026".into(),
        operation: EsSourceOperation::ProbeConnection,
        limit: None,
        cursor: None,
        timestamp_after: None,
        field_filter: Vec::new(),
        query_filter: None,
    };

    driver
        .execute(
            &endpoint,
            &ResolvedCredential::Basic {
                username: "user".into(),
                password: "pass".into(),
            },
            &request,
            10,
        )
        .expect("basic probe");
    driver
        .execute(
            &endpoint,
            &ResolvedCredential::ApiKey {
                api_key: "key-123".into(),
            },
            &request,
            10,
        )
        .expect("api key probe");
    driver
        .execute(
            &endpoint,
            &ResolvedCredential::Bearer {
                token: "token-123".into(),
            },
            &request,
            10,
        )
        .expect("bearer probe");

    let recorded = server.finish();
    assert_eq!(
        recorded[0].headers.get("authorization").map(String::as_str),
        Some("Basic dXNlcjpwYXNz")
    );
    assert_eq!(
        recorded[1].headers.get("authorization").map(String::as_str),
        Some("ApiKey key-123")
    );
    assert_eq!(
        recorded[2].headers.get("authorization").map(String::as_str),
        Some("Bearer token-123")
    );
}

#[test]
fn retryable_http_statuses_map_to_link_errors() {
    let server = MockEsServer::start(vec![MockResponse::status(
        "GET",
        "/",
        429,
        r#"{"error":"busy"}"#,
    )]);
    let driver = HttpEsSourceDriver::default();
    let endpoint = endpoint(server.base_url.clone());
    let request = EsSourceRequest {
        object_ref: "es-prod".into(),
        index: "logs-2026".into(),
        operation: EsSourceOperation::ProbeConnection,
        limit: None,
        cursor: None,
        timestamp_after: None,
        field_filter: Vec::new(),
        query_filter: None,
    };

    let err = driver
        .execute(&endpoint, &ResolvedCredential::None, &request, 10)
        .unwrap_err();

    assert!(matches!(err, CnxError::LinkError(message) if message.contains("status=429")));
    let recorded = server.finish();
    assert_eq!(recorded.len(), 1);
}

#[test]
fn access_denied_http_statuses_map_to_access_denied() {
    let server = MockEsServer::start(vec![MockResponse::status(
        "GET",
        "/",
        401,
        r#"{"error":"unauthorized"}"#,
    )]);
    let driver = HttpEsSourceDriver::default();
    let endpoint = endpoint(server.base_url.clone());
    let request = EsSourceRequest {
        object_ref: "es-prod".into(),
        index: "logs-2026".into(),
        operation: EsSourceOperation::ProbeConnection,
        limit: None,
        cursor: None,
        timestamp_after: None,
        field_filter: Vec::new(),
        query_filter: None,
    };

    let err = driver
        .execute(&endpoint, &ResolvedCredential::None, &request, 10)
        .unwrap_err();

    assert!(matches!(err, CnxError::AccessDenied(message) if message.contains("status=401")));
    let recorded = server.finish();
    assert_eq!(recorded.len(), 1);
}

#[test]
fn snapshot_search_failure_closes_opened_pit() {
    let server = MockEsServer::start(vec![
        MockResponse::ok("POST", "/logs-2026/_pit?keep_alive=1m", r#"{"id":"pit-1"}"#),
        MockResponse::status("POST", "/_search", 503, r#"{"error":"unavailable"}"#),
        MockResponse::ok("DELETE", "/_pit", r#"{"succeeded":true}"#),
    ]);
    let driver = HttpEsSourceDriver::default();
    let endpoint = endpoint(server.base_url.clone());
    let request = EsSourceRequest::snapshot_page("es-prod", "logs-2026");

    let err = driver
        .execute(&endpoint, &ResolvedCredential::None, &request, 10)
        .unwrap_err();

    assert!(matches!(err, CnxError::LinkError(message) if message.contains("status=503")));
    let recorded = server.finish();
    assert_eq!(recorded.len(), 3);
    assert_eq!(recorded[2].method, "DELETE");
    assert_eq!(recorded[2].target, "/_pit");
}

#[test]
fn terminal_close_pit_failure_is_reported_as_batch_diagnostics() {
    let server = MockEsServer::start(vec![
        MockResponse::ok("POST", "/logs-2026/_pit?keep_alive=1m", r#"{"id":"pit-1"}"#),
        MockResponse::ok(
            "POST",
            "/_search",
            r#"{"hits":{"hits":[{"_index":"logs-2026","_id":"doc-1","_source":{"@timestamp":"2026-05-09T00:00:00Z"},"sort":["2026-05-09T00:00:00Z",1]}]}}"#,
        ),
        MockResponse::status("DELETE", "/_pit", 500, r#"{"error":"close failed"}"#),
    ]);
    let driver = HttpEsSourceDriver::default();
    let endpoint = endpoint(server.base_url.clone());
    let request = EsSourceRequest::snapshot_page("es-prod", "logs-2026");

    let payload = driver
        .execute(&endpoint, &ResolvedCredential::None, &request, 2)
        .expect("snapshot execute");

    let EsSourcePayload::Batch(batch) = payload else {
        panic!("expected batch payload");
    };
    assert!(
        batch
            .diagnostics
            .as_deref()
            .is_some_and(|diagnostics| diagnostics.contains("close PIT failed"))
    );
    let recorded = server.finish();
    assert_eq!(recorded.len(), 3);
}

#[test]
fn snapshot_continuation_reuses_existing_pit_without_opening_or_closing() {
    let server = MockEsServer::start(vec![MockResponse::ok(
        "POST",
        "/_search",
        r#"{"hits":{"hits":[{"_index":"logs-2026","_id":"doc-1","_source":{"@timestamp":"2026-05-09T00:00:00Z"},"sort":["2026-05-09T00:00:00Z",1]}]}}"#,
    )]);
    let driver = HttpEsSourceDriver::default();
    let endpoint = endpoint(server.base_url.clone());
    let mut request = EsSourceRequest::snapshot_page("es-prod", "logs-2026");
    request.cursor = Some(EsSourceCursor::Snapshot(EsSnapshotCursor {
        pit_id: "pit-1".into(),
        search_after: vec![json!("2026-05-08T00:00:00Z"), json!(1)],
        expires_at_unix_ms: None,
    }));

    let payload = driver
        .execute(&endpoint, &ResolvedCredential::None, &request, 1)
        .expect("snapshot continuation");

    let EsSourcePayload::Batch(batch) = payload else {
        panic!("expected batch payload");
    };
    assert!(batch.has_more);
    assert!(matches!(
        batch.next_cursor,
        Some(EsSourceCursor::Snapshot(_))
    ));
    let recorded = server.finish();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].method, "POST");
    assert_eq!(recorded[0].target, "/_search");
}

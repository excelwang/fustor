use std::collections::VecDeque;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use es_source::shared_types::{
    EsDocumentEventKind, EsFieldCatalog, EsPollCursor, EsSourceBatch, EsSourceCursor,
    EsSourceOperation, EsSourcePayload, EsSourceRequest,
};
use es_source::{ResolvedCredential, ResolvedEsEndpoint};
use es_source_runtime::{EsSourceDriver, HttpEsSourceDriver};
use serde_json::{Value, json};

struct MockResponse {
    method: &'static str,
    target: &'static str,
    body: &'static str,
}

#[derive(Debug, Clone)]
struct RecordedRequest {
    method: String,
    target: String,
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
                write_response(&mut stream, expected.body);
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
        body,
        json_body,
    }
}

fn header_end(buffer: &[u8]) -> Option<usize> {
    buffer.windows(4).position(|window| window == b"\r\n\r\n")
}

fn write_response(stream: &mut TcpStream, body: &str) {
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    stream
        .write_all(response.as_bytes())
        .expect("write response");
}

#[test]
fn snapshot_page_opens_pit_searches_and_closes_terminal_pit() {
    let server = MockEsServer::start(vec![
        MockResponse {
            method: "POST",
            target: "/logs-2026/_pit?keep_alive=1m",
            body: r#"{"id":"pit-1"}"#,
        },
        MockResponse {
            method: "POST",
            target: "/_search",
            body: r#"{"hits":{"hits":[{"_index":"logs-2026","_id":"doc-1","_seq_no":7,"_primary_term":1,"_source":{"@timestamp":"2026-05-09T00:00:00Z","message":"hello"},"sort":["2026-05-09T00:00:00Z",1]}]}}"#,
        },
        MockResponse {
            method: "DELETE",
            target: "/_pit",
            body: r#"{"succeeded":true}"#,
        },
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

    let payload = driver
        .execute(&endpoint, &ResolvedCredential::None, &request, 2)
        .expect("snapshot execute");

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
fn poll_since_sends_timestamp_filter_search_after_and_returns_next_cursor() {
    let server = MockEsServer::start(vec![MockResponse {
        method: "POST",
        target: "/logs-2026/_search",
        body: r#"{"hits":{"hits":[{"_index":"logs-2026","_id":"doc-2","_source":{"@timestamp":"2026-05-09T00:01:00Z","message":"next"},"sort":["2026-05-09T00:01:00Z","doc-2"]}]}}"#,
    }]);
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
    assert_eq!(recorded[0].body.is_empty(), false);
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
    let server = MockEsServer::start(vec![MockResponse {
        method: "GET",
        target: "/logs-2026/_mapping",
        body: r#"{"logs-2026":{"mappings":{"properties":{"@timestamp":{"type":"date"},"message":{"type":"text","fields":{"keyword":{"type":"keyword"}}},"user":{"properties":{"id":{"type":"keyword"}}}}}}}"#,
    }]);
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

use std::fs;
use std::sync::Arc;
use std::time::Duration;

use capanix_app_sdk::runtime::NodeId;
use jsonl_source::product_model::routes::fetch_request_route_for;
use jsonl_source::shared_types::{JsonlSourceOperation, JsonlSourcePayload, JsonlSourceRequest};
use jsonl_source::{
    GrantedJsonlEndpoint, JsonlEndpointConfig, JsonlSourceConfig, JsonlSourceRuntimeConfig,
    JsonlSourceRuntimeInputs,
};
use jsonl_source_runtime::{JsonlSourceService, NativeJsonlSourceDriver};
use source_kit::{
    DecodedSourceResponse, SourceExecutionContext, SourceExecutionEnvelope, SourceResponseEnvelope,
    decode_source_response, encode_source_request, source_now_ms,
};
use source_test_kit::ask_bound_route;
use tempfile::tempdir;

async fn ask_source(
    service: Arc<JsonlSourceService>,
    request_id: &str,
    request: JsonlSourceRequest,
) -> SourceResponseEnvelope<JsonlSourcePayload> {
    let now_ms = source_now_ms();
    let envelope = SourceExecutionEnvelope::new(
        SourceExecutionContext::one_shot(request_id, 1, now_ms, now_ms + 5_000, 100, 1024 * 1024),
        request,
    );
    let replies = ask_bound_route(
        fetch_request_route_for("jsonl-native-node"),
        encode_source_request(&envelope)
            .expect("encode request")
            .into(),
        Duration::from_secs(5),
        move |events| service.handle_events(events),
    )
    .await
    .expect("JSONL bound route");
    let DecodedSourceResponse::Envelope(response) =
        decode_source_response::<JsonlSourcePayload>(replies[0].payload_bytes())
            .expect("decode response")
    else {
        panic!("JSONL native request returned a legacy response")
    };
    assert_eq!(response.request_id, request_id);
    *response
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_jsonl_bound_route_covers_sampled_fields_offsets_blank_utf8_malformed_and_change() {
    let root = tempdir().expect("root tempdir");
    fs::write(
        root.path().join("records.jsonl"),
        "\n{\"id\":1,\"name\":\"张三\"}\n\n{\"id\":2,\"active\":true}\n",
    )
    .expect("write JSONL");
    fs::write(
        root.path().join("malformed.jsonl"),
        "{\"id\":1}\nnot-json\n",
    )
    .expect("write malformed JSONL");
    let large = (0..1_000)
        .map(|value| format!("{{\"id\":{value},\"value\":\"v{value}\"}}"))
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(root.path().join("large.jsonl"), format!("{large}\n")).expect("write large JSONL");
    fs::write(
        root.path().join("changing.jsonl"),
        "{\"id\":1}\n{\"id\":2}\n",
    )
    .expect("write changing JSONL");

    let service = Arc::new(JsonlSourceService::new(
        JsonlSourceRuntimeConfig {
            product: JsonlSourceConfig {
                endpoints: vec![JsonlEndpointConfig {
                    object_ref: "jsonl-native".into(),
                    root_path: root.path().to_string_lossy().to_string(),
                    path_scopes: vec!["*.jsonl".into()],
                    active: true,
                }],
                default_page_size: 10,
                max_page_size: 100,
                field_sample_limit: 100,
            },
            runtime: JsonlSourceRuntimeInputs {
                endpoint_grants: vec![GrantedJsonlEndpoint {
                    object_ref: "jsonl-native".into(),
                    path_scopes: Vec::new(),
                    interfaces: vec!["read".into()],
                    active: true,
                    grant_epoch: Some(1),
                }],
            },
        },
        Arc::new(NativeJsonlSourceDriver),
        NodeId("jsonl-source-native-test".into()),
    ));

    let mut fields = JsonlSourceRequest::snapshot_page("jsonl-native", "records.jsonl");
    fields.operation = JsonlSourceOperation::DiscoverFields;
    let response = ask_source(service.clone(), "jsonl-native-fields", fields).await;
    assert!(
        matches!(response.payload, JsonlSourcePayload::Fields(catalog) if catalog.fields.iter().map(|field| field.name.as_str()).collect::<Vec<_>>() == vec!["active", "id", "name"])
    );

    let mut page = JsonlSourceRequest::snapshot_page("jsonl-native", "records.jsonl");
    page.limit = Some(1);
    let response = ask_source(service.clone(), "jsonl-native-page-1", page).await;
    let cursor = match response.payload {
        JsonlSourcePayload::Records(batch) => {
            assert_eq!(batch.records[0].line_number, 2);
            assert_eq!(batch.records[0].values["name"], "张三");
            batch.next_cursor.expect("JSONL cursor")
        }
        other => panic!("expected JSONL records, got {other:?}"),
    };
    assert!(response.receipt.is_some());

    let mut page = JsonlSourceRequest::snapshot_page("jsonl-native", "records.jsonl");
    page.limit = Some(10);
    page.cursor = Some(cursor);
    let response = ask_source(service.clone(), "jsonl-native-page-2", page).await;
    assert!(
        matches!(response.payload, JsonlSourcePayload::Records(batch) if batch.records.len() == 1 && batch.records[0].line_number == 4)
    );

    let malformed = JsonlSourceRequest::snapshot_page("jsonl-native", "malformed.jsonl");
    let response = ask_source(service.clone(), "jsonl-native-malformed", malformed).await;
    assert!(response.receipt.is_none());
    assert!(matches!(response.payload, JsonlSourcePayload::Error(_)));

    let mut large = JsonlSourceRequest::snapshot_page("jsonl-native", "large.jsonl");
    large.limit = Some(10);
    let response = ask_source(service.clone(), "jsonl-native-large", large).await;
    assert!(
        matches!(response.payload, JsonlSourcePayload::Records(batch) if batch.records.len() == 10 && batch.has_more)
    );

    let mut changing = JsonlSourceRequest::snapshot_page("jsonl-native", "changing.jsonl");
    changing.limit = Some(1);
    let response = ask_source(service.clone(), "jsonl-native-changing-1", changing).await;
    let cursor = match response.payload {
        JsonlSourcePayload::Records(batch) => batch.next_cursor.expect("changing cursor"),
        other => panic!("expected JSONL records, got {other:?}"),
    };
    fs::write(
        root.path().join("changing.jsonl"),
        "{\"id\":1}\n{\"id\":2}\n{\"id\":3}\n",
    )
    .expect("mutate changing JSONL");
    let mut changing = JsonlSourceRequest::snapshot_page("jsonl-native", "changing.jsonl");
    changing.limit = Some(1);
    changing.cursor = Some(cursor);
    let response = ask_source(service, "jsonl-native-changing-2", changing).await;
    assert!(response.receipt.is_none());
    assert!(matches!(response.payload, JsonlSourcePayload::Error(_)));
}

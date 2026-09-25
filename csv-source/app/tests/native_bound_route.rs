use std::fs;
use std::sync::Arc;
use std::time::Duration;

use capanix_app_sdk::runtime::NodeId;
use csv_source::product_model::routes::fetch_request_route_for;
use csv_source::shared_types::{CsvSourceOperation, CsvSourcePayload, CsvSourceRequest};
use csv_source::{
    CsvEndpointConfig, CsvSourceConfig, CsvSourceRuntimeConfig, CsvSourceRuntimeInputs,
    GrantedCsvEndpoint,
};
use csv_source_runtime::{CsvSourceService, NativeCsvSourceDriver};
use source_kit::{
    DecodedSourceResponse, SourceExecutionContext, SourceExecutionEnvelope, SourceResponseEnvelope,
    decode_source_response, encode_source_request, source_now_ms,
};
use source_test_kit::ask_bound_route;
use tempfile::tempdir;

async fn ask_source(
    service: Arc<CsvSourceService>,
    request_id: &str,
    request: CsvSourceRequest,
) -> SourceResponseEnvelope<CsvSourcePayload> {
    let now_ms = source_now_ms();
    let envelope = SourceExecutionEnvelope::new(
        SourceExecutionContext::one_shot(request_id, 1, now_ms, now_ms + 5_000, 100, 1024 * 1024),
        request,
    );
    let replies = ask_bound_route(
        fetch_request_route_for("csv-native-node"),
        encode_source_request(&envelope)
            .expect("encode request")
            .into(),
        Duration::from_secs(5),
        move |events| service.handle_events(events),
    )
    .await
    .expect("CSV bound route");
    let DecodedSourceResponse::Envelope(response) =
        decode_source_response::<CsvSourcePayload>(replies[0].payload_bytes())
            .expect("decode response")
    else {
        panic!("CSV native request returned a legacy response")
    };
    assert_eq!(response.request_id, request_id);
    *response
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_csv_bound_route_covers_quoted_headerless_malformed_large_and_changing_files() {
    let root = tempdir().expect("root tempdir");
    fs::write(
        root.path().join("quoted.csv"),
        "id,name,note\n1,\"Alice, A.\",\"line one\nline two\"\n2,Bob,plain\n",
    )
    .expect("write quoted csv");
    fs::write(root.path().join("headerless.csv"), "1,Alice\n2,Bob\n")
        .expect("write headerless csv");
    fs::write(
        root.path().join("malformed.csv"),
        "id,name\n1,Alice,extra\n",
    )
    .expect("write malformed csv");
    let large = std::iter::once("id,value".to_string())
        .chain((0..1_000).map(|value| format!("{value},value-{value}")))
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(root.path().join("large.csv"), format!("{large}\n")).expect("write large csv");
    fs::write(root.path().join("changing.csv"), "id,value\n1,a\n2,b\n")
        .expect("write changing csv");

    let root_path = root.path().to_string_lossy().to_string();
    let service = Arc::new(CsvSourceService::new(
        CsvSourceRuntimeConfig {
            product: CsvSourceConfig {
                endpoints: vec![
                    CsvEndpointConfig {
                        object_ref: "csv-header".into(),
                        root_path: root_path.clone(),
                        path_scopes: vec!["*.csv".into()],
                        has_headers: true,
                        delimiter: b',',
                        active: true,
                    },
                    CsvEndpointConfig {
                        object_ref: "csv-headerless".into(),
                        root_path,
                        path_scopes: vec!["*.csv".into()],
                        has_headers: false,
                        delimiter: b',',
                        active: true,
                    },
                ],
                default_page_size: 10,
                max_page_size: 100,
            },
            runtime: CsvSourceRuntimeInputs {
                endpoint_grants: vec![
                    GrantedCsvEndpoint {
                        object_ref: "csv-header".into(),
                        path_scopes: Vec::new(),
                        interfaces: vec!["read".into()],
                        active: true,
                        grant_epoch: Some(1),
                    },
                    GrantedCsvEndpoint {
                        object_ref: "csv-headerless".into(),
                        path_scopes: Vec::new(),
                        interfaces: vec!["read".into()],
                        active: true,
                        grant_epoch: Some(1),
                    },
                ],
            },
        },
        Arc::new(NativeCsvSourceDriver),
        NodeId("csv-source-native-test".into()),
    ));

    let mut fields = CsvSourceRequest::snapshot_page("csv-header", "quoted.csv");
    fields.operation = CsvSourceOperation::DiscoverFields;
    let response = ask_source(service.clone(), "csv-native-fields", fields).await;
    assert!(
        matches!(response.payload, CsvSourcePayload::Fields(catalog) if catalog.fields.iter().map(|field| field.name.as_str()).collect::<Vec<_>>() == vec!["id", "name", "note"])
    );

    let mut quoted = CsvSourceRequest::snapshot_page("csv-header", "quoted.csv");
    quoted.limit = Some(1);
    let response = ask_source(service.clone(), "csv-native-quoted", quoted).await;
    let cursor = match response.payload {
        CsvSourcePayload::Records(batch) => {
            assert_eq!(batch.records[0].values["name"], "Alice, A.");
            assert_eq!(batch.records[0].values["note"], "line one\nline two");
            batch.next_cursor.expect("quoted cursor")
        }
        other => panic!("expected CSV records, got {other:?}"),
    };
    assert!(response.receipt.is_some());

    let mut quoted = CsvSourceRequest::snapshot_page("csv-header", "quoted.csv");
    quoted.limit = Some(10);
    quoted.cursor = Some(cursor);
    let response = ask_source(service.clone(), "csv-native-quoted-page", quoted).await;
    assert!(
        matches!(response.payload, CsvSourcePayload::Records(batch) if batch.records.len() == 1 && !batch.has_more)
    );

    let headerless = CsvSourceRequest::snapshot_page("csv-headerless", "headerless.csv");
    let response = ask_source(service.clone(), "csv-native-headerless", headerless).await;
    assert!(
        matches!(response.payload, CsvSourcePayload::Records(batch) if batch.fields == vec!["field_1", "field_2"] && batch.records[0].row_number == 1)
    );

    let malformed = CsvSourceRequest::snapshot_page("csv-header", "malformed.csv");
    let response = ask_source(service.clone(), "csv-native-malformed", malformed).await;
    assert!(response.receipt.is_none());
    assert!(matches!(response.payload, CsvSourcePayload::Error(_)));

    let mut large = CsvSourceRequest::snapshot_page("csv-header", "large.csv");
    large.limit = Some(10);
    let response = ask_source(service.clone(), "csv-native-large", large).await;
    assert!(
        matches!(response.payload, CsvSourcePayload::Records(batch) if batch.records.len() == 10 && batch.has_more)
    );

    let mut changing = CsvSourceRequest::snapshot_page("csv-header", "changing.csv");
    changing.limit = Some(1);
    let response = ask_source(service.clone(), "csv-native-changing-1", changing).await;
    let cursor = match response.payload {
        CsvSourcePayload::Records(batch) => batch.next_cursor.expect("changing cursor"),
        other => panic!("expected CSV records, got {other:?}"),
    };
    fs::write(
        root.path().join("changing.csv"),
        "id,value\n1,a\n2,b\n3,c\n",
    )
    .expect("mutate changing csv");
    let mut changing = CsvSourceRequest::snapshot_page("csv-header", "changing.csv");
    changing.limit = Some(1);
    changing.cursor = Some(cursor);
    let response = ask_source(service, "csv-native-changing-2", changing).await;
    assert!(response.receipt.is_none());
    assert!(matches!(response.payload, CsvSourcePayload::Error(_)));
}

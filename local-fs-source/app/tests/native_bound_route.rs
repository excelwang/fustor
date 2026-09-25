use std::fs;
use std::sync::Arc;
use std::time::Duration;

use capanix_app_sdk::runtime::NodeId;
use local_fs_source::product_model::routes::fetch_request_route_for;
use local_fs_source::shared_types::{
    LocalFsSourceCursor, LocalFsSourceOperation, LocalFsSourcePayload, LocalFsSourceRequest,
};
use local_fs_source::{
    GrantedLocalFsEndpoint, LocalFsEndpointConfig, LocalFsSourceConfig, LocalFsSourceRuntimeConfig,
    LocalFsSourceRuntimeInputs,
};
use local_fs_source_runtime::{LocalFsSourceService, NativeLocalFsSourceDriver};
use source_kit::{
    DecodedSourceResponse, SourceExecutionContext, SourceExecutionEnvelope, SourceResponseEnvelope,
    decode_source_response, encode_source_request, source_now_ms,
};
use source_test_kit::ask_bound_route;
use tempfile::tempdir;

async fn ask_source(
    service: Arc<LocalFsSourceService>,
    request_id: &str,
    request: LocalFsSourceRequest,
    max_bytes: u64,
) -> SourceResponseEnvelope<LocalFsSourcePayload> {
    let now_ms = source_now_ms();
    let envelope = SourceExecutionEnvelope::new(
        SourceExecutionContext::one_shot(request_id, 1, now_ms, now_ms + 5_000, 100, max_bytes),
        request,
    );
    let replies = ask_bound_route(
        fetch_request_route_for("local-fs-native-node"),
        encode_source_request(&envelope)
            .expect("encode request")
            .into(),
        Duration::from_secs(5),
        move |events| service.handle_events(events),
    )
    .await
    .expect("local-fs bound route");
    let DecodedSourceResponse::Envelope(response) =
        decode_source_response::<LocalFsSourcePayload>(replies[0].payload_bytes())
            .expect("decode response")
    else {
        panic!("local-fs native request returned a legacy response")
    };
    assert_eq!(response.request_id, request_id);
    if let Some(receipt) = response.receipt.as_ref() {
        assert_eq!(receipt.source_kind, "local-fs");
        assert_eq!(
            receipt.actual.bytes,
            replies[0].payload_bytes().len() as u64
        );
    }
    *response
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_local_fs_bound_route_covers_discovery_paging_scope_and_byte_budget() {
    let root = tempdir().expect("root tempdir");
    let outside = tempdir().expect("outside tempdir");
    fs::create_dir(root.path().join("datasets")).expect("create datasets");
    fs::write(root.path().join("datasets/a.txt"), b"a").expect("write a");
    fs::write(root.path().join("datasets/b.txt"), b"b").expect("write b");
    fs::write(outside.path().join("secret.txt"), b"secret").expect("write outside");
    #[cfg(unix)]
    std::os::unix::fs::symlink(outside.path(), root.path().join("escape"))
        .expect("create escaping symlink");

    let service = Arc::new(LocalFsSourceService::new(
        LocalFsSourceRuntimeConfig {
            product: LocalFsSourceConfig {
                endpoints: vec![LocalFsEndpointConfig {
                    object_ref: "local-native".into(),
                    root_path: root.path().to_string_lossy().to_string(),
                    path_scopes: vec!["*".into()],
                    active: true,
                }],
                default_page_size: 1,
                max_page_size: 100,
            },
            runtime: LocalFsSourceRuntimeInputs {
                endpoint_grants: vec![GrantedLocalFsEndpoint {
                    object_ref: "local-native".into(),
                    path_scopes: Vec::new(),
                    interfaces: vec!["read".into()],
                    active: true,
                    grant_epoch: Some(1),
                }],
            },
        },
        Arc::new(NativeLocalFsSourceDriver),
        NodeId("local-fs-source-native-test".into()),
    ));

    let probe = LocalFsSourceRequest {
        object_ref: "local-native".into(),
        path: None,
        operation: LocalFsSourceOperation::ProbeConnection,
        limit: None,
        cursor: None,
    };
    let response = ask_source(service.clone(), "local-native-probe", probe, 1_048_576).await;
    assert!(matches!(response.payload, LocalFsSourcePayload::Probe(status) if status.reachable));

    let fields = LocalFsSourceRequest {
        object_ref: "local-native".into(),
        path: None,
        operation: LocalFsSourceOperation::DiscoverFields,
        limit: None,
        cursor: None,
    };
    let response = ask_source(service.clone(), "local-native-fields", fields, 1_048_576).await;
    assert!(
        matches!(response.payload, LocalFsSourcePayload::Fields(catalog) if catalog.fields.iter().any(|field| field.name == "path"))
    );

    let mut snapshot = LocalFsSourceRequest::snapshot_page("local-native");
    snapshot.path = Some("datasets".into());
    snapshot.limit = Some(1);
    let response = ask_source(service.clone(), "local-native-page-1", snapshot, 1_048_576).await;
    let cursor = match response.payload {
        LocalFsSourcePayload::Entries(batch) => {
            assert_eq!(batch.entries.len(), 1);
            batch.next_cursor.expect("cursor")
        }
        other => panic!("expected local-fs entries, got {other:?}"),
    };
    assert!(matches!(cursor, LocalFsSourceCursor::Snapshot(_)));

    let mut snapshot = LocalFsSourceRequest::snapshot_page("local-native");
    snapshot.path = Some("datasets".into());
    snapshot.limit = Some(10);
    snapshot.cursor = Some(cursor);
    let response = ask_source(service.clone(), "local-native-page-2", snapshot, 1_048_576).await;
    assert!(
        matches!(response.payload, LocalFsSourcePayload::Entries(batch) if !batch.entries.is_empty() && !batch.has_more)
    );

    let mut traversal = LocalFsSourceRequest::snapshot_page("local-native");
    traversal.path = Some("../outside".into());
    let response = ask_source(
        service.clone(),
        "local-native-traversal",
        traversal,
        1_048_576,
    )
    .await;
    assert!(response.receipt.is_none());
    assert!(matches!(response.payload, LocalFsSourcePayload::Error(_)));

    #[cfg(unix)]
    {
        let listing = LocalFsSourceRequest::snapshot_page("local-native");
        let response = ask_source(
            service.clone(),
            "local-native-symlink-listing",
            listing,
            1_048_576,
        )
        .await;
        assert!(response.receipt.is_none());
        assert!(matches!(
            response.payload,
            LocalFsSourcePayload::Error(error) if error.code == "scope-denied"
        ));

        let mut symlink = LocalFsSourceRequest::snapshot_page("local-native");
        symlink.path = Some("escape/secret.txt".into());
        let response =
            ask_source(service.clone(), "local-native-symlink", symlink, 1_048_576).await;
        assert!(response.receipt.is_none());
        assert!(matches!(response.payload, LocalFsSourcePayload::Error(_)));
    }

    let mut bounded = LocalFsSourceRequest::snapshot_page("local-native");
    bounded.path = Some("datasets".into());
    bounded.limit = Some(10);
    let response = ask_source(service, "local-native-byte-limit", bounded, 256).await;
    assert!(response.receipt.is_none());
    assert!(matches!(
        response.payload,
        LocalFsSourcePayload::Error(error) if error.code == "budget-exceeded"
    ));
}

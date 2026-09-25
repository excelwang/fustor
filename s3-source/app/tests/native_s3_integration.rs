use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_credential_types::Credentials;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::primitives::ByteStream;
use capanix_app_sdk::runtime::{ConfigValue, NodeId};
use s3_source::product_model::routes::fetch_request_route_for;
use s3_source::shared_types::{
    S3SourceCursor, S3SourceOperation, S3SourcePayload, S3SourceRequest,
};
use s3_source::{
    GrantedS3Endpoint, S3EndpointConfig, S3SourceConfig, S3SourceRuntimeConfig,
    S3SourceRuntimeInputs,
};
use s3_source_runtime::{NativeS3SourceDriver, S3SourceService};
use source_kit::{
    DecodedSourceResponse, SourceExecutionContext, SourceExecutionEnvelope, decode_source_response,
    encode_source_request, source_now_ms,
};
use source_test_kit::ask_bound_route;

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
}

fn required_env(key: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| panic!("missing required native S3 test env {key}"))
}

fn native_service(env: &S3TestEnv, prefix: &str) -> S3SourceService {
    let credential_ref = "s3-native".to_string();
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
            ConfigValue::String("FUSTOR_S3_SOURCE_ACCESS_KEY".into()),
        ),
        (
            "password_env".to_string(),
            ConfigValue::String("FUSTOR_S3_SOURCE_SECRET_KEY".into()),
        ),
    ]))
    .expect("S3 credential config");
    S3SourceService::new(
        S3SourceRuntimeConfig {
            product: S3SourceConfig {
                endpoints: vec![S3EndpointConfig {
                    object_ref: "s3-it".into(),
                    endpoint_uri: env.endpoint_uri.clone(),
                    bucket: env.bucket.clone(),
                    region: Some(env.region.clone()),
                    prefix: prefix.to_string(),
                    credential_ref: Some(credential_ref.clone()),
                    bucket_scopes: vec![env.bucket.clone()],
                    prefix_scopes: vec![format!("{prefix}*")],
                    active: true,
                }],
                credentials: vec![credential],
                default_page_size: 1,
                max_page_size: 100,
            },
            runtime: S3SourceRuntimeInputs {
                endpoint_grants: vec![GrantedS3Endpoint {
                    object_ref: "s3-it".into(),
                    endpoint_ref: None,
                    endpoint_uri: None,
                    bucket: None,
                    credential_ref: Some(credential_ref),
                    bucket_scopes: Vec::new(),
                    prefix_scopes: Vec::new(),
                    interfaces: vec!["read".into()],
                    active: true,
                    grant_epoch: Some(1),
                }],
            },
        },
        Arc::new(NativeS3SourceDriver),
        NodeId("s3-source-native-test".into()),
    )
}

async fn ask_source(
    service: Arc<S3SourceService>,
    request_id: &str,
    request: S3SourceRequest,
) -> (S3SourcePayload, source_kit::SourceObservationReceipt) {
    let now_ms = source_now_ms();
    let envelope = SourceExecutionEnvelope::new(
        SourceExecutionContext::one_shot(request_id, 1, now_ms, now_ms + 20_000, 100, 1024 * 1024),
        request,
    );
    let replies = ask_bound_route(
        fetch_request_route_for("s3-native-node"),
        encode_source_request(&envelope)
            .expect("encode S3 source request")
            .into(),
        Duration::from_secs(20),
        move |events| service.handle_events(events),
    )
    .await
    .expect("S3 bound route");
    let DecodedSourceResponse::Envelope(response) =
        decode_source_response::<S3SourcePayload>(replies[0].payload_bytes())
            .expect("decode S3 source response")
    else {
        panic!("native S3 request returned a legacy response")
    };
    assert_eq!(response.request_id, request_id);
    let receipt = response.receipt.expect("S3 source receipt");
    assert_eq!(receipt.source_kind, "s3");
    assert_eq!(receipt.grant_epoch, 1);
    assert_eq!(
        receipt.actual.bytes,
        replies[0].payload_bytes().len() as u64
    );
    (response.payload, receipt)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires FUSTOR_S3_SOURCE_ENDPOINT_URI, FUSTOR_S3_SOURCE_ACCESS_KEY, FUSTOR_S3_SOURCE_SECRET_KEY, FUSTOR_S3_SOURCE_BUCKET"]
async fn native_s3_bound_route_covers_probe_fields_and_snapshot_continuation() {
    let env = S3TestEnv::from_env();
    let prefix = format!("fustor-it-{}/snapshot/", unique_suffix());
    let client = setup_bucket(&env).await;
    put_object(&client, &env.bucket, &format!("{prefix}a.txt"), "a").await;
    put_object(&client, &env.bucket, &format!("{prefix}b.txt"), "b").await;
    let service = Arc::new(native_service(&env, &prefix));

    let mut probe = S3SourceRequest::snapshot_page("s3-it");
    probe.operation = S3SourceOperation::ProbeConnection;
    let (payload, _) = ask_source(service.clone(), "s3-native-probe", probe).await;
    assert!(matches!(payload, S3SourcePayload::Probe(status) if status.reachable));

    let mut fields = S3SourceRequest::snapshot_page("s3-it");
    fields.operation = S3SourceOperation::DiscoverFields;
    let (payload, _) = ask_source(service.clone(), "s3-native-fields", fields).await;
    assert!(
        matches!(payload, S3SourcePayload::Fields(catalog) if catalog.fields.iter().any(|field| field.name == "Key"))
    );

    let mut snapshot = S3SourceRequest::snapshot_page("s3-it");
    snapshot.limit = Some(1);
    let (payload, receipt) = ask_source(service.clone(), "s3-native-snapshot-1", snapshot).await;
    assert_eq!(receipt.actual.rows, 1);
    assert!(receipt.source_revision.starts_with("s3-list-observation:"));
    let cursor = match payload {
        S3SourcePayload::Objects(batch) => {
            assert_eq!(batch.source_revision, receipt.source_revision);
            assert!(batch.has_more);
            batch.next_cursor.expect("snapshot cursor")
        }
        other => panic!("expected S3 object batch, got {other:?}"),
    };
    let mut snapshot = S3SourceRequest::snapshot_page("s3-it");
    snapshot.limit = Some(10);
    snapshot.cursor = Some(cursor);
    let (payload, receipt) = ask_source(service, "s3-native-snapshot-2", snapshot).await;
    match payload {
        S3SourcePayload::Objects(batch) => {
            assert_eq!(batch.source_revision, receipt.source_revision);
            assert!(!batch.objects.is_empty());
        }
        other => panic!("expected S3 continuation batch, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires FUSTOR_S3_SOURCE_ENDPOINT_URI, FUSTOR_S3_SOURCE_ACCESS_KEY, FUSTOR_S3_SOURCE_SECRET_KEY, FUSTOR_S3_SOURCE_BUCKET"]
async fn native_s3_bound_route_poll_checkpoint_is_ordered_and_nonduplicating() {
    let env = S3TestEnv::from_env();
    let prefix = format!("fustor-it-{}/poll/", unique_suffix());
    let client = setup_bucket(&env).await;
    for key in ["a.txt", "b.txt", "c.txt"] {
        put_object(&client, &env.bucket, &format!("{prefix}{key}"), key).await;
    }
    let service = Arc::new(native_service(&env, &prefix));
    let mut poll = S3SourceRequest::snapshot_page("s3-it");
    poll.operation = S3SourceOperation::PollSince;
    poll.limit = Some(1);
    let (payload, receipt) = ask_source(service.clone(), "s3-native-poll-1", poll).await;
    assert!(receipt.source_revision.starts_with("s3-list-observation:"));
    let (first_key, cursor) = match payload {
        S3SourcePayload::Objects(batch) => {
            assert_eq!(batch.source_revision, receipt.source_revision);
            assert_eq!(batch.objects.len(), 1);
            assert!(batch.has_more);
            (
                batch.objects[0].key.clone(),
                batch.next_cursor.expect("poll cursor"),
            )
        }
        other => panic!("expected S3 poll batch, got {other:?}"),
    };
    assert!(matches!(cursor, S3SourceCursor::Poll(_)));

    let mut poll = S3SourceRequest::snapshot_page("s3-it");
    poll.operation = S3SourceOperation::PollSince;
    poll.limit = Some(10);
    poll.cursor = Some(cursor);
    let (payload, receipt) = ask_source(service, "s3-native-poll-2", poll).await;
    match payload {
        S3SourcePayload::Objects(batch) => {
            assert_eq!(batch.source_revision, receipt.source_revision);
            assert!(!batch.objects.is_empty());
            assert!(batch.objects.iter().all(|object| object.key > first_key));
        }
        other => panic!("expected S3 poll continuation, got {other:?}"),
    }
}

async fn setup_bucket(env: &S3TestEnv) -> Client {
    let client = Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .region(Region::new(env.region.clone()))
            .endpoint_url(env.endpoint_uri.clone())
            .force_path_style(true)
            .credentials_provider(Credentials::new(
                env.access_key.clone(),
                env.secret_key.clone(),
                None,
                None,
                "fustor-s3-source-it",
            ))
            .build(),
    );
    let _ = client
        .create_bucket()
        .bucket(env.bucket.clone())
        .send()
        .await;
    client
}

async fn put_object(client: &Client, bucket: &str, key: &str, body: &str) {
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(body.as_bytes().to_vec()))
        .send()
        .await
        .expect("put object");
}

fn unique_suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_micros()
}

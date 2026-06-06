use aws_credential_types::Credentials;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::types::Object;
use capanix_app_sdk::{CnxError, Result};
use s3_source::shared_types::{
    S3FieldCatalog, S3FieldDescriptor, S3ObjectEvent, S3PollCursor, S3ProbeStatus, S3SnapshotCursor,
};
use s3_source::{ResolvedCredential, ResolvedS3Endpoint};
use serde_json::{Value, json};
use tokio::runtime::{Builder as TokioRuntimeBuilder, Runtime};

pub trait S3SourceDriver: Send + Sync {
    fn probe(
        &self,
        endpoint: &ResolvedS3Endpoint,
        credential: &ResolvedCredential,
    ) -> Result<S3ProbeStatus>;

    fn discover_fields(
        &self,
        endpoint: &ResolvedS3Endpoint,
        credential: &ResolvedCredential,
    ) -> Result<S3FieldCatalog>;

    fn snapshot_page(
        &self,
        endpoint: &ResolvedS3Endpoint,
        credential: &ResolvedCredential,
        limit: usize,
        cursor: Option<&S3SnapshotCursor>,
    ) -> Result<S3ObjectPage>;

    fn poll_since(
        &self,
        endpoint: &ResolvedS3Endpoint,
        credential: &ResolvedCredential,
        limit: usize,
        cursor: Option<&S3PollCursor>,
    ) -> Result<S3ObjectPage>;
}

#[derive(Debug, Clone, PartialEq)]
pub struct S3ObjectPage {
    pub objects: Vec<S3ObjectEvent>,
    pub next_snapshot_cursor: Option<S3SnapshotCursor>,
    pub next_poll_cursor: Option<S3PollCursor>,
    pub has_more: bool,
    pub diagnostics: Option<String>,
}

#[derive(Default)]
pub struct NativeS3SourceDriver;

impl S3SourceDriver for NativeS3SourceDriver {
    fn probe(
        &self,
        endpoint: &ResolvedS3Endpoint,
        credential: &ResolvedCredential,
    ) -> Result<S3ProbeStatus> {
        let client = build_client(endpoint, credential)?;
        block_on_s3("probe s3 endpoint", async move {
            client
                .head_bucket()
                .bucket(endpoint.bucket.clone())
                .send()
                .await
                .map_err(s3_error("probe s3 endpoint"))?;
            Ok(S3ProbeStatus {
                object_ref: endpoint.object_ref.clone(),
                endpoint_uri: endpoint.endpoint_uri.clone(),
                bucket: endpoint.bucket.clone(),
                reachable: true,
                diagnostics: None,
            })
        })
    }

    fn discover_fields(
        &self,
        endpoint: &ResolvedS3Endpoint,
        _credential: &ResolvedCredential,
    ) -> Result<S3FieldCatalog> {
        Ok(S3FieldCatalog {
            object_ref: endpoint.object_ref.clone(),
            fields: default_s3_fields(),
        })
    }

    fn snapshot_page(
        &self,
        endpoint: &ResolvedS3Endpoint,
        credential: &ResolvedCredential,
        limit: usize,
        cursor: Option<&S3SnapshotCursor>,
    ) -> Result<S3ObjectPage> {
        let client = build_client(endpoint, credential)?;
        let continuation_token = cursor.and_then(|cursor| cursor.continuation_token.clone());
        block_on_s3("list s3 snapshot page", async move {
            let mut request = client
                .list_objects_v2()
                .bucket(endpoint.bucket.clone())
                .prefix(endpoint.prefix.clone())
                .fetch_owner(true)
                .max_keys(s3_max_keys(limit));
            if let Some(token) = continuation_token {
                request = request.continuation_token(token);
            }
            let response = request
                .send()
                .await
                .map_err(s3_error("list s3 snapshot page"))?;
            let objects = response
                .contents()
                .iter()
                .map(object_to_event)
                .collect::<Result<Vec<_>>>()?;
            let next_token = response
                .next_continuation_token()
                .filter(|token| !token.trim().is_empty())
                .map(str::to_string);
            let has_more = response.is_truncated().unwrap_or(false) && next_token.is_some();
            Ok(S3ObjectPage {
                objects,
                next_snapshot_cursor: next_token.map(|continuation_token| S3SnapshotCursor {
                    continuation_token: Some(continuation_token),
                }),
                next_poll_cursor: None,
                has_more,
                diagnostics: None,
            })
        })
    }

    fn poll_since(
        &self,
        endpoint: &ResolvedS3Endpoint,
        credential: &ResolvedCredential,
        limit: usize,
        cursor: Option<&S3PollCursor>,
    ) -> Result<S3ObjectPage> {
        let client = build_client(endpoint, credential)?;
        let poll_cursor = cursor.cloned();
        block_on_s3("poll s3 objects", async move {
            let mut continuation_token = None;
            let mut objects = Vec::new();
            loop {
                let mut request = client
                    .list_objects_v2()
                    .bucket(endpoint.bucket.clone())
                    .prefix(endpoint.prefix.clone())
                    .fetch_owner(true)
                    .max_keys(1_000);
                if let Some(token) = continuation_token.take() {
                    request = request.continuation_token(token);
                }
                let response = request.send().await.map_err(s3_error("poll s3 objects"))?;
                for object in response.contents() {
                    let event = object_to_event(object)?;
                    if poll_object_is_after(&event, poll_cursor.as_ref()) {
                        objects.push(event);
                    }
                }
                let next_token = response
                    .next_continuation_token()
                    .filter(|token| !token.trim().is_empty())
                    .map(str::to_string);
                if response.is_truncated().unwrap_or(false) && next_token.is_some() {
                    continuation_token = next_token;
                } else {
                    break;
                }
            }
            objects.sort_by(|left, right| {
                (left.last_modified_unix_ms, &left.key)
                    .cmp(&(right.last_modified_unix_ms, &right.key))
            });
            let has_more = objects.len() > limit;
            if has_more {
                objects.truncate(limit);
            }
            let next_poll_cursor = objects.last().map(|last| S3PollCursor {
                last_modified_unix_ms: last.last_modified_unix_ms,
                last_key: last.key.clone(),
            });
            Ok(S3ObjectPage {
                objects,
                next_snapshot_cursor: None,
                next_poll_cursor,
                has_more,
                diagnostics: None,
            })
        })
    }
}

#[derive(Default)]
pub struct UnsupportedS3SourceDriver;

impl S3SourceDriver for UnsupportedS3SourceDriver {
    fn probe(
        &self,
        _endpoint: &ResolvedS3Endpoint,
        _credential: &ResolvedCredential,
    ) -> Result<S3ProbeStatus> {
        Err(CnxError::NotSupported(
            "s3-source real S3 client is not linked in this runtime artifact".into(),
        ))
    }

    fn discover_fields(
        &self,
        _endpoint: &ResolvedS3Endpoint,
        _credential: &ResolvedCredential,
    ) -> Result<S3FieldCatalog> {
        Err(CnxError::NotSupported(
            "s3-source field discovery requires a linked S3 client".into(),
        ))
    }

    fn snapshot_page(
        &self,
        _endpoint: &ResolvedS3Endpoint,
        _credential: &ResolvedCredential,
        _limit: usize,
        _cursor: Option<&S3SnapshotCursor>,
    ) -> Result<S3ObjectPage> {
        Err(CnxError::NotSupported(
            "s3-source snapshot requires a linked S3 client".into(),
        ))
    }

    fn poll_since(
        &self,
        _endpoint: &ResolvedS3Endpoint,
        _credential: &ResolvedCredential,
        _limit: usize,
        _cursor: Option<&S3PollCursor>,
    ) -> Result<S3ObjectPage> {
        Err(CnxError::NotSupported(
            "s3-source polling requires a linked S3 client".into(),
        ))
    }
}

fn build_client(endpoint: &ResolvedS3Endpoint, credential: &ResolvedCredential) -> Result<Client> {
    let region = endpoint
        .region
        .clone()
        .unwrap_or_else(|| "us-east-1".to_string());
    let mut builder = aws_sdk_s3::Config::builder()
        .region(Region::new(region))
        .endpoint_url(endpoint.endpoint_uri.clone())
        .force_path_style(true);
    match credential {
        ResolvedCredential::None => {}
        ResolvedCredential::Basic { username, password } => {
            builder = builder.credentials_provider(Credentials::new(
                username.clone(),
                password.clone(),
                None,
                None,
                "fustor-s3-source",
            ));
        }
        ResolvedCredential::ApiKey { .. } | ResolvedCredential::Bearer { .. } => {
            return Err(CnxError::InvalidInput(
                "s3-source supports basic_env credentials for S3-compatible endpoints".into(),
            ));
        }
    }
    Ok(Client::from_conf(builder.build()))
}

fn block_on_s3<T>(
    context: &'static str,
    future: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    s3_runtime(context)?.block_on(future)
}

fn s3_runtime(context: &'static str) -> Result<Runtime> {
    TokioRuntimeBuilder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| CnxError::Internal(format!("{context}: build tokio runtime: {err}")))
}

fn object_to_event(object: &Object) -> Result<S3ObjectEvent> {
    let key = object
        .key()
        .ok_or_else(|| CnxError::PeerError("s3 list object entry missing key".into()))?
        .to_string();
    let size = u64::try_from(object.size().unwrap_or(0))
        .map_err(|_| CnxError::PeerError(format!("s3 object '{key}' reported a negative size")))?;
    let last_modified_unix_ms = match object.last_modified() {
        Some(timestamp) => u64::try_from(timestamp.to_millis().map_err(|err| {
            CnxError::PeerError(format!(
                "s3 object '{key}' last_modified conversion failed: {err}"
            ))
        })?)
        .map_err(|_| {
            CnxError::PeerError(format!(
                "s3 object '{key}' last_modified is before unix epoch"
            ))
        })?,
        None => 0,
    };
    let owner = object.owner();
    Ok(S3ObjectEvent {
        key,
        size,
        last_modified_unix_ms,
        etag: object.e_tag().map(str::to_string),
        storage_class: object
            .storage_class()
            .map(|value| value.as_str().to_string()),
        owner_id: owner.and_then(|owner| owner.id().map(str::to_string)),
        owner_display_name: owner.and_then(|owner| owner.display_name().map(str::to_string)),
        metadata: object_metadata(object),
    })
}

fn object_metadata(object: &Object) -> Value {
    json!({
        "checksum_algorithm": object
            .checksum_algorithm()
            .iter()
            .map(|value| value.as_str().to_string())
            .collect::<Vec<_>>(),
        "checksum_type": object.checksum_type().map(|value| value.as_str().to_string()),
    })
}

fn poll_object_is_after(event: &S3ObjectEvent, cursor: Option<&S3PollCursor>) -> bool {
    let Some(cursor) = cursor else {
        return true;
    };
    (event.last_modified_unix_ms, event.key.as_str())
        > (cursor.last_modified_unix_ms, cursor.last_key.as_str())
}

fn s3_max_keys(limit: usize) -> i32 {
    i32::try_from(limit.clamp(1, 1_000)).unwrap_or(1_000)
}

fn default_s3_fields() -> Vec<S3FieldDescriptor> {
    [
        ("Key", "string"),
        ("Size", "u64"),
        ("LastModified", "unix-ms"),
        ("ETag", "string"),
        ("StorageClass", "string"),
        ("OwnerId", "string"),
        ("OwnerDisplayName", "string"),
    ]
    .into_iter()
    .map(|(name, field_type)| S3FieldDescriptor {
        name: name.into(),
        field_type: field_type.into(),
    })
    .collect()
}

fn s3_error<E, R>(
    context: &'static str,
) -> impl FnOnce(aws_sdk_s3::error::SdkError<E, R>) -> CnxError
where
    E: std::fmt::Debug,
    R: std::fmt::Debug,
{
    move |err| CnxError::PeerError(format!("{context}: {err:?}"))
}

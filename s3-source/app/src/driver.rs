use std::time::Duration;

use aws_credential_types::Credentials;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::types::Object;
use capanix_app_sdk::{CnxError, Result};
use s3_source::shared_types::{
    S3FieldCatalog, S3FieldDescriptor, S3ObjectEvent, S3PollCursor, S3ProbeStatus, S3SnapshotCursor,
};
use s3_source::{ResolvedCredential, ResolvedS3Endpoint};
use serde_json::{Value, json};
use source_kit::SourceOperationBudget;
use tokio::runtime::{Builder as TokioRuntimeBuilder, Runtime};

const DEFAULT_POLL_SCAN_PAGES: usize = 10;

pub trait S3SourceDriver: Send + Sync {
    fn sample_content(&self, _endpoint: &ResolvedS3Endpoint, _credential: &ResolvedCredential, _format: &str,
        _limit: usize, _budget: &mut SourceOperationBudget) -> Result<source_kit::SourceContentSample> {
        Err(CnxError::NotSupported("bounded object content reader not linked".into()))
    }
    fn probe(
        &self,
        endpoint: &ResolvedS3Endpoint,
        credential: &ResolvedCredential,
    ) -> Result<S3ProbeStatus>;

    fn probe_with_timeout(
        &self,
        endpoint: &ResolvedS3Endpoint,
        credential: &ResolvedCredential,
        timeout: Option<Duration>,
    ) -> Result<S3ProbeStatus> {
        let _ = timeout;
        self.probe(endpoint, credential)
    }

    fn probe_with_budget(
        &self,
        endpoint: &ResolvedS3Endpoint,
        credential: &ResolvedCredential,
        budget: &mut SourceOperationBudget,
    ) -> Result<S3ProbeStatus> {
        budget.observe_fanout(1)?;
        budget.consume_call(1)?;
        self.probe_with_timeout(endpoint, credential, budget.remaining_timeout()?)
    }

    fn discover_fields(
        &self,
        endpoint: &ResolvedS3Endpoint,
        credential: &ResolvedCredential,
    ) -> Result<S3FieldCatalog>;

    fn discover_fields_with_timeout(
        &self,
        endpoint: &ResolvedS3Endpoint,
        credential: &ResolvedCredential,
        timeout: Option<Duration>,
    ) -> Result<S3FieldCatalog> {
        let _ = timeout;
        self.discover_fields(endpoint, credential)
    }

    fn discover_fields_with_budget(
        &self,
        endpoint: &ResolvedS3Endpoint,
        credential: &ResolvedCredential,
        budget: &mut SourceOperationBudget,
    ) -> Result<S3FieldCatalog> {
        budget.observe_fanout(1)?;
        budget.consume_call(1)?;
        self.discover_fields_with_timeout(endpoint, credential, budget.remaining_timeout()?)
    }

    fn snapshot_page(
        &self,
        endpoint: &ResolvedS3Endpoint,
        credential: &ResolvedCredential,
        limit: usize,
        cursor: Option<&S3SnapshotCursor>,
    ) -> Result<S3ObjectPage>;

    fn snapshot_page_with_timeout(
        &self,
        endpoint: &ResolvedS3Endpoint,
        credential: &ResolvedCredential,
        limit: usize,
        cursor: Option<&S3SnapshotCursor>,
        timeout: Option<Duration>,
    ) -> Result<S3ObjectPage> {
        let _ = timeout;
        self.snapshot_page(endpoint, credential, limit, cursor)
    }

    fn snapshot_page_with_budget(
        &self,
        endpoint: &ResolvedS3Endpoint,
        credential: &ResolvedCredential,
        limit: usize,
        cursor: Option<&S3SnapshotCursor>,
        budget: &mut SourceOperationBudget,
    ) -> Result<S3ObjectPage> {
        budget.observe_fanout(1)?;
        budget.consume_call(1)?;
        self.snapshot_page_with_timeout(
            endpoint,
            credential,
            limit,
            cursor,
            budget.remaining_timeout()?,
        )
    }

    fn poll_since(
        &self,
        endpoint: &ResolvedS3Endpoint,
        credential: &ResolvedCredential,
        limit: usize,
        cursor: Option<&S3PollCursor>,
    ) -> Result<S3ObjectPage>;

    fn poll_since_with_timeout(
        &self,
        endpoint: &ResolvedS3Endpoint,
        credential: &ResolvedCredential,
        limit: usize,
        cursor: Option<&S3PollCursor>,
        timeout: Option<Duration>,
    ) -> Result<S3ObjectPage> {
        let _ = timeout;
        self.poll_since(endpoint, credential, limit, cursor)
    }

    fn poll_since_with_budget(
        &self,
        endpoint: &ResolvedS3Endpoint,
        credential: &ResolvedCredential,
        limit: usize,
        cursor: Option<&S3PollCursor>,
        budget: &mut SourceOperationBudget,
    ) -> Result<S3ObjectPage> {
        budget.observe_fanout(1)?;
        budget.consume_call(1)?;
        self.poll_since_with_timeout(
            endpoint,
            credential,
            limit,
            cursor,
            budget.remaining_timeout()?,
        )
    }
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
    fn sample_content(&self, endpoint: &ResolvedS3Endpoint, credential: &ResolvedCredential, format: &str,
        limit: usize, budget: &mut SourceOperationBudget) -> Result<source_kit::SourceContentSample> {
        if endpoint.prefix.is_empty() || endpoint.prefix.ends_with('/') {
            return Err(CnxError::InvalidInput("content sample prefix must be an exact object key".into()));
        }
        budget.observe_fanout(1)?;
        budget.ensure_call_capacity(2, 2)?;
        let client = build_client(endpoint, credential)?;
        budget.consume_call(1)?;
        let head = block_on_s3("stat content object", budget.remaining_timeout()?, async {
            client.head_object().bucket(&endpoint.bucket).key(&endpoint.prefix).send().await.map_err(s3_error("stat content object"))
        })?;
        let size = u64::try_from(head.content_length().unwrap_or(-1)).map_err(|_| CnxError::PeerError("missing object length".into()))?;
        let cap = budget.remaining_bytes().unwrap_or(source_kit::MAX_CONTENT_PREFIX_BYTES).min(source_kit::MAX_CONTENT_PREFIX_BYTES).min(size);
        let version = head.version_id().map(str::to_owned);
        let etag = head.e_tag().ok_or_else(|| CnxError::PeerError("object sample requires native ETag condition".into()))?.to_owned();
        let revision = source_kit::source_native_revision_digest("s3-object-version", &[endpoint.bucket.as_bytes(), endpoint.prefix.as_bytes(), version.as_deref().unwrap_or("").as_bytes(), etag.as_bytes(), &size.to_le_bytes()]);
        let mut raw = Vec::new();
        if cap > 0 {
            budget.consume_call(1)?;
            raw = block_on_s3("read bounded object prefix", budget.remaining_timeout()?, async {
                let mut request = client.get_object().bucket(&endpoint.bucket).key(&endpoint.prefix)
                    .if_match(&etag).range(format!("bytes=0-{}", cap - 1));
                if let Some(version) = &version { request = request.version_id(version); }
                let response = request.send().await.map_err(s3_error("read bounded object prefix"))?;
                if response.content_length() != Some(cap as i64) || response.e_tag() != Some(etag.as_str())
                    || (version.is_some() && response.version_id() != version.as_deref()) {
                    return Err(CnxError::NotReady("object range/version condition was not honored".into()));
                }
                let mut stream = response.body;
                let mut bytes = Vec::new();
                while let Some(chunk) = stream.try_next().await.map_err(|_| CnxError::PeerError("read object body failed".into()))? {
                    budget.check_deadline()?;
                    if bytes.len() as u64 + chunk.len() as u64 > cap { return Err(CnxError::ResourceExhausted("object range exceeded max_bytes".into())); }
                    budget.consume_bytes(chunk.len() as u64)?;
                    bytes.extend_from_slice(&chunk);
                }
                if bytes.len() as u64 != cap { return Err(CnxError::PeerError("incomplete object range response".into())); }
                Ok(bytes)
            })?;
        }
        let complete = raw.len() as u64 == size;
        let rows = source_kit::parse_content_prefix(&raw, format, complete, limit, budget)?;
        Ok(source_kit::SourceContentSample { object_ref: endpoint.object_ref.clone(), native_ref: format!("s3://{}/{}", endpoint.bucket, endpoint.prefix),
            source_revision: revision, format: format.into(), grant_epoch: endpoint.grant_epoch,
            complete: complete && rows.len() < limit, rows, prefix_sha256: source_kit::content_prefix_digest(&raw), bytes_read: raw.len() as u64 })
    }
    fn probe(
        &self,
        endpoint: &ResolvedS3Endpoint,
        credential: &ResolvedCredential,
    ) -> Result<S3ProbeStatus> {
        self.probe_with_timeout(endpoint, credential, None)
    }

    fn probe_with_timeout(
        &self,
        endpoint: &ResolvedS3Endpoint,
        credential: &ResolvedCredential,
        timeout: Option<Duration>,
    ) -> Result<S3ProbeStatus> {
        let client = build_client(endpoint, credential)?;
        block_on_s3("probe s3 endpoint", timeout, async move {
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
        self.discover_fields_with_timeout(endpoint, _credential, None)
    }

    fn discover_fields_with_timeout(
        &self,
        endpoint: &ResolvedS3Endpoint,
        _credential: &ResolvedCredential,
        timeout: Option<Duration>,
    ) -> Result<S3FieldCatalog> {
        if timeout == Some(Duration::ZERO) {
            return Err(CnxError::Timeout);
        }
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
        self.snapshot_page_with_timeout(endpoint, credential, limit, cursor, None)
    }

    fn snapshot_page_with_timeout(
        &self,
        endpoint: &ResolvedS3Endpoint,
        credential: &ResolvedCredential,
        limit: usize,
        cursor: Option<&S3SnapshotCursor>,
        timeout: Option<Duration>,
    ) -> Result<S3ObjectPage> {
        let client = build_client(endpoint, credential)?;
        let continuation_token = cursor.and_then(|cursor| cursor.continuation_token.clone());
        block_on_s3("list s3 snapshot page", timeout, async move {
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
        self.poll_since_with_timeout(endpoint, credential, limit, cursor, None)
    }

    fn poll_since_with_timeout(
        &self,
        endpoint: &ResolvedS3Endpoint,
        credential: &ResolvedCredential,
        limit: usize,
        cursor: Option<&S3PollCursor>,
        timeout: Option<Duration>,
    ) -> Result<S3ObjectPage> {
        let mut budget = SourceOperationBudget::new(timeout, None);
        budget.observe_fanout(1)?;
        self.poll_since_with_budget(endpoint, credential, limit, cursor, &mut budget)
    }

    fn poll_since_with_budget(
        &self,
        endpoint: &ResolvedS3Endpoint,
        credential: &ResolvedCredential,
        limit: usize,
        cursor: Option<&S3PollCursor>,
        budget: &mut SourceOperationBudget,
    ) -> Result<S3ObjectPage> {
        let client = build_client(endpoint, credential)?;
        let poll_cursor = cursor.cloned();
        budget.observe_fanout(1)?;
        let timeout = budget.remaining_timeout()?;
        block_on_s3("poll s3 objects", timeout, async move {
            let mut continuation_token = poll_cursor
                .as_ref()
                .and_then(|cursor| cursor.continuation_token.clone());
            let mut pending_continuation_token = None;
            let mut scanned_pages = 0usize;
            let mut objects = Vec::new();
            while scanned_pages < DEFAULT_POLL_SCAN_PAGES && objects.len() <= limit {
                budget.consume_call(1)?;
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
                scanned_pages += 1;
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
                    pending_continuation_token = next_token.clone();
                    continuation_token = next_token;
                } else {
                    pending_continuation_token = None;
                    break;
                }
            }
            objects.sort_by(|left, right| {
                (left.last_modified_unix_ms, &left.key)
                    .cmp(&(right.last_modified_unix_ms, &right.key))
            });
            let overflowed = objects.len() > limit;
            if overflowed {
                objects.truncate(limit);
            }
            let continuation_token = if overflowed {
                None
            } else {
                pending_continuation_token
            };
            let has_more = overflowed || continuation_token.is_some();
            let next_poll_cursor = objects
                .last()
                .map(|last| S3PollCursor {
                    last_modified_unix_ms: last.last_modified_unix_ms,
                    last_key: last.key.clone(),
                    continuation_token: continuation_token.clone(),
                })
                .or_else(|| {
                    continuation_token.map(|token| S3PollCursor {
                        last_modified_unix_ms: poll_cursor
                            .as_ref()
                            .map(|cursor| cursor.last_modified_unix_ms)
                            .unwrap_or(0),
                        last_key: poll_cursor
                            .as_ref()
                            .map(|cursor| cursor.last_key.clone())
                            .unwrap_or_default(),
                        continuation_token: Some(token),
                    })
                });
            let diagnostics = if has_more {
                Some(format!(
                    "poll scanned {scanned_pages} S3 list page(s); more objects may remain"
                ))
            } else {
                Some(format!("poll scanned {scanned_pages} S3 list page(s)"))
            };
            Ok(S3ObjectPage {
                objects,
                next_snapshot_cursor: None,
                next_poll_cursor,
                has_more,
                diagnostics,
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
        .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
        .region(Region::new(region))
        .endpoint_url(endpoint.endpoint_uri.clone())
        .force_path_style(true);
    // Provider retries are calls too; a passive one-shot never silently multiplies them.
    builder = builder.retry_config(aws_sdk_s3::config::retry::RetryConfig::standard().with_max_attempts(1));
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
    timeout: Option<Duration>,
    future: impl std::future::Future<Output = Result<T>> + Send,
) -> Result<T>
where
    T: Send,
{
    std::thread::scope(|scope| {
        scope
            .spawn(move || {
                s3_runtime(context)?.block_on(async move {
                    tokio::time::timeout(effective_timeout(timeout), future)
                        .await
                        .map_err(|_| CnxError::Timeout)?
                })
            })
            .join()
            .map_err(|_| CnxError::Internal(format!("{context}: S3 runtime thread panicked")))?
    })
}

fn effective_timeout(timeout: Option<Duration>) -> Duration {
    timeout
        .unwrap_or_else(|| Duration::from_secs(30))
        .max(Duration::from_millis(1))
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
    E: std::fmt::Debug + ProvideErrorMetadata,
    R: std::fmt::Debug,
{
    move |err| {
        if let Some(code) = err.as_service_error().and_then(ProvideErrorMetadata::code) {
            return classify_s3_service_code(context, code);
        }
        match err {
            SdkError::TimeoutError(_) => CnxError::Timeout,
            SdkError::DispatchFailure(_) | SdkError::ResponseError(_) => {
                CnxError::LinkError(format!("{context}: s3 transport failure"))
            }
            SdkError::ConstructionFailure(_) => {
                CnxError::InvalidInput(format!("{context}: invalid s3 request"))
            }
            SdkError::ServiceError(_) => {
                CnxError::PeerError(format!("{context}: unclassified s3 service failure"))
            }
            _ => CnxError::PeerError(format!("{context}: unclassified s3 sdk failure")),
        }
    }
}

fn classify_s3_service_code(context: &str, code: &str) -> CnxError {
    let message = format!("{context}: s3 service error code={code}");
    match code {
        "AccessDenied" | "InvalidAccessKeyId" | "SignatureDoesNotMatch" | "ExpiredToken" => {
            CnxError::AccessDenied(message)
        }
        "SlowDown"
        | "Throttling"
        | "ThrottlingException"
        | "RequestTimeout"
        | "RequestTimeoutException"
        | "InternalError"
        | "ServiceUnavailable" => CnxError::LinkError(message),
        "InvalidArgument" | "InvalidRequest" | "InvalidBucketName" => {
            CnxError::InvalidInput(message)
        }
        _ => CnxError::PeerError(message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn async_s3_operation_is_cancelled_at_the_source_budget() {
        let error = block_on_s3("test s3 timeout", Some(Duration::from_millis(5)), async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(())
        })
        .expect_err("operation must time out");

        assert!(matches!(error, CnxError::Timeout));
    }

    #[test]
    fn s3_service_errors_preserve_access_retry_and_input_classes() {
        assert!(matches!(
            classify_s3_service_code("test", "AccessDenied"),
            CnxError::AccessDenied(_)
        ));
        assert!(matches!(
            classify_s3_service_code("test", "SlowDown"),
            CnxError::LinkError(_)
        ));
        assert!(matches!(
            classify_s3_service_code("test", "InvalidArgument"),
            CnxError::InvalidInput(_)
        ));
    }
}

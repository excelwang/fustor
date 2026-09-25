use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use capanix_app_sdk::runtime::{EventMetadata, NodeId};
use capanix_app_sdk::{CnxError, Event, Result};
use local_fs_source::shared_types::{
    LocalFsEntryBatch, LocalFsFieldDescriptor, LocalFsSourceCursor, LocalFsSourceOperation,
    LocalFsSourcePayload, LocalFsSourceRequest,
};
use local_fs_source::{LocalFsSourceRuntimeConfig, ResolvedLocalFsEndpoint};
use source_kit::{
    DecodedSourceRequest, SourceCapabilityReport, SourceConsistencyCapability,
    SourceContinuationCapability, SourceControlState, SourceErrorPayload, SourceOperationBudget,
    SourceOperationCapability, decode_source_request, encode_msgpack, encode_source_result,
    source_now_ms, validate_source_grant_epoch, validate_source_request_limit,
};

use crate::driver::{LocalFsEntryPage, LocalFsSourceDriver};

#[derive(Clone)]
pub struct LocalFsSourceService {
    control_state: SourceControlState<LocalFsSourceRuntimeConfig>,
    driver: Arc<dyn LocalFsSourceDriver>,
    origin_id: NodeId,
}

impl LocalFsSourceService {
    pub fn new(
        config: LocalFsSourceRuntimeConfig,
        driver: Arc<dyn LocalFsSourceDriver>,
        origin_id: NodeId,
    ) -> Self {
        Self::with_control_state(SourceControlState::new(config), driver, origin_id)
    }

    pub fn with_control_state(
        control_state: SourceControlState<LocalFsSourceRuntimeConfig>,
        driver: Arc<dyn LocalFsSourceDriver>,
        origin_id: NodeId,
    ) -> Self {
        Self {
            control_state,
            driver,
            origin_id,
        }
    }

    pub fn handle_events(&self, events: Vec<Event>) -> Vec<Event> {
        events
            .into_iter()
            .map(|event| self.handle_event(event))
            .collect()
    }

    pub fn handle_status_events(
        &self,
        events: Vec<Event>,
        fetch_route: &str,
        status_route: &str,
    ) -> Vec<Event> {
        events
            .into_iter()
            .map(|event| self.handle_status_event(event, fetch_route, status_route))
            .collect()
    }

    pub fn handle_event(&self, event: Event) -> Event {
        let correlation_id = event.metadata().correlation_id;
        let started = Instant::now();
        let encoded = match decode_source_request::<LocalFsSourceRequest>(
            event.payload_bytes(),
            source_now_ms(),
        ) {
            Ok(decoded) => encode_source_result(
                &decoded,
                self.validate_execution_context(&decoded)
                    .and_then(|()| {
                        SourceOperationBudget::from_context(
                            decoded.context.as_ref(),
                            source_now_ms(),
                        )
                    })
                    .and_then(|mut budget| self.handle_request(&decoded.request, &mut budget)),
                started.elapsed().as_millis() as u64,
                source_now_ms(),
                |error| {
                    LocalFsSourcePayload::Error(source_kit::error_payload_from_cnx(
                        error,
                        "local-fs-transport",
                    ))
                },
            ),
            Err(error) => encode_msgpack(&LocalFsSourcePayload::Error(
                source_kit::error_payload_from_cnx(error, "local-fs-transport"),
            ))
            .map(|bytes| bytes.to_vec()),
        };
        self.reply_encoded_event(encoded, correlation_id)
            .unwrap_or_else(|err| self.fallback_error_event(err, correlation_id))
    }

    fn handle_request(
        &self,
        request: &LocalFsSourceRequest,
        budget: &mut SourceOperationBudget,
    ) -> Result<LocalFsSourcePayload> {
        if !self.control_state.snapshot()?.fetch_route_active {
            return Err(CnxError::NotReady(
                "local-fs-source fetch route is inactive".into(),
            ));
        }
        match request.operation {
            LocalFsSourceOperation::ProbeConnection => self.handle_probe(request, budget),
            LocalFsSourceOperation::DiscoverFields => self.handle_discover_fields(request, budget),
            LocalFsSourceOperation::SnapshotPage => self.handle_snapshot_page(request, budget),
            LocalFsSourceOperation::SampleJsonl | LocalFsSourceOperation::SampleCsv => {
                if request.cursor.is_some() { return Err(CnxError::InvalidInput("content samples have no continuation".into())); }
                let path = request.path.as_deref().ok_or_else(|| CnxError::InvalidInput("content sample requires exact path".into()))?;
                let config = self.control_state.config()?;
                let endpoint = config.endpoint_for(&request.object_ref, Some(path))?;
                let format = if request.operation == LocalFsSourceOperation::SampleJsonl { "jsonl" } else { "csv" };
                Ok(LocalFsSourcePayload::Content(self.driver.sample_content(&endpoint, path, format, config.effective_limit(request.limit), budget)?))
            }
        }
    }

    fn validate_execution_context(
        &self,
        decoded: &DecodedSourceRequest<LocalFsSourceRequest>,
    ) -> Result<()> {
        let Some(context) = decoded.context.as_ref() else {
            return Ok(());
        };
        validate_source_request_limit(Some(context), decoded.request.limit)?;
        let endpoint = self
            .control_state
            .config()?
            .endpoint_for(&decoded.request.object_ref, decoded.request.path.as_deref())?;
        validate_source_grant_epoch(Some(context), endpoint.grant_epoch, "local-fs")
    }

    fn handle_status_event(&self, event: Event, fetch_route: &str, status_route: &str) -> Event {
        let correlation_id = event.metadata().correlation_id;
        self.reply_event(
            LocalFsSourcePayload::Status(self.status_snapshot(fetch_route, status_route)),
            correlation_id,
        )
        .unwrap_or_else(|err| self.fallback_error_event(err, correlation_id))
    }

    fn status_snapshot(
        &self,
        fetch_route: &str,
        status_route: &str,
    ) -> source_kit::SourceRuntimeStatus {
        let snapshot = match self.control_state.snapshot() {
            Ok(snapshot) => snapshot,
            Err(err) => {
                return source_kit::SourceRuntimeStatus::new(
                    "local-fs",
                    0,
                    0,
                    vec![format!("control state unavailable: {err}")],
                )
                .with_capabilities(local_fs_capabilities());
            }
        };
        let configured_endpoints = snapshot.config.product.endpoints.len();
        let active_grants = snapshot
            .config
            .runtime
            .endpoint_grants
            .iter()
            .filter(|grant| grant.active)
            .count();
        let mut diagnostics = vec![
            "fetch route is request/reply only".to_string(),
            format!("control_version={}", snapshot.control_version),
            format!("fetch_route_active={}", snapshot.fetch_route_active),
            format!("status_route_active={}", snapshot.status_route_active),
            format!("fetch_route={fetch_route}"),
            format!("status_route={status_route}"),
        ];
        if configured_endpoints == 0 {
            diagnostics.push("no configured local-fs endpoints".to_string());
        }
        if active_grants == 0 {
            diagnostics.push("no active runtime local-fs endpoint grants".to_string());
        }
        let mut status = source_kit::SourceRuntimeStatus::new(
            "local-fs",
            configured_endpoints,
            active_grants,
            diagnostics,
        )
        .with_capabilities(local_fs_capabilities());
        if !snapshot.fetch_route_active || !snapshot.status_route_active {
            status.ready = false;
        }
        status
    }

    fn handle_probe(
        &self,
        request: &LocalFsSourceRequest,
        budget: &mut SourceOperationBudget,
    ) -> Result<LocalFsSourcePayload> {
        let config = self.control_state.config()?;
        let endpoint = config.endpoint_for(&request.object_ref, request.path.as_deref())?;
        Ok(LocalFsSourcePayload::Probe(
            self.driver.probe_with_budget(&endpoint, budget)?,
        ))
    }

    fn handle_discover_fields(
        &self,
        request: &LocalFsSourceRequest,
        budget: &mut SourceOperationBudget,
    ) -> Result<LocalFsSourcePayload> {
        let config = self.control_state.config()?;
        let endpoint = config.endpoint_for(&request.object_ref, None)?;
        let mut catalog = self.driver.discover_fields_with_budget(&endpoint, budget)?;
        if catalog.fields.is_empty() {
            catalog.fields = default_local_fs_fields();
        }
        Ok(LocalFsSourcePayload::Fields(catalog))
    }

    fn handle_snapshot_page(
        &self,
        request: &LocalFsSourceRequest,
        budget: &mut SourceOperationBudget,
    ) -> Result<LocalFsSourcePayload> {
        let config = self.control_state.config()?;
        let endpoint = config.endpoint_for(&request.object_ref, request.path.as_deref())?;
        let cursor = request
            .cursor
            .as_ref()
            .map(|LocalFsSourceCursor::Snapshot(cursor)| cursor);
        let limit = config.effective_limit(request.limit);
        let page = self.driver.snapshot_page_with_budget(
            &endpoint,
            request.path.as_deref(),
            limit,
            cursor,
            budget,
        )?;
        Ok(LocalFsSourcePayload::Entries(entry_batch_from_page(
            &endpoint,
            request.path.as_deref(),
            page,
        )))
    }

    fn reply_event(
        &self,
        payload: LocalFsSourcePayload,
        correlation_id: Option<u64>,
    ) -> Result<Event> {
        Ok(Event::new(
            EventMetadata {
                origin_id: self.origin_id.clone(),
                timestamp_us: now_us(),
                logical_ts: None,
                correlation_id,
                ingress_auth: None,
                trace: None,
            },
            encode_msgpack(&payload)?,
        ))
    }

    fn reply_encoded_event(
        &self,
        encoded: Result<Vec<u8>>,
        correlation_id: Option<u64>,
    ) -> Result<Event> {
        Ok(Event::new(
            EventMetadata {
                origin_id: self.origin_id.clone(),
                timestamp_us: now_us(),
                logical_ts: None,
                correlation_id,
                ingress_auth: None,
                trace: None,
            },
            encoded?.into(),
        ))
    }

    fn fallback_error_event(&self, err: CnxError, correlation_id: Option<u64>) -> Event {
        let payload = LocalFsSourcePayload::Error(SourceErrorPayload::new(
            "encode-error",
            err.to_string(),
            false,
        ));
        let bytes = rmp_serde::to_vec_named(&payload).unwrap_or_else(|_| b"{}".to_vec());
        Event::new(
            EventMetadata {
                origin_id: self.origin_id.clone(),
                timestamp_us: now_us(),
                logical_ts: None,
                correlation_id,
                ingress_auth: None,
                trace: None,
            },
            bytes.into(),
        )
    }
}

fn local_fs_capabilities() -> SourceCapabilityReport {
    SourceCapabilityReport::new(
        "local-fs",
        vec![
            SourceOperationCapability::BoundedJsonlContent,
            SourceOperationCapability::BoundedCsvContent,
            SourceOperationCapability::Probe,
            SourceOperationCapability::FieldDiscovery,
            SourceOperationCapability::BoundedSnapshot,
            SourceOperationCapability::PointLookup,
        ],
        SourceConsistencyCapability::StableRevisionCursor,
        SourceContinuationCapability::Cursor,
    )
}

fn entry_batch_from_page(
    endpoint: &ResolvedLocalFsEndpoint,
    path: Option<&str>,
    page: LocalFsEntryPage,
) -> LocalFsEntryBatch {
    LocalFsEntryBatch {
        object_ref: endpoint.object_ref.clone(),
        root_path: endpoint.root_path.clone(),
        path: path.unwrap_or_default().to_string(),
        source_revision: page.source_revision,
        entries: page.entries,
        next_cursor: page.next_cursor.map(LocalFsSourceCursor::Snapshot),
        has_more: page.has_more,
        grant_epoch: endpoint.grant_epoch,
        diagnostics: page.diagnostics,
    }
}

fn default_local_fs_fields() -> Vec<LocalFsFieldDescriptor> {
    [
        ("path", "string"),
        ("file_name", "string"),
        ("is_dir", "bool"),
        ("size", "u64"),
        ("modified_unix_ms", "u64?"),
    ]
    .into_iter()
    .map(|(name, field_type)| LocalFsFieldDescriptor {
        name: name.into(),
        field_type: field_type.into(),
    })
    .collect()
}

fn now_us() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_micros() as u64,
        Err(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use capanix_app_sdk::runtime::ConfigValue;
    use local_fs_source::shared_types::{
        LocalFsEntry, LocalFsFieldCatalog, LocalFsProbeStatus, LocalFsSnapshotCursor,
    };
    use local_fs_source::{
        GrantedLocalFsEndpoint, LocalFsEndpointConfig, LocalFsSourceConfig,
        LocalFsSourceRuntimeInputs,
    };
    use source_kit::decode_msgpack;

    use super::*;

    struct FakeDriver {
        calls: AtomicUsize,
    }

    impl FakeDriver {
        fn new() -> Self {
            Self {
                calls: AtomicUsize::new(0),
            }
        }
    }

    impl LocalFsSourceDriver for FakeDriver {
        fn probe(&self, endpoint: &ResolvedLocalFsEndpoint) -> Result<LocalFsProbeStatus> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(LocalFsProbeStatus {
                object_ref: endpoint.object_ref.clone(),
                root_path: endpoint.root_path.clone(),
                reachable: true,
                diagnostics: Some(format!("grant_epoch={:?}", endpoint.grant_epoch)),
            })
        }

        fn discover_fields(
            &self,
            endpoint: &ResolvedLocalFsEndpoint,
        ) -> Result<LocalFsFieldCatalog> {
            Ok(LocalFsFieldCatalog {
                object_ref: endpoint.object_ref.clone(),
                fields: Vec::new(),
            })
        }

        fn snapshot_page(
            &self,
            _endpoint: &ResolvedLocalFsEndpoint,
            _path: Option<&str>,
            _limit: usize,
            _cursor: Option<&LocalFsSnapshotCursor>,
        ) -> Result<LocalFsEntryPage> {
            Ok(LocalFsEntryPage {
                entries: vec![LocalFsEntry {
                    path: "datasets/a.csv".into(),
                    file_name: "a.csv".into(),
                    is_dir: false,
                    size: 7,
                    modified_unix_ms: None,
                }],
                source_revision: "fs:test-revision".into(),
                next_cursor: None,
                has_more: false,
                diagnostics: None,
            })
        }
    }

    fn test_config() -> LocalFsSourceRuntimeConfig {
        LocalFsSourceRuntimeConfig {
            product: LocalFsSourceConfig {
                endpoints: vec![LocalFsEndpointConfig {
                    object_ref: "local".into(),
                    root_path: "/tmp".into(),
                    path_scopes: vec!["datasets".into(), "datasets/*".into()],
                    active: true,
                }],
                default_page_size: 50,
                max_page_size: 100,
            },
            runtime: LocalFsSourceRuntimeInputs {
                endpoint_grants: vec![GrantedLocalFsEndpoint {
                    object_ref: "local".into(),
                    path_scopes: Vec::new(),
                    interfaces: vec!["read".into()],
                    active: true,
                    grant_epoch: Some(8),
                }],
            },
        }
    }

    fn event_for(request: &LocalFsSourceRequest) -> Event {
        Event::new(
            EventMetadata {
                origin_id: NodeId("caller".into()),
                timestamp_us: 1,
                logical_ts: None,
                correlation_id: Some(99),
                ingress_auth: None,
                trace: None,
            },
            encode_msgpack(request).expect("encode"),
        )
    }

    #[test]
    fn snapshot_page_returns_entries() {
        let driver = Arc::new(FakeDriver::new());
        let service =
            LocalFsSourceService::new(test_config(), driver, NodeId("local-fs-source-test".into()));
        let mut request = LocalFsSourceRequest::snapshot_page("local");
        request.path = Some("datasets".into());

        let reply = service.handle_event(event_for(&request));
        let payload: LocalFsSourcePayload = decode_msgpack(reply.payload_bytes()).expect("payload");

        match payload {
            LocalFsSourcePayload::Entries(batch) => {
                assert_eq!(batch.entries[0].path, "datasets/a.csv");
                assert_eq!(batch.grant_epoch, Some(8));
            }
            other => panic!("expected entries payload, got {other:?}"),
        }
    }

    #[test]
    fn out_of_scope_path_returns_error_payload() {
        let service = LocalFsSourceService::new(
            test_config(),
            Arc::new(FakeDriver::new()),
            NodeId("local-fs-source-test".into()),
        );
        let mut request = LocalFsSourceRequest::snapshot_page("local");
        request.path = Some("secrets".into());

        let reply = service.handle_event(event_for(&request));
        let payload: LocalFsSourcePayload = decode_msgpack(reply.payload_bytes()).expect("payload");

        match payload {
            LocalFsSourcePayload::Error(err) => assert_eq!(err.code, "scope-denied"),
            other => panic!("expected error payload, got {other:?}"),
        }
    }

    #[test]
    fn manifest_config_parser_accepts_local_fs_grants() {
        let string = |value: &str| ConfigValue::String(value.to_string());
        let cfg = std::collections::HashMap::from([
            (
                "endpoints".to_string(),
                ConfigValue::Array(vec![ConfigValue::Map(std::collections::HashMap::from([
                    ("object_ref".to_string(), string("local")),
                    ("root_path".to_string(), string("/tmp")),
                ]))]),
            ),
            (
                "__cnx_runtime".to_string(),
                ConfigValue::Map(std::collections::HashMap::from([(
                    "resource_grants".to_string(),
                    ConfigValue::Array(vec![ConfigValue::Map(std::collections::HashMap::from([
                        ("resource_kind".to_string(), string("local-fs")),
                        ("object_ref".to_string(), string("local")),
                    ]))]),
                )])),
            ),
        ]);

        let parsed = LocalFsSourceRuntimeConfig::from_manifest_config(&cfg).expect("config");

        assert_eq!(parsed.product.endpoints[0].object_ref, "local");
    }
}

use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use capanix_app_sdk::runtime::{EventMetadata, NodeId};
use capanix_app_sdk::{CnxError, Event, Result};
use jsonl_source::shared_types::{
    JsonlRecordBatch, JsonlSourceCursor, JsonlSourceOperation, JsonlSourcePayload,
    JsonlSourceRequest,
};
use jsonl_source::{JsonlSourceRuntimeConfig, ResolvedJsonlEndpoint};
use source_kit::{
    DecodedSourceRequest, SourceCapabilityReport, SourceConsistencyCapability,
    SourceContinuationCapability, SourceControlState, SourceErrorPayload, SourceOperationBudget,
    SourceOperationCapability, decode_source_request, encode_msgpack, encode_source_result,
    source_now_ms, validate_source_grant_epoch, validate_source_request_limit,
};

use crate::driver::{JsonlRecordPage, JsonlSourceDriver};

#[derive(Clone)]
pub struct JsonlSourceService {
    control_state: SourceControlState<JsonlSourceRuntimeConfig>,
    driver: Arc<dyn JsonlSourceDriver>,
    origin_id: NodeId,
}

impl JsonlSourceService {
    pub fn new(
        config: JsonlSourceRuntimeConfig,
        driver: Arc<dyn JsonlSourceDriver>,
        origin_id: NodeId,
    ) -> Self {
        Self::with_control_state(SourceControlState::new(config), driver, origin_id)
    }

    pub fn with_control_state(
        control_state: SourceControlState<JsonlSourceRuntimeConfig>,
        driver: Arc<dyn JsonlSourceDriver>,
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
        let encoded = match decode_source_request::<JsonlSourceRequest>(
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
                    JsonlSourcePayload::Error(source_kit::error_payload_from_cnx(
                        error,
                        "jsonl-transport",
                    ))
                },
            ),
            Err(error) => encode_msgpack(&JsonlSourcePayload::Error(
                source_kit::error_payload_from_cnx(error, "jsonl-transport"),
            ))
            .map(|bytes| bytes.to_vec()),
        };
        self.reply_encoded_event(encoded, correlation_id)
            .unwrap_or_else(|err| self.fallback_error_event(err, correlation_id))
    }

    fn handle_request(
        &self,
        request: &JsonlSourceRequest,
        budget: &mut SourceOperationBudget,
    ) -> Result<JsonlSourcePayload> {
        if !self.control_state.snapshot()?.fetch_route_active {
            return Err(CnxError::NotReady(
                "jsonl-source fetch route is inactive".into(),
            ));
        }
        match request.operation {
            JsonlSourceOperation::ProbeConnection => self.handle_probe(request, budget),
            JsonlSourceOperation::DiscoverFields => self.handle_discover_fields(request, budget),
            JsonlSourceOperation::SnapshotPage => self.handle_snapshot_page(request, budget),
        }
    }

    fn validate_execution_context(
        &self,
        decoded: &DecodedSourceRequest<JsonlSourceRequest>,
    ) -> Result<()> {
        let Some(context) = decoded.context.as_ref() else {
            return Ok(());
        };
        validate_source_request_limit(Some(context), decoded.request.limit)?;
        let path = match decoded.request.operation {
            JsonlSourceOperation::ProbeConnection => decoded.request.path.as_deref(),
            JsonlSourceOperation::DiscoverFields | JsonlSourceOperation::SnapshotPage => {
                Some(required_path(decoded.request.path.as_deref())?)
            }
        };
        let endpoint = self
            .control_state
            .config()?
            .endpoint_for(&decoded.request.object_ref, path)?;
        validate_source_grant_epoch(Some(context), endpoint.grant_epoch, "jsonl")
    }

    fn handle_status_event(&self, event: Event, fetch_route: &str, status_route: &str) -> Event {
        let correlation_id = event.metadata().correlation_id;
        self.reply_event(
            JsonlSourcePayload::Status(self.status_snapshot(fetch_route, status_route)),
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
                    "jsonl",
                    0,
                    0,
                    vec![format!("control state unavailable: {err}")],
                )
                .with_capabilities(jsonl_capabilities());
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
            diagnostics.push("no configured JSONL endpoints".to_string());
        }
        if active_grants == 0 {
            diagnostics.push("no active runtime JSONL endpoint grants".to_string());
        }
        let mut status = source_kit::SourceRuntimeStatus::new(
            "jsonl",
            configured_endpoints,
            active_grants,
            diagnostics,
        )
        .with_capabilities(jsonl_capabilities());
        if !snapshot.fetch_route_active || !snapshot.status_route_active {
            status.ready = false;
        }
        status
    }

    fn handle_probe(
        &self,
        request: &JsonlSourceRequest,
        budget: &mut SourceOperationBudget,
    ) -> Result<JsonlSourcePayload> {
        let config = self.control_state.config()?;
        let endpoint = config.endpoint_for(&request.object_ref, request.path.as_deref())?;
        Ok(JsonlSourcePayload::Probe(
            self.driver.probe_with_budget(&endpoint, budget)?,
        ))
    }

    fn handle_discover_fields(
        &self,
        request: &JsonlSourceRequest,
        budget: &mut SourceOperationBudget,
    ) -> Result<JsonlSourcePayload> {
        let path = required_path(request.path.as_deref())?;
        let config = self.control_state.config()?;
        let endpoint = config.endpoint_for(&request.object_ref, Some(path))?;
        Ok(JsonlSourcePayload::Fields(
            self.driver
                .discover_fields_with_budget(&endpoint, path, budget)?,
        ))
    }

    fn handle_snapshot_page(
        &self,
        request: &JsonlSourceRequest,
        budget: &mut SourceOperationBudget,
    ) -> Result<JsonlSourcePayload> {
        let path = required_path(request.path.as_deref())?;
        let config = self.control_state.config()?;
        let endpoint = config.endpoint_for(&request.object_ref, Some(path))?;
        let cursor = request
            .cursor
            .as_ref()
            .map(|JsonlSourceCursor::Snapshot(cursor)| cursor);
        let limit = config.effective_limit(request.limit);
        let page = self
            .driver
            .snapshot_page_with_budget(&endpoint, path, limit, cursor, budget)?;
        Ok(JsonlSourcePayload::Records(record_batch_from_page(
            &endpoint, path, page,
        )))
    }

    fn reply_event(
        &self,
        payload: JsonlSourcePayload,
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
        let payload = JsonlSourcePayload::Error(SourceErrorPayload::new(
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

fn jsonl_capabilities() -> SourceCapabilityReport {
    SourceCapabilityReport::new(
        "jsonl",
        vec![
            SourceOperationCapability::Probe,
            SourceOperationCapability::FieldDiscovery,
            SourceOperationCapability::BoundedSnapshot,
        ],
        SourceConsistencyCapability::StableRevisionCursor,
        SourceContinuationCapability::Cursor,
    )
}

fn record_batch_from_page(
    endpoint: &ResolvedJsonlEndpoint,
    path: &str,
    page: JsonlRecordPage,
) -> JsonlRecordBatch {
    JsonlRecordBatch {
        object_ref: endpoint.object_ref.clone(),
        root_path: endpoint.root_path.clone(),
        path: path.to_string(),
        source_revision: page.source_revision,
        records: page.records,
        next_cursor: page.next_cursor.map(JsonlSourceCursor::Snapshot),
        has_more: page.has_more,
        grant_epoch: endpoint.grant_epoch,
        diagnostics: page.diagnostics,
    }
}

fn required_path(value: Option<&str>) -> Result<&str> {
    value
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| CnxError::InvalidInput("path is required".into()))
}

fn now_us() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_micros() as u64,
        Err(_) => 0,
    }
}

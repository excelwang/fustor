use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use capanix_app_sdk::runtime::{EventMetadata, NodeId};
use capanix_app_sdk::{CnxError, Event, Result};
use csv_source::shared_types::{
    CsvRecordBatch, CsvSourceCursor, CsvSourceOperation, CsvSourcePayload, CsvSourceRequest,
};
use csv_source::{CsvSourceRuntimeConfig, ResolvedCsvEndpoint};
use source_kit::{
    DecodedSourceRequest, SourceCapabilityReport, SourceConsistencyCapability,
    SourceContinuationCapability, SourceControlState, SourceErrorPayload, SourceOperationBudget,
    SourceOperationCapability, decode_source_request, encode_msgpack, encode_source_result,
    source_now_ms, validate_source_grant_epoch, validate_source_request_limit,
};

use crate::driver::{CsvRecordPage, CsvSourceDriver};

#[derive(Clone)]
pub struct CsvSourceService {
    control_state: SourceControlState<CsvSourceRuntimeConfig>,
    driver: Arc<dyn CsvSourceDriver>,
    origin_id: NodeId,
}

impl CsvSourceService {
    pub fn new(
        config: CsvSourceRuntimeConfig,
        driver: Arc<dyn CsvSourceDriver>,
        origin_id: NodeId,
    ) -> Self {
        Self::with_control_state(SourceControlState::new(config), driver, origin_id)
    }

    pub fn with_control_state(
        control_state: SourceControlState<CsvSourceRuntimeConfig>,
        driver: Arc<dyn CsvSourceDriver>,
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
        let encoded =
            match decode_source_request::<CsvSourceRequest>(event.payload_bytes(), source_now_ms())
            {
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
                        CsvSourcePayload::Error(source_kit::error_payload_from_cnx(
                            error,
                            "csv-transport",
                        ))
                    },
                ),
                Err(error) => encode_msgpack(&CsvSourcePayload::Error(
                    source_kit::error_payload_from_cnx(error, "csv-transport"),
                ))
                .map(|bytes| bytes.to_vec()),
            };
        self.reply_encoded_event(encoded, correlation_id)
            .unwrap_or_else(|err| self.fallback_error_event(err, correlation_id))
    }

    fn handle_request(
        &self,
        request: &CsvSourceRequest,
        budget: &mut SourceOperationBudget,
    ) -> Result<CsvSourcePayload> {
        if !self.control_state.snapshot()?.fetch_route_active {
            return Err(CnxError::NotReady(
                "csv-source fetch route is inactive".into(),
            ));
        }
        match request.operation {
            CsvSourceOperation::ProbeConnection => self.handle_probe(request, budget),
            CsvSourceOperation::DiscoverFields => self.handle_discover_fields(request, budget),
            CsvSourceOperation::SnapshotPage => self.handle_snapshot_page(request, budget),
        }
    }

    fn validate_execution_context(
        &self,
        decoded: &DecodedSourceRequest<CsvSourceRequest>,
    ) -> Result<()> {
        let Some(context) = decoded.context.as_ref() else {
            return Ok(());
        };
        validate_source_request_limit(Some(context), decoded.request.limit)?;
        let path = match decoded.request.operation {
            CsvSourceOperation::ProbeConnection => decoded.request.path.as_deref(),
            CsvSourceOperation::DiscoverFields | CsvSourceOperation::SnapshotPage => {
                Some(required_path(decoded.request.path.as_deref())?)
            }
        };
        let endpoint = self
            .control_state
            .config()?
            .endpoint_for(&decoded.request.object_ref, path)?;
        validate_source_grant_epoch(Some(context), endpoint.grant_epoch, "csv")
    }

    fn handle_status_event(&self, event: Event, fetch_route: &str, status_route: &str) -> Event {
        let correlation_id = event.metadata().correlation_id;
        self.reply_event(
            CsvSourcePayload::Status(self.status_snapshot(fetch_route, status_route)),
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
                    "csv",
                    0,
                    0,
                    vec![format!("control state unavailable: {err}")],
                )
                .with_capabilities(csv_capabilities());
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
            diagnostics.push("no configured CSV endpoints".to_string());
        }
        if active_grants == 0 {
            diagnostics.push("no active runtime CSV endpoint grants".to_string());
        }
        let mut status = source_kit::SourceRuntimeStatus::new(
            "csv",
            configured_endpoints,
            active_grants,
            diagnostics,
        )
        .with_capabilities(csv_capabilities());
        if !snapshot.fetch_route_active || !snapshot.status_route_active {
            status.ready = false;
        }
        status
    }

    fn handle_probe(
        &self,
        request: &CsvSourceRequest,
        budget: &mut SourceOperationBudget,
    ) -> Result<CsvSourcePayload> {
        let config = self.control_state.config()?;
        let endpoint = config.endpoint_for(&request.object_ref, request.path.as_deref())?;
        Ok(CsvSourcePayload::Probe(
            self.driver.probe_with_budget(&endpoint, budget)?,
        ))
    }

    fn handle_discover_fields(
        &self,
        request: &CsvSourceRequest,
        budget: &mut SourceOperationBudget,
    ) -> Result<CsvSourcePayload> {
        let path = required_path(request.path.as_deref())?;
        let config = self.control_state.config()?;
        let endpoint = config.endpoint_for(&request.object_ref, Some(path))?;
        Ok(CsvSourcePayload::Fields(
            self.driver
                .discover_fields_with_budget(&endpoint, path, budget)?,
        ))
    }

    fn handle_snapshot_page(
        &self,
        request: &CsvSourceRequest,
        budget: &mut SourceOperationBudget,
    ) -> Result<CsvSourcePayload> {
        let path = required_path(request.path.as_deref())?;
        let config = self.control_state.config()?;
        let endpoint = config.endpoint_for(&request.object_ref, Some(path))?;
        let cursor = request
            .cursor
            .as_ref()
            .map(|CsvSourceCursor::Snapshot(cursor)| cursor);
        let limit = config.effective_limit(request.limit);
        let page = self
            .driver
            .snapshot_page_with_budget(&endpoint, path, limit, cursor, budget)?;
        Ok(CsvSourcePayload::Records(record_batch_from_page(
            &endpoint, path, page,
        )))
    }

    fn reply_event(&self, payload: CsvSourcePayload, correlation_id: Option<u64>) -> Result<Event> {
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
        let payload = CsvSourcePayload::Error(SourceErrorPayload::new(
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

fn csv_capabilities() -> SourceCapabilityReport {
    SourceCapabilityReport::new(
        "csv",
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
    endpoint: &ResolvedCsvEndpoint,
    path: &str,
    page: CsvRecordPage,
) -> CsvRecordBatch {
    CsvRecordBatch {
        object_ref: endpoint.object_ref.clone(),
        root_path: endpoint.root_path.clone(),
        path: path.to_string(),
        source_revision: page.source_revision,
        fields: page.fields,
        records: page.records,
        next_cursor: page.next_cursor.map(CsvSourceCursor::Snapshot),
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

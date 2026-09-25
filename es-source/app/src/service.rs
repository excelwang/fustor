use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use capanix_app_sdk::runtime::{EventMetadata, NodeId};
use capanix_app_sdk::{CnxError, Event, Result};
use es_source::shared_types::{
    EsSnapshotCursor, EsSourceBatch, EsSourceCursor, EsSourceErrorPayload, EsSourceOperation,
    EsSourcePayload, EsSourceRequest, EsSourceStatus, encode_msgpack,
};
use es_source::{EsSourceRuntimeConfig, ResolvedCredential, ResolvedEsEndpoint};
use serde_json::Value;

use crate::driver::EsSourceDriver;
use source_kit::{
    DecodedSourceRequest, SourceCapabilityReport, SourceConsistencyCapability,
    SourceContinuationCapability, SourceControlChange, SourceControlState,
    SourceControlSubscription, SourceOperationBudget, SourceOperationCapability,
    decode_source_request, encode_accounted_source_result, source_now_ms,
    validate_source_grant_epoch, validate_source_request_limit,
};
use uuid::Uuid;

const MAX_ACTIVE_SNAPSHOT_SESSIONS: usize = 128;

#[derive(Clone)]
pub struct EsSourceService {
    control_state: SourceControlState<EsSourceRuntimeConfig>,
    driver: Arc<dyn EsSourceDriver>,
    origin_id: NodeId,
    sessions: Arc<SnapshotSessionRegistry>,
    _control_subscription: Option<SourceControlSubscription>,
}

struct SnapshotSessionEntry {
    object_ref: String,
    index: String,
    endpoint: ResolvedEsEndpoint,
    credential: ResolvedCredential,
    native_pit_id: String,
    source_revision: String,
    expected_search_after: Vec<Value>,
    expires_at_unix_ms: u64,
    field_filter: Vec<String>,
    query_filter: Option<Value>,
}

struct SnapshotSessionRegistry {
    entries: Arc<Mutex<HashMap<String, SnapshotSessionEntry>>>,
    driver: Arc<dyn EsSourceDriver>,
    janitor_control: Arc<(Mutex<bool>, Condvar)>,
    janitor: Mutex<Option<JoinHandle<()>>>,
}

impl SnapshotSessionRegistry {
    fn new(driver: Arc<dyn EsSourceDriver>) -> Arc<Self> {
        let entries = Arc::new(Mutex::new(HashMap::new()));
        let janitor_control = Arc::new((Mutex::new(false), Condvar::new()));
        let janitor_entries = entries.clone();
        let janitor_driver = driver.clone();
        let thread_control = janitor_control.clone();
        let janitor = thread::Builder::new()
            .name("es-source-session-expiry".into())
            .spawn(move || es_session_expiry_loop(janitor_entries, janitor_driver, thread_control))
            .expect("start Elasticsearch source session expiry worker");
        Arc::new(Self {
            entries,
            driver,
            janitor_control,
            janitor: Mutex::new(Some(janitor)),
        })
    }

    fn insert(&self, session_id: String, entry: SnapshotSessionEntry) -> Result<()> {
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| CnxError::Internal("es snapshot session lock poisoned".into()))?;
        if entries.len() >= MAX_ACTIVE_SNAPSHOT_SESSIONS && !entries.contains_key(&session_id) {
            drop(entries);
            let _ = self.driver.close_snapshot(
                &entry.endpoint,
                &entry.credential,
                &entry.native_pit_id,
            );
            return Err(CnxError::ResourceExhausted(format!(
                "elasticsearch active snapshot session limit {MAX_ACTIVE_SNAPSHOT_SESSIONS} reached"
            )));
        }
        entries.insert(session_id, entry);
        drop(entries);
        self.janitor_control.1.notify_one();
        Ok(())
    }

    fn take(&self, session_id: &str) -> Result<SnapshotSessionEntry> {
        self.entries
            .lock()
            .map_err(|_| CnxError::Internal("es snapshot session lock poisoned".into()))?
            .remove(session_id)
            .ok_or_else(|| {
                CnxError::NotReady("elasticsearch snapshot session is not active".into())
            })
    }

    fn active_count(&self) -> usize {
        self.entries
            .lock()
            .map(|entries| entries.len())
            .unwrap_or(0)
    }

    fn close_all(&self) {
        let Ok(mut entries) = self.entries.lock() else {
            return;
        };
        for (_, entry) in entries.drain() {
            let _ = self.driver.close_snapshot(
                &entry.endpoint,
                &entry.credential,
                &entry.native_pit_id,
            );
        }
    }
}

impl Drop for SnapshotSessionRegistry {
    fn drop(&mut self) {
        let (shutdown, wake) = &*self.janitor_control;
        *shutdown.lock().unwrap_or_else(|err| err.into_inner()) = true;
        wake.notify_all();
        if let Some(janitor) = self
            .janitor
            .get_mut()
            .unwrap_or_else(|err| err.into_inner())
            .take()
        {
            let _ = janitor.join();
        }
        let mut entries = self.entries.lock().unwrap_or_else(|err| err.into_inner());
        for (_, entry) in entries.drain() {
            let _ = self.driver.close_snapshot(
                &entry.endpoint,
                &entry.credential,
                &entry.native_pit_id,
            );
        }
    }
}

fn es_session_expiry_loop(
    entries: Arc<Mutex<HashMap<String, SnapshotSessionEntry>>>,
    driver: Arc<dyn EsSourceDriver>,
    control: Arc<(Mutex<bool>, Condvar)>,
) {
    loop {
        let (expired, wait) = {
            let Ok(mut entries) = entries.lock() else {
                return;
            };
            let now = now_ms();
            let expired_ids = entries
                .iter()
                .filter(|(_, entry)| entry.expires_at_unix_ms <= now)
                .map(|(session_id, _)| session_id.clone())
                .collect::<Vec<_>>();
            let expired = expired_ids
                .into_iter()
                .filter_map(|session_id| entries.remove(&session_id))
                .collect::<Vec<_>>();
            let wait = entries
                .values()
                .map(|entry| entry.expires_at_unix_ms.saturating_sub(now))
                .min()
                .map(Duration::from_millis)
                .unwrap_or_else(|| Duration::from_secs(60));
            (expired, wait)
        };
        for entry in expired {
            let _ = driver.close_snapshot(&entry.endpoint, &entry.credential, &entry.native_pit_id);
        }

        let (shutdown, wake) = &*control;
        let Ok(shutdown) = shutdown.lock() else {
            return;
        };
        if *shutdown {
            return;
        }
        let Ok((shutdown, _)) = wake.wait_timeout(shutdown, wait.max(Duration::from_millis(1)))
        else {
            return;
        };
        if *shutdown {
            return;
        }
    }
}

impl EsSourceService {
    pub fn new(
        config: EsSourceRuntimeConfig,
        driver: Arc<dyn EsSourceDriver>,
        origin_id: NodeId,
    ) -> Self {
        Self::with_control_state(SourceControlState::new(config), driver, origin_id)
    }

    pub fn with_control_state(
        control_state: SourceControlState<EsSourceRuntimeConfig>,
        driver: Arc<dyn EsSourceDriver>,
        origin_id: NodeId,
    ) -> Self {
        let sessions = SnapshotSessionRegistry::new(driver.clone());
        let weak_sessions = Arc::downgrade(&sessions);
        let control_subscription = control_state
            .subscribe(move |change| {
                let should_close = matches!(
                    change,
                    SourceControlChange::ConfigReplaced { .. }
                        | SourceControlChange::FetchRouteActive { active: false }
                );
                if should_close && let Some(sessions) = weak_sessions.upgrade() {
                    sessions.close_all();
                }
            })
            .ok();
        Self {
            control_state,
            driver,
            origin_id,
            sessions,
            _control_subscription: control_subscription,
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
        let encoded = match decode_source_request::<EsSourceRequest>(
            event.payload_bytes(),
            source_now_ms(),
        ) {
            Ok(decoded) => encode_accounted_source_result(
                &decoded,
                self.validate_execution_context(&decoded)
                    .and_then(|()| {
                        SourceOperationBudget::from_context(
                            decoded.context.as_ref(),
                            source_now_ms(),
                        )
                    })
                    .and_then(|mut budget| {
                        let payload = self.handle_request(&decoded.request, &mut budget)?;
                        Ok((payload, budget.accounting()))
                    }),
                started.elapsed().as_millis() as u64,
                source_now_ms(),
                error_payload_from_cnx,
            ),
            Err(error) => {
                encode_msgpack(&error_payload_from_cnx(error)).map(|bytes| bytes.to_vec())
            }
        };
        self.reply_encoded_event(encoded, correlation_id)
            .unwrap_or_else(|err| self.fallback_error_event(err, correlation_id))
    }

    fn handle_request(
        &self,
        request: &EsSourceRequest,
        budget: &mut SourceOperationBudget,
    ) -> Result<EsSourcePayload> {
        if !self.control_state.snapshot()?.fetch_route_active {
            self.sessions.close_all();
            return Err(CnxError::NotReady(
                "es-source fetch route is inactive".into(),
            ));
        }
        validate_request(request)?;
        let config = self.control_state.config()?;
        let endpoint = config.endpoint_for(&request.object_ref, &request.index)?;
        let credential = config.resolve_credential(endpoint.credential_ref.as_deref())?;
        let limit = config.effective_limit(request.limit);
        match request.operation {
            EsSourceOperation::SnapshotPage => {
                self.handle_snapshot_page(request, endpoint, credential, limit, budget)
            }
            EsSourceOperation::SnapshotClose => {
                self.handle_snapshot_close(request, endpoint, credential, budget)
            }
            EsSourceOperation::ProbeConnection
            | EsSourceOperation::DiscoverFields
            | EsSourceOperation::PollSince => {
                self.execute_driver(&endpoint, &credential, request, limit, budget)
            }
        }
    }

    fn execute_driver(
        &self,
        endpoint: &ResolvedEsEndpoint,
        credential: &ResolvedCredential,
        request: &EsSourceRequest,
        limit: usize,
        budget: &mut SourceOperationBudget,
    ) -> Result<EsSourcePayload> {
        self.driver
            .execute_with_budget(endpoint, credential, request, limit, budget)
            .map_err(|err| match err {
                CnxError::AccessDenied(_)
                | CnxError::InvalidInput(_)
                | CnxError::ScopeDenied(_)
                | CnxError::PeerError(_)
                | CnxError::LinkError(_)
                | CnxError::ResourceExhausted(_)
                | CnxError::Timeout => err,
                other => CnxError::PeerError(other.to_string()),
            })
    }

    fn handle_snapshot_page(
        &self,
        request: &EsSourceRequest,
        endpoint: ResolvedEsEndpoint,
        credential: ResolvedCredential,
        limit: usize,
        budget: &mut SourceOperationBudget,
    ) -> Result<EsSourcePayload> {
        let Some(EsSourceCursor::Snapshot(cursor)) = request.cursor.as_ref() else {
            if request.cursor.is_some() {
                return Err(CnxError::InvalidInput(
                    "snapshot request received non-snapshot cursor".into(),
                ));
            }
            let payload = self.execute_driver(&endpoint, &credential, request, limit, budget)?;
            return self.register_open_snapshot(request, endpoint, credential, payload);
        };
        let session_id = cursor.pit_id.clone();
        let entry = self.take_session(&session_id)?;
        if let Err(error) = validate_snapshot_cursor(request, cursor, &entry, true) {
            let _ = self.driver.close_snapshot_with_timeout(
                &entry.endpoint,
                &entry.credential,
                &entry.native_pit_id,
                budget.remaining_timeout().ok().flatten(),
            );
            return Err(error);
        }
        if endpoint != entry.endpoint || credential != entry.credential {
            let _ = self.driver.close_snapshot_with_timeout(
                &entry.endpoint,
                &entry.credential,
                &entry.native_pit_id,
                budget.remaining_timeout().ok().flatten(),
            );
            return Err(CnxError::AccessDenied(
                "elasticsearch snapshot authority or grant changed during continuation".into(),
            ));
        }
        if entry.expires_at_unix_ms <= now_ms() {
            let _ = self.driver.close_snapshot_with_timeout(
                &entry.endpoint,
                &entry.credential,
                &entry.native_pit_id,
                budget.remaining_timeout().ok().flatten(),
            );
            return Err(CnxError::NotReady(
                "elasticsearch snapshot session expired".into(),
            ));
        }
        let mut native_request = request.clone();
        native_request.cursor = Some(EsSourceCursor::Snapshot(EsSnapshotCursor {
            pit_id: entry.native_pit_id.clone(),
            search_after: entry.expected_search_after.clone(),
            expires_at_unix_ms: Some(entry.expires_at_unix_ms),
        }));
        let payload =
            match self.execute_driver(&endpoint, &credential, &native_request, limit, budget) {
                Ok(payload) => payload,
                Err(error) => {
                    let _ = self.driver.close_snapshot_with_timeout(
                        &entry.endpoint,
                        &entry.credential,
                        &entry.native_pit_id,
                        budget.remaining_timeout().ok().flatten(),
                    );
                    return Err(error);
                }
            };
        self.continue_snapshot(session_id, entry, payload, budget.remaining_timeout()?)
    }

    fn register_open_snapshot(
        &self,
        request: &EsSourceRequest,
        endpoint: ResolvedEsEndpoint,
        credential: ResolvedCredential,
        payload: EsSourcePayload,
    ) -> Result<EsSourcePayload> {
        let EsSourcePayload::Batch(mut batch) = payload else {
            return Err(CnxError::ProtocolViolation(
                "elasticsearch snapshot returned a non-batch payload".into(),
            ));
        };
        if !batch.has_more {
            return Ok(EsSourcePayload::Batch(batch));
        }
        if batch.source_revision.trim().is_empty() {
            return Err(CnxError::ProtocolViolation(
                "elasticsearch snapshot is missing its source-native revision".into(),
            ));
        }
        let native_cursor = take_snapshot_cursor(&mut batch)?;
        let session_id = Uuid::new_v4().to_string();
        let expires_at_unix_ms = native_cursor
            .expires_at_unix_ms
            .ok_or_else(|| CnxError::ProtocolViolation("snapshot cursor missing expiry".into()))?;
        self.sessions.insert(
            session_id.clone(),
            SnapshotSessionEntry {
                object_ref: request.object_ref.clone(),
                index: request.index.clone(),
                endpoint,
                credential,
                native_pit_id: native_cursor.pit_id,
                source_revision: batch.source_revision.clone(),
                expected_search_after: native_cursor.search_after.clone(),
                expires_at_unix_ms,
                field_filter: request.field_filter.clone(),
                query_filter: request.query_filter.clone(),
            },
        )?;
        batch.next_cursor = Some(EsSourceCursor::Snapshot(EsSnapshotCursor {
            pit_id: session_id,
            search_after: native_cursor.search_after,
            expires_at_unix_ms: Some(expires_at_unix_ms),
        }));
        Ok(EsSourcePayload::Batch(batch))
    }

    fn continue_snapshot(
        &self,
        session_id: String,
        mut entry: SnapshotSessionEntry,
        payload: EsSourcePayload,
        timeout: Option<Duration>,
    ) -> Result<EsSourcePayload> {
        let EsSourcePayload::Batch(mut batch) = payload else {
            let _ = self.driver.close_snapshot_with_timeout(
                &entry.endpoint,
                &entry.credential,
                &entry.native_pit_id,
                timeout,
            );
            return Err(CnxError::ProtocolViolation(
                "elasticsearch snapshot continuation returned a non-batch payload".into(),
            ));
        };
        if batch.source_revision != entry.source_revision {
            let _ = self.driver.close_snapshot_with_timeout(
                &entry.endpoint,
                &entry.credential,
                &entry.native_pit_id,
                timeout,
            );
            return Err(CnxError::ProtocolViolation(
                "elasticsearch snapshot source-native revision changed during continuation".into(),
            ));
        }
        if batch.has_more {
            let native_cursor = match take_snapshot_cursor(&mut batch) {
                Ok(cursor) => cursor,
                Err(error) => {
                    let _ = self.driver.close_snapshot_with_timeout(
                        &entry.endpoint,
                        &entry.credential,
                        &entry.native_pit_id,
                        timeout,
                    );
                    return Err(error);
                }
            };
            entry.native_pit_id = native_cursor.pit_id;
            entry.expected_search_after = native_cursor.search_after.clone();
            let Some(expires_at_unix_ms) = native_cursor.expires_at_unix_ms else {
                let _ = self.driver.close_snapshot_with_timeout(
                    &entry.endpoint,
                    &entry.credential,
                    &entry.native_pit_id,
                    timeout,
                );
                return Err(CnxError::ProtocolViolation(
                    "snapshot cursor missing expiry".into(),
                ));
            };
            entry.expires_at_unix_ms = expires_at_unix_ms;
            batch.next_cursor = Some(EsSourceCursor::Snapshot(EsSnapshotCursor {
                pit_id: session_id.clone(),
                search_after: native_cursor.search_after,
                expires_at_unix_ms: Some(entry.expires_at_unix_ms),
            }));
            self.sessions.insert(session_id, entry)?;
        }
        Ok(EsSourcePayload::Batch(batch))
    }

    fn handle_snapshot_close(
        &self,
        request: &EsSourceRequest,
        endpoint: ResolvedEsEndpoint,
        credential: ResolvedCredential,
        budget: &mut SourceOperationBudget,
    ) -> Result<EsSourcePayload> {
        let Some(EsSourceCursor::Snapshot(cursor)) = request.cursor.as_ref() else {
            return Err(CnxError::InvalidInput(
                "snapshot close cursor is required".into(),
            ));
        };
        let entry = self.take_session(&cursor.pit_id)?;
        if let Err(error) = validate_snapshot_cursor(request, cursor, &entry, false) {
            let _ = self.driver.close_snapshot_with_timeout(
                &entry.endpoint,
                &entry.credential,
                &entry.native_pit_id,
                budget.remaining_timeout().ok().flatten(),
            );
            return Err(error);
        }
        if endpoint != entry.endpoint || credential != entry.credential {
            let _ = self.driver.close_snapshot_with_timeout(
                &entry.endpoint,
                &entry.credential,
                &entry.native_pit_id,
                budget.remaining_timeout().ok().flatten(),
            );
            return Err(CnxError::AccessDenied(
                "elasticsearch snapshot authority or grant changed before close".into(),
            ));
        }
        self.driver.close_snapshot_with_budget(
            &entry.endpoint,
            &entry.credential,
            &entry.native_pit_id,
            budget,
        )?;
        Ok(EsSourcePayload::Batch(EsSourceBatch {
            object_ref: entry.object_ref,
            index: entry.index,
            source_revision: entry.source_revision,
            documents: Vec::new(),
            next_cursor: None,
            has_more: false,
            grant_epoch: entry.endpoint.grant_epoch,
            diagnostics: Some("point-in-time snapshot session closed".into()),
        }))
    }

    fn take_session(&self, session_id: &str) -> Result<SnapshotSessionEntry> {
        self.sessions.take(session_id)
    }

    fn validate_execution_context(
        &self,
        decoded: &DecodedSourceRequest<EsSourceRequest>,
    ) -> Result<()> {
        let Some(context) = decoded.context.as_ref() else {
            return Ok(());
        };
        validate_source_request_limit(Some(context), decoded.request.limit)?;
        validate_request(&decoded.request)?;
        let endpoint = self
            .control_state
            .config()?
            .endpoint_for(&decoded.request.object_ref, &decoded.request.index)?;
        validate_source_grant_epoch(Some(context), endpoint.grant_epoch, "elasticsearch")
    }

    fn handle_status_event(&self, event: Event, fetch_route: &str, status_route: &str) -> Event {
        let correlation_id = event.metadata().correlation_id;
        self.reply_event(
            EsSourcePayload::Status(self.status_snapshot(fetch_route, status_route)),
            correlation_id,
        )
        .unwrap_or_else(|err| self.fallback_error_event(err, correlation_id))
    }

    fn status_snapshot(&self, fetch_route: &str, status_route: &str) -> EsSourceStatus {
        let snapshot = match self.control_state.snapshot() {
            Ok(snapshot) => snapshot,
            Err(err) => {
                return EsSourceStatus {
                    node_id: self.origin_id.0.clone(),
                    fetch_route: fetch_route.to_string(),
                    status_route: status_route.to_string(),
                    configured_endpoints: 0,
                    active_grants: 0,
                    ready: false,
                    capabilities: es_capabilities(),
                    diagnostics: vec![format!("control state unavailable: {err}")],
                };
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
            format!("active_snapshot_sessions={}", self.sessions.active_count()),
            format!("max_snapshot_sessions={MAX_ACTIVE_SNAPSHOT_SESSIONS}"),
        ];
        if configured_endpoints == 0 {
            diagnostics.push("no configured ES endpoints".to_string());
        }
        if active_grants == 0 {
            diagnostics.push("no active runtime ES endpoint grants".to_string());
        }
        EsSourceStatus {
            node_id: self.origin_id.0.clone(),
            fetch_route: fetch_route.to_string(),
            status_route: status_route.to_string(),
            configured_endpoints,
            active_grants,
            ready: configured_endpoints > 0
                && active_grants > 0
                && snapshot.fetch_route_active
                && snapshot.status_route_active,
            capabilities: es_capabilities(),
            diagnostics,
        }
    }

    fn reply_event(&self, payload: EsSourcePayload, correlation_id: Option<u64>) -> Result<Event> {
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
        let payload = EsSourcePayload::Error(EsSourceErrorPayload::new(
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

pub fn error_payload_from_cnx(err: CnxError) -> EsSourcePayload {
    let (code, retryable) = match &err {
        CnxError::AccessDenied(_) => ("access-denied", false),
        CnxError::ScopeDenied(_) => ("scope-denied", false),
        CnxError::InvalidInput(_) | CnxError::ProtocolViolation(_) => ("invalid-request", false),
        CnxError::Timeout
        | CnxError::Backpressure
        | CnxError::ChannelClosed
        | CnxError::NotReady(_) => ("transient-runtime", true),
        CnxError::LinkError(_) | CnxError::TransportClosed(_) => ("es-transport", true),
        CnxError::PeerError(_) => ("es-peer", false),
        _ => ("internal", true),
    };
    EsSourcePayload::Error(EsSourceErrorPayload::new(code, err.to_string(), retryable))
}

fn es_capabilities() -> SourceCapabilityReport {
    SourceCapabilityReport::new(
        "elasticsearch",
        vec![
            SourceOperationCapability::Probe,
            SourceOperationCapability::FieldDiscovery,
            SourceOperationCapability::BoundedSnapshot,
            SourceOperationCapability::IncrementalPoll,
            SourceOperationCapability::ExplicitSessionClose,
        ],
        SourceConsistencyCapability::PointInTime,
        SourceContinuationCapability::NativeSession,
    )
}

fn validate_request(request: &EsSourceRequest) -> Result<()> {
    validate_index(&request.index)?;
    if let Some(es_source::EsSourceCursor::Snapshot(cursor)) = &request.cursor
        && cursor.pit_id.trim().is_empty()
    {
        return Err(CnxError::InvalidInput(
            "snapshot cursor pit_id must be non-empty".into(),
        ));
    }
    if request.operation == EsSourceOperation::SnapshotClose
        && !matches!(request.cursor, Some(EsSourceCursor::Snapshot(_)))
    {
        return Err(CnxError::InvalidInput(
            "snapshot close cursor is required".into(),
        ));
    }
    Ok(())
}

fn take_snapshot_cursor(batch: &mut EsSourceBatch) -> Result<EsSnapshotCursor> {
    match batch.next_cursor.take() {
        Some(EsSourceCursor::Snapshot(cursor)) if !cursor.pit_id.trim().is_empty() => Ok(cursor),
        _ => Err(CnxError::ProtocolViolation(
            "elasticsearch snapshot page with has_more=true missing native cursor".into(),
        )),
    }
}

fn validate_snapshot_cursor(
    request: &EsSourceRequest,
    cursor: &EsSnapshotCursor,
    entry: &SnapshotSessionEntry,
    validate_query: bool,
) -> Result<()> {
    if request.object_ref != entry.object_ref || request.index != entry.index {
        return Err(CnxError::AccessDenied(
            "elasticsearch snapshot cursor authority does not match active session".into(),
        ));
    }
    if cursor.expires_at_unix_ms != Some(entry.expires_at_unix_ms) {
        return Err(CnxError::InvalidInput(
            "elasticsearch snapshot cursor expiry does not match active session".into(),
        ));
    }
    if cursor.search_after != entry.expected_search_after {
        return Err(CnxError::InvalidInput(
            "elasticsearch snapshot continuation does not match active session".into(),
        ));
    }
    if validate_query
        && (request.field_filter != entry.field_filter
            || request.query_filter != entry.query_filter)
    {
        return Err(CnxError::InvalidInput(
            "elasticsearch snapshot query does not match active session".into(),
        ));
    }
    Ok(())
}

fn validate_index(index: &str) -> Result<()> {
    let trimmed = index.trim();
    if trimmed.is_empty() {
        return Err(CnxError::InvalidInput("index must be non-empty".into()));
    }
    if trimmed.contains('/')
        || trimmed.contains('\\')
        || trimmed.contains('?')
        || trimmed.contains('#')
    {
        return Err(CnxError::InvalidInput(format!(
            "index '{index}' contains unsupported path or query characters"
        )));
    }
    Ok(())
}

fn now_us() -> u64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => duration.as_micros() as u64,
        Err(_) => 0,
    }
}

fn now_ms() -> u64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as u64,
        Err(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use capanix_app_sdk::runtime::ConfigValue;
    use es_source::shared_types::{
        EsDocumentEvent, EsDocumentEventKind, EsProbeStatus, EsSourceOperation, EsSourceStatus,
        decode_msgpack, encode_msgpack,
    };
    use es_source::{
        CredentialSource, EsCredentialConfig, EsEndpointConfig, EsSourceConfig,
        EsSourceRuntimeInputs, GrantedEsEndpoint, ResolvedCredential, ResolvedEsEndpoint,
    };
    use serde_json::json;

    use super::*;

    struct FakeDriver {
        calls: Mutex<
            Vec<(
                ResolvedEsEndpoint,
                ResolvedCredential,
                EsSourceRequest,
                usize,
            )>,
        >,
        closed_pits: Mutex<Vec<String>>,
        snapshot_ttl_ms: u64,
    }

    impl Default for FakeDriver {
        fn default() -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                closed_pits: Mutex::new(Vec::new()),
                snapshot_ttl_ms: 60_000,
            }
        }
    }

    impl FakeDriver {
        fn with_snapshot_ttl(snapshot_ttl_ms: u64) -> Self {
            Self {
                snapshot_ttl_ms,
                ..Self::default()
            }
        }
    }

    impl EsSourceDriver for FakeDriver {
        fn execute(
            &self,
            endpoint: &ResolvedEsEndpoint,
            credential: &ResolvedCredential,
            request: &EsSourceRequest,
            limit: usize,
        ) -> Result<EsSourcePayload> {
            self.calls.lock().unwrap().push((
                endpoint.clone(),
                credential.clone(),
                request.clone(),
                limit,
            ));
            match request.operation {
                EsSourceOperation::SnapshotPage => {
                    let continuation = request.cursor.is_some();
                    if continuation {
                        self.closed_pits.lock().unwrap().push("native-pit".into());
                    }
                    Ok(EsSourcePayload::Batch(EsSourceBatch {
                        object_ref: endpoint.object_ref.clone(),
                        index: request.index.clone(),
                        source_revision: "es-pit:test".into(),
                        documents: vec![EsDocumentEvent {
                            event_kind: EsDocumentEventKind::SnapshotInsert,
                            index: request.index.clone(),
                            id: if continuation { "doc-2" } else { "doc-1" }.into(),
                            timestamp_value: Some(json!(if continuation { 2 } else { 1 })),
                            sort: vec![json!(if continuation { 2 } else { 1 })],
                            seq_no: None,
                            primary_term: None,
                            source: json!({"value": if continuation { 2 } else { 1 }}),
                            metadata: Default::default(),
                        }],
                        next_cursor: if continuation {
                            None
                        } else {
                            Some(EsSourceCursor::Snapshot(EsSnapshotCursor {
                                pit_id: "native-pit".into(),
                                search_after: vec![json!(1)],
                                expires_at_unix_ms: Some(now_ms() + self.snapshot_ttl_ms),
                            }))
                        },
                        has_more: !continuation,
                        grant_epoch: endpoint.grant_epoch,
                        diagnostics: None,
                    }))
                }
                _ => Ok(EsSourcePayload::Probe(EsProbeStatus {
                    object_ref: endpoint.object_ref.clone(),
                    endpoint_uri: endpoint.endpoint_uri.clone(),
                    reachable: true,
                    cluster_name: Some("fake".into()),
                    diagnostics: None,
                })),
            }
        }

        fn close_snapshot(
            &self,
            _endpoint: &ResolvedEsEndpoint,
            _credential: &ResolvedCredential,
            pit_id: &str,
        ) -> Result<()> {
            self.closed_pits.lock().unwrap().push(pit_id.to_string());
            Ok(())
        }
    }

    fn test_config() -> EsSourceRuntimeConfig {
        EsSourceRuntimeConfig {
            product: EsSourceConfig {
                endpoints: vec![EsEndpointConfig {
                    object_ref: "es-prod".into(),
                    endpoint_uri: "https://es.internal".into(),
                    credential_ref: None,
                    index_scopes: vec!["logs-*".into()],
                    timestamp_field: "@timestamp".into(),
                    tie_breaker_field: "_id".into(),
                    active: true,
                }],
                default_page_size: 50,
                max_page_size: 100,
                ..EsSourceConfig::default()
            },
            runtime: EsSourceRuntimeInputs {
                endpoint_grants: vec![GrantedEsEndpoint {
                    object_ref: "es-prod".into(),
                    endpoint_ref: Some("cluster-a".into()),
                    endpoint_uri: None,
                    credential_ref: None,
                    index_scopes: vec!["logs-2026-*".into()],
                    interfaces: vec!["read".into()],
                    active: true,
                    grant_epoch: Some(9),
                }],
            },
        }
    }

    fn test_service(driver: Arc<FakeDriver>) -> EsSourceService {
        EsSourceService::new(test_config(), driver, NodeId("es-source-test".into()))
    }

    fn request_event(request: &EsSourceRequest) -> Event {
        Event::new(
            EventMetadata {
                origin_id: NodeId("caller".into()),
                timestamp_us: 1,
                logical_ts: None,
                correlation_id: Some(42),
                ingress_auth: None,
                trace: None,
            },
            encode_msgpack(request).expect("encode request"),
        )
    }

    fn open_cursor(service: &EsSourceService) -> EsSnapshotCursor {
        let request = EsSourceRequest::snapshot_page("es-prod", "logs-2026-05");
        let reply = service.handle_event(request_event(&request));
        match decode_msgpack::<EsSourcePayload>(reply.payload_bytes()).expect("payload") {
            EsSourcePayload::Batch(batch) => match batch.next_cursor.expect("cursor") {
                EsSourceCursor::Snapshot(cursor) => cursor,
                other => panic!("expected snapshot cursor, got {other:?}"),
            },
            other => panic!("expected snapshot batch, got {other:?}"),
        }
    }

    fn page_request(cursor: EsSnapshotCursor) -> EsSourceRequest {
        let mut request = EsSourceRequest::snapshot_page("es-prod", "logs-2026-05");
        request.cursor = Some(EsSourceCursor::Snapshot(cursor));
        request
    }

    #[test]
    fn handle_event_returns_msgpack_reply_with_correlation() {
        let driver = Arc::new(FakeDriver::default());
        let service = test_service(driver.clone());
        let mut request = EsSourceRequest::snapshot_page("es-prod", "logs-2026-05");
        request.operation = EsSourceOperation::ProbeConnection;
        request.limit = Some(1_000);

        let reply = service.handle_event(request_event(&request));
        let payload: EsSourcePayload = decode_msgpack(reply.payload_bytes()).expect("payload");

        assert_eq!(reply.metadata().correlation_id, Some(42));
        assert!(matches!(payload, EsSourcePayload::Probe(_)));
        let calls = driver.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].3, 100);
        assert_eq!(calls[0].0.grant_epoch, Some(9));
    }

    #[test]
    fn handle_event_rejects_out_of_scope_index_before_driver_call() {
        let driver = Arc::new(FakeDriver::default());
        let service = test_service(driver.clone());
        let request = EsSourceRequest::snapshot_page("es-prod", "logs-2025-05");

        let reply = service.handle_event(request_event(&request));
        let payload: EsSourcePayload = decode_msgpack(reply.payload_bytes()).expect("payload");

        match payload {
            EsSourcePayload::Error(err) => assert_eq!(err.code, "scope-denied"),
            other => panic!("expected error payload, got {other:?}"),
        }
        assert!(driver.calls.lock().unwrap().is_empty());
    }

    #[test]
    fn handle_event_rejects_invalid_index_before_driver_call() {
        let driver = Arc::new(FakeDriver::default());
        let service = test_service(driver.clone());
        let request = EsSourceRequest::snapshot_page("es-prod", "logs/2026");

        let reply = service.handle_event(request_event(&request));
        let payload: EsSourcePayload = decode_msgpack(reply.payload_bytes()).expect("payload");

        match payload {
            EsSourcePayload::Error(err) => assert_eq!(err.code, "invalid-request"),
            other => panic!("expected error payload, got {other:?}"),
        }
        assert!(driver.calls.lock().unwrap().is_empty());
    }

    #[test]
    fn missing_env_credential_fails_closed_before_driver_call() {
        let driver = Arc::new(FakeDriver::default());
        let service = EsSourceService::new(
            EsSourceRuntimeConfig {
                product: EsSourceConfig {
                    endpoints: vec![EsEndpointConfig {
                        object_ref: "es-prod".into(),
                        endpoint_uri: "https://es.internal".into(),
                        credential_ref: Some("prod-es".into()),
                        index_scopes: vec!["logs-*".into()],
                        timestamp_field: "@timestamp".into(),
                        tie_breaker_field: "_id".into(),
                        active: true,
                    }],
                    credentials: vec![EsCredentialConfig {
                        credential_ref: "prod-es".into(),
                        source: CredentialSource::ApiKeyEnv {
                            api_key_env: "FUSTOR_ES_SOURCE_TEST_MISSING_API_KEY".into(),
                        },
                    }],
                    default_page_size: 50,
                    max_page_size: 100,
                },
                runtime: EsSourceRuntimeInputs {
                    endpoint_grants: vec![GrantedEsEndpoint {
                        object_ref: "es-prod".into(),
                        endpoint_ref: Some("cluster-a".into()),
                        endpoint_uri: None,
                        credential_ref: None,
                        index_scopes: vec!["logs-*".into()],
                        interfaces: vec!["read".into()],
                        active: true,
                        grant_epoch: Some(9),
                    }],
                },
            },
            driver.clone(),
            NodeId("es-source-test".into()),
        );
        let request = EsSourceRequest::snapshot_page("es-prod", "logs-2026");

        let reply = service.handle_event(request_event(&request));
        let payload: EsSourcePayload = decode_msgpack(reply.payload_bytes()).expect("payload");

        match payload {
            EsSourcePayload::Error(err) => assert_eq!(err.code, "access-denied"),
            other => panic!("expected error payload, got {other:?}"),
        }
        assert!(driver.calls.lock().unwrap().is_empty());
    }

    #[test]
    fn status_handler_returns_typed_readiness_without_driver_call() {
        let driver = Arc::new(FakeDriver::default());
        let service = test_service(driver.clone());
        let event = Event::new(
            EventMetadata {
                origin_id: NodeId("caller".into()),
                timestamp_us: 1,
                logical_ts: None,
                correlation_id: Some(99),
                ingress_auth: None,
                trace: None,
            },
            bytes::Bytes::from_static(b"ignored"),
        );

        let replies = service.handle_status_events(
            vec![event],
            "es-source.fetch.node_a:v1.req",
            "es-source.status.node_a:v1.req",
        );

        assert_eq!(replies.len(), 1);
        assert_eq!(replies[0].metadata().correlation_id, Some(99));
        let payload: EsSourcePayload = decode_msgpack(replies[0].payload_bytes()).expect("payload");
        match payload {
            EsSourcePayload::Status(EsSourceStatus {
                configured_endpoints,
                active_grants,
                ready,
                fetch_route,
                status_route,
                ..
            }) => {
                assert_eq!(configured_endpoints, 1);
                assert_eq!(active_grants, 1);
                assert!(ready);
                assert_eq!(fetch_route, "es-source.fetch.node_a:v1.req");
                assert_eq!(status_route, "es-source.status.node_a:v1.req");
            }
            other => panic!("expected status payload, got {other:?}"),
        }
        assert!(driver.calls.lock().unwrap().is_empty());
    }

    #[test]
    fn malformed_msgpack_returns_invalid_request_payload() {
        let driver = Arc::new(FakeDriver::default());
        let service = test_service(driver);
        let event = Event::new(
            EventMetadata {
                origin_id: NodeId("caller".into()),
                timestamp_us: 1,
                logical_ts: None,
                correlation_id: None,
                ingress_auth: None,
                trace: None,
            },
            bytes::Bytes::from_static(b"not-msgpack"),
        );

        let reply = service.handle_event(event);
        let payload: EsSourcePayload = decode_msgpack(reply.payload_bytes()).expect("payload");

        match payload {
            EsSourcePayload::Error(err) => assert_eq!(err.code, "invalid-request"),
            other => panic!("expected error payload, got {other:?}"),
        }
    }

    #[test]
    fn snapshot_cursor_is_opaque_and_native_pit_stays_inside_source_boundary() {
        let driver = Arc::new(FakeDriver::default());
        let service = test_service(driver.clone());
        let cursor = open_cursor(&service);
        assert_ne!(cursor.pit_id, "native-pit");

        let reply = service.handle_event(request_event(&page_request(cursor)));
        match decode_msgpack::<EsSourcePayload>(reply.payload_bytes()).expect("payload") {
            EsSourcePayload::Batch(batch) => {
                assert_eq!(batch.documents[0].id, "doc-2");
                assert!(!batch.has_more);
                assert!(batch.next_cursor.is_none());
            }
            other => panic!("expected continuation batch, got {other:?}"),
        }
        let calls = driver.calls.lock().unwrap();
        let native_cursor = match calls[1].2.cursor.as_ref().expect("native cursor") {
            EsSourceCursor::Snapshot(cursor) => cursor,
            other => panic!("expected native snapshot cursor, got {other:?}"),
        };
        assert_eq!(native_cursor.pit_id, "native-pit");
        drop(calls);
        assert_eq!(*driver.closed_pits.lock().unwrap(), vec!["native-pit"]);
    }

    #[test]
    fn tamper_config_change_route_deactivation_and_drop_close_pit_sessions() {
        let driver = Arc::new(FakeDriver::default());
        let control_state = SourceControlState::new(test_config());
        let service = EsSourceService::with_control_state(
            control_state.clone(),
            driver.clone(),
            NodeId("es-source-test".into()),
        );
        let mut cursor = open_cursor(&service);
        cursor.search_after = vec![json!(999)];
        let reply = service.handle_event(request_event(&page_request(cursor)));
        match decode_msgpack::<EsSourcePayload>(reply.payload_bytes()).expect("payload") {
            EsSourcePayload::Error(error) => assert_eq!(error.code, "invalid-request"),
            other => panic!("expected tamper error, got {other:?}"),
        }
        assert_eq!(driver.closed_pits.lock().unwrap().len(), 1);

        let _cursor = open_cursor(&service);
        let mut changed = test_config();
        changed.runtime.endpoint_grants[0].grant_epoch = Some(10);
        assert!(
            control_state
                .replace_config_if_newer(1, changed)
                .expect("replace config")
        );
        assert_eq!(driver.closed_pits.lock().unwrap().len(), 2);

        let _cursor = open_cursor(&service);
        control_state
            .set_fetch_route_active(false)
            .expect("deactivate fetch route");
        assert_eq!(driver.closed_pits.lock().unwrap().len(), 3);
        control_state
            .set_fetch_route_active(true)
            .expect("reactivate fetch route");

        let lost_cursor = open_cursor(&service);
        drop(service);
        assert_eq!(driver.closed_pits.lock().unwrap().len(), 4);
        let restarted = test_service(driver.clone());
        let reply = restarted.handle_event(request_event(&page_request(lost_cursor)));
        match decode_msgpack::<EsSourcePayload>(reply.payload_bytes()).expect("payload") {
            EsSourcePayload::Error(error) => assert_eq!(error.code, "transient-runtime"),
            other => panic!("expected restart-loss error, got {other:?}"),
        }
    }

    #[test]
    fn explicit_snapshot_close_releases_native_pit() {
        let driver = Arc::new(FakeDriver::default());
        let service = test_service(driver.clone());
        let cursor = open_cursor(&service);
        let request = EsSourceRequest::snapshot_close("es-prod", "logs-2026-05", cursor);

        let reply = service.handle_event(request_event(&request));
        match decode_msgpack::<EsSourcePayload>(reply.payload_bytes()).expect("payload") {
            EsSourcePayload::Batch(batch) => {
                assert!(!batch.has_more);
                assert!(batch.documents.is_empty());
            }
            other => panic!("expected close batch, got {other:?}"),
        }
        assert_eq!(*driver.closed_pits.lock().unwrap(), vec!["native-pit"]);
    }

    #[test]
    fn snapshot_session_expires_and_closes_without_followup_request() {
        let driver = Arc::new(FakeDriver::with_snapshot_ttl(20));
        let service = test_service(driver.clone());
        let _cursor = open_cursor(&service);
        let deadline = Instant::now() + Duration::from_secs(2);
        while driver.closed_pits.lock().unwrap().is_empty() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }

        assert_eq!(*driver.closed_pits.lock().unwrap(), vec!["native-pit"]);
        assert_eq!(service.sessions.active_count(), 0);
    }

    #[test]
    fn snapshot_session_registry_fails_closed_at_capacity() {
        let driver = Arc::new(FakeDriver::default());
        let sessions = SnapshotSessionRegistry::new(driver.clone());
        let endpoint = test_config()
            .endpoint_for("es-prod", "logs-2026-05")
            .expect("endpoint");
        for index in 0..MAX_ACTIVE_SNAPSHOT_SESSIONS {
            sessions
                .insert(
                    format!("session-{index}"),
                    SnapshotSessionEntry {
                        object_ref: "es-prod".into(),
                        index: "logs-2026-05".into(),
                        endpoint: endpoint.clone(),
                        credential: ResolvedCredential::None,
                        native_pit_id: format!("pit-{index}"),
                        source_revision: format!("es-pit:{index}"),
                        expected_search_after: vec![json!(index)],
                        expires_at_unix_ms: now_ms() + 60_000,
                        field_filter: Vec::new(),
                        query_filter: None,
                    },
                )
                .expect("fill session registry");
        }

        let error = sessions
            .insert(
                "session-overflow".into(),
                SnapshotSessionEntry {
                    object_ref: "es-prod".into(),
                    index: "logs-2026-05".into(),
                    endpoint,
                    credential: ResolvedCredential::None,
                    native_pit_id: "pit-overflow".into(),
                    source_revision: "es-pit:overflow".into(),
                    expected_search_after: Vec::new(),
                    expires_at_unix_ms: now_ms() + 60_000,
                    field_filter: Vec::new(),
                    query_filter: None,
                },
            )
            .expect_err("session cap must fail closed");

        assert!(matches!(error, CnxError::ResourceExhausted(_)));
        assert_eq!(sessions.active_count(), MAX_ACTIVE_SNAPSHOT_SESSIONS);
        assert_eq!(driver.closed_pits.lock().unwrap().len(), 1);
        sessions.close_all();
        assert_eq!(
            driver.closed_pits.lock().unwrap().len(),
            MAX_ACTIVE_SNAPSHOT_SESSIONS + 1
        );
    }

    #[test]
    fn manifest_shape_can_build_service_config() {
        let cfg = std::collections::HashMap::from([
            (
                "endpoints".to_string(),
                ConfigValue::Array(vec![ConfigValue::Map(std::collections::HashMap::from([
                    (
                        "object_ref".to_string(),
                        ConfigValue::String("es-prod".into()),
                    ),
                    (
                        "endpoint_uri".to_string(),
                        ConfigValue::String("https://es.internal".into()),
                    ),
                ]))]),
            ),
            (
                "__cnx_runtime".to_string(),
                ConfigValue::Map(std::collections::HashMap::from([(
                    "resource_grants".to_string(),
                    ConfigValue::Array(vec![ConfigValue::Map(std::collections::HashMap::from([
                        (
                            "resource_kind".to_string(),
                            ConfigValue::String("elasticsearch".into()),
                        ),
                        (
                            "object_ref".to_string(),
                            ConfigValue::String("es-prod".into()),
                        ),
                        (
                            "interfaces".to_string(),
                            ConfigValue::Array(vec![ConfigValue::String("read".into())]),
                        ),
                    ]))]),
                )])),
            ),
        ]);

        let config = EsSourceRuntimeConfig::from_manifest_config(&cfg).expect("manifest config");

        assert!(config.endpoint_for("es-prod", "anything").is_ok());
    }
}

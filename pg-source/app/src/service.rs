use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use capanix_app_sdk::runtime::{EventMetadata, NodeId};
use capanix_app_sdk::{CnxError, Event, Result};
use pg_source::shared_types::{
    PgRowBatch, PgSnapshotCursor, PgSourceCursor, PgSourceOperation, PgSourcePayload,
    PgSourceRequest,
};
use pg_source::{PgSourceRuntimeConfig, ResolvedPgEndpoint};
use serde_json::Value;
use source_kit::{
    DecodedSourceRequest, SourceCapabilityReport, SourceConsistencyCapability,
    SourceContinuationCapability, SourceControlChange, SourceControlState,
    SourceControlSubscription, SourceErrorPayload, SourceOperationBudget,
    SourceOperationCapability, decode_source_request, encode_accounted_source_result,
    encode_msgpack, source_now_ms, validate_source_grant_epoch, validate_source_request_limit,
};
use uuid::Uuid;

use crate::driver::{PgSnapshotSession, PgSourceDriver};

const MAX_ACTIVE_SNAPSHOT_SESSIONS: usize = 128;

#[derive(Clone)]
pub struct PgSourceService {
    control_state: SourceControlState<PgSourceRuntimeConfig>,
    driver: Arc<dyn PgSourceDriver>,
    origin_id: NodeId,
    sessions: Arc<SnapshotSessionRegistry>,
    _control_subscription: Option<SourceControlSubscription>,
}

struct SnapshotSessionEntry {
    object_ref: String,
    schema: String,
    table: String,
    endpoint: ResolvedPgEndpoint,
    expires_at_unix_ms: u64,
    expected_last_key: Option<Value>,
    expected_offset: u64,
    source_revision: String,
    session: Box<dyn PgSnapshotSession>,
}

struct SnapshotSessionRegistry {
    entries: Arc<Mutex<HashMap<String, SnapshotSessionEntry>>>,
    janitor_control: Arc<(Mutex<bool>, Condvar)>,
    janitor: Mutex<Option<JoinHandle<()>>>,
}

impl SnapshotSessionRegistry {
    fn new() -> Arc<Self> {
        let entries = Arc::new(Mutex::new(HashMap::new()));
        let janitor_control = Arc::new((Mutex::new(false), Condvar::new()));
        let janitor_entries = entries.clone();
        let thread_control = janitor_control.clone();
        let janitor = thread::Builder::new()
            .name("pg-source-session-expiry".into())
            .spawn(move || pg_session_expiry_loop(janitor_entries, thread_control))
            .expect("start PostgreSQL source session expiry worker");
        Arc::new(Self {
            entries,
            janitor_control,
            janitor: Mutex::new(Some(janitor)),
        })
    }

    fn insert(&self, session_id: String, mut entry: SnapshotSessionEntry) -> Result<()> {
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| CnxError::Internal("pg snapshot session lock poisoned".into()))?;
        if entries.len() >= MAX_ACTIVE_SNAPSHOT_SESSIONS && !entries.contains_key(&session_id) {
            drop(entries);
            let _ = entry.session.close();
            return Err(CnxError::ResourceExhausted(format!(
                "pg active snapshot session limit {MAX_ACTIVE_SNAPSHOT_SESSIONS} reached"
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
            .map_err(|_| CnxError::Internal("pg snapshot session lock poisoned".into()))?
            .remove(session_id)
            .ok_or_else(|| CnxError::NotReady("pg snapshot session is not active".into()))
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
        for (_, mut entry) in entries.drain() {
            let _ = entry.session.close();
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
        for (_, mut entry) in entries.drain() {
            let _ = entry.session.close();
        }
    }
}

fn pg_session_expiry_loop(
    entries: Arc<Mutex<HashMap<String, SnapshotSessionEntry>>>,
    control: Arc<(Mutex<bool>, Condvar)>,
) {
    loop {
        let (mut expired, wait) = {
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
        for entry in &mut expired {
            let _ = entry.session.close();
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

impl PgSourceService {
    pub fn new(
        config: PgSourceRuntimeConfig,
        driver: Arc<dyn PgSourceDriver>,
        origin_id: NodeId,
    ) -> Self {
        Self::with_control_state(SourceControlState::new(config), driver, origin_id)
    }

    pub fn with_control_state(
        control_state: SourceControlState<PgSourceRuntimeConfig>,
        driver: Arc<dyn PgSourceDriver>,
        origin_id: NodeId,
    ) -> Self {
        let sessions = SnapshotSessionRegistry::new();
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
        let encoded = match decode_source_request::<PgSourceRequest>(
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
                |error| {
                    PgSourcePayload::Error(source_kit::error_payload_from_cnx(
                        error,
                        "pg-transport",
                    ))
                },
            ),
            Err(error) => encode_msgpack(&PgSourcePayload::Error(
                source_kit::error_payload_from_cnx(error, "pg-transport"),
            ))
            .map(|bytes| bytes.to_vec()),
        };
        self.reply_encoded_event(encoded, correlation_id)
            .unwrap_or_else(|err| self.fallback_error_event(err, correlation_id))
    }

    fn handle_request(
        &self,
        request: &PgSourceRequest,
        budget: &mut SourceOperationBudget,
    ) -> Result<PgSourcePayload> {
        if !self.control_state.snapshot()?.fetch_route_active {
            self.sessions.close_all();
            return Err(CnxError::NotReady(
                "pg-source fetch route is inactive".into(),
            ));
        }
        match request.operation {
            PgSourceOperation::ProbeConnection => self.handle_probe(request, budget),
            PgSourceOperation::DiscoverFields => self.handle_discover_fields(request, budget),
            PgSourceOperation::CatalogMetadata => self.handle_catalog_metadata(request, budget),
            PgSourceOperation::SnapshotOpen => self.handle_snapshot_open(request, budget),
            PgSourceOperation::SnapshotPage => self.handle_snapshot_page(request, budget),
            PgSourceOperation::SnapshotClose => self.handle_snapshot_close(request, budget),
        }
    }

    fn validate_execution_context(
        &self,
        decoded: &DecodedSourceRequest<PgSourceRequest>,
    ) -> Result<()> {
        let Some(context) = decoded.context.as_ref() else {
            return Ok(());
        };
        validate_source_request_limit(Some(context), decoded.request.limit)?;
        let (schema, table) = request_scope(&decoded.request)?;
        let endpoint = self.control_state.config()?.endpoint_for(
            &decoded.request.object_ref,
            schema,
            table,
        )?;
        validate_source_grant_epoch(Some(context), endpoint.grant_epoch, "pg")
    }

    fn handle_status_event(&self, event: Event, fetch_route: &str, status_route: &str) -> Event {
        let correlation_id = event.metadata().correlation_id;
        self.reply_event(
            PgSourcePayload::Status(self.status_snapshot(fetch_route, status_route)),
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
                    "pg",
                    0,
                    0,
                    vec![format!("control state unavailable: {err}")],
                )
                .with_capabilities(pg_capabilities());
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
            format!("active_snapshot_sessions={}", self.sessions.active_count()),
            format!("max_snapshot_sessions={MAX_ACTIVE_SNAPSHOT_SESSIONS}"),
        ];
        if configured_endpoints == 0 {
            diagnostics.push("no configured PostgreSQL endpoints".to_string());
        }
        if active_grants == 0 {
            diagnostics.push("no active runtime PostgreSQL endpoint grants".to_string());
        }
        let mut status = source_kit::SourceRuntimeStatus::new(
            "pg",
            configured_endpoints,
            active_grants,
            diagnostics,
        )
        .with_capabilities(pg_capabilities());
        if !snapshot.fetch_route_active || !snapshot.status_route_active {
            status.ready = false;
        }
        status
    }

    fn handle_probe(
        &self,
        request: &PgSourceRequest,
        budget: &mut SourceOperationBudget,
    ) -> Result<PgSourcePayload> {
        let config = self.control_state.config()?;
        let endpoint = config.endpoint_for(&request.object_ref, None, None)?;
        let credential = config.resolve_credential(endpoint.credential_ref.as_deref())?;
        Ok(PgSourcePayload::Probe(self.driver.probe_with_budget(
            &endpoint,
            &credential,
            budget,
        )?))
    }

    fn handle_discover_fields(
        &self,
        request: &PgSourceRequest,
        budget: &mut SourceOperationBudget,
    ) -> Result<PgSourcePayload> {
        let config = self.control_state.config()?;
        let endpoint = config.endpoint_for(&request.object_ref, None, None)?;
        let credential = config.resolve_credential(endpoint.credential_ref.as_deref())?;
        Ok(PgSourcePayload::Fields(
            self.driver
                .discover_fields_with_budget(&endpoint, &credential, budget)?,
        ))
    }

    fn handle_catalog_metadata(
        &self,
        request: &PgSourceRequest,
        budget: &mut SourceOperationBudget,
    ) -> Result<PgSourcePayload> {
        request_scope(request)?;
        let config = self.control_state.config()?;
        let endpoint = config.endpoint_for(&request.object_ref, None, None)?;
        let credential = config.resolve_credential(endpoint.credential_ref.as_deref())?;
        Ok(PgSourcePayload::Catalog(self.driver.catalog_metadata_with_budget(
            &endpoint, &credential, config.effective_limit(request.limit), budget,
        )?))
    }

    fn handle_snapshot_open(
        &self,
        request: &PgSourceRequest,
        budget: &mut SourceOperationBudget,
    ) -> Result<PgSourcePayload> {
        let schema = required_request_field(request.schema.as_deref(), "schema")?;
        let table = required_request_field(request.table.as_deref(), "table")?;
        let config = self.control_state.config()?;
        let endpoint = config.endpoint_for(&request.object_ref, Some(schema), Some(table))?;
        let credential = config.resolve_credential(endpoint.credential_ref.as_deref())?;
        let mut session = self.driver.open_snapshot_with_budget(
            &endpoint,
            &credential,
            schema,
            table,
            &request.field_filter,
            budget,
        )?;
        let expires_at_unix_ms = now_ms() + config.product.snapshot_ttl_ms;
        let session_id = Uuid::new_v4().to_string();
        let limit = config.effective_limit(request.limit);
        let page = match session.fetch_page_with_budget(limit, None, budget) {
            Ok(page) => page,
            Err(error) => {
                let _ = session.close();
                return Err(error);
            }
        };
        if page.source_revision.trim().is_empty() {
            let _ = session.close();
            return Err(CnxError::ProtocolViolation(
                "pg snapshot page is missing its source-native revision".into(),
            ));
        }
        let next_cursor = if page.has_more {
            let Some(last_key) = page.last_key.clone() else {
                let _ = session.close();
                return Err(CnxError::ProtocolViolation(
                    "pg snapshot page with has_more=true missing keyset last_key".into(),
                ));
            };
            Some(PgSnapshotCursor {
                snapshot_session_id: session_id.clone(),
                schema: schema.to_string(),
                table: table.to_string(),
                last_key: Some(last_key),
                offset: page.rows.len() as u64,
                expires_at_unix_ms,
            })
        } else {
            None
        };
        if page.has_more || next_cursor.is_some() {
            self.sessions.insert(
                session_id.clone(),
                SnapshotSessionEntry {
                    object_ref: request.object_ref.clone(),
                    schema: schema.to_string(),
                    table: table.to_string(),
                    endpoint: endpoint.clone(),
                    expires_at_unix_ms,
                    expected_last_key: page.last_key.clone(),
                    expected_offset: page.rows.len() as u64,
                    source_revision: page.source_revision.clone(),
                    session,
                },
            )?;
        } else {
            session.close_with_budget(budget)?;
        }
        Ok(PgSourcePayload::Rows(PgRowBatch {
            object_ref: request.object_ref.clone(),
            schema: schema.to_string(),
            table: table.to_string(),
            source_revision: page.source_revision,
            fields: page.fields,
            rows: page.rows,
            next_cursor: next_cursor.map(|cursor| {
                PgSourceCursor::Snapshot(PgSnapshotCursor {
                    snapshot_session_id: cursor.snapshot_session_id,
                    schema: cursor.schema,
                    table: cursor.table,
                    last_key: cursor.last_key,
                    offset: cursor.offset,
                    expires_at_unix_ms,
                })
            }),
            has_more: page.has_more,
            grant_epoch: endpoint.grant_epoch,
            diagnostics: Some("transaction snapshot session opened".into()),
        }))
    }

    fn handle_snapshot_page(
        &self,
        request: &PgSourceRequest,
        budget: &mut SourceOperationBudget,
    ) -> Result<PgSourcePayload> {
        let PgSourceCursor::Snapshot(cursor) = request
            .cursor
            .as_ref()
            .ok_or_else(|| CnxError::InvalidInput("snapshot page cursor is required".into()))?;
        let config = self.control_state.config()?;
        let limit = config.effective_limit(request.limit);
        let mut entry = self.take_session(&cursor.snapshot_session_id)?;
        if let Err(error) = validate_snapshot_cursor(request, cursor, &entry) {
            let _ = entry.session.close();
            return Err(error);
        }
        let current_endpoint = match config.endpoint_for(
            &entry.object_ref,
            Some(entry.schema.as_str()),
            Some(entry.table.as_str()),
        ) {
            Ok(endpoint) => endpoint,
            Err(error) => {
                let _ = entry.session.close();
                return Err(error);
            }
        };
        if current_endpoint != entry.endpoint {
            let _ = entry.session.close();
            return Err(CnxError::AccessDenied(
                "pg snapshot authority or grant changed during continuation".into(),
            ));
        }
        if entry.expires_at_unix_ms <= now_ms() {
            let _ = entry.session.close();
            return Err(CnxError::NotReady(format!(
                "pg snapshot session '{}' expired",
                cursor.snapshot_session_id
            )));
        }
        let page = match entry.session.fetch_page_with_budget(
            limit,
            entry.expected_last_key.as_ref(),
            budget,
        ) {
            Ok(page) => page,
            Err(error) => {
                let _ = entry.session.close();
                return Err(error);
            }
        };
        if page.source_revision != entry.source_revision {
            let _ = entry.session.close();
            return Err(CnxError::ProtocolViolation(
                "pg snapshot source-native revision changed during continuation".into(),
            ));
        }
        let page_row_count = page.rows.len() as u64;
        let next_last_key = page.last_key.clone();
        let next_cursor = if page.has_more {
            let Some(last_key) = next_last_key.clone() else {
                let _ = entry.session.close();
                return Err(CnxError::ProtocolViolation(
                    "pg snapshot page with has_more=true missing keyset last_key".into(),
                ));
            };
            Some(PgSourceCursor::Snapshot(PgSnapshotCursor {
                snapshot_session_id: cursor.snapshot_session_id.clone(),
                schema: entry.schema.clone(),
                table: entry.table.clone(),
                last_key: Some(last_key),
                offset: entry.expected_offset + page_row_count,
                expires_at_unix_ms: entry.expires_at_unix_ms,
            }))
        } else {
            entry.session.close_with_budget(budget)?;
            None
        };
        let has_more = page.has_more;
        let batch = PgRowBatch {
            object_ref: entry.object_ref.clone(),
            schema: entry.schema.clone(),
            table: entry.table.clone(),
            source_revision: page.source_revision,
            fields: page.fields,
            rows: page.rows,
            next_cursor,
            has_more,
            grant_epoch: current_endpoint.grant_epoch,
            diagnostics: None,
        };
        if has_more {
            entry.expected_last_key = next_last_key;
            entry.expected_offset += page_row_count;
            self.sessions
                .insert(cursor.snapshot_session_id.clone(), entry)?;
        }
        Ok(PgSourcePayload::Rows(batch))
    }

    fn handle_snapshot_close(
        &self,
        request: &PgSourceRequest,
        budget: &mut SourceOperationBudget,
    ) -> Result<PgSourcePayload> {
        let PgSourceCursor::Snapshot(cursor) = request
            .cursor
            .as_ref()
            .ok_or_else(|| CnxError::InvalidInput("snapshot close cursor is required".into()))?;
        let mut entry = self.take_session(&cursor.snapshot_session_id)?;
        if let Err(error) = validate_snapshot_cursor(request, cursor, &entry) {
            let _ = entry.session.close();
            return Err(error);
        }
        let grant_epoch = entry.endpoint.grant_epoch;
        entry.session.close_with_budget(budget)?;
        Ok(PgSourcePayload::Rows(PgRowBatch {
            object_ref: request.object_ref.clone(),
            schema: cursor.schema.clone(),
            table: cursor.table.clone(),
            source_revision: entry.source_revision,
            fields: Vec::new(),
            rows: Vec::new(),
            next_cursor: None,
            has_more: false,
            grant_epoch,
            diagnostics: Some("transaction snapshot session closed".into()),
        }))
    }

    fn take_session(&self, session_id: &str) -> Result<SnapshotSessionEntry> {
        self.sessions.take(session_id)
    }

    fn reply_event(&self, payload: PgSourcePayload, correlation_id: Option<u64>) -> Result<Event> {
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
        let payload = PgSourcePayload::Error(SourceErrorPayload::new(
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

fn pg_capabilities() -> SourceCapabilityReport {
    SourceCapabilityReport::new(
        "pg",
        vec![
            SourceOperationCapability::Probe,
            SourceOperationCapability::FieldDiscovery,
            SourceOperationCapability::CatalogMetadata,
            SourceOperationCapability::BoundedSnapshot,
            SourceOperationCapability::ExplicitSessionClose,
        ],
        SourceConsistencyCapability::TransactionSnapshot,
        SourceContinuationCapability::NativeSession,
    )
}

fn request_scope(request: &PgSourceRequest) -> Result<(Option<&str>, Option<&str>)> {
    match request.operation {
        PgSourceOperation::CatalogMetadata => {
            if request.schema.is_some() || request.table.is_some() || request.cursor.is_some()
                || !request.field_filter.is_empty() || request.limit.is_none_or(|n| n == 0 || n > 5000)
            {
                return Err(CnxError::InvalidInput("catalog metadata accepts only an object reference and a bounded limit".into()));
            }
            Ok((None, None))
        }
        PgSourceOperation::ProbeConnection | PgSourceOperation::DiscoverFields => Ok((None, None)),
        PgSourceOperation::SnapshotOpen => Ok((
            Some(required_request_field(request.schema.as_deref(), "schema")?),
            Some(required_request_field(request.table.as_deref(), "table")?),
        )),
        PgSourceOperation::SnapshotPage | PgSourceOperation::SnapshotClose => {
            let PgSourceCursor::Snapshot(cursor) = request
                .cursor
                .as_ref()
                .ok_or_else(|| CnxError::InvalidInput("snapshot cursor is required".into()))?;
            Ok((Some(&cursor.schema), Some(&cursor.table)))
        }
    }
}

fn validate_snapshot_cursor(
    request: &PgSourceRequest,
    cursor: &PgSnapshotCursor,
    entry: &SnapshotSessionEntry,
) -> Result<()> {
    if request.object_ref != entry.object_ref {
        return Err(CnxError::AccessDenied(
            "snapshot cursor authority object does not match active session".into(),
        ));
    }
    if entry.schema != cursor.schema || entry.table != cursor.table {
        return Err(CnxError::InvalidInput(
            "snapshot cursor schema/table does not match active session".into(),
        ));
    }
    if cursor.expires_at_unix_ms != entry.expires_at_unix_ms {
        return Err(CnxError::InvalidInput(
            "snapshot cursor expiry does not match active session".into(),
        ));
    }
    if cursor.last_key != entry.expected_last_key || cursor.offset != entry.expected_offset {
        return Err(CnxError::InvalidInput(
            "snapshot cursor continuation does not match active session".into(),
        ));
    }
    Ok(())
}

fn required_request_field<'a>(value: Option<&'a str>, key: &str) -> Result<&'a str> {
    value
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| CnxError::InvalidInput(format!("{key} is required")))
}

fn now_us() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_micros() as u64,
        Err(_) => 0,
    }
}

fn now_ms() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as u64,
        Err(_) => 0,
    }
}

#[allow(dead_code)]
fn default_snapshot_ttl() -> Duration {
    Duration::from_millis(pg_source::config::DEFAULT_SNAPSHOT_TTL_MS)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use capanix_app_sdk::runtime::ConfigValue;
    use pg_source::shared_types::{PgFieldCatalog, PgProbeStatus, PgRowEvent, PgSourceRequest};
    use pg_source::{
        GrantedPgEndpoint, PgEndpointConfig, PgSourceConfig, PgSourceRuntimeInputs,
        ResolvedCredential,
    };
    use serde_json::json;
    use source_kit::{
        DecodedSourceResponse, SourceExecutionContext, SourceExecutionEnvelope,
        SourceExecutionLimits, decode_msgpack, decode_source_response, encode_source_request,
        source_now_ms,
    };

    use super::*;
    use crate::driver::{PgSnapshotPage, PgSnapshotSession};

    struct FakeDriver {
        opened: AtomicUsize,
        closed: Arc<AtomicUsize>,
        fail_on_page: Option<usize>,
    }

    impl FakeDriver {
        fn new() -> Self {
            Self {
                opened: AtomicUsize::new(0),
                closed: Arc::new(AtomicUsize::new(0)),
                fail_on_page: None,
            }
        }

        fn failing_on_second_page() -> Self {
            Self {
                opened: AtomicUsize::new(0),
                closed: Arc::new(AtomicUsize::new(0)),
                fail_on_page: Some(2),
            }
        }
    }

    impl PgSourceDriver for FakeDriver {
        fn probe(
            &self,
            endpoint: &ResolvedPgEndpoint,
            _credential: &ResolvedCredential,
        ) -> Result<PgProbeStatus> {
            Ok(PgProbeStatus {
                object_ref: endpoint.object_ref.clone(),
                endpoint_uri: endpoint.endpoint_uri.clone(),
                reachable: true,
                server_version: Some("fake-pg".into()),
                diagnostics: None,
            })
        }

        fn discover_fields(
            &self,
            endpoint: &ResolvedPgEndpoint,
            _credential: &ResolvedCredential,
        ) -> Result<PgFieldCatalog> {
            Ok(PgFieldCatalog {
                object_ref: endpoint.object_ref.clone(),
                fields: Vec::new(),
            })
        }

        fn open_snapshot(
            &self,
            _endpoint: &ResolvedPgEndpoint,
            _credential: &ResolvedCredential,
            _schema: &str,
            _table: &str,
            _fields: &[String],
        ) -> Result<Box<dyn PgSnapshotSession>> {
            self.opened.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(FakeSnapshotSession {
                pages: 0,
                closed: self.closed.clone(),
                fail_on_page: self.fail_on_page,
            }))
        }
    }

    struct FakeSnapshotSession {
        pages: usize,
        closed: Arc<AtomicUsize>,
        fail_on_page: Option<usize>,
    }

    impl PgSnapshotSession for FakeSnapshotSession {
        fn fetch_page(
            &mut self,
            _limit: usize,
            last_key: Option<&serde_json::Value>,
        ) -> Result<PgSnapshotPage> {
            self.pages += 1;
            if self.fail_on_page == Some(self.pages) {
                return Err(CnxError::PeerError(
                    "injected PostgreSQL continuation failure".into(),
                ));
            }
            let has_more = self.pages == 1;
            let id = last_key
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0)
                .saturating_add(1);
            Ok(PgSnapshotPage {
                fields: vec!["id".into()],
                rows: vec![PgRowEvent {
                    values: json!({ "id": id }),
                }],
                source_revision: "pg-txid-snapshot:test".into(),
                last_key: Some(json!(id)),
                has_more,
            })
        }

        fn close(&mut self) -> Result<()> {
            self.closed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn test_config(snapshot_ttl_ms: u64) -> PgSourceRuntimeConfig {
        PgSourceRuntimeConfig {
            product: PgSourceConfig {
                endpoints: vec![PgEndpointConfig {
                    object_ref: "pg-prod".into(),
                    endpoint_uri: "pg://db:3306".into(),
                    credential_ref: None,
                    schema_scopes: vec!["app".into()],
                    table_scopes: vec!["users".into()],
                    primary_key: Some("id".into()),
                    active: true,
                }],
                snapshot_ttl_ms,
                ..PgSourceConfig::default()
            },
            runtime: PgSourceRuntimeInputs {
                endpoint_grants: vec![GrantedPgEndpoint {
                    object_ref: "pg-prod".into(),
                    endpoint_ref: Some("db-a".into()),
                    endpoint_uri: None,
                    credential_ref: None,
                    schema_scopes: Vec::new(),
                    table_scopes: Vec::new(),
                    interfaces: vec!["read".into()],
                    active: true,
                    grant_epoch: Some(3),
                }],
            },
        }
    }

    fn event_for(request: &PgSourceRequest) -> Event {
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

    fn accounted_event_for(request: PgSourceRequest, max_calls: u64) -> Event {
        let now_ms = source_now_ms();
        let envelope = SourceExecutionEnvelope::new(
            SourceExecutionContext {
                request_id: format!("pg-accounting-{max_calls}"),
                grant_epoch: 3,
                issued_at_ms: now_ms,
                deadline_ms: now_ms + 5_000,
                limits: SourceExecutionLimits {
                    max_rows: 100,
                    max_bytes: 1_048_576,
                    max_calls,
                    max_fanout: 1,
                    max_latency_ms: 5_000,
                    max_cost_units: max_calls,
                },
            },
            request,
        );
        Event::new(
            EventMetadata {
                origin_id: NodeId("caller".into()),
                timestamp_us: 1,
                logical_ts: None,
                correlation_id: Some(44),
                ingress_auth: None,
                trace: None,
            },
            encode_source_request(&envelope)
                .expect("encode source envelope")
                .into(),
        )
    }

    fn empty_event() -> Event {
        Event::new(
            EventMetadata {
                origin_id: NodeId("caller".into()),
                timestamp_us: 1,
                logical_ts: None,
                correlation_id: Some(43),
                ingress_auth: None,
                trace: None,
            },
            Vec::<u8>::new().into(),
        )
    }

    fn open_cursor(service: &PgSourceService) -> PgSnapshotCursor {
        let mut open = PgSourceRequest::snapshot_open("pg-prod", "app", "users");
        open.limit = Some(1);
        let reply = service.handle_event(event_for(&open));
        match decode_msgpack::<PgSourcePayload>(reply.payload_bytes()).expect("payload") {
            PgSourcePayload::Rows(batch) => match batch.next_cursor.expect("cursor") {
                PgSourceCursor::Snapshot(cursor) => cursor,
            },
            other => panic!("expected rows, got {other:?}"),
        }
    }

    fn page_request(cursor: PgSnapshotCursor) -> PgSourceRequest {
        PgSourceRequest {
            object_ref: "pg-prod".into(),
            schema: Some("app".into()),
            table: Some("users".into()),
            operation: PgSourceOperation::SnapshotPage,
            limit: Some(1),
            cursor: Some(PgSourceCursor::Snapshot(cursor)),
            field_filter: Vec::new(),
        }
    }

    #[test]
    fn snapshot_open_and_page_use_same_session_and_close_on_terminal_page() {
        let driver = Arc::new(FakeDriver::new());
        let service = PgSourceService::new(
            test_config(60_000),
            driver.clone(),
            NodeId("pg-test".into()),
        );
        let mut open = PgSourceRequest::snapshot_open("pg-prod", "app", "users");
        open.limit = Some(1);

        let reply = service.handle_event(event_for(&open));
        let payload: PgSourcePayload = decode_msgpack(reply.payload_bytes()).expect("payload");
        let cursor = match payload {
            PgSourcePayload::Rows(batch) => {
                assert_eq!(batch.rows[0].values["id"], json!(1));
                assert_eq!(batch.grant_epoch, Some(3));
                assert!(batch.has_more);
                match batch.next_cursor.expect("cursor") {
                    PgSourceCursor::Snapshot(cursor) => cursor,
                }
            }
            other => panic!("expected rows, got {other:?}"),
        };

        let page = PgSourceRequest {
            object_ref: "pg-prod".into(),
            schema: Some("app".into()),
            table: Some("users".into()),
            operation: PgSourceOperation::SnapshotPage,
            limit: Some(1),
            cursor: Some(PgSourceCursor::Snapshot(cursor)),
            field_filter: Vec::new(),
        };
        let reply = service.handle_event(event_for(&page));
        let payload: PgSourcePayload = decode_msgpack(reply.payload_bytes()).expect("payload");

        match payload {
            PgSourcePayload::Rows(batch) => {
                assert_eq!(batch.rows[0].values["id"], json!(2));
                assert!(!batch.has_more);
                assert!(batch.next_cursor.is_none());
            }
            other => panic!("expected rows, got {other:?}"),
        }
        assert_eq!(driver.opened.load(Ordering::SeqCst), 1);
        assert_eq!(driver.closed.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn snapshot_open_accounts_open_and_page_and_fails_before_overspend() {
        let driver = Arc::new(FakeDriver::new());
        let service = PgSourceService::new(
            test_config(60_000),
            driver,
            NodeId("pg-accounting-test".into()),
        );
        let mut open = PgSourceRequest::snapshot_open("pg-prod", "app", "users");
        open.limit = Some(1);
        let reply = service.handle_event(accounted_event_for(open, 2));
        let DecodedSourceResponse::Envelope(response) =
            decode_source_response::<PgSourcePayload>(reply.payload_bytes())
                .expect("decode response")
        else {
            panic!("expected versioned response");
        };
        let receipt = response.receipt.expect("receipt");
        assert_eq!(receipt.actual.calls, 2);
        assert_eq!(receipt.actual.cost_units, 2);

        let driver = Arc::new(FakeDriver::new());
        let service = PgSourceService::new(
            test_config(60_000),
            driver.clone(),
            NodeId("pg-accounting-reject".into()),
        );
        let mut open = PgSourceRequest::snapshot_open("pg-prod", "app", "users");
        open.limit = Some(1);
        let reply = service.handle_event(accounted_event_for(open, 1));
        let DecodedSourceResponse::Envelope(response) =
            decode_source_response::<PgSourcePayload>(reply.payload_bytes())
                .expect("decode rejected response")
        else {
            panic!("expected versioned response");
        };
        assert!(response.receipt.is_none());
        assert!(matches!(
            response.payload,
            PgSourcePayload::Error(error) if error.code == "budget-exceeded"
        ));
        assert_eq!(driver.closed.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn status_route_returns_local_readiness_without_driver_call() {
        let driver = Arc::new(FakeDriver::new());
        let service = PgSourceService::new(
            test_config(60_000),
            driver.clone(),
            NodeId("pg-test".into()),
        );

        let replies = service.handle_status_events(
            vec![empty_event()],
            "pg-source.fetch.node:v1.req",
            "pg-source.status.node:v1.req",
        );
        let payload: PgSourcePayload = decode_msgpack(replies[0].payload_bytes()).expect("payload");

        match payload {
            PgSourcePayload::Status(status) => {
                assert!(status.ready);
                assert_eq!(status.source_kind, "pg");
                assert_eq!(status.configured_endpoints, 1);
                assert_eq!(status.active_grants, 1);
                assert!(
                    status
                        .diagnostics
                        .iter()
                        .any(|item| item == "control_version=0")
                );
            }
            other => panic!("expected status, got {other:?}"),
        }
        assert_eq!(driver.opened.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn control_state_disables_fetch_and_updates_runtime_grants() {
        let driver = Arc::new(FakeDriver::new());
        let control_state = SourceControlState::new(test_config(60_000));
        let service = PgSourceService::with_control_state(
            control_state.clone(),
            driver.clone(),
            NodeId("pg-test".into()),
        );
        let open = PgSourceRequest::snapshot_open("pg-prod", "app", "users");

        control_state
            .set_fetch_route_active(false)
            .expect("disable fetch route");
        let reply = service.handle_event(event_for(&open));
        let payload: PgSourcePayload = decode_msgpack(reply.payload_bytes()).expect("payload");
        match payload {
            PgSourcePayload::Error(err) => {
                assert_eq!(err.code, "transient-runtime");
                assert!(err.retryable);
            }
            other => panic!("expected inactive route error, got {other:?}"),
        }
        assert_eq!(driver.opened.load(Ordering::SeqCst), 0);

        control_state
            .set_fetch_route_active(true)
            .expect("enable fetch route");
        let mut disabled = test_config(60_000);
        disabled.runtime.endpoint_grants[0].active = false;
        assert!(
            control_state
                .replace_config_if_newer(5, disabled)
                .expect("replace grants")
        );
        let replies = service.handle_status_events(
            vec![empty_event()],
            "pg-source.fetch.node:v1.req",
            "pg-source.status.node:v1.req",
        );
        let payload: PgSourcePayload = decode_msgpack(replies[0].payload_bytes()).expect("payload");
        match payload {
            PgSourcePayload::Status(status) => {
                assert!(!status.ready);
                assert_eq!(status.active_grants, 0);
                assert!(
                    status
                        .diagnostics
                        .iter()
                        .any(|item| item == "control_version=5")
                );
            }
            other => panic!("expected status, got {other:?}"),
        }

        let reply = service.handle_event(event_for(&open));
        let payload: PgSourcePayload = decode_msgpack(reply.payload_bytes()).expect("payload");
        match payload {
            PgSourcePayload::Error(err) => {
                assert_eq!(err.code, "access-denied");
                assert!(!err.retryable);
            }
            other => panic!("expected inactive grant error, got {other:?}"),
        }
        assert_eq!(driver.opened.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn snapshot_page_after_expiry_returns_retryable_not_ready_error() {
        let driver = Arc::new(FakeDriver::new());
        let service =
            PgSourceService::new(test_config(0), driver.clone(), NodeId("pg-test".into()));
        let open = PgSourceRequest::snapshot_open("pg-prod", "app", "users");
        let reply = service.handle_event(event_for(&open));
        let payload: PgSourcePayload = decode_msgpack(reply.payload_bytes()).expect("payload");
        let cursor = match payload {
            PgSourcePayload::Rows(batch) => match batch.next_cursor.expect("cursor") {
                PgSourceCursor::Snapshot(cursor) => cursor,
            },
            other => panic!("expected rows, got {other:?}"),
        };
        let page = PgSourceRequest {
            object_ref: "pg-prod".into(),
            schema: Some("app".into()),
            table: Some("users".into()),
            operation: PgSourceOperation::SnapshotPage,
            limit: None,
            cursor: Some(PgSourceCursor::Snapshot(cursor)),
            field_filter: Vec::new(),
        };

        let reply = service.handle_event(event_for(&page));
        let payload: PgSourcePayload = decode_msgpack(reply.payload_bytes()).expect("payload");

        match payload {
            PgSourcePayload::Error(err) => {
                assert_eq!(err.code, "transient-runtime");
                assert!(err.retryable);
            }
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[test]
    fn snapshot_session_expires_and_closes_without_followup_request() {
        let driver = Arc::new(FakeDriver::new());
        let service = PgSourceService::new(
            test_config(20),
            driver.clone(),
            NodeId("pg-expiry-test".into()),
        );
        let _cursor = open_cursor(&service);
        let deadline = Instant::now() + Duration::from_secs(2);
        while driver.closed.load(Ordering::SeqCst) == 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }

        assert_eq!(driver.closed.load(Ordering::SeqCst), 1);
        assert_eq!(service.sessions.active_count(), 0);
    }

    #[test]
    fn snapshot_session_registry_fails_closed_at_capacity() {
        let sessions = SnapshotSessionRegistry::new();
        let closed = Arc::new(AtomicUsize::new(0));
        let endpoint = test_config(60_000)
            .endpoint_for("pg-prod", Some("app"), Some("users"))
            .expect("endpoint");
        for index in 0..MAX_ACTIVE_SNAPSHOT_SESSIONS {
            sessions
                .insert(
                    format!("session-{index}"),
                    SnapshotSessionEntry {
                        object_ref: "pg-prod".into(),
                        schema: "app".into(),
                        table: "users".into(),
                        endpoint: endpoint.clone(),
                        expires_at_unix_ms: now_ms() + 60_000,
                        expected_last_key: Some(json!(index)),
                        expected_offset: index as u64,
                        source_revision: "pg-txid-snapshot:test".into(),
                        session: Box::new(FakeSnapshotSession {
                            pages: 0,
                            closed: closed.clone(),
                            fail_on_page: None,
                        }),
                    },
                )
                .expect("fill session registry");
        }

        let error = sessions
            .insert(
                "session-overflow".into(),
                SnapshotSessionEntry {
                    object_ref: "pg-prod".into(),
                    schema: "app".into(),
                    table: "users".into(),
                    endpoint,
                    expires_at_unix_ms: now_ms() + 60_000,
                    expected_last_key: None,
                    expected_offset: 0,
                    source_revision: "pg-txid-snapshot:test".into(),
                    session: Box::new(FakeSnapshotSession {
                        pages: 0,
                        closed: closed.clone(),
                        fail_on_page: None,
                    }),
                },
            )
            .expect_err("session cap must fail closed");

        assert!(matches!(error, CnxError::ResourceExhausted(_)));
        assert_eq!(sessions.active_count(), MAX_ACTIVE_SNAPSHOT_SESSIONS);
        assert_eq!(closed.load(Ordering::SeqCst), 1);
        sessions.close_all();
        assert_eq!(
            closed.load(Ordering::SeqCst),
            MAX_ACTIVE_SNAPSHOT_SESSIONS + 1
        );
    }

    #[test]
    fn config_change_and_route_deactivation_close_active_snapshot_sessions_immediately() {
        let driver = Arc::new(FakeDriver::new());
        let control_state = SourceControlState::new(test_config(60_000));
        let service = PgSourceService::with_control_state(
            control_state.clone(),
            driver.clone(),
            NodeId("pg-test".into()),
        );
        let cursor = open_cursor(&service);
        let mut changed = test_config(60_000);
        changed.runtime.endpoint_grants[0].grant_epoch = Some(4);

        assert!(
            control_state
                .replace_config_if_newer(1, changed)
                .expect("replace config")
        );
        assert_eq!(driver.closed.load(Ordering::SeqCst), 1);
        let reply = service.handle_event(event_for(&page_request(cursor)));
        assert!(matches!(
            decode_msgpack::<PgSourcePayload>(reply.payload_bytes()).expect("payload"),
            PgSourcePayload::Error(_)
        ));

        let cursor = open_cursor(&service);
        control_state
            .set_fetch_route_active(false)
            .expect("deactivate route");
        assert_eq!(driver.closed.load(Ordering::SeqCst), 2);
        let reply = service.handle_event(event_for(&page_request(cursor)));
        assert!(matches!(
            decode_msgpack::<PgSourcePayload>(reply.payload_bytes()).expect("payload"),
            PgSourcePayload::Error(_)
        ));
    }

    #[test]
    fn tampered_cursor_and_native_continuation_failure_close_session() {
        let driver = Arc::new(FakeDriver::new());
        let service = PgSourceService::new(
            test_config(60_000),
            driver.clone(),
            NodeId("pg-test".into()),
        );
        let mut cursor = open_cursor(&service);
        cursor.offset += 1;
        let reply = service.handle_event(event_for(&page_request(cursor)));
        match decode_msgpack::<PgSourcePayload>(reply.payload_bytes()).expect("payload") {
            PgSourcePayload::Error(error) => assert_eq!(error.code, "invalid-request"),
            other => panic!("expected cursor error, got {other:?}"),
        }
        assert_eq!(driver.closed.load(Ordering::SeqCst), 1);

        let driver = Arc::new(FakeDriver::failing_on_second_page());
        let service = PgSourceService::new(
            test_config(60_000),
            driver.clone(),
            NodeId("pg-test".into()),
        );
        let cursor = open_cursor(&service);
        let reply = service.handle_event(event_for(&page_request(cursor)));
        assert!(matches!(
            decode_msgpack::<PgSourcePayload>(reply.payload_bytes()).expect("payload"),
            PgSourcePayload::Error(_)
        ));
        assert_eq!(driver.closed.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn explicit_close_runtime_drop_and_restart_loss_fail_closed() {
        let driver = Arc::new(FakeDriver::new());
        let service = PgSourceService::new(
            test_config(60_000),
            driver.clone(),
            NodeId("pg-test".into()),
        );
        let cursor = open_cursor(&service);
        let close = PgSourceRequest {
            operation: PgSourceOperation::SnapshotClose,
            cursor: Some(PgSourceCursor::Snapshot(cursor.clone())),
            ..page_request(cursor.clone())
        };
        let reply = service.handle_event(event_for(&close));
        assert!(matches!(
            decode_msgpack::<PgSourcePayload>(reply.payload_bytes()).expect("payload"),
            PgSourcePayload::Rows(_)
        ));
        assert_eq!(driver.closed.load(Ordering::SeqCst), 1);

        let lost_cursor = open_cursor(&service);
        drop(service);
        assert_eq!(driver.closed.load(Ordering::SeqCst), 2);
        let restarted = PgSourceService::new(
            test_config(60_000),
            driver,
            NodeId("pg-test-restarted".into()),
        );
        let reply = restarted.handle_event(event_for(&page_request(lost_cursor)));
        match decode_msgpack::<PgSourcePayload>(reply.payload_bytes()).expect("payload") {
            PgSourcePayload::Error(error) => assert_eq!(error.code, "transient-runtime"),
            other => panic!("expected restart-loss error, got {other:?}"),
        }
    }

    #[test]
    fn manifest_runtime_config_shape_parses() {
        let cfg = std::collections::HashMap::from([
            (
                "endpoints".to_string(),
                ConfigValue::Array(vec![ConfigValue::Map(std::collections::HashMap::from([
                    (
                        "object_ref".to_string(),
                        ConfigValue::String("pg-prod".into()),
                    ),
                    (
                        "endpoint_uri".to_string(),
                        ConfigValue::String("pg://db:3306".into()),
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
                            ConfigValue::String("pg".into()),
                        ),
                        (
                            "object_ref".to_string(),
                            ConfigValue::String("pg-prod".into()),
                        ),
                        (
                            "interfaces".to_string(),
                            ConfigValue::Array(vec![ConfigValue::String("read".into())]),
                        ),
                    ]))]),
                )])),
            ),
        ]);

        let config = PgSourceRuntimeConfig::from_manifest_config(&cfg).expect("config");

        assert!(
            config
                .endpoint_for("pg-prod", Some("any"), Some("any"))
                .is_ok()
        );
    }
}

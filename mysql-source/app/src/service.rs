use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use capanix_app_sdk::runtime::{EventMetadata, NodeId};
use capanix_app_sdk::{CnxError, Event, Result};
use mysql_source::shared_types::{
    MysqlRowBatch, MysqlSnapshotCursor, MysqlSourceCursor, MysqlSourceOperation,
    MysqlSourcePayload, MysqlSourceRequest,
};
use mysql_source::{MysqlSourceRuntimeConfig, ResolvedMysqlEndpoint};
use source_kit::{SourceErrorPayload, decode_msgpack, encode_msgpack};

use crate::driver::{MysqlSnapshotSession, MysqlSourceDriver};

#[derive(Clone)]
pub struct MysqlSourceService {
    config: MysqlSourceRuntimeConfig,
    driver: Arc<dyn MysqlSourceDriver>,
    origin_id: NodeId,
    sessions: Arc<Mutex<HashMap<String, SnapshotSessionEntry>>>,
}

struct SnapshotSessionEntry {
    object_ref: String,
    schema: String,
    table: String,
    endpoint: ResolvedMysqlEndpoint,
    expires_at_unix_ms: u64,
    session: Box<dyn MysqlSnapshotSession>,
}

impl MysqlSourceService {
    pub fn new(
        config: MysqlSourceRuntimeConfig,
        driver: Arc<dyn MysqlSourceDriver>,
        origin_id: NodeId,
    ) -> Self {
        Self {
            config,
            driver,
            origin_id,
            sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn handle_events(&self, events: Vec<Event>) -> Vec<Event> {
        events
            .into_iter()
            .map(|event| self.handle_event(event))
            .collect()
    }

    pub fn handle_event(&self, event: Event) -> Event {
        let correlation_id = event.metadata().correlation_id;
        let payload = match self.handle_event_payload(event.payload_bytes()) {
            Ok(payload) => payload,
            Err(err) => MysqlSourcePayload::Error(source_kit::error_payload_from_cnx(
                err,
                "mysql-transport",
            )),
        };
        self.reply_event(payload, correlation_id)
            .unwrap_or_else(|err| self.fallback_error_event(err, correlation_id))
    }

    fn handle_event_payload(&self, payload: &[u8]) -> Result<MysqlSourcePayload> {
        let request: MysqlSourceRequest = decode_msgpack(payload)?;
        match request.operation {
            MysqlSourceOperation::ProbeConnection => self.handle_probe(&request),
            MysqlSourceOperation::DiscoverFields => self.handle_discover_fields(&request),
            MysqlSourceOperation::SnapshotOpen => self.handle_snapshot_open(&request),
            MysqlSourceOperation::SnapshotPage => self.handle_snapshot_page(&request),
            MysqlSourceOperation::SnapshotClose => self.handle_snapshot_close(&request),
        }
    }

    fn handle_probe(&self, request: &MysqlSourceRequest) -> Result<MysqlSourcePayload> {
        let endpoint = self.config.endpoint_for(&request.object_ref, None, None)?;
        let credential = self
            .config
            .resolve_credential(endpoint.credential_ref.as_deref())?;
        Ok(MysqlSourcePayload::Probe(
            self.driver.probe(&endpoint, &credential)?,
        ))
    }

    fn handle_discover_fields(&self, request: &MysqlSourceRequest) -> Result<MysqlSourcePayload> {
        let endpoint = self.config.endpoint_for(&request.object_ref, None, None)?;
        let credential = self
            .config
            .resolve_credential(endpoint.credential_ref.as_deref())?;
        Ok(MysqlSourcePayload::Fields(
            self.driver.discover_fields(&endpoint, &credential)?,
        ))
    }

    fn handle_snapshot_open(&self, request: &MysqlSourceRequest) -> Result<MysqlSourcePayload> {
        let schema = required_request_field(request.schema.as_deref(), "schema")?;
        let table = required_request_field(request.table.as_deref(), "table")?;
        let endpoint = self
            .config
            .endpoint_for(&request.object_ref, Some(schema), Some(table))?;
        let credential = self
            .config
            .resolve_credential(endpoint.credential_ref.as_deref())?;
        let mut session = self.driver.open_snapshot(
            &endpoint,
            &credential,
            schema,
            table,
            &request.field_filter,
        )?;
        let expires_at_unix_ms = now_ms() + self.config.product.snapshot_ttl_ms;
        let session_id = format!(
            "{}-{}-{}-{}",
            request.object_ref,
            sanitize_id(schema),
            sanitize_id(table),
            now_us()
        );
        let limit = self.config.effective_limit(request.limit);
        let page = session.fetch_page(limit, 0)?;
        let next_cursor = if page.has_more {
            Some(MysqlSnapshotCursor {
                snapshot_session_id: session_id.clone(),
                schema: schema.to_string(),
                table: table.to_string(),
                last_key: None,
                offset: page.rows.len() as u64,
                expires_at_unix_ms,
            })
        } else {
            None
        };
        if page.has_more || next_cursor.is_some() {
            self.sessions
                .lock()
                .map_err(|_| CnxError::Internal("mysql snapshot session lock poisoned".into()))?
                .insert(
                    session_id.clone(),
                    SnapshotSessionEntry {
                        object_ref: request.object_ref.clone(),
                        schema: schema.to_string(),
                        table: table.to_string(),
                        endpoint: endpoint.clone(),
                        expires_at_unix_ms,
                        session,
                    },
                );
        } else {
            let _ = session.close();
        }
        Ok(MysqlSourcePayload::Rows(MysqlRowBatch {
            object_ref: request.object_ref.clone(),
            schema: schema.to_string(),
            table: table.to_string(),
            fields: page.fields,
            rows: page.rows,
            next_cursor: next_cursor.map(|cursor| {
                MysqlSourceCursor::Snapshot(MysqlSnapshotCursor {
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

    fn handle_snapshot_page(&self, request: &MysqlSourceRequest) -> Result<MysqlSourcePayload> {
        let MysqlSourceCursor::Snapshot(cursor) = request
            .cursor
            .as_ref()
            .ok_or_else(|| CnxError::InvalidInput("snapshot page cursor is required".into()))?;
        if cursor.expires_at_unix_ms <= now_ms() {
            self.drop_session(&cursor.snapshot_session_id)?;
            return Err(CnxError::NotReady(format!(
                "mysql snapshot session '{}' expired",
                cursor.snapshot_session_id
            )));
        }
        let limit = self.config.effective_limit(request.limit);
        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| CnxError::Internal("mysql snapshot session lock poisoned".into()))?;
        let entry = sessions
            .get_mut(&cursor.snapshot_session_id)
            .ok_or_else(|| {
                CnxError::NotReady(format!(
                    "mysql snapshot session '{}' is not active",
                    cursor.snapshot_session_id
                ))
            })?;
        if entry.schema != cursor.schema || entry.table != cursor.table {
            return Err(CnxError::InvalidInput(
                "snapshot cursor schema/table does not match active session".into(),
            ));
        }
        if entry.expires_at_unix_ms <= now_ms() {
            return Err(CnxError::NotReady(format!(
                "mysql snapshot session '{}' expired",
                cursor.snapshot_session_id
            )));
        }
        let page = entry.session.fetch_page(limit, cursor.offset)?;
        let next_cursor = if page.has_more {
            Some(MysqlSourceCursor::Snapshot(MysqlSnapshotCursor {
                snapshot_session_id: cursor.snapshot_session_id.clone(),
                schema: entry.schema.clone(),
                table: entry.table.clone(),
                last_key: None,
                offset: cursor.offset + page.rows.len() as u64,
                expires_at_unix_ms: entry.expires_at_unix_ms,
            }))
        } else {
            let _ = entry.session.close();
            None
        };
        let has_more = page.has_more;
        let batch = MysqlRowBatch {
            object_ref: entry.object_ref.clone(),
            schema: entry.schema.clone(),
            table: entry.table.clone(),
            fields: page.fields,
            rows: page.rows,
            next_cursor,
            has_more,
            grant_epoch: entry.endpoint.grant_epoch,
            diagnostics: None,
        };
        if !has_more {
            sessions.remove(&cursor.snapshot_session_id);
        }
        Ok(MysqlSourcePayload::Rows(batch))
    }

    fn handle_snapshot_close(&self, request: &MysqlSourceRequest) -> Result<MysqlSourcePayload> {
        let MysqlSourceCursor::Snapshot(cursor) = request
            .cursor
            .as_ref()
            .ok_or_else(|| CnxError::InvalidInput("snapshot close cursor is required".into()))?;
        self.drop_session(&cursor.snapshot_session_id)?;
        Ok(MysqlSourcePayload::Rows(MysqlRowBatch {
            object_ref: request.object_ref.clone(),
            schema: cursor.schema.clone(),
            table: cursor.table.clone(),
            fields: Vec::new(),
            rows: Vec::new(),
            next_cursor: None,
            has_more: false,
            grant_epoch: None,
            diagnostics: Some("transaction snapshot session closed".into()),
        }))
    }

    fn drop_session(&self, session_id: &str) -> Result<()> {
        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| CnxError::Internal("mysql snapshot session lock poisoned".into()))?;
        if let Some(mut entry) = sessions.remove(session_id) {
            let _ = entry.session.close();
        }
        Ok(())
    }

    fn reply_event(
        &self,
        payload: MysqlSourcePayload,
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

    fn fallback_error_event(&self, err: CnxError, correlation_id: Option<u64>) -> Event {
        let payload = MysqlSourcePayload::Error(SourceErrorPayload::new(
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

fn required_request_field<'a>(value: Option<&'a str>, key: &str) -> Result<&'a str> {
    value
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| CnxError::InvalidInput(format!("{key} is required")))
}

fn sanitize_id(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect()
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
    Duration::from_millis(mysql_source::config::DEFAULT_SNAPSHOT_TTL_MS)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use capanix_app_sdk::runtime::ConfigValue;
    use mysql_source::shared_types::{
        MysqlFieldCatalog, MysqlProbeStatus, MysqlRowEvent, MysqlSourceRequest,
    };
    use mysql_source::{
        GrantedMysqlEndpoint, MysqlEndpointConfig, MysqlSourceConfig, MysqlSourceRuntimeInputs,
        ResolvedCredential,
    };
    use serde_json::json;

    use super::*;
    use crate::driver::{MysqlSnapshotPage, MysqlSnapshotSession};

    struct FakeDriver {
        opened: AtomicUsize,
        closed: Arc<AtomicUsize>,
    }

    impl FakeDriver {
        fn new() -> Self {
            Self {
                opened: AtomicUsize::new(0),
                closed: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    impl MysqlSourceDriver for FakeDriver {
        fn probe(
            &self,
            endpoint: &ResolvedMysqlEndpoint,
            _credential: &ResolvedCredential,
        ) -> Result<MysqlProbeStatus> {
            Ok(MysqlProbeStatus {
                object_ref: endpoint.object_ref.clone(),
                endpoint_uri: endpoint.endpoint_uri.clone(),
                reachable: true,
                server_version: Some("fake-mysql".into()),
                diagnostics: None,
            })
        }

        fn discover_fields(
            &self,
            endpoint: &ResolvedMysqlEndpoint,
            _credential: &ResolvedCredential,
        ) -> Result<MysqlFieldCatalog> {
            Ok(MysqlFieldCatalog {
                object_ref: endpoint.object_ref.clone(),
                fields: Vec::new(),
            })
        }

        fn open_snapshot(
            &self,
            _endpoint: &ResolvedMysqlEndpoint,
            _credential: &ResolvedCredential,
            _schema: &str,
            _table: &str,
            _fields: &[String],
        ) -> Result<Box<dyn MysqlSnapshotSession>> {
            self.opened.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(FakeSnapshotSession {
                pages: 0,
                closed: self.closed.clone(),
            }))
        }
    }

    struct FakeSnapshotSession {
        pages: usize,
        closed: Arc<AtomicUsize>,
    }

    impl MysqlSnapshotSession for FakeSnapshotSession {
        fn fetch_page(&mut self, _limit: usize, offset: u64) -> Result<MysqlSnapshotPage> {
            self.pages += 1;
            let has_more = self.pages == 1;
            Ok(MysqlSnapshotPage {
                fields: vec!["id".into()],
                rows: vec![MysqlRowEvent {
                    values: json!({ "id": offset + 1 }),
                }],
                next_cursor: None,
                has_more,
            })
        }

        fn close(&mut self) -> Result<()> {
            self.closed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn test_config(snapshot_ttl_ms: u64) -> MysqlSourceRuntimeConfig {
        MysqlSourceRuntimeConfig {
            product: MysqlSourceConfig {
                endpoints: vec![MysqlEndpointConfig {
                    object_ref: "mysql-prod".into(),
                    endpoint_uri: "mysql://db:3306".into(),
                    credential_ref: None,
                    schema_scopes: vec!["app".into()],
                    table_scopes: vec!["users".into()],
                    primary_key: Some("id".into()),
                    active: true,
                }],
                snapshot_ttl_ms,
                ..MysqlSourceConfig::default()
            },
            runtime: MysqlSourceRuntimeInputs {
                endpoint_grants: vec![GrantedMysqlEndpoint {
                    object_ref: "mysql-prod".into(),
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

    fn event_for(request: &MysqlSourceRequest) -> Event {
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

    #[test]
    fn snapshot_open_and_page_use_same_session_and_close_on_terminal_page() {
        let driver = Arc::new(FakeDriver::new());
        let service = MysqlSourceService::new(
            test_config(60_000),
            driver.clone(),
            NodeId("mysql-test".into()),
        );
        let mut open = MysqlSourceRequest::snapshot_open("mysql-prod", "app", "users");
        open.limit = Some(1);

        let reply = service.handle_event(event_for(&open));
        let payload: MysqlSourcePayload = decode_msgpack(reply.payload_bytes()).expect("payload");
        let cursor = match payload {
            MysqlSourcePayload::Rows(batch) => {
                assert_eq!(batch.rows[0].values["id"], json!(1));
                assert_eq!(batch.grant_epoch, Some(3));
                assert!(batch.has_more);
                match batch.next_cursor.expect("cursor") {
                    MysqlSourceCursor::Snapshot(cursor) => cursor,
                }
            }
            other => panic!("expected rows, got {other:?}"),
        };

        let page = MysqlSourceRequest {
            object_ref: "mysql-prod".into(),
            schema: Some("app".into()),
            table: Some("users".into()),
            operation: MysqlSourceOperation::SnapshotPage,
            limit: Some(1),
            cursor: Some(MysqlSourceCursor::Snapshot(cursor)),
            field_filter: Vec::new(),
        };
        let reply = service.handle_event(event_for(&page));
        let payload: MysqlSourcePayload = decode_msgpack(reply.payload_bytes()).expect("payload");

        match payload {
            MysqlSourcePayload::Rows(batch) => {
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
    fn snapshot_page_after_expiry_returns_retryable_not_ready_error() {
        let driver = Arc::new(FakeDriver::new());
        let service =
            MysqlSourceService::new(test_config(0), driver.clone(), NodeId("mysql-test".into()));
        let open = MysqlSourceRequest::snapshot_open("mysql-prod", "app", "users");
        let reply = service.handle_event(event_for(&open));
        let payload: MysqlSourcePayload = decode_msgpack(reply.payload_bytes()).expect("payload");
        let cursor = match payload {
            MysqlSourcePayload::Rows(batch) => match batch.next_cursor.expect("cursor") {
                MysqlSourceCursor::Snapshot(cursor) => cursor,
            },
            other => panic!("expected rows, got {other:?}"),
        };
        let page = MysqlSourceRequest {
            object_ref: "mysql-prod".into(),
            schema: Some("app".into()),
            table: Some("users".into()),
            operation: MysqlSourceOperation::SnapshotPage,
            limit: None,
            cursor: Some(MysqlSourceCursor::Snapshot(cursor)),
            field_filter: Vec::new(),
        };

        let reply = service.handle_event(event_for(&page));
        let payload: MysqlSourcePayload = decode_msgpack(reply.payload_bytes()).expect("payload");

        match payload {
            MysqlSourcePayload::Error(err) => {
                assert_eq!(err.code, "transient-runtime");
                assert!(err.retryable);
            }
            other => panic!("expected error, got {other:?}"),
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
                        ConfigValue::String("mysql-prod".into()),
                    ),
                    (
                        "endpoint_uri".to_string(),
                        ConfigValue::String("mysql://db:3306".into()),
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
                            ConfigValue::String("mysql".into()),
                        ),
                        (
                            "object_ref".to_string(),
                            ConfigValue::String("mysql-prod".into()),
                        ),
                    ]))]),
                )])),
            ),
        ]);

        let config = MysqlSourceRuntimeConfig::from_manifest_config(&cfg).expect("config");

        assert!(
            config
                .endpoint_for("mysql-prod", Some("any"), Some("any"))
                .is_ok()
        );
    }
}

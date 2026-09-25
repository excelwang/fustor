use std::collections::{HashMap, HashSet};
use std::env;
use std::fs::Metadata;
use std::sync::{Arc, Mutex, Weak};

use bytes::Bytes;
use capanix_app_sdk::runtime::{ConfigValue, ControlEnvelope, ControlFrame};
use capanix_app_sdk::{CnxError, Result};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

mod wire;
mod estimate;
mod content;

pub use wire::*;
pub use estimate::*;
pub use content::*;

pub const SOURCE_RESOURCE_GRANTS_CHANGED_KIND: &str = "fustor.source.resource_grants_changed.v1";

pub const FORBIDDEN_SECRET_KEYS: &[&str] = &[
    "password",
    "passwd",
    "api_key",
    "apikey",
    "authorization",
    "bearer_token",
    "token",
    "secret",
    "secret_value",
    "access_key",
    "access_key_id",
    "secret_access_key",
    "client_secret",
    "private_key",
    "signature",
    "awsaccesskeyid",
    "x-amz-credential",
    "x-amz-signature",
    "x-amz-security-token",
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceErrorPayload {
    pub code: String,
    pub message: String,
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget_exhaustion: Option<String>,
}

impl SourceErrorPayload {
    pub fn new(code: impl Into<String>, message: impl Into<String>, retryable: bool) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            retryable,
            budget_exhaustion: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceRuntimeStatus {
    pub ready: bool,
    pub source_kind: String,
    pub configured_endpoints: usize,
    pub active_grants: usize,
    #[serde(default)]
    pub capabilities: SourceCapabilityReport,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SourceOperationCapability {
    Probe,
    FieldDiscovery,
    CatalogMetadata,
    BoundedSnapshot,
    PointLookup,
    IncrementalPoll,
    OrderedStream,
    ExplicitSessionClose,
    BoundedJsonlContent,
    BoundedCsvContent,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SourceConsistencyCapability {
    #[default]
    StatelessPage,
    StableRevisionCursor,
    TransactionSnapshot,
    PointInTime,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SourceContinuationCapability {
    #[default]
    None,
    Cursor,
    NativeSession,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceCapabilityReport {
    pub source_kind: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub operations: Vec<SourceOperationCapability>,
    pub consistency: SourceConsistencyCapability,
    pub continuation: SourceContinuationCapability,
    pub supports_status: bool,
}

impl SourceCapabilityReport {
    pub fn new(
        source_kind: impl Into<String>,
        mut operations: Vec<SourceOperationCapability>,
        consistency: SourceConsistencyCapability,
        continuation: SourceContinuationCapability,
    ) -> Self {
        operations.sort_unstable();
        operations.dedup();
        Self {
            source_kind: source_kind.into(),
            operations,
            consistency,
            continuation,
            supports_status: true,
        }
    }

    pub fn supports(&self, capability: SourceOperationCapability) -> bool {
        self.operations.binary_search(&capability).is_ok()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceResourceGrantsChanged {
    pub source_kind: String,
    pub version: u64,
    #[serde(default)]
    pub resource_grants: Vec<HashMap<String, ConfigValue>>,
}

#[derive(Clone)]
pub struct SourceControlState<T: Clone> {
    inner: Arc<Mutex<SourceControlStateInner<T>>>,
}

struct SourceControlStateInner<T: Clone> {
    config: T,
    control_version: u64,
    fetch_route_active: bool,
    status_route_active: bool,
    listeners: Vec<Weak<dyn Fn(SourceControlChange) + Send + Sync>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceControlChange {
    ConfigReplaced { version: u64 },
    FetchRouteActive { active: bool },
    StatusRouteActive { active: bool },
}

#[derive(Clone)]
pub struct SourceControlSubscription {
    _listener: Arc<dyn Fn(SourceControlChange) + Send + Sync>,
}

impl<T: Clone> SourceControlState<T> {
    pub fn new(config: T) -> Self {
        Self {
            inner: Arc::new(Mutex::new(SourceControlStateInner {
                config,
                control_version: 0,
                fetch_route_active: true,
                status_route_active: true,
                listeners: Vec::new(),
            })),
        }
    }

    pub fn config(&self) -> Result<T> {
        Ok(self
            .inner
            .lock()
            .map_err(|_| CnxError::Internal("source control state lock poisoned".into()))?
            .config
            .clone())
    }

    pub fn replace_config_if_newer(&self, version: u64, config: T) -> Result<bool> {
        let listeners = {
            let mut guard = self
                .inner
                .lock()
                .map_err(|_| CnxError::Internal("source control state lock poisoned".into()))?;
            if version <= guard.control_version {
                return Ok(false);
            }
            guard.config = config;
            guard.control_version = version;
            live_control_listeners(&mut guard.listeners)
        };
        notify_control_listeners(listeners, SourceControlChange::ConfigReplaced { version });
        Ok(true)
    }

    pub fn set_fetch_route_active(&self, active: bool) -> Result<()> {
        let listeners = {
            let mut guard = self
                .inner
                .lock()
                .map_err(|_| CnxError::Internal("source control state lock poisoned".into()))?;
            if guard.fetch_route_active == active {
                return Ok(());
            }
            guard.fetch_route_active = active;
            live_control_listeners(&mut guard.listeners)
        };
        notify_control_listeners(listeners, SourceControlChange::FetchRouteActive { active });
        Ok(())
    }

    pub fn set_status_route_active(&self, active: bool) -> Result<()> {
        let listeners = {
            let mut guard = self
                .inner
                .lock()
                .map_err(|_| CnxError::Internal("source control state lock poisoned".into()))?;
            if guard.status_route_active == active {
                return Ok(());
            }
            guard.status_route_active = active;
            live_control_listeners(&mut guard.listeners)
        };
        notify_control_listeners(listeners, SourceControlChange::StatusRouteActive { active });
        Ok(())
    }

    pub fn subscribe(
        &self,
        listener: impl Fn(SourceControlChange) + Send + Sync + 'static,
    ) -> Result<SourceControlSubscription> {
        let listener: Arc<dyn Fn(SourceControlChange) + Send + Sync> = Arc::new(listener);
        self.inner
            .lock()
            .map_err(|_| CnxError::Internal("source control state lock poisoned".into()))?
            .listeners
            .push(Arc::downgrade(&listener));
        Ok(SourceControlSubscription {
            _listener: listener,
        })
    }

    pub fn snapshot(&self) -> Result<SourceControlSnapshot<T>> {
        let guard = self
            .inner
            .lock()
            .map_err(|_| CnxError::Internal("source control state lock poisoned".into()))?;
        Ok(SourceControlSnapshot {
            config: guard.config.clone(),
            control_version: guard.control_version,
            fetch_route_active: guard.fetch_route_active,
            status_route_active: guard.status_route_active,
        })
    }
}

fn live_control_listeners(
    listeners: &mut Vec<Weak<dyn Fn(SourceControlChange) + Send + Sync>>,
) -> Vec<Arc<dyn Fn(SourceControlChange) + Send + Sync>> {
    let live = listeners
        .iter()
        .filter_map(Weak::upgrade)
        .collect::<Vec<_>>();
    listeners.retain(|listener| listener.strong_count() > 0);
    live
}

fn notify_control_listeners(
    listeners: Vec<Arc<dyn Fn(SourceControlChange) + Send + Sync>>,
    change: SourceControlChange,
) {
    for listener in listeners {
        listener(change);
    }
}

#[derive(Clone)]
pub struct SourceControlSnapshot<T: Clone> {
    pub config: T,
    pub control_version: u64,
    pub fetch_route_active: bool,
    pub status_route_active: bool,
}

impl SourceRuntimeStatus {
    pub fn new(
        source_kind: impl Into<String>,
        configured_endpoints: usize,
        active_grants: usize,
        diagnostics: Vec<String>,
    ) -> Self {
        Self {
            ready: configured_endpoints > 0 && active_grants > 0,
            source_kind: source_kind.into(),
            configured_endpoints,
            active_grants,
            capabilities: SourceCapabilityReport::default(),
            diagnostics,
        }
    }

    pub fn with_capabilities(mut self, capabilities: SourceCapabilityReport) -> Self {
        debug_assert!(
            capabilities.source_kind.is_empty() || capabilities.source_kind == self.source_kind
        );
        self.capabilities = capabilities;
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceCredentialConfig {
    pub credential_ref: String,
    pub source: CredentialSource,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CredentialSource {
    None,
    BasicEnv {
        username_env: String,
        password_env: String,
    },
    ApiKeyEnv {
        api_key_env: String,
    },
    BearerEnv {
        token_env: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResolvedCredential {
    None,
    Basic { username: String, password: String },
    ApiKey { api_key: String },
    Bearer { token: String },
}

impl SourceCredentialConfig {
    pub fn resolve_from_env(&self) -> Result<ResolvedCredential> {
        match &self.source {
            CredentialSource::None => Ok(ResolvedCredential::None),
            CredentialSource::BasicEnv {
                username_env,
                password_env,
            } => Ok(ResolvedCredential::Basic {
                username: read_env(username_env, &self.credential_ref)?,
                password: read_env(password_env, &self.credential_ref)?,
            }),
            CredentialSource::ApiKeyEnv { api_key_env } => Ok(ResolvedCredential::ApiKey {
                api_key: read_env(api_key_env, &self.credential_ref)?,
            }),
            CredentialSource::BearerEnv { token_env } => Ok(ResolvedCredential::Bearer {
                token: read_env(token_env, &self.credential_ref)?,
            }),
        }
    }
}

pub fn encode_msgpack<T: Serialize>(value: &T) -> Result<Bytes> {
    rmp_serde::to_vec_named(value)
        .map(Bytes::from)
        .map_err(|err| CnxError::Internal(format!("encode source msgpack payload: {err}")))
}

pub fn decode_msgpack<T: DeserializeOwned>(payload: &[u8]) -> Result<T> {
    rmp_serde::from_slice(payload)
        .map_err(|err| CnxError::InvalidInput(format!("decode source msgpack payload: {err}")))
}

pub fn encode_source_resource_grants_changed(
    changed: &SourceResourceGrantsChanged,
) -> Result<ControlEnvelope> {
    let payload = rmp_serde::to_vec_named(changed)
        .map_err(|err| CnxError::InvalidInput(format!("encode source grants changed: {err}")))?;
    Ok(ControlEnvelope::Frame(ControlFrame {
        kind: SOURCE_RESOURCE_GRANTS_CHANGED_KIND.to_string(),
        payload,
    }))
}

pub fn decode_source_resource_grants_changed(
    envelope: &ControlEnvelope,
) -> Result<Option<SourceResourceGrantsChanged>> {
    let ControlEnvelope::Frame(frame) = envelope else {
        return Ok(None);
    };
    if frame.kind != SOURCE_RESOURCE_GRANTS_CHANGED_KIND {
        return Ok(None);
    }
    rmp_serde::from_slice::<SourceResourceGrantsChanged>(&frame.payload)
        .map(Some)
        .map_err(|err| CnxError::InvalidInput(format!("decode source grants changed: {err}")))
}

pub fn parse_credential_config(
    row: &HashMap<String, ConfigValue>,
) -> Result<SourceCredentialConfig> {
    if contains_forbidden_secret_key(row) {
        return Err(CnxError::InvalidInput(
            "credentials[] must contain env var references, not secret material".into(),
        ));
    }
    let credential_ref = required_str(row, "credential_ref")?;
    let auth_type = optional_nonempty_str(row, "auth_type").unwrap_or_else(|| "none".into());
    let source = match auth_type.as_str() {
        "none" => CredentialSource::None,
        "basic_env" => CredentialSource::BasicEnv {
            username_env: required_str(row, "username_env")?,
            password_env: required_str(row, "password_env")?,
        },
        "api_key_env" => CredentialSource::ApiKeyEnv {
            api_key_env: required_str(row, "api_key_env")?,
        },
        "bearer_env" => CredentialSource::BearerEnv {
            token_env: required_str(row, "token_env")?,
        },
        other => {
            return Err(CnxError::InvalidInput(format!(
                "unsupported credentials[].auth_type '{other}'"
            )));
        }
    };
    Ok(SourceCredentialConfig {
        credential_ref,
        source,
    })
}

pub fn resolve_credential(
    credentials: &[SourceCredentialConfig],
    credential_ref: Option<&str>,
) -> Result<ResolvedCredential> {
    let Some(credential_ref) = credential_ref else {
        return Ok(ResolvedCredential::None);
    };
    let credential = credentials
        .iter()
        .find(|credential| credential.credential_ref == credential_ref)
        .ok_or_else(|| {
            CnxError::AccessDenied(format!(
                "missing local credential binding '{credential_ref}'"
            ))
        })?;
    credential.resolve_from_env()
}

pub fn contains_forbidden_secret_key(row: &HashMap<String, ConfigValue>) -> bool {
    fn scan_map(row: &HashMap<String, ConfigValue>) -> bool {
        row.iter().any(|(key, value)| {
            let normalized = key.trim().to_ascii_lowercase();
            FORBIDDEN_SECRET_KEYS
                .iter()
                .any(|forbidden| normalized == *forbidden)
                || scan_value(value)
        })
    }
    fn scan_value(value: &ConfigValue) -> bool {
        match value {
            ConfigValue::Array(items) => items.iter().any(scan_value),
            ConfigValue::Map(row) => scan_map(row),
            _ => false,
        }
    }
    scan_map(row)
}

pub fn reject_secret_material(row: &HashMap<String, ConfigValue>, context: &str) -> Result<()> {
    if contains_forbidden_secret_key(row) {
        Err(CnxError::InvalidInput(format!(
            "{context} must not contain credential material"
        )))
    } else {
        Ok(())
    }
}

pub fn pattern_allowed(value: &str, scopes: &[String]) -> bool {
    scopes.iter().any(|scope| pattern_matches(scope, value))
}

pub fn pattern_allowed_by_product_and_grant(
    value: &str,
    product_scopes: &[String],
    grant_scopes: &[String],
) -> bool {
    pattern_allowed(value, product_scopes)
        && (grant_scopes.is_empty() || pattern_allowed(value, grant_scopes))
}

pub fn require_grant_interface(interfaces: &[String], required: &str, context: &str) -> Result<()> {
    if interfaces
        .iter()
        .any(|interface| interface.eq_ignore_ascii_case(required))
    {
        Ok(())
    } else {
        Err(CnxError::AccessDenied(format!(
            "{context} runtime grant does not expose required '{required}' interface"
        )))
    }
}

pub fn validate_secret_free_endpoint_uri(value: &str, context: &str) -> Result<()> {
    let uri = url::Url::parse(value)
        .map_err(|err| CnxError::InvalidInput(format!("invalid {context}: {err}")))?;
    let has_user_info = !uri.username().is_empty() || uri.password().is_some();
    let has_secret_query = uri.query_pairs().any(|(key, _)| {
        let key = key.trim().to_ascii_lowercase();
        FORBIDDEN_SECRET_KEYS
            .iter()
            .any(|forbidden| key == *forbidden)
    });
    if has_user_info || has_secret_query {
        return Err(CnxError::InvalidInput(format!(
            "{context} must not contain credential material"
        )));
    }
    Ok(())
}

pub fn pattern_matches(scope: &str, value: &str) -> bool {
    if scope == "*" || scope == value {
        return true;
    }
    match scope.split_once('*') {
        Some((prefix, suffix)) => value.starts_with(prefix) && value.ends_with(suffix),
        None => false,
    }
}

pub fn effective_limit(
    requested: Option<usize>,
    default_page_size: usize,
    max_page_size: usize,
) -> usize {
    let limit = requested.unwrap_or(default_page_size).max(1);
    limit.min(max_page_size.max(1))
}

pub fn filesystem_revision(metadata: &Metadata) -> Result<String> {
    let modified_ns = metadata
        .modified()
        .map_err(|err| CnxError::PeerError(format!("read filesystem modification time: {err}")))?
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| {
            CnxError::PeerError("filesystem modification time is before unix epoch".into())
        })?
        .as_nanos();
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        Ok(format!(
            "fs:{}:{}:{}:{}:{}:{}",
            metadata.dev(),
            metadata.ino(),
            metadata.len(),
            modified_ns,
            metadata.ctime(),
            metadata.ctime_nsec()
        ))
    }
    #[cfg(not(unix))]
    {
        Ok(format!(
            "fs:{}:{}:{}",
            metadata.len(),
            modified_ns,
            metadata.is_dir()
        ))
    }
}

pub fn reject_duplicate_refs<'a, I>(refs: I, context: &str) -> Result<()>
where
    I: IntoIterator<Item = &'a str>,
{
    let mut seen = HashSet::new();
    for object_ref in refs {
        if !seen.insert(object_ref) {
            return Err(CnxError::InvalidInput(format!(
                "duplicate {context} object_ref '{object_ref}'"
            )));
        }
    }
    Ok(())
}

pub fn validate_grants_match_product_endpoints<'a, E, G>(
    endpoint_refs: E,
    grant_refs: G,
    context: &str,
) -> Result<()>
where
    E: IntoIterator<Item = &'a str>,
    G: IntoIterator<Item = &'a str>,
{
    let endpoint_refs: HashSet<&str> = endpoint_refs.into_iter().collect();
    for grant_ref in grant_refs {
        if !endpoint_refs.contains(grant_ref) {
            return Err(CnxError::InvalidInput(format!(
                "{context} grants unknown endpoint_ref '{grant_ref}'"
            )));
        }
    }
    Ok(())
}

pub fn normalize_page_size(value: i64, key: &str) -> Result<usize> {
    if value <= 0 {
        return Err(CnxError::InvalidInput(format!("{key} must be positive")));
    }
    usize::try_from(value).map_err(|_| CnxError::InvalidInput(format!("{key} is too large")))
}

pub fn required_str(row: &HashMap<String, ConfigValue>, key: &str) -> Result<String> {
    optional_nonempty_str(row, key)
        .ok_or_else(|| CnxError::InvalidInput(format!("{key} is required")))
}

pub fn optional_nonempty_str(row: &HashMap<String, ConfigValue>, key: &str) -> Option<String> {
    match row.get(key) {
        Some(ConfigValue::String(value)) if !value.trim().is_empty() => {
            Some(value.trim().to_string())
        }
        _ => None,
    }
}

pub fn string_array(row: &HashMap<String, ConfigValue>, key: &str) -> Option<Vec<String>> {
    let ConfigValue::Array(items) = row.get(key)? else {
        return None;
    };
    let values = items
        .iter()
        .filter_map(|item| match item {
            ConfigValue::String(value) if !value.trim().is_empty() => {
                Some(value.trim().to_string())
            }
            _ => None,
        })
        .collect();
    Some(values)
}

pub fn bool_value(row: &HashMap<String, ConfigValue>, key: &str, default: bool) -> bool {
    match row.get(key) {
        Some(ConfigValue::Bool(value)) => *value,
        _ => default,
    }
}

pub fn get_int(row: &HashMap<String, ConfigValue>, key: &str) -> Option<i64> {
    match row.get(key) {
        Some(ConfigValue::Int(value)) => Some(*value),
        _ => None,
    }
}

pub fn get_str<'a>(row: &'a HashMap<String, ConfigValue>, key: &str) -> Option<&'a str> {
    match row.get(key) {
        Some(ConfigValue::String(value)) if !value.trim().is_empty() => Some(value.trim()),
        _ => None,
    }
}

pub fn get_map<'a>(
    row: &'a HashMap<String, ConfigValue>,
    key: &str,
) -> Option<&'a HashMap<String, ConfigValue>> {
    match row.get(key) {
        Some(ConfigValue::Map(map)) => Some(map),
        _ => None,
    }
}

pub fn error_payload_from_cnx(err: CnxError, transport_code: &'static str) -> SourceErrorPayload {
    let (code, retryable) = match &err {
        CnxError::AccessDenied(_) => ("access-denied", false),
        CnxError::ScopeDenied(_) => ("scope-denied", false),
        CnxError::InvalidInput(_) | CnxError::ProtocolViolation(_) => ("invalid-request", false),
        CnxError::ResourceExhausted(_) => ("budget-exceeded", false),
        CnxError::Timeout
        | CnxError::Backpressure
        | CnxError::ChannelClosed
        | CnxError::NotReady(_) => ("transient-runtime", true),
        CnxError::LinkError(_) | CnxError::TransportClosed(_) => (transport_code, true),
        CnxError::PeerError(_) => ("source-peer", false),
        _ => ("internal", true),
    };
    let axis = match &err {
        CnxError::Timeout => Some("max_latency_ms"),
        CnxError::ResourceExhausted(reason) if reason.contains("max_bytes") => Some("max_bytes"),
        CnxError::ResourceExhausted(reason) if reason.contains("max_latency_ms") => Some("max_latency_ms"),
        _ => None,
    };
    let mut payload = SourceErrorPayload::new(code, err.to_string(), retryable);
    payload.budget_exhaustion = axis.map(str::to_string);
    payload
}

fn read_env(var: &str, credential_ref: &str) -> Result<String> {
    env::var(var).map_err(|_| {
        CnxError::AccessDenied(format!(
            "credential '{credential_ref}' references missing env var '{var}'"
        ))
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn string(value: &str) -> ConfigValue {
        ConfigValue::String(value.to_string())
    }

    #[test]
    fn secret_material_detection_scans_nested_maps() {
        let row = HashMap::from([(
            "nested".to_string(),
            ConfigValue::Map(HashMap::from([("api_key".to_string(), string("secret"))])),
        )]);

        assert!(contains_forbidden_secret_key(&row));
    }

    #[test]
    fn pattern_scope_supports_single_wildcard() {
        assert!(pattern_allowed("logs-2026-05", &["logs-*".to_string()]));
        assert!(pattern_allowed("a-middle-z", &["a-*-z".to_string()]));
        assert!(!pattern_allowed("a-middle-y", &["a-*-z".to_string()]));
    }

    #[test]
    fn msgpack_roundtrip_works_for_error_payload() {
        let payload = SourceErrorPayload::new("invalid-request", "bad", false);
        let encoded = encode_msgpack(&payload).expect("encode");
        let restored: SourceErrorPayload = decode_msgpack(&encoded).expect("decode");

        assert_eq!(restored, payload);
    }

    #[test]
    fn status_payload_roundtrips() {
        let payload = SourceRuntimeStatus::new(
            "mysql",
            1,
            1,
            vec!["fetch route is request/reply only".to_string()],
        );
        let encoded = encode_msgpack(&payload).expect("encode");
        let restored: SourceRuntimeStatus = decode_msgpack(&encoded).expect("decode");

        assert_eq!(restored, payload);
        assert!(restored.ready);
    }

    #[test]
    fn source_resource_grants_changed_control_frame_roundtrips() {
        let changed = SourceResourceGrantsChanged {
            source_kind: "pg".into(),
            version: 7,
            resource_grants: vec![HashMap::from([
                ("resource_kind".to_string(), string("pg")),
                ("object_ref".to_string(), string("warehouse")),
            ])],
        };

        let envelope = encode_source_resource_grants_changed(&changed).expect("encode");
        let restored = decode_source_resource_grants_changed(&envelope)
            .expect("decode")
            .expect("source grants frame");

        assert_eq!(restored.source_kind, "pg");
        assert_eq!(restored.version, 7);
        assert_eq!(restored.resource_grants.len(), 1);
        assert_eq!(
            get_str(&restored.resource_grants[0], "resource_kind"),
            Some("pg")
        );
        assert_eq!(
            get_str(&restored.resource_grants[0], "object_ref"),
            Some("warehouse")
        );
    }

    #[test]
    fn source_resource_grants_changed_decoder_ignores_unrelated_frames() {
        let envelope = ControlEnvelope::Frame(ControlFrame {
            kind: "runtime.exec.activate.v1".into(),
            payload: Vec::new(),
        });

        assert!(
            decode_source_resource_grants_changed(&envelope)
                .expect("decode")
                .is_none()
        );
    }

    #[test]
    fn source_control_state_applies_only_newer_configs_and_route_flags() {
        let state = SourceControlState::new("v1".to_string());
        let observed = Arc::new(Mutex::new(Vec::new()));
        let observed_by_listener = observed.clone();
        let _subscription = state
            .subscribe(move |change| observed_by_listener.lock().unwrap().push(change))
            .expect("subscribe");

        assert!(
            state
                .replace_config_if_newer(2, "v2".to_string())
                .expect("replace")
        );
        assert!(
            !state
                .replace_config_if_newer(1, "stale".to_string())
                .expect("stale")
        );
        state.set_fetch_route_active(false).expect("fetch route");
        state.set_status_route_active(false).expect("status route");

        let snapshot = state.snapshot().expect("snapshot");
        assert_eq!(snapshot.config, "v2");
        assert_eq!(snapshot.control_version, 2);
        assert!(!snapshot.fetch_route_active);
        assert!(!snapshot.status_route_active);
        assert_eq!(
            *observed.lock().unwrap(),
            vec![
                SourceControlChange::ConfigReplaced { version: 2 },
                SourceControlChange::FetchRouteActive { active: false },
                SourceControlChange::StatusRouteActive { active: false },
            ]
        );
    }

    #[test]
    fn duplicate_refs_fail_closed() {
        let err = reject_duplicate_refs(["alpha", "beta", "alpha"], "mysql endpoint")
            .expect_err("duplicate ref should fail");

        assert!(err.to_string().contains("duplicate mysql endpoint"));
    }

    #[test]
    fn orphan_runtime_grants_fail_closed() {
        let err = validate_grants_match_product_endpoints(
            ["endpoint-a"],
            ["endpoint-a", "endpoint-b"],
            "s3",
        )
        .expect_err("unknown grant should fail");

        assert!(
            err.to_string()
                .contains("unknown endpoint_ref 'endpoint-b'")
        );
    }

    #[test]
    fn grant_scopes_cannot_widen_product_scope() {
        assert!(pattern_allowed_by_product_and_grant(
            "logs-2026",
            &["logs-*".to_string()],
            &["logs-2026".to_string()]
        ));
        assert!(!pattern_allowed_by_product_and_grant(
            "secrets-2026",
            &["logs-*".to_string()],
            &["*".to_string()]
        ));
    }

    #[test]
    fn runtime_grant_requires_declared_interface() {
        require_grant_interface(&["read".into()], "read", "pg endpoint").expect("read interface");
        assert!(matches!(
            require_grant_interface(&["status".into()], "read", "pg endpoint"),
            Err(CnxError::AccessDenied(_))
        ));
        assert!(matches!(
            require_grant_interface(&[], "read", "pg endpoint"),
            Err(CnxError::AccessDenied(_))
        ));
    }

    #[test]
    fn endpoint_uri_rejects_user_info_and_secret_query_material() {
        validate_secret_free_endpoint_uri("https://example.test:443/path", "es endpoint_uri")
            .expect("secret-free URI");
        for uri in [
            "https://user:pass@example.test/path",
            "https://example.test/path?api_key=secret",
            "s3://example.test/path?secret_access_key=secret",
            "https://example.test/path?X-Amz-Credential=key&X-Amz-Signature=secret",
        ] {
            assert!(
                validate_secret_free_endpoint_uri(uri, "source endpoint_uri").is_err(),
                "credential-bearing URI must fail: {uri}"
            );
        }
    }

    #[test]
    fn source_operation_budget_enforces_deadline_and_scanned_bytes() {
        let mut bytes = SourceOperationBudget::new(None, Some(4));
        bytes.consume_bytes(4).expect("consume exact budget");
        assert!(matches!(
            bytes.consume_bytes(1),
            Err(CnxError::ResourceExhausted(_))
        ));
        assert_eq!(bytes.consumed_bytes(), 4);

        let deadline = SourceOperationBudget::new(Some(Duration::ZERO), None);
        assert!(matches!(deadline.check_deadline(), Err(CnxError::Timeout)));

        let now_ms = source_now_ms();
        let context = SourceExecutionContext {
            request_id: "budget:test".into(),
            grant_epoch: 1,
            issued_at_ms: now_ms,
            deadline_ms: now_ms + 1_000,
            limits: SourceExecutionLimits {
                max_rows: 1,
                max_bytes: 1,
                max_calls: 1,
                max_fanout: 1,
                max_latency_ms: 1_000,
                max_cost_units: 1,
            },
        };
        let mut calls = SourceOperationBudget::from_context(Some(&context), now_ms)
            .expect("bounded operation budget");
        calls.observe_fanout(1).expect("one source");
        calls.consume_call(1).expect("first call");
        assert!(matches!(
            calls.consume_call(1),
            Err(CnxError::ResourceExhausted(_))
        ));
        assert_eq!(calls.accounting(), SourceOperationAccounting::one_call());
    }

    #[test]
    fn resource_exhaustion_maps_to_non_retryable_budget_error() {
        let payload = error_payload_from_cnx(
            CnxError::ResourceExhausted("bounded scan exhausted".into()),
            "source-transport",
        );

        assert_eq!(payload.code, "budget-exceeded");
        assert!(!payload.retryable);
    }
}

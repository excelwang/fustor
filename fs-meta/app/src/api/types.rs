use crate::source::config::GrantedMountRoot;
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionUser {
    pub username: String,
    pub uid: u32,
    pub gid: u32,
    pub groups: Vec<String>,
    pub home: String,
    pub shell: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoginResponse {
    pub token: String,
    pub expires_in_secs: u64,
    pub user: SessionUser,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueryApiKeySummary {
    pub key_id: String,
    pub label: String,
    pub created_at_us: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateQueryApiKeyRequest {
    pub label: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueryApiKeysResponse {
    pub keys: Vec<QueryApiKeySummary>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CreateQueryApiKeyResponse {
    pub api_key: String,
    pub key: QueryApiKeySummary,
}

#[derive(Debug, Clone, Serialize)]
pub struct RevokeQueryApiKeyResponse {
    pub revoked: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct DegradedRoot {
    pub root_key: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatusSourceLogicalRoot {
    pub root_id: String,
    pub status: String,
    pub matched_grants: usize,
    pub active_members: usize,
    pub coverage_mode: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatusSourceConcreteRoot {
    pub root_key: String,
    pub logical_root_id: String,
    pub object_ref: String,
    pub status: String,
    pub coverage_mode: String,
    pub watch_enabled: bool,
    pub scan_enabled: bool,
    pub is_group_primary: bool,
    pub active: bool,
    pub watch_lru_capacity: usize,
    pub audit_interval_ms: u64,
    pub overflow_count: u64,
    pub overflow_pending: bool,
    pub rescan_pending: bool,
    pub last_rescan_reason: Option<String>,
    pub last_error: Option<String>,
    pub last_audit_started_at_us: Option<u64>,
    pub last_audit_completed_at_us: Option<u64>,
    pub last_audit_duration_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatusSource {
    pub lifecycle_state: String,
    pub host_object_grants_version: u64,
    pub grants_count: usize,
    pub roots_count: usize,
    pub degraded_roots: Vec<DegradedRoot>,
    pub logical_roots: Vec<StatusSourceLogicalRoot>,
    pub concrete_roots: Vec<StatusSourceConcreteRoot>,
    pub debug: StatusSourceDebug,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatusSourceDebug {
    pub source_primary_by_group: BTreeMap<String, String>,
    pub last_force_find_runner_by_group: BTreeMap<String, String>,
    pub force_find_inflight_groups: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatusSinkGroup {
    pub group_id: String,
    pub primary_object_ref: String,
    pub total_nodes: u64,
    pub live_nodes: u64,
    pub tombstoned_count: u64,
    pub attested_count: u64,
    pub suspect_count: u64,
    pub blind_spot_count: u64,
    pub shadow_time_us: u64,
    pub shadow_lag_us: u64,
    pub overflow_pending_audit: bool,
    pub initial_audit_completed: bool,
    pub estimated_heap_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatusSink {
    pub live_nodes: u64,
    pub tombstoned_count: u64,
    pub attested_count: u64,
    pub suspect_count: u64,
    pub blind_spot_count: u64,
    pub shadow_time_us: u64,
    pub estimated_heap_bytes: u64,
    pub groups: Vec<StatusSinkGroup>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatusFacadePending {
    pub route_key: String,
    pub generation: u64,
    pub resource_ids: Vec<String>,
    pub runtime_managed: bool,
    pub runtime_exposure_confirmed: bool,
    pub reason: String,
    pub retry_attempts: u64,
    pub pending_since_us: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_attempt_at_us: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error_at_us: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_backoff_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_retry_at_us: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatusFacade {
    pub pending: StatusFacadePending,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatusResponse {
    pub source: StatusSource,
    pub sink: StatusSink,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facade: Option<StatusFacade>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RuntimeGrantsResponse {
    pub grants: Vec<GrantedMountRoot>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RootSelectorEntry {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mount_point: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fs_source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fs_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_ip: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_ref: Option<String>,
}

impl RootSelectorEntry {
    pub fn is_empty(&self) -> bool {
        self.mount_point.is_none()
            && self.fs_source.is_none()
            && self.fs_type.is_none()
            && self.host_ip.is_none()
            && self.host_ref.is_none()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RootEntry {
    pub id: String,
    #[serde(default)]
    pub selector: RootSelectorEntry,
    #[serde(default = "default_root_subpath_scope")]
    pub subpath_scope: String,
    #[serde(default = "default_true")]
    pub watch: bool,
    #[serde(default = "default_true")]
    pub scan: bool,
    #[serde(default)]
    pub audit_interval_ms: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RootUpdateEntry {
    pub id: String,
    #[serde(default)]
    pub selector: RootSelectorEntry,
    #[serde(default = "default_root_subpath_scope")]
    pub subpath_scope: String,
    #[serde(default = "default_true")]
    pub watch: bool,
    #[serde(default = "default_true")]
    pub scan: bool,
    #[serde(default)]
    pub audit_interval_ms: Option<u64>,
    #[serde(
        default,
        rename = "source_locator",
        deserialize_with = "deserialize_field_presence"
    )]
    pub source_locator_present: bool,
    #[serde(
        default,
        rename = "path",
        deserialize_with = "deserialize_field_presence"
    )]
    pub path_present: bool,
}

fn default_true() -> bool {
    true
}

fn default_root_subpath_scope() -> String {
    "/".to_string()
}

fn deserialize_field_presence<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    let _ = serde_json::Value::deserialize(deserializer)?;
    Ok(true)
}

#[derive(Debug, Clone, Serialize)]
pub struct RootsResponse {
    pub roots: Vec<RootEntry>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RootsUpdateRequest {
    pub roots: Vec<RootUpdateEntry>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RootsUpdateResponse {
    pub roots_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct RootPreviewItem {
    pub root_id: String,
    pub matched_grants: Vec<GrantedMountRoot>,
    pub monitor_paths: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RootsPreviewResponse {
    pub preview: Vec<RootPreviewItem>,
    pub unmatched_roots: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RescanResponse {
    pub accepted: bool,
}

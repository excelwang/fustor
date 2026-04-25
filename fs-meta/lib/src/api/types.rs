use crate::domain_state::{
    FacadeServiceState, GroupServiceState, NodeParticipationState, RolloutGenerationState,
};
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
pub struct ObservationCoverageCapabilities {
    pub exists_coverage: bool,
    pub file_count_coverage: bool,
    pub file_metadata_coverage: bool,
    pub mtime_size_coverage: bool,
    pub watch_freshness_coverage: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatusSourceLogicalRoot {
    pub root_id: String,
    pub service_state: GroupServiceState,
    pub matched_grants: usize,
    pub active_members: usize,
    pub coverage_mode: String,
    pub coverage_capabilities: ObservationCoverageCapabilities,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatusSourceConcreteRoot {
    pub root_key: String,
    pub logical_root_id: String,
    pub object_ref: String,
    pub participation_state: NodeParticipationState,
    pub coverage_mode: String,
    pub coverage_capabilities: ObservationCoverageCapabilities,
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
    pub emitted_batch_count: u64,
    pub emitted_event_count: u64,
    pub emitted_control_event_count: u64,
    pub emitted_data_event_count: u64,
    pub emitted_path_capture_target: Option<String>,
    pub emitted_path_event_count: u64,
    pub last_emitted_at_us: Option<u64>,
    pub last_emitted_origins: Vec<String>,
    pub forwarded_batch_count: u64,
    pub forwarded_event_count: u64,
    pub forwarded_path_event_count: u64,
    pub last_forwarded_at_us: Option<u64>,
    pub last_forwarded_origins: Vec<String>,
    pub current_revision: Option<u64>,
    pub current_stream_generation: Option<u64>,
    pub candidate_revision: Option<u64>,
    pub candidate_stream_generation: Option<u64>,
    pub candidate_participation_state: Option<NodeParticipationState>,
    pub draining_revision: Option<u64>,
    pub draining_stream_generation: Option<u64>,
    pub draining_participation_state: Option<NodeParticipationState>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatusSource {
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
    pub lifecycle_state: String,
    pub current_stream_generation: Option<u64>,
    pub source_primary_by_group: BTreeMap<String, String>,
    pub last_force_find_runner_by_group: BTreeMap<String, String>,
    pub last_force_find_runners_by_group: BTreeMap<String, Vec<String>>,
    pub force_find_inflight_groups: Vec<String>,
    pub scheduled_source_groups_by_node: BTreeMap<String, Vec<String>>,
    pub scheduled_scan_groups_by_node: BTreeMap<String, Vec<String>>,
    pub last_control_frame_signals_by_node: BTreeMap<String, Vec<String>>,
    pub published_batches_by_node: BTreeMap<String, u64>,
    pub published_events_by_node: BTreeMap<String, u64>,
    pub published_control_events_by_node: BTreeMap<String, u64>,
    pub published_data_events_by_node: BTreeMap<String, u64>,
    pub last_published_at_us_by_node: BTreeMap<String, u64>,
    pub last_published_origins_by_node: BTreeMap<String, Vec<String>>,
    pub published_origin_counts_by_node: BTreeMap<String, Vec<String>>,
    pub published_path_capture_target: Option<String>,
    pub enqueued_path_origin_counts_by_node: BTreeMap<String, Vec<String>>,
    pub pending_path_origin_counts_by_node: BTreeMap<String, Vec<String>>,
    pub yielded_path_origin_counts_by_node: BTreeMap<String, Vec<String>>,
    pub summarized_path_origin_counts_by_node: BTreeMap<String, Vec<String>>,
    pub published_path_origin_counts_by_node: BTreeMap<String, Vec<String>>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum StatusSinkGroupMaterializationReadiness {
    PendingMaterialization,
    WaitingForMaterializedRoot,
    Ready,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatusSinkGroup {
    pub group_id: String,
    pub service_state: GroupServiceState,
    pub primary_object_ref: String,
    pub total_nodes: u64,
    pub live_nodes: u64,
    pub tombstoned_count: u64,
    pub attested_count: u64,
    pub suspect_count: u64,
    pub blind_spot_count: u64,
    pub shadow_time_us: u64,
    pub shadow_lag_us: u64,
    pub overflow_pending_materialization: bool,
    pub initial_audit_completed: bool,
    pub materialization_readiness: StatusSinkGroupMaterializationReadiness,
    pub estimated_heap_bytes: u64,
}

impl StatusSinkGroup {
    pub fn is_ready(&self) -> bool {
        matches!(
            self.materialization_readiness,
            StatusSinkGroupMaterializationReadiness::Ready
        )
    }
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
    pub debug: StatusSinkDebug,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatusSinkDebug {
    pub scheduled_groups_by_node: BTreeMap<String, Vec<String>>,
    pub last_control_frame_signals_by_node: BTreeMap<String, Vec<String>>,
    pub received_batches_by_node: BTreeMap<String, u64>,
    pub received_events_by_node: BTreeMap<String, u64>,
    pub received_control_events_by_node: BTreeMap<String, u64>,
    pub received_data_events_by_node: BTreeMap<String, u64>,
    pub last_received_at_us_by_node: BTreeMap<String, u64>,
    pub last_received_origins_by_node: BTreeMap<String, Vec<String>>,
    pub received_origin_counts_by_node: BTreeMap<String, Vec<String>>,
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
    pub state: FacadeServiceState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending: Option<StatusFacadePending>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatusRollout {
    pub state: RolloutGenerationState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub serving_generation: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub candidate_generation: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retiring_generation: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RuntimeArtifactEvidence {
    pub available: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AuthorityEpochEvidence {
    pub roots_signature: String,
    pub grants_signature: String,
    pub source_stream_generation: Option<u64>,
    pub sink_materialization_generation: String,
    pub facade_runtime_generation: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReadinessPlanesEvidence {
    pub api_facade_liveness: bool,
    pub management_write_readiness: bool,
    pub trusted_observation_readiness: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatusResponse {
    pub runtime_artifact: RuntimeArtifactEvidence,
    pub authority_epoch: AuthorityEpochEvidence,
    pub readiness_planes: ReadinessPlanesEvidence,
    pub source: StatusSource,
    pub sink: StatusSink,
    pub rollout: StatusRollout,
    pub facade: StatusFacade,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_source_serializes_lifecycle_state_under_debug_only() {
        let value = serde_json::to_value(StatusSource {
            host_object_grants_version: 7,
            grants_count: 2,
            roots_count: 1,
            degraded_roots: Vec::new(),
            logical_roots: Vec::new(),
            concrete_roots: Vec::new(),
            debug: StatusSourceDebug {
                lifecycle_state: "ready".to_string(),
                current_stream_generation: Some(3),
                source_primary_by_group: BTreeMap::new(),
                last_force_find_runner_by_group: BTreeMap::new(),
                last_force_find_runners_by_group: BTreeMap::new(),
                force_find_inflight_groups: Vec::new(),
                scheduled_source_groups_by_node: BTreeMap::new(),
                scheduled_scan_groups_by_node: BTreeMap::new(),
                last_control_frame_signals_by_node: BTreeMap::new(),
                published_batches_by_node: BTreeMap::new(),
                published_events_by_node: BTreeMap::new(),
                published_control_events_by_node: BTreeMap::new(),
                published_data_events_by_node: BTreeMap::new(),
                last_published_at_us_by_node: BTreeMap::new(),
                last_published_origins_by_node: BTreeMap::new(),
                published_origin_counts_by_node: BTreeMap::new(),
                published_path_capture_target: None,
                enqueued_path_origin_counts_by_node: BTreeMap::new(),
                pending_path_origin_counts_by_node: BTreeMap::new(),
                yielded_path_origin_counts_by_node: BTreeMap::new(),
                summarized_path_origin_counts_by_node: BTreeMap::new(),
                published_path_origin_counts_by_node: BTreeMap::new(),
            },
        })
        .expect("serialize status source");

        assert!(
            value.get("lifecycle_state").is_none(),
            "top-level source lifecycle_state should no longer be public status surface"
        );
        assert_eq!(value["debug"]["lifecycle_state"], "ready");
    }

    #[test]
    fn status_sink_group_serializes_materialization_readiness_field() {
        let value = serde_json::to_value(StatusSinkGroup {
            group_id: "nfs1".to_string(),
            service_state: GroupServiceState::ServingTrusted,
            primary_object_ref: "node-a::nfs1".to_string(),
            total_nodes: 1,
            live_nodes: 1,
            tombstoned_count: 0,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 11,
            shadow_lag_us: 12,
            overflow_pending_materialization: false,
            initial_audit_completed: true,
            materialization_readiness: StatusSinkGroupMaterializationReadiness::Ready,
            estimated_heap_bytes: 13,
        })
        .expect("serialize sink group");

        assert_eq!(value["materialization_readiness"], "ready");
        assert!(
            value.get("readiness").is_none(),
            "legacy readiness field should no longer be serialized"
        );
    }
}

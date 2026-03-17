use std::collections::BTreeMap;

use capanix_app_sdk::runtime::ControlEnvelope;
use capanix_app_sdk::{CnxError, Result};

use crate::source::SourceStatusSnapshot;
use crate::source::config::{GrantedMountRoot, RootSpec, SourceConfig};

pub const SOURCE_WORKER_CONTROL_ROUTE_KEY: &str = "fs-meta.source-worker.rpc:v1";
pub const WORKER_CONTROL_ROUTE_KEY_ENV: &str = "FS_META_WORKER_CONTROL_ROUTE_KEY";
pub const SOURCE_WORKER_BOOTSTRAP_CONTROL_FRAME_KIND: &str = "fs-meta.source-worker.bootstrap:v1";

fn scoped_worker_control_route_key(base: &str, node_id: &str) -> String {
    let suffix: String = node_id
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect();
    match base.rsplit_once(':') {
        Some((stem, version)) => format!("{stem}.{suffix}:{version}"),
        None => format!("{base}.{suffix}"),
    }
}

pub fn source_worker_control_route_key_for(node_id: &str) -> String {
    scoped_worker_control_route_key(SOURCE_WORKER_CONTROL_ROUTE_KEY, node_id)
}

pub fn source_worker_control_route_key_from_env() -> String {
    std::env::var(WORKER_CONTROL_ROUTE_KEY_ENV)
        .unwrap_or_else(|_| SOURCE_WORKER_CONTROL_ROUTE_KEY.to_string())
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum SourceWorkerRequest {
    Ping,
    Init {
        node_id: String,
        config: SourceConfig,
    },
    Start,
    UpdateLogicalRoots {
        roots: Vec<RootSpec>,
    },
    LogicalRootsSnapshot,
    HostObjectGrantsSnapshot,
    HostObjectGrantsVersionSnapshot,
    StatusSnapshot,
    LifecycleState,
    ScheduledSourceGroupIds,
    ScheduledScanGroupIds,
    SourcePrimaryByGroupSnapshot,
    LastForceFindRunnerByGroupSnapshot,
    ForceFindInflightGroupsSnapshot,
    ResolveGroupIdForObjectRef {
        object_ref: String,
    },
    TriggerRescanWhenReady,
    OnControlFrame {
        envelopes: Vec<ControlEnvelope>,
    },
    Close,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum SourceWorkerResponse {
    Ack,
    LogicalRoots(Vec<RootSpec>),
    HostObjectGrants(Vec<GrantedMountRoot>),
    HostObjectGrantsVersion(u64),
    StatusSnapshot(SourceStatusSnapshot),
    LifecycleState(String),
    ScheduledGroupIds(Option<Vec<String>>),
    SourcePrimaryByGroup(BTreeMap<String, String>),
    LastForceFindRunnerByGroup(BTreeMap<String, String>),
    ForceFindInflightGroups(Vec<String>),
    ResolveGroupIdForObjectRef(Option<String>),
    InvalidInput(String),
    Error(String),
}

pub fn encode_request(request: &SourceWorkerRequest) -> Result<Vec<u8>> {
    rmp_serde::to_vec_named(request).map_err(|err| {
        CnxError::InvalidInput(format!("source worker request encode failed: {err}"))
    })
}

pub fn decode_request(payload: &[u8]) -> Result<SourceWorkerRequest> {
    rmp_serde::from_slice::<SourceWorkerRequest>(payload).map_err(|err| {
        CnxError::InvalidInput(format!("source worker request decode failed: {err}"))
    })
}

pub fn encode_response(response: &SourceWorkerResponse) -> Result<Vec<u8>> {
    rmp_serde::to_vec_named(response)
        .map_err(|err| CnxError::Internal(format!("source worker response encode failed: {err}")))
}

pub fn decode_response(payload: &[u8]) -> Result<SourceWorkerResponse> {
    rmp_serde::from_slice::<SourceWorkerResponse>(payload).map_err(|err| {
        CnxError::ProtocolViolation(format!("source worker response decode failed: {err}"))
    })
}

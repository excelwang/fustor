use std::collections::BTreeMap;

use capanix_app_sdk::runtime::ControlEnvelope;
use capanix_app_sdk::{CnxError, Event, Result};

use crate::query::request::InternalQueryRequest;
use crate::source::SourceStatusSnapshot;
use crate::source::config::{GrantedMountRoot, RootSpec};
use crate::workers::source::SourceObservabilitySnapshot;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum SourceWorkerRequest {
    UpdateLogicalRoots { roots: Vec<RootSpec> },
    LogicalRootsSnapshot,
    HostObjectGrantsSnapshot,
    HostObjectGrantsVersionSnapshot,
    StatusSnapshot,
    ObservabilitySnapshot,
    LifecycleState,
    ScheduledSourceGroupIds,
    ScheduledScanGroupIds,
    SourcePrimaryByGroupSnapshot,
    LastForceFindRunnerByGroupSnapshot,
    ForceFindInflightGroupsSnapshot,
    ForceFind { request: InternalQueryRequest },
    ResolveGroupIdForObjectRef { object_ref: String },
    PublishManualRescanSignal,
    TriggerRescanWhenReady,
    OnControlFrame { envelopes: Vec<ControlEnvelope> },
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum SourceWorkerResponse {
    Ack,
    LogicalRoots(Vec<RootSpec>),
    HostObjectGrants(Vec<GrantedMountRoot>),
    HostObjectGrantsVersion(u64),
    StatusSnapshot(SourceStatusSnapshot),
    ObservabilitySnapshot(SourceObservabilitySnapshot),
    LifecycleState(String),
    ScheduledGroupIds(Option<Vec<String>>),
    SourcePrimaryByGroup(BTreeMap<String, String>),
    LastForceFindRunnerByGroup(BTreeMap<String, String>),
    ForceFindInflightGroups(Vec<String>),
    Events(Vec<Event>),
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

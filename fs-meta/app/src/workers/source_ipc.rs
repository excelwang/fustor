use std::collections::BTreeMap;

use capanix_app_sdk::runtime::ControlEnvelope;
use capanix_app_sdk::{CnxError, Event, Result};

use crate::query::request::InternalQueryRequest;
use crate::runtime::orchestration::EventRouteScopeProjection;
use crate::source::config::{GrantedMountRoot, RootSpec};
use crate::source::{
    SourceProgressSnapshot, SourceStatusSnapshot, SourceTargetedRescanDeliveryAcceptance,
};
use crate::workers::source::SourceObservabilitySnapshot;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum SourceWorkerRequest {
    UpdateLogicalRoots {
        roots: Vec<RootSpec>,
    },
    UpdateLogicalRootsDeferScanAudit {
        roots: Vec<RootSpec>,
    },
    ApplyPropagatedLogicalRoots {
        roots: Vec<RootSpec>,
        generation: u64,
        defer_scan_audit: bool,
    },
    LogicalRootsSnapshot,
    LogicalRootsGenerationSnapshot,
    HostObjectGrantsSnapshot,
    HostObjectGrantsVersionSnapshot,
    StatusSnapshot,
    ProgressSnapshot,
    ObservabilitySnapshot,
    LifecycleState,
    ScheduledSourceGroupIds,
    ScheduledScanGroupIds,
    SourcePrimaryByGroupSnapshot,
    LastForceFindRunnerByGroupSnapshot,
    ForceFindInflightGroupsSnapshot,
    StartRuntimeEndpoints,
    RearmSourceRescanEndpoints,
    ForceFind {
        request: InternalQueryRequest,
    },
    ResolveGroupIdForObjectRef {
        object_ref: String,
    },
    UpdateEventRouteScopes {
        route_scopes_by_group: BTreeMap<String, EventRouteScopeProjection>,
    },
    PublishManualRescanSignal,
    SubmitRescanRequestEpoch,
    SubmitTargetedRescanRequestEpoch,
    TriggerRescanWhenReadyEpoch,
    OpenScanAuditAdmissionAndTriggerRescanWhenReadyEpoch,
    TriggerTargetedRescanWhenReadyEpoch,
    CheckTargetedRescanDeliveryAcceptance,
    AcceptTargetedRescanDelivery,
    OnControlFrame {
        envelopes: Vec<ControlEnvelope>,
    },
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum SourceWorkerResponse {
    Ack,
    LogicalRoots(Vec<RootSpec>),
    LogicalRootsGeneration(u64),
    HostObjectGrants(Vec<GrantedMountRoot>),
    HostObjectGrantsVersion(u64),
    StatusSnapshot(SourceStatusSnapshot),
    ProgressSnapshot(SourceProgressSnapshot),
    ObservabilitySnapshot(SourceObservabilitySnapshot),
    LifecycleState(String),
    ScheduledGroupIds(Option<Vec<String>>),
    SourcePrimaryByGroup(BTreeMap<String, String>),
    LastForceFindRunnerByGroup(BTreeMap<String, String>),
    ForceFindInflightGroups(Vec<String>),
    Events(Vec<Event>),
    ResolveGroupIdForObjectRef(Option<String>),
    RescanRequestEpoch(u64),
    TargetedRescanDeliveryAcceptance(SourceTargetedRescanDeliveryAcceptance),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn propagated_logical_roots_request_round_trips_generation_and_policy() {
        let request = SourceWorkerRequest::ApplyPropagatedLogicalRoots {
            roots: vec![RootSpec::new("nfs1", "/mnt/nfs1")],
            generation: 7,
            defer_scan_audit: true,
        };

        let decoded = decode_request(&encode_request(&request).expect("encode request"))
            .expect("decode request");
        let SourceWorkerRequest::ApplyPropagatedLogicalRoots {
            roots,
            generation,
            defer_scan_audit,
        } = decoded
        else {
            panic!("decoded request changed variant");
        };
        assert_eq!(generation, 7);
        assert!(defer_scan_audit);
        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0].id, "nfs1");
        assert_eq!(
            roots[0].selected_mount_point(),
            Some(std::path::Path::new("/mnt/nfs1"))
        );
    }

    #[test]
    fn event_route_scope_update_round_trips_generation_and_group_map() {
        let request = SourceWorkerRequest::UpdateEventRouteScopes {
            route_scopes_by_group: BTreeMap::from([
                (
                    "nfs1".to_string(),
                    EventRouteScopeProjection {
                        generation: 9,
                        route_scope: "node-b".to_string(),
                    },
                ),
                (
                    "nfs2".to_string(),
                    EventRouteScopeProjection {
                        generation: 8,
                        route_scope: "node-c".to_string(),
                    },
                ),
            ]),
        };

        let decoded = decode_request(&encode_request(&request).expect("encode request"))
            .expect("decode request");
        let SourceWorkerRequest::UpdateEventRouteScopes {
            route_scopes_by_group,
        } = decoded
        else {
            panic!("decoded request changed variant");
        };
        assert_eq!(
            route_scopes_by_group,
            BTreeMap::from([
                (
                    "nfs1".to_string(),
                    EventRouteScopeProjection {
                        generation: 9,
                        route_scope: "node-b".to_string(),
                    },
                ),
                (
                    "nfs2".to_string(),
                    EventRouteScopeProjection {
                        generation: 8,
                        route_scope: "node-c".to_string(),
                    },
                ),
            ])
        );
    }
}

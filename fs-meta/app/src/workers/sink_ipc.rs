use capanix_app_sdk::runtime::{ControlEnvelope, RecvOpts};
use capanix_app_sdk::{CnxError, Event, Result};

use crate::query::models::HealthStats;
use crate::query::request::InternalQueryRequest;

use crate::sink::{SinkStatusSnapshot, VisibilityLagSample};
use crate::source::config::{GrantedMountRoot, RootSpec};

pub const SINK_WORKER_ROUTE_KEY: &str = "fs-meta.sink-worker:v1";

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SinkWorkerInitConfig {
    pub roots: Vec<RootSpec>,
    pub host_object_grants: Vec<GrantedMountRoot>,
    pub sink_tombstone_ttl_ms: u64,
    pub sink_tombstone_tolerance_us: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum SinkWorkerRequest {
    Init {
        node_id: String,
        config: SinkWorkerInitConfig,
    },
    Start,
    UpdateLogicalRoots {
        roots: Vec<RootSpec>,
        host_object_grants: Vec<GrantedMountRoot>,
    },
    LogicalRootsSnapshot,
    ScheduledGroupIds,
    Health,
    StatusSnapshot,
    VisibilityLagSamplesSince {
        since_us: u64,
    },
    MaterializedQuery {
        request: InternalQueryRequest,
    },
    Send {
        events: Vec<Event>,
    },
    Recv {
        timeout_ms: Option<u64>,
        limit: Option<usize>,
    },
    OnControlFrame {
        envelopes: Vec<ControlEnvelope>,
    },
    Close,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum SinkWorkerResponse {
    Ack,
    LogicalRoots(Vec<RootSpec>),
    ScheduledGroupIds(Option<Vec<String>>),
    Health(HealthStats),
    StatusSnapshot(SinkStatusSnapshot),
    VisibilityLagSamples(Vec<VisibilityLagSample>),
    Events(Vec<Event>),
    Error(String),
}

pub fn recv_opts(timeout_ms: Option<u64>, limit: Option<usize>) -> RecvOpts {
    RecvOpts {
        timeout: timeout_ms.map(std::time::Duration::from_millis),
        limit,
    }
}

pub fn encode_request(request: &SinkWorkerRequest) -> Result<Vec<u8>> {
    rmp_serde::to_vec_named(request)
        .map_err(|err| CnxError::InvalidInput(format!("sink worker request encode failed: {err}")))
}

pub fn decode_request(payload: &[u8]) -> Result<SinkWorkerRequest> {
    rmp_serde::from_slice::<SinkWorkerRequest>(payload)
        .map_err(|err| CnxError::InvalidInput(format!("sink worker request decode failed: {err}")))
}

pub fn encode_response(response: &SinkWorkerResponse) -> Result<Vec<u8>> {
    rmp_serde::to_vec_named(response)
        .map_err(|err| CnxError::Internal(format!("sink worker response encode failed: {err}")))
}

pub fn decode_response(payload: &[u8]) -> Result<SinkWorkerResponse> {
    rmp_serde::from_slice::<SinkWorkerResponse>(payload).map_err(|err| {
        CnxError::ProtocolViolation(format!("sink worker response decode failed: {err}"))
    })
}

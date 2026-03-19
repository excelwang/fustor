use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, RwLock};

use capanix_app_sdk::raw::{ChannelBoundary, ChannelIoSubset};
use capanix_app_sdk::runtime::NodeId;

use super::facade_status::SharedFacadePendingStatusCell;
use crate::query::api::ProjectionPolicy;
use crate::workers::sink::SinkFacade;
use crate::workers::source::SourceFacade;

use super::auth::AuthService;

#[derive(Clone)]
pub struct ApiState {
    pub node_id: NodeId,
    pub runtime_control: Option<Arc<dyn ChannelBoundary>>,
    pub runtime_boundary: Option<Arc<dyn ChannelIoSubset>>,
    pub query_runtime_boundary: Option<Arc<dyn ChannelIoSubset>>,
    pub force_find_inflight: Arc<Mutex<BTreeSet<String>>>,
    pub source: Arc<SourceFacade>,
    pub sink: Arc<SinkFacade>,
    pub query_sink: Arc<SinkFacade>,
    pub auth: Arc<AuthService>,
    pub projection_policy: Arc<RwLock<ProjectionPolicy>>,
    pub facade_pending: SharedFacadePendingStatusCell,
}

use std::sync::{Arc, RwLock};

use capanix_app_sdk::raw::ChannelIoSubset;
use capanix_app_sdk::runtime::NodeId;

use crate::query::api::ProjectionPolicy;
use crate::workers::sink::SinkFacade;
use crate::workers::source::SourceFacade;

use super::auth::AuthService;

#[derive(Clone)]
pub struct ApiState {
    pub node_id: NodeId,
    pub runtime_boundary: Option<Arc<dyn ChannelIoSubset>>,
    pub source: Arc<SourceFacade>,
    pub sink: Arc<SinkFacade>,
    pub auth: Arc<AuthService>,
    pub projection_policy: Arc<RwLock<ProjectionPolicy>>,
}

use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use capanix_app_sdk::runtime::NodeId;
use capanix_host_adapter_fs::{
    ExchangeHostAdapter, HostFsFacade, PostBindDispatchTable, exchange_host_adapter_from_channel_boundary,
    local_host_fs_facade,
};
use capanix_runtime_host_sdk::boundary::ChannelIoSubset;

/// Narrow infra seam for adapting app-sdk ordinary boundaries into the
/// kernel-facing carrier shapes still required by host-adapter internals.
pub(crate) fn exchange_host_adapter(
    boundary: Arc<dyn ChannelIoSubset>,
    node_id: NodeId,
    routes: Arc<PostBindDispatchTable>,
) -> ExchangeHostAdapter {
    exchange_host_adapter_from_channel_boundary(boundary, node_id, routes)
}

/// Keep runtime-api boundary conversion out of business modules.
pub(crate) fn resolve_host_fs_facade(
    root_path: PathBuf,
    _boundary: Option<Arc<dyn ChannelIoSubset>>,
    _caller_node: &NodeId,
    _target_host_ref: &str,
    object_ref: &str,
) -> io::Result<HostFsFacade> {
    Ok(local_host_fs_facade(root_path, object_ref))
}

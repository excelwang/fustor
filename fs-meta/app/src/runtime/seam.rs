use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use capanix_app_sdk::raw::{ChannelIoSubset, channel_boundary_into_kernel};
use capanix_app_sdk::runtime::NodeId;
use capanix_host_adapter_fs_meta::{ExchangeHostAdapter, HostFsFacade, RouteLookupTable};

/// Narrow infra seam for adapting app-sdk ordinary boundaries into the
/// kernel-facing carrier shapes still required by host-adapter internals.
pub(crate) fn exchange_host_adapter(
    boundary: Arc<dyn ChannelIoSubset>,
    node_id: NodeId,
    routes: Arc<RouteLookupTable>,
) -> ExchangeHostAdapter {
    ExchangeHostAdapter::new(channel_boundary_into_kernel(boundary), node_id, routes)
}

/// Keep runtime-api boundary conversion out of business modules.
pub(crate) fn resolve_host_fs_facade(
    root_path: PathBuf,
    boundary: Option<Arc<dyn ChannelIoSubset>>,
    caller_node: &NodeId,
    target_host_ref: &str,
    object_ref: &str,
) -> io::Result<HostFsFacade> {
    HostFsFacade::resolve(
        root_path,
        boundary.map(channel_boundary_into_kernel),
        caller_node,
        target_host_ref,
        object_ref,
    )
}

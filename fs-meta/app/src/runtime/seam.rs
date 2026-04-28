use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use capanix_app_sdk::runtime::NodeId;
use capanix_host_adapter_fs::{
    ExchangeHostAdapter, HostFsFacade, PostBindDispatchTable,
    exchange_host_adapter_from_channel_boundary, local_host_fs_facade,
    local_mount_root_host_fs_facade,
};
use capanix_runtime_entry_sdk::advanced::boundary::ChannelIoSubset;

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
    fs_type: &str,
    fs_source: &str,
) -> io::Result<HostFsFacade> {
    let requires_mount_root_guard = host_grant_requires_mount_root_guard(fs_type, fs_source);
    if requires_mount_root_guard {
        Ok(local_mount_root_host_fs_facade(root_path, object_ref))
    } else {
        Ok(local_host_fs_facade(root_path, object_ref))
    }
}

fn host_grant_requires_mount_root_guard(_fs_type: &str, fs_source: &str) -> bool {
    fs_source.contains(":/")
}

#[cfg(test)]
mod tests {
    use super::host_grant_requires_mount_root_guard;

    #[test]
    fn remote_host_grant_sources_use_mount_root_guard() {
        assert!(host_grant_requires_mount_root_guard(
            "nfs",
            "127.0.0.1:/exports/nfs1"
        ));
        assert!(host_grant_requires_mount_root_guard(
            "NFS",
            "server.example:/exports/nfs1"
        ));
        assert!(!host_grant_requires_mount_root_guard(
            "nfs",
            "/tmp/fs-meta-unit-root"
        ));
        assert!(host_grant_requires_mount_root_guard(
            "mount-root",
            "127.0.0.1:/exports/nfs1"
        ));
    }

    #[test]
    fn remote_mount_root_sources_use_mount_root_guard_regardless_of_kind_label() {
        assert!(host_grant_requires_mount_root_guard(
            "mount-root",
            "127.0.0.1:/exports/nfs1"
        ));
        assert!(host_grant_requires_mount_root_guard(
            "fs",
            "nfs.example:/exports/nfs1"
        ));
    }
}

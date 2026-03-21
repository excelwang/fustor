use std::io::Result;
use std::path::Path;
use std::sync::Arc;

pub fn run_scan_worker_server(control_socket_path: &Path, data_socket_path: &Path) -> Result<()> {
    capanix_app_fs_meta_worker_source::run_source_worker_server(
        control_socket_path,
        data_socket_path,
    )
}

pub fn run_scan_worker_runtime_loop(
    boundary: Arc<dyn capanix_app_sdk::RuntimeBoundary>,
    io_boundary: Arc<dyn capanix_app_sdk::raw::ChannelIoSubset>,
    runtime: &tokio::runtime::Runtime,
) -> Result<()> {
    capanix_app_fs_meta_worker_source::run_source_worker_runtime_loop(
        boundary,
        io_boundary,
        runtime,
    )
}

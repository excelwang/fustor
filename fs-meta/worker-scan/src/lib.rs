use std::io::Result;
use std::path::Path;
use std::sync::Arc;

use capanix_app_sdk::RuntimeBoundary;
use capanix_app_sdk::raw::ChannelIoSubset;

pub fn run_scan_worker_server(control_socket_path: &Path, data_socket_path: &Path) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let shared_boundary = Arc::new(
        runtime
            .block_on(
                capanix_unit_sidecar::UnitRuntimeIpcBoundary::bind_and_accept(
                    control_socket_path,
                    data_socket_path,
                ),
            )
            .map_err(|err| {
                std::io::Error::other(format!("scan worker sidecar bind failed: {err}"))
            })?,
    );
    let boundary: Arc<dyn RuntimeBoundary> = shared_boundary.clone();
    let io_boundary: Arc<dyn ChannelIoSubset> = shared_boundary;
    run_scan_worker_runtime_loop(boundary, io_boundary, &runtime)
}

pub fn run_scan_worker_runtime_loop(
    boundary: Arc<dyn RuntimeBoundary>,
    io_boundary: Arc<dyn ChannelIoSubset>,
    runtime: &tokio::runtime::Runtime,
) -> Result<()> {
    capanix_app_fs_meta_worker_source::run_source_worker_runtime_loop(
        boundary,
        io_boundary,
        runtime,
    )
}

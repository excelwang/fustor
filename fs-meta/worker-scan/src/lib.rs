use std::io::Result;
use std::path::Path;

pub fn run_scan_worker_server(worker_socket_path: &Path) -> Result<()> {
    capanix_app_fs_meta_worker_source::run_source_worker_server(worker_socket_path)
}

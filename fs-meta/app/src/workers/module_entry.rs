use std::ffi::c_char;
use std::io;

use capanix_runtime_host_sdk::worker_runtime::run_worker_module_entry;

use crate::workers::sink_server::run_sink_worker_server;
use crate::workers::source_server::run_source_worker_server;

#[unsafe(no_mangle)]
pub extern "C" fn capanix_run_worker_module(payload: *const c_char) -> i32 {
    run_worker_module_entry(payload, |launch| match launch.role_id.as_str() {
        "source" => run_source_worker_server(&launch.control_socket_path, &launch.data_socket_path),
        "sink" => run_sink_worker_server(&launch.control_socket_path, &launch.data_socket_path),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("fs-meta worker module does not support role '{other}'"),
        )),
    })
}

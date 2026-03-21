use std::ffi::CStr;
use std::os::raw::c_char;

pub use capanix_app_fs_meta::FSMetaRuntimeApp;
use capanix_unit_entry_macros::capanix_unit_entry;
use capanix_worker_runtime_support::{
    WORKER_CONTROL_ROUTE_KEY_ENV, decode_worker_module_launch_config,
};

// Embedded facade-worker entry wiring stays artifact-local; product mode remains
// only `embedded | external`.
capanix_unit_entry!(FSMetaRuntimeApp);

#[unsafe(no_mangle)]
pub extern "C" fn capanix_run_worker_module(payload: *const c_char) -> i32 {
    if payload.is_null() {
        eprintln!("fs-meta worker module launch payload is null");
        return 1;
    }

    let payload = unsafe { CStr::from_ptr(payload) };
    let payload = match payload.to_str() {
        Ok(payload) => payload,
        Err(err) => {
            eprintln!("fs-meta worker module launch payload is not valid UTF-8: {err}");
            return 1;
        }
    };
    let launch = match decode_worker_module_launch_config(payload) {
        Ok(launch) => launch,
        Err(err) => {
            eprintln!("fs-meta worker module launch decode failed: {err}");
            return 1;
        }
    };

    unsafe {
        std::env::set_var(WORKER_CONTROL_ROUTE_KEY_ENV, &launch.route_key);
    }

    let result = match launch.role_id.as_str() {
        "source" => capanix_app_fs_meta_worker_source::run_source_worker_server(
            &launch.control_socket_path,
            &launch.data_socket_path,
        ),
        "scan" => capanix_app_fs_meta_worker_scan::run_scan_worker_server(
            &launch.control_socket_path,
            &launch.data_socket_path,
        ),
        "sink" => capanix_app_fs_meta_worker_sink::run_sink_worker_server(
            &launch.control_socket_path,
            &launch.data_socket_path,
        ),
        other => {
            eprintln!("fs-meta worker module does not support role '{other}'");
            return 1;
        }
    };

    match result {
        Ok(()) => 0,
        Err(err) => {
            eprintln!("fs-meta worker module failed: {err}");
            1
        }
    }
}

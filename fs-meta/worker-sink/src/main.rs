use std::path::PathBuf;

const WORKER_CONTROL_ROUTE_KEY_ENV: &str = "FS_META_WORKER_CONTROL_ROUTE_KEY";

fn parse_worker_args(args: &[String]) -> Result<(PathBuf, PathBuf, String), String> {
    let mut control = None::<PathBuf>;
    let mut data = None::<PathBuf>;
    let mut route_key = None::<String>;
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--worker-route-key" => {
                let Some(value) = iter.next() else {
                    return Err("--worker-route-key requires a value".to_string());
                };
                if value.trim().is_empty() {
                    return Err("--worker-route-key must not be empty".to_string());
                }
                route_key = Some(value.to_string());
            }
            "--worker-control-socket" => {
                let Some(path) = iter.next() else {
                    return Err("--worker-control-socket requires a path".to_string());
                };
                let socket = PathBuf::from(path);
                if !socket.is_absolute() {
                    return Err("--worker-control-socket path must be absolute".to_string());
                }
                control = Some(socket);
            }
            "--worker-data-socket" => {
                let Some(path) = iter.next() else {
                    return Err("--worker-data-socket requires a path".to_string());
                };
                let socket = PathBuf::from(path);
                if !socket.is_absolute() {
                    return Err("--worker-data-socket path must be absolute".to_string());
                }
                data = Some(socket);
            }
            _ => {}
        }
    }
    match (control, data, route_key) {
        (Some(control), Some(data), Some(route_key)) => Ok((control, data, route_key)),
        _ => Err("missing --worker-route-key <route-key> and --worker-control-socket <absolute-path> and --worker-data-socket <absolute-path>".to_string()),
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let (control_socket_path, data_socket_path, route_key) = match parse_worker_args(&args[1..]) {
        Ok(paths) => paths,
        Err(err) => {
            eprintln!("fs_meta_sink_worker arg error: {err}");
            std::process::exit(2);
        }
    };
    unsafe {
        std::env::set_var(WORKER_CONTROL_ROUTE_KEY_ENV, &route_key);
    }
    if let Err(err) = capanix_app_fs_meta_worker_sink::run_sink_worker_server(
        &control_socket_path,
        &data_socket_path,
    ) {
        eprintln!("fs_meta_sink_worker failed: {err}");
        std::process::exit(1);
    }
}

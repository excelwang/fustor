use std::path::PathBuf;

fn parse_socket_path(args: &[String]) -> Result<PathBuf, String> {
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        if arg == "--worker-socket" {
            let Some(path) = iter.next() else {
                return Err("--worker-socket requires a path".to_string());
            };
            let socket = PathBuf::from(path);
            if !socket.is_absolute() {
                return Err("--worker-socket path must be absolute".to_string());
            }
            return Ok(socket);
        }
    }
    Err("missing --worker-socket <absolute-path>".to_string())
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let socket_path = match parse_socket_path(&args[1..]) {
        Ok(path) => path,
        Err(err) => {
            eprintln!("fs_meta_scan_worker arg error: {err}");
            std::process::exit(2);
        }
    };
    if let Err(err) = capanix_app_fs_meta_worker_scan::run_scan_worker_server(&socket_path) {
        eprintln!("fs_meta_scan_worker failed: {err}");
        std::process::exit(1);
    }
}

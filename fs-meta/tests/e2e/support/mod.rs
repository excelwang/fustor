#![cfg(target_os = "linux")]

pub mod api_client;
pub mod cluster5;
#[path = "../../common/control_protocol.rs"]
pub mod control_protocol;
pub mod cpu_budget;
pub mod nfs_lab;
pub mod oracle;
#[path = "../../common/runtime_admin.rs"]
pub mod runtime_admin;

use std::net::TcpListener;
use std::thread;
use std::time::{Duration, Instant};

pub fn skip_unless_real_nfs_enabled() -> Option<String> {
    let preflight = nfs_lab::RealNfsPreflight::detect();
    if preflight.enabled {
        None
    } else {
        Some(
            preflight
                .reason
                .unwrap_or_else(|| "real-nfs e2e disabled".to_string()),
        )
    }
}

pub fn reserve_http_addrs(count: usize) -> Result<Vec<String>, String> {
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let socket = TcpListener::bind("127.0.0.1:0")
            .map_err(|e| format!("reserve HTTP addr failed: {e}"))?;
        let addr = socket
            .local_addr()
            .map_err(|e| format!("query reserved HTTP addr failed: {e}"))?;
        out.push(format!("127.0.0.1:{}", addr.port()));
    }
    Ok(out)
}

pub fn wait_until<F>(timeout: Duration, label: &str, mut check: F) -> Result<(), String>
where
    F: FnMut() -> Result<bool, String>,
{
    let deadline = Instant::now() + timeout;
    let mut last_err = String::new();
    loop {
        match check() {
            Ok(true) => return Ok(()),
            Ok(false) => {}
            Err(err) => last_err = err,
        }
        if Instant::now() > deadline {
            return Err(format!("timeout waiting for {label}: {last_err}"));
        }
        thread::sleep(Duration::from_millis(250));
    }
}

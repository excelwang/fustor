use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use capanix_app_sdk::raw::ChannelIoSubset;
use capanix_app_sdk::runtime::{NodeId, RouteKey};
use capanix_app_sdk::{BoundRouteClient, CnxError, Event, Result, RuntimeBoundary};

use crate::bridge::RuntimeSupportBridge;
use crate::error::contextualize_transport_error;

pub(crate) struct RuntimeSupportTransport {
    route: BoundRouteClient,
    child: Option<Child>,
    worker_socket_path: PathBuf,
    bridge: Option<RuntimeSupportBridge>,
}

struct RuntimeSupportPaths {
    worker_socket_path: PathBuf,
    stdout_log_path: PathBuf,
    stderr_log_path: PathBuf,
}

impl RuntimeSupportPaths {
    fn new(socket_dir: &Path, label: &str) -> Self {
        let pid = std::process::id();
        let suffix = now_us();
        Self {
            worker_socket_path: socket_dir
                .join(format!("capanix-{label}-worker-{pid}-{suffix}.sock")),
            stdout_log_path: socket_dir.join(format!("capanix-{label}-{pid}-{suffix}.stdout.log")),
            stderr_log_path: socket_dir.join(format!("capanix-{label}-{pid}-{suffix}.stderr.log")),
        }
    }
}

impl RuntimeSupportTransport {
    pub(crate) fn spawn<T>(
        node_id: &NodeId,
        route_key: RouteKey,
        bin_path: &Path,
        socket_dir: &Path,
        label: &str,
        boundary: Arc<T>,
    ) -> Result<Self>
    where
        T: RuntimeBoundary + ChannelIoSubset + 'static,
    {
        fs::create_dir_all(socket_dir).map_err(|err| {
            CnxError::Internal(format!(
                "create worker socket dir failed ({}): {err}",
                socket_dir.display()
            ))
        })?;

        let paths = RuntimeSupportPaths::new(socket_dir, label);
        let _ = fs::remove_file(&paths.worker_socket_path);

        let stdout_log = open_log_file(&paths.stdout_log_path, "stdout")?;
        let stderr_log = open_log_file(&paths.stderr_log_path, "stderr")?;
        let child =
            spawn_worker_process(bin_path, &paths.worker_socket_path, stdout_log, stderr_log)?;
        let bridge =
            RuntimeSupportBridge::spawn(paths.worker_socket_path.clone(), boundary.clone());
        let data_boundary: Arc<dyn ChannelIoSubset> = boundary;
        let route = BoundRouteClient::open(data_boundary, route_key, node_id.clone())?;

        Ok(Self {
            route,
            child: Some(child),
            worker_socket_path: paths.worker_socket_path,
            bridge: Some(bridge),
        })
    }

    pub(crate) fn ask(
        &self,
        payload: Bytes,
        timeout: Duration,
        unavailable_label: &str,
    ) -> Result<Vec<Event>> {
        self.route
            .ask(payload, timeout)
            .map_err(|err| contextualize_transport_error(unavailable_label, err))
    }

    pub(crate) fn close(&mut self, close_payload: Bytes, timeout: Duration) {
        let _ = self.route.ask(close_payload, timeout);
        self.route.close();
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        if let Some(mut bridge) = self.bridge.take() {
            bridge.join();
        }
        let _ = fs::remove_file(&self.worker_socket_path);
    }
}

fn open_log_file(path: &Path, label: &str) -> Result<std::fs::File> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|err| {
            CnxError::Internal(format!(
                "open worker {label} log failed ({}): {err}",
                path.display()
            ))
        })
}

fn spawn_worker_process(
    bin_path: &Path,
    worker_socket_path: &Path,
    stdout_log: std::fs::File,
    stderr_log: std::fs::File,
) -> Result<Child> {
    Command::new(bin_path)
        .arg("--worker-socket")
        .arg(worker_socket_path)
        .stdout(Stdio::from(stdout_log))
        .stderr(Stdio::from(stderr_log))
        .spawn()
        .map_err(|err| {
            CnxError::Internal(format!(
                "spawn worker failed ({}): {err}",
                bin_path.display()
            ))
        })
}

fn now_us() -> u64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => duration.as_micros() as u64,
        Err(_) => 0,
    }
}

use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc::{Receiver, TryRecvError};
use std::time::{Duration, Instant};

use bytes::Bytes;
use capanix_app_sdk::raw::ChannelIoSubset;
use capanix_app_sdk::runtime::{ControlEnvelope, NodeId, RouteKey};
use capanix_app_sdk::{BoundRouteClient, CnxError, Event, Result, RuntimeBoundary};

use crate::bridge::RuntimeSupportBridge;
use crate::error::contextualize_transport_error;

pub(crate) struct RuntimeSupportTransport {
    route: BoundRouteClient,
    bridge: RuntimeSupportBridge,
    shutdown: Mutex<Option<RuntimeSupportShutdown>>,
}

struct RuntimeSupportShutdown {
    child: Child,
    control_socket_path: PathBuf,
    data_socket_path: PathBuf,
}

struct RuntimeSupportPaths {
    control_socket_path: PathBuf,
    data_socket_path: PathBuf,
    stdout_log_path: PathBuf,
    stderr_log_path: PathBuf,
}

impl RuntimeSupportPaths {
    fn new(socket_dir: &Path, label: &str) -> Self {
        let pid = std::process::id();
        let suffix = now_us();
        Self {
            control_socket_path: socket_dir.join(format!(
                "capanix-{label}-worker-{pid}-{suffix}.control.sock"
            )),
            data_socket_path: socket_dir
                .join(format!("capanix-{label}-worker-{pid}-{suffix}.data.sock")),
            stdout_log_path: socket_dir.join(format!("capanix-{label}-{pid}-{suffix}.stdout.log")),
            stderr_log_path: socket_dir.join(format!("capanix-{label}-{pid}-{suffix}.stderr.log")),
        }
    }
}

const WORKER_BRIDGE_READY_TIMEOUT: Duration = Duration::from_secs(8);
const WORKER_BRIDGE_READY_POLL: Duration = Duration::from_millis(50);

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
        let _ = fs::remove_file(&paths.control_socket_path);
        let _ = fs::remove_file(&paths.data_socket_path);

        let stdout_log = open_log_file(&paths.stdout_log_path, "stdout")?;
        let stderr_log = open_log_file(&paths.stderr_log_path, "stderr")?;
        let mut child = spawn_worker_process(
            bin_path,
            &route_key.0,
            &paths.control_socket_path,
            &paths.data_socket_path,
            stdout_log,
            stderr_log,
        )?;
        let (bridge, ready_rx) = RuntimeSupportBridge::spawn(
            paths.control_socket_path.clone(),
            paths.data_socket_path.clone(),
            boundary.clone(),
        )?;
        if let Err(err) = wait_for_bridge_ready(
            &mut child,
            &paths.control_socket_path,
            &paths.data_socket_path,
            ready_rx,
        ) {
            let _ = child.kill();
            let _ = child.wait();
            bridge.join();
            let _ = fs::remove_file(&paths.control_socket_path);
            let _ = fs::remove_file(&paths.data_socket_path);
            return Err(err);
        }
        let data_boundary: Arc<dyn ChannelIoSubset> = boundary;
        let route = BoundRouteClient::open(data_boundary, route_key, node_id.clone())?;

        Ok(Self {
            route,
            bridge,
            shutdown: Mutex::new(Some(RuntimeSupportShutdown {
                child,
                control_socket_path: paths.control_socket_path,
                data_socket_path: paths.data_socket_path,
            })),
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

    pub(crate) fn on_control_frame(
        &self,
        envelopes: &[ControlEnvelope],
        timeout: Duration,
    ) -> Result<()> {
        self.bridge.on_control_frame(envelopes, timeout)
    }

    pub(crate) fn close(&self, close_payload: Bytes, timeout: Duration) {
        eprintln!("runtime-support: transport close requested");
        let _ = self.route.ask(close_payload, timeout);
        self.shutdown();
    }

    pub(crate) fn shutdown(&self) {
        eprintln!("runtime-support: transport shutdown requested");
        self.route.close();
        let shutdown = self.shutdown.lock().ok().and_then(|mut guard| guard.take());
        if let Some(mut shutdown) = shutdown {
            let _ = shutdown.child.kill();
            let _ = shutdown.child.wait();
            self.bridge.join();
            let _ = fs::remove_file(&shutdown.control_socket_path);
            let _ = fs::remove_file(&shutdown.data_socket_path);
        } else {
            self.bridge.join();
        }
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
    route_key: &str,
    control_socket_path: &Path,
    data_socket_path: &Path,
    stdout_log: std::fs::File,
    stderr_log: std::fs::File,
) -> Result<Child> {
    Command::new(bin_path)
        .arg("--worker-route-key")
        .arg(route_key)
        .arg("--worker-control-socket")
        .arg(control_socket_path)
        .arg("--worker-data-socket")
        .arg(data_socket_path)
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

fn wait_for_bridge_ready(
    child: &mut Child,
    control_socket_path: &Path,
    data_socket_path: &Path,
    ready_rx: Receiver<std::result::Result<(), String>>,
) -> Result<()> {
    let deadline = Instant::now() + WORKER_BRIDGE_READY_TIMEOUT;
    loop {
        match ready_rx.try_recv() {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(message)) => {
                return Err(CnxError::TransportClosed(format!(
                    "worker bridge connect failed (control={}, data={}): {message}",
                    control_socket_path.display(),
                    data_socket_path.display()
                )));
            }
            Err(TryRecvError::Disconnected) => {
                return Err(CnxError::TransportClosed(format!(
                    "worker bridge stopped before ready (control={}, data={})",
                    control_socket_path.display(),
                    data_socket_path.display()
                )));
            }
            Err(TryRecvError::Empty) => {}
        }

        match child.try_wait() {
            Ok(Some(status)) => {
                return Err(CnxError::TransportClosed(format!(
                    "worker process exited before ready (control={}, data={}) with status {status}",
                    control_socket_path.display(),
                    data_socket_path.display()
                )));
            }
            Ok(None) => {}
            Err(err) => {
                return Err(CnxError::Internal(format!(
                    "check worker process status failed (control={}, data={}): {err}",
                    control_socket_path.display(),
                    data_socket_path.display()
                )));
            }
        }

        if Instant::now() >= deadline {
            return Err(CnxError::Timeout);
        }

        std::thread::sleep(WORKER_BRIDGE_READY_POLL);
    }
}

fn now_us() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    now.as_micros().min(u64::MAX as u128) as u64
}

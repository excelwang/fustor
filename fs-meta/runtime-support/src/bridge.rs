use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc::Receiver;
use std::time::Duration;

use capanix_app_sdk::Result;
use capanix_app_sdk::RuntimeBoundary;
use capanix_app_sdk::raw::ChannelIoSubset;
use capanix_app_sdk::runtime::ControlEnvelope;
use capanix_unit_sidecar::UnitRuntimeBridgeHost;

pub(crate) struct RuntimeSupportBridge {
    host: Arc<Mutex<UnitRuntimeBridgeHost>>,
}

impl RuntimeSupportBridge {
    pub(crate) fn spawn<T>(
        control_socket_path: PathBuf,
        data_socket_path: PathBuf,
        boundary: Arc<T>,
    ) -> Result<(Self, Receiver<std::result::Result<(), String>>)>
    where
        T: RuntimeBoundary + ChannelIoSubset + 'static,
    {
        let (host, ready_rx) = capanix_unit_sidecar::spawn_unit_runtime_bridge_host(
            &control_socket_path,
            &data_socket_path,
            boundary,
        )?;
        Ok((
            Self {
                host: Arc::new(Mutex::new(host)),
            },
            ready_rx,
        ))
    }

    pub(crate) fn on_control_frame(
        &self,
        envelopes: &[ControlEnvelope],
        timeout: Duration,
    ) -> Result<()> {
        self.host
            .lock()
            .map_err(|_| capanix_app_sdk::CnxError::Internal("bridge host lock poisoned".into()))?
            .on_control_frame(envelopes, timeout)
    }

    pub(crate) fn join(&self) {
        if let Ok(mut host) = self.host.lock() {
            host.join();
        }
    }

    pub(crate) fn join_async(&self) {
        let host = self.host.clone();
        std::thread::spawn(move || {
            if let Ok(mut host) = host.lock() {
                host.join();
            }
        });
    }
}

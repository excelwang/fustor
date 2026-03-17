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
    host: Mutex<Option<UnitRuntimeBridgeHost>>,
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
        Ok((Self { host: Mutex::new(Some(host)) }, ready_rx))
    }

    pub(crate) fn on_control_frame(
        &self,
        envelopes: &[ControlEnvelope],
        timeout: Duration,
    ) -> Result<()> {
        let guard = self
            .host
            .lock()
            .map_err(|_| capanix_app_sdk::CnxError::Internal("runtime-support bridge lock poisoned".into()))?;
        let host = guard.as_ref().ok_or_else(|| {
            capanix_app_sdk::CnxError::TransportClosed(
                "worker runtime-support bridge is not available".into(),
            )
        })?;
        host.on_control_frame(envelopes, timeout)
    }

    pub(crate) fn join(&self) {
        let host = self.host.lock().ok().and_then(|mut guard| guard.take());
        if let Some(mut host) = host {
            host.join();
        }
    }
}

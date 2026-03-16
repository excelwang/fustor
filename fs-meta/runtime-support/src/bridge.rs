use std::path::PathBuf;
use std::sync::Arc;
use std::sync::mpsc::{Receiver, sync_channel};

use capanix_app_sdk::RuntimeBoundary;
use capanix_app_sdk::raw::ChannelIoSubset;
use capanix_app_sdk::Result;

pub(crate) struct RuntimeSupportBridge {
    join: Option<std::thread::JoinHandle<()>>,
}

impl RuntimeSupportBridge {
    pub(crate) fn spawn<T>(
        socket_path: PathBuf,
        boundary: Arc<T>,
    ) -> Result<(Self, Receiver<std::result::Result<(), String>>)>
    where
        T: RuntimeBoundary + ChannelIoSubset + 'static,
    {
        let (ready_tx, ready_rx) = sync_channel(1);
        let join = std::thread::spawn(move || {
            if let Err(err) =
                capanix_unit_sidecar::run_unit_runtime_bridge_loop_with_ready_signal(
                    &socket_path,
                    boundary,
                    ready_tx,
                )
            {
                eprintln!("[runtime-support] runtime bridge stopped: {err}");
            }
        });
        Ok((Self { join: Some(join) }, ready_rx))
    }

    pub(crate) fn join(&mut self) {
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

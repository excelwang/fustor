use std::path::PathBuf;
use std::sync::Arc;

use capanix_app_sdk::RuntimeBoundary;
use capanix_app_sdk::raw::ChannelIoSubset;

pub(crate) struct RuntimeSupportBridge {
    join: Option<std::thread::JoinHandle<()>>,
}

impl RuntimeSupportBridge {
    pub(crate) fn spawn<T>(socket_path: PathBuf, boundary: Arc<T>) -> Self
    where
        T: RuntimeBoundary + ChannelIoSubset + 'static,
    {
        let join = std::thread::spawn(move || {
            if let Err(err) =
                capanix_unit_sidecar::run_unit_runtime_bridge_loop(&socket_path, boundary)
            {
                eprintln!("[runtime-support] runtime bridge stopped: {err}");
            }
        });
        Self { join: Some(join) }
    }

    pub(crate) fn join(&mut self) {
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

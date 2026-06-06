use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use async_trait::async_trait;
use capanix_app_sdk::raw::ChannelIoSubset;
use capanix_app_sdk::runtime::{ConfigValue, ControlEnvelope, NodeId, RecvOpts, RouteKey};
use capanix_app_sdk::{
    BoundRouteServer, CnxError, Event, Result, RuntimeBoundary, RuntimeBoundaryApp,
};
use s3_source::S3SourceRuntimeConfig;
use s3_source::product_model::routes::fetch_request_route_for;
use tokio_util::sync::CancellationToken;

use crate::driver::NativeS3SourceDriver;
use crate::service::S3SourceService;

pub struct S3SourceRuntimeApp {
    runtime_boundary: Arc<dyn RuntimeBoundary>,
    data_boundary: Arc<dyn ChannelIoSubset>,
    origin_id: NodeId,
    fetch_task: Mutex<Option<RuntimeServerTask>>,
}

struct RuntimeServerTask {
    shutdown: CancellationToken,
    join: JoinHandle<Result<()>>,
}

impl S3SourceRuntimeApp {
    pub fn new(
        runtime_boundary: Arc<dyn RuntimeBoundary>,
        data_boundary: Arc<dyn ChannelIoSubset>,
    ) -> Self {
        Self {
            runtime_boundary,
            data_boundary,
            origin_id: NodeId("s3-source".into()),
            fetch_task: Mutex::new(None),
        }
    }

    fn manifest_config(&self) -> Result<S3SourceRuntimeConfig> {
        let cfg = self
            .runtime_boundary
            .app_manifest_config()
            .ok_or_else(|| CnxError::InvalidInput("missing app manifest config".into()))?;
        S3SourceRuntimeConfig::from_manifest_config(&cfg)
    }

    fn local_node_id(&self) -> String {
        let Some(cfg) = self.runtime_boundary.app_manifest_config() else {
            return self.origin_id.0.clone();
        };
        let Some(ConfigValue::Map(runtime)) = cfg.get("__cnx_runtime") else {
            return self.origin_id.0.clone();
        };
        match runtime.get("local_host_ref") {
            Some(ConfigValue::String(value)) if !value.trim().is_empty() => {
                value.trim().to_string()
            }
            _ => self.origin_id.0.clone(),
        }
    }

    fn fetch_route(&self) -> RouteKey {
        fetch_request_route_for(&self.local_node_id())
    }

    fn stop_fetch_task(slot: &mut Option<RuntimeServerTask>) -> Result<()> {
        let Some(task) = slot.take() else {
            return Ok(());
        };
        task.shutdown.cancel();
        match task.join.join() {
            Ok(result) => result,
            Err(_) => Err(CnxError::Internal("s3-source fetch task panicked".into())),
        }
    }
}

#[async_trait]
impl RuntimeBoundaryApp for S3SourceRuntimeApp {
    async fn start(&self) -> Result<()> {
        let config = self.manifest_config()?;
        let service = Arc::new(S3SourceService::new(
            config,
            Arc::new(NativeS3SourceDriver),
            self.origin_id.clone(),
        ));
        let route = self.fetch_route();
        let server = BoundRouteServer::open(self.data_boundary.clone(), route)?;
        let shutdown = CancellationToken::new();
        let task_shutdown = shutdown.clone();
        let task_service = service.clone();
        let join = thread::Builder::new()
            .name("s3-source-fetch".into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|err| {
                        CnxError::Internal(format!("s3-source fetch runtime init failed: {err}"))
                    })?;
                runtime.block_on(async move {
                    server
                        .bind_concurrent_until(&task_shutdown, 8, move |events| {
                            task_service.handle_events(events)
                        })
                        .await
                })
            })
            .map_err(|err| {
                CnxError::Internal(format!("s3-source fetch thread spawn failed: {err}"))
            })?;
        let mut guard = self
            .fetch_task
            .lock()
            .map_err(|_| CnxError::Internal("s3-source fetch task lock poisoned".into()))?;
        Self::stop_fetch_task(&mut guard)?;
        guard.replace(RuntimeServerTask { shutdown, join });
        Ok(())
    }

    async fn send(&self, _events: &[Event], _timeout: Duration) -> Result<()> {
        Err(CnxError::NotSupported(
            "s3-source data plane is served through bound internal route".into(),
        ))
    }

    async fn recv(&self, _opts: RecvOpts) -> Result<Vec<Event>> {
        Err(CnxError::NotSupported(
            "s3-source does not expose stream recv in v1".into(),
        ))
    }

    async fn close(&self) -> Result<()> {
        let mut guard = self
            .fetch_task
            .lock()
            .map_err(|_| CnxError::Internal("s3-source fetch task lock poisoned".into()))?;
        Self::stop_fetch_task(&mut guard)
    }

    async fn on_control_frame(&self, _envelopes: &[ControlEnvelope]) -> Result<()> {
        Ok(())
    }
}

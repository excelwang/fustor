use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use async_trait::async_trait;
use capanix_app_sdk::raw::ChannelIoSubset;
use capanix_app_sdk::runtime::{ConfigValue, ControlEnvelope, NodeId, RecvOpts, RouteKey};
use capanix_app_sdk::{
    BoundRouteServer, CnxError, Event, Result, RuntimeBoundary, RuntimeBoundaryApp,
};
use tokio_util::sync::CancellationToken;
use union_graph::product_model::routes::{ingest_request_route_for, query_request_route_for};
use union_graph::{UnionGraphStore, skeleton_from_manifest_config};

use crate::service::UnionGraphService;

pub struct UnionGraphRuntimeApp {
    runtime_boundary: Arc<dyn RuntimeBoundary>,
    data_boundary: Arc<dyn ChannelIoSubset>,
    origin_id: NodeId,
    query_task: Mutex<Option<RuntimeServerTask>>,
    ingest_task: Mutex<Option<RuntimeServerTask>>,
}

struct RuntimeServerTask {
    shutdown: CancellationToken,
    join: JoinHandle<Result<()>>,
}

impl UnionGraphRuntimeApp {
    pub fn new(
        runtime_boundary: Arc<dyn RuntimeBoundary>,
        data_boundary: Arc<dyn ChannelIoSubset>,
    ) -> Self {
        Self {
            runtime_boundary,
            data_boundary,
            origin_id: NodeId("union-graph".into()),
            query_task: Mutex::new(None),
            ingest_task: Mutex::new(None),
        }
    }

    fn initial_store(&self) -> Result<UnionGraphStore> {
        let cfg = self
            .runtime_boundary
            .app_manifest_config()
            .ok_or_else(|| CnxError::InvalidInput("missing app manifest config".into()))?;
        let mut store = UnionGraphStore::default();
        store.ingest(skeleton_from_manifest_config(&cfg)?)?;
        Ok(store)
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

    fn query_route(&self) -> RouteKey {
        query_request_route_for(&self.local_node_id())
    }

    fn ingest_route(&self) -> RouteKey {
        ingest_request_route_for(&self.local_node_id())
    }

    fn stop_task(slot: &mut Option<RuntimeServerTask>, name: &str) -> Result<()> {
        let Some(task) = slot.take() else {
            return Ok(());
        };
        task.shutdown.cancel();
        match task.join.join() {
            Ok(result) => result,
            Err(_) => Err(CnxError::Internal(format!("{name} task panicked"))),
        }
    }

    fn spawn_route_server(
        &self,
        name: &'static str,
        route: RouteKey,
        service: Arc<UnionGraphService>,
    ) -> Result<RuntimeServerTask> {
        let server = BoundRouteServer::open(self.data_boundary.clone(), route)?;
        let shutdown = CancellationToken::new();
        let task_shutdown = shutdown.clone();
        let join = thread::Builder::new()
            .name(name.into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|err| {
                        CnxError::Internal(format!("{name} runtime init failed: {err}"))
                    })?;
                runtime.block_on(async move {
                    server
                        .bind_concurrent_until(&task_shutdown, 8, move |events| {
                            service.handle_events(events)
                        })
                        .await
                })
            })
            .map_err(|err| CnxError::Internal(format!("{name} thread spawn failed: {err}")))?;
        Ok(RuntimeServerTask { shutdown, join })
    }
}

#[async_trait]
impl RuntimeBoundaryApp for UnionGraphRuntimeApp {
    async fn start(&self) -> Result<()> {
        let service = Arc::new(UnionGraphService::new(
            self.initial_store()?,
            self.origin_id.clone(),
        ));
        let query_task =
            self.spawn_route_server("union-graph-query", self.query_route(), service.clone())?;
        let ingest_task =
            self.spawn_route_server("union-graph-ingest", self.ingest_route(), service)?;

        let mut query_guard = self
            .query_task
            .lock()
            .map_err(|_| CnxError::Internal("union graph query task lock poisoned".into()))?;
        Self::stop_task(&mut query_guard, "union graph query")?;
        query_guard.replace(query_task);

        let mut ingest_guard = self
            .ingest_task
            .lock()
            .map_err(|_| CnxError::Internal("union graph ingest task lock poisoned".into()))?;
        Self::stop_task(&mut ingest_guard, "union graph ingest")?;
        ingest_guard.replace(ingest_task);
        Ok(())
    }

    async fn send(&self, _events: &[Event], _timeout: Duration) -> Result<()> {
        Err(CnxError::NotSupported(
            "union-graph data plane is served through bound internal routes".into(),
        ))
    }

    async fn recv(&self, _opts: RecvOpts) -> Result<Vec<Event>> {
        Err(CnxError::NotSupported(
            "union-graph does not expose stream recv in v1".into(),
        ))
    }

    async fn close(&self) -> Result<()> {
        let mut query_guard = self
            .query_task
            .lock()
            .map_err(|_| CnxError::Internal("union graph query task lock poisoned".into()))?;
        Self::stop_task(&mut query_guard, "union graph query")?;
        let mut ingest_guard = self
            .ingest_task
            .lock()
            .map_err(|_| CnxError::Internal("union graph ingest task lock poisoned".into()))?;
        Self::stop_task(&mut ingest_guard, "union graph ingest")
    }

    async fn on_control_frame(&self, _envelopes: &[ControlEnvelope]) -> Result<()> {
        Ok(())
    }
}

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use async_trait::async_trait;
use capanix_app_sdk::raw::ChannelIoSubset;
use capanix_app_sdk::runtime::{ConfigValue, ControlEnvelope, NodeId, RecvOpts, RouteKey};
use capanix_app_sdk::{
    BoundRouteServer, CnxError, Event, Result, RuntimeBoundary, RuntimeBoundaryApp,
};
use data_view::FileDataViewPersistence;
use tokio_util::sync::CancellationToken;
use union_graph::product_model::routes::{
    data_view_command_request_route_for, data_view_query_request_route_for,
};

use crate::data_view_service::DataViewRuntimeService;

const DATA_VIEW_STATE_PATH_CONFIG: &str = "data_view_state_path";
const DATA_VIEW_ID_CONFIG: &str = "data_view_id";

pub struct UnionGraphRuntimeApp {
    runtime_boundary: Arc<dyn RuntimeBoundary>,
    data_boundary: Arc<dyn ChannelIoSubset>,
    origin_id: NodeId,
    query_task: Mutex<Option<RuntimeServerTask>>,
    command_task: Mutex<Option<RuntimeServerTask>>,
}

struct RuntimeServerTask {
    shutdown: CancellationToken,
    join: JoinHandle<Result<()>>,
}

#[derive(Clone, Copy)]
enum RouteLane {
    Query,
    Command,
}

impl UnionGraphRuntimeApp {
    pub fn new(
        runtime_boundary: Arc<dyn RuntimeBoundary>,
        data_boundary: Arc<dyn ChannelIoSubset>,
    ) -> Self {
        Self {
            runtime_boundary,
            data_boundary,
            origin_id: NodeId("fustor-data-view".into()),
            query_task: Mutex::new(None),
            command_task: Mutex::new(None),
        }
    }

    fn manifest_config(&self) -> Result<std::collections::HashMap<String, ConfigValue>> {
        self.runtime_boundary
            .app_manifest_config()
            .ok_or_else(|| CnxError::InvalidInput("missing app manifest config".into()))
    }

    fn data_view_state_path(&self) -> Result<PathBuf> {
        let cfg = self.manifest_config()?;
        match cfg.get(DATA_VIEW_STATE_PATH_CONFIG) {
            Some(ConfigValue::String(value)) if !value.trim().is_empty() => {
                Ok(PathBuf::from(value.trim()))
            }
            _ => Err(CnxError::InvalidInput(format!(
                "missing non-empty {DATA_VIEW_STATE_PATH_CONFIG}"
            ))),
        }
    }

    fn data_view_id(&self) -> Result<String> {
        let cfg = self.manifest_config()?;
        match cfg.get(DATA_VIEW_ID_CONFIG) {
            Some(ConfigValue::String(value)) if !value.trim().is_empty() => {
                Ok(value.trim().to_string())
            }
            Some(ConfigValue::String(_)) => Err(CnxError::InvalidInput(format!(
                "{DATA_VIEW_ID_CONFIG} must be non-empty"
            ))),
            _ => Ok("global".into()),
        }
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
        data_view_query_request_route_for(&self.local_node_id())
    }

    fn command_route(&self) -> RouteKey {
        data_view_command_request_route_for(&self.local_node_id())
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
        service: Arc<DataViewRuntimeService>,
        lane: RouteLane,
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
                        .bind_concurrent_until(&task_shutdown, 8, move |events| match lane {
                            RouteLane::Query => service.handle_query_events(events),
                            RouteLane::Command => service.handle_command_events(events),
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
        let mut query_guard = self
            .query_task
            .lock()
            .map_err(|_| CnxError::Internal("data view query task lock poisoned".into()))?;
        let mut command_guard = self
            .command_task
            .lock()
            .map_err(|_| CnxError::Internal("data view command task lock poisoned".into()))?;
        Self::stop_task(&mut query_guard, "data view query")?;
        Self::stop_task(&mut command_guard, "data view command")?;

        let persistence = Arc::new(
            FileDataViewPersistence::new_exclusive(self.data_view_state_path()?)
                .map_err(|error| CnxError::Internal(error.to_string()))?,
        );
        let service = Arc::new(
            DataViewRuntimeService::with_persistence(
                self.data_view_id()?,
                self.origin_id.clone(),
                persistence,
            )
            .map_err(|error| CnxError::Internal(error.to_string()))?,
        );
        if let Some(config) = self.manifest_config()?.get("declared_result_policies") {
            let policies: Vec<data_view::DeclaredResultPolicy> = serde_json::from_value(
                serde_json::to_value(config).map_err(|_| CnxError::InvalidInput("declared result policy config".into()))?
            ).map_err(|_| CnxError::InvalidInput("declared result policy config".into()))?;
            if policies.len() > 20 { return Err(CnxError::InvalidInput("too many declared result policies".into())); }
            for policy in policies { service.register_declared_policy(policy).map_err(|error| CnxError::InvalidInput(error.to_string()))?; }
        }
        let query_task = self.spawn_route_server(
            "fustor-data-view-query",
            self.query_route(),
            service.clone(),
            RouteLane::Query,
        )?;
        let command_task = match self.spawn_route_server(
            "fustor-data-view-command",
            self.command_route(),
            service,
            RouteLane::Command,
        ) {
            Ok(task) => task,
            Err(error) => {
                let mut query_task = Some(query_task);
                let _ = Self::stop_task(&mut query_task, "data view query");
                return Err(error);
            }
        };
        query_guard.replace(query_task);
        command_guard.replace(command_task);
        Ok(())
    }

    async fn send(&self, _events: &[Event], _timeout: Duration) -> Result<()> {
        Err(CnxError::NotSupported(
            "Fustor DataView is served through bound internal routes".into(),
        ))
    }

    async fn recv(&self, _opts: RecvOpts) -> Result<Vec<Event>> {
        Err(CnxError::NotSupported(
            "Fustor DataView does not expose stream recv".into(),
        ))
    }

    async fn close(&self) -> Result<()> {
        let mut query_guard = self
            .query_task
            .lock()
            .map_err(|_| CnxError::Internal("data view query task lock poisoned".into()))?;
        Self::stop_task(&mut query_guard, "data view query")?;
        let mut command_guard = self
            .command_task
            .lock()
            .map_err(|_| CnxError::Internal("data view command task lock poisoned".into()))?;
        Self::stop_task(&mut command_guard, "data view command")
    }

    async fn on_control_frame(&self, _envelopes: &[ControlEnvelope]) -> Result<()> {
        Ok(())
    }
}

use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use async_trait::async_trait;
use capanix_app_sdk::raw::ChannelIoSubset;
use capanix_app_sdk::runtime::{ConfigValue, ControlEnvelope, NodeId, RecvOpts, RouteKey};
use capanix_app_sdk::{
    BoundRouteServer, CnxError, Event, Result, RuntimeBoundary, RuntimeBoundaryApp,
};
use capanix_runtime_entry_sdk::control::{RuntimeExecControl, decode_runtime_exec_control};
use mysql_source::MysqlSourceRuntimeConfig;
use mysql_source::product_model::routes::{fetch_request_route_for, status_request_route_for};
use source_kit::{SourceControlState, decode_source_resource_grants_changed};
use tokio_util::sync::CancellationToken;

use crate::driver::NativeMysqlSourceDriver;
use crate::service::MysqlSourceService;

pub struct MysqlSourceRuntimeApp {
    runtime_boundary: Arc<dyn RuntimeBoundary>,
    data_boundary: Arc<dyn ChannelIoSubset>,
    origin_id: NodeId,
    server_tasks: Mutex<RuntimeServerTasks>,
    control_state: Mutex<Option<SourceControlState<MysqlSourceRuntimeConfig>>>,
}

struct RuntimeServerTask {
    shutdown: CancellationToken,
    join: JoinHandle<Result<()>>,
}

#[derive(Default)]
struct RuntimeServerTasks {
    fetch: Option<RuntimeServerTask>,
    status: Option<RuntimeServerTask>,
}

impl MysqlSourceRuntimeApp {
    pub fn new(
        runtime_boundary: Arc<dyn RuntimeBoundary>,
        data_boundary: Arc<dyn ChannelIoSubset>,
    ) -> Self {
        Self {
            runtime_boundary,
            data_boundary,
            origin_id: NodeId("mysql-source".into()),
            server_tasks: Mutex::new(RuntimeServerTasks::default()),
            control_state: Mutex::new(None),
        }
    }

    fn manifest_config(&self) -> Result<MysqlSourceRuntimeConfig> {
        let cfg = self
            .runtime_boundary
            .app_manifest_config()
            .ok_or_else(|| CnxError::InvalidInput("missing app manifest config".into()))?;
        MysqlSourceRuntimeConfig::from_manifest_config(&cfg)
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

    fn status_route(&self) -> RouteKey {
        status_request_route_for(&self.local_node_id())
    }

    fn control_state(&self) -> Result<SourceControlState<MysqlSourceRuntimeConfig>> {
        let mut guard = self
            .control_state
            .lock()
            .map_err(|_| CnxError::Internal("mysql-source control state lock poisoned".into()))?;
        if let Some(state) = guard.as_ref() {
            return Ok(state.clone());
        }
        let state = SourceControlState::new(self.manifest_config()?);
        guard.replace(state.clone());
        Ok(state)
    }

    fn config_with_resource_grants(
        &self,
        grants: &[std::collections::HashMap<String, ConfigValue>],
    ) -> Result<MysqlSourceRuntimeConfig> {
        let mut cfg = self
            .runtime_boundary
            .app_manifest_config()
            .ok_or_else(|| CnxError::InvalidInput("missing app manifest config".into()))?;
        let mut runtime = match cfg.remove("__cnx_runtime") {
            Some(ConfigValue::Map(row)) => row,
            Some(_) => {
                return Err(CnxError::InvalidInput(
                    "__cnx_runtime must be a map for mysql-source".into(),
                ));
            }
            None => std::collections::HashMap::new(),
        };
        runtime.insert(
            "resource_grants".into(),
            ConfigValue::Array(grants.iter().cloned().map(ConfigValue::Map).collect()),
        );
        cfg.insert("__cnx_runtime".into(), ConfigValue::Map(runtime));
        MysqlSourceRuntimeConfig::from_manifest_config(&cfg)
    }

    fn apply_route_control(&self, route_key: &str, active: bool) -> Result<()> {
        let state = self.control_state()?;
        if route_key == self.fetch_route().0 {
            state.set_fetch_route_active(active)?;
        }
        if route_key == self.status_route().0 {
            state.set_status_route_active(active)?;
        }
        Ok(())
    }

    fn stop_server_task(slot: &mut Option<RuntimeServerTask>, label: &str) -> Result<()> {
        let Some(task) = slot.take() else {
            return Ok(());
        };
        task.shutdown.cancel();
        match task.join.join() {
            Ok(result) => result,
            Err(_) => Err(CnxError::Internal(format!(
                "mysql-source {label} task panicked"
            ))),
        }
    }

    fn stop_server_tasks(tasks: &mut RuntimeServerTasks) -> Result<()> {
        let fetch = Self::stop_server_task(&mut tasks.fetch, "fetch");
        let status = Self::stop_server_task(&mut tasks.status, "status");
        fetch?;
        status
    }
}

#[async_trait]
impl RuntimeBoundaryApp for MysqlSourceRuntimeApp {
    async fn start(&self) -> Result<()> {
        let config = self.manifest_config()?;
        let control_state = {
            let mut guard = self.control_state.lock().map_err(|_| {
                CnxError::Internal("mysql-source control state lock poisoned".into())
            })?;
            guard
                .get_or_insert_with(|| SourceControlState::new(config))
                .clone()
        };
        let service = Arc::new(MysqlSourceService::with_control_state(
            control_state,
            Arc::new(NativeMysqlSourceDriver),
            self.origin_id.clone(),
        ));
        let fetch_route = self.fetch_route();
        let status_route = self.status_route();
        let fetch_server = BoundRouteServer::open(self.data_boundary.clone(), fetch_route.clone())?;
        let status_server =
            BoundRouteServer::open(self.data_boundary.clone(), status_route.clone())?;

        let fetch_shutdown = CancellationToken::new();
        let fetch_task_shutdown = fetch_shutdown.clone();
        let fetch_service = service.clone();
        let fetch_join = thread::Builder::new()
            .name("mysql-source-fetch".into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|err| {
                        CnxError::Internal(format!("mysql-source fetch runtime init failed: {err}"))
                    })?;
                runtime.block_on(async move {
                    fetch_server
                        .bind_concurrent_until(&fetch_task_shutdown, 8, move |events| {
                            fetch_service.handle_events(events)
                        })
                        .await
                })
            })
            .map_err(|err| {
                CnxError::Internal(format!("mysql-source fetch thread spawn failed: {err}"))
            })?;

        let status_shutdown = CancellationToken::new();
        let status_task_shutdown = status_shutdown.clone();
        let status_service = service.clone();
        let fetch_route_key = fetch_route.0.clone();
        let status_route_key = status_route.0.clone();
        let status_join = match thread::Builder::new()
            .name("mysql-source-status".into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|err| {
                        CnxError::Internal(format!(
                            "mysql-source status runtime init failed: {err}"
                        ))
                    })?;
                runtime.block_on(async move {
                    status_server
                        .bind_concurrent_until(&status_task_shutdown, 4, move |events| {
                            status_service.handle_status_events(
                                events,
                                &fetch_route_key,
                                &status_route_key,
                            )
                        })
                        .await
                })
            }) {
            Ok(join) => join,
            Err(err) => {
                let mut fetch = Some(RuntimeServerTask {
                    shutdown: fetch_shutdown,
                    join: fetch_join,
                });
                let _ = Self::stop_server_task(&mut fetch, "fetch");
                return Err(CnxError::Internal(format!(
                    "mysql-source status thread spawn failed: {err}"
                )));
            }
        };
        let mut guard = self
            .server_tasks
            .lock()
            .map_err(|_| CnxError::Internal("mysql-source server task lock poisoned".into()))?;
        Self::stop_server_tasks(&mut guard)?;
        guard.fetch.replace(RuntimeServerTask {
            shutdown: fetch_shutdown,
            join: fetch_join,
        });
        guard.status.replace(RuntimeServerTask {
            shutdown: status_shutdown,
            join: status_join,
        });
        Ok(())
    }

    async fn send(&self, _events: &[Event], _timeout: Duration) -> Result<()> {
        Err(CnxError::NotSupported(
            "mysql-source data plane is served through bound internal route".into(),
        ))
    }

    async fn recv(&self, _opts: RecvOpts) -> Result<Vec<Event>> {
        Err(CnxError::NotSupported(
            "mysql-source does not expose stream recv in v1".into(),
        ))
    }

    async fn close(&self) -> Result<()> {
        let mut guard = self
            .server_tasks
            .lock()
            .map_err(|_| CnxError::Internal("mysql-source server task lock poisoned".into()))?;
        Self::stop_server_tasks(&mut guard)
    }

    async fn on_control_frame(&self, envelopes: &[ControlEnvelope]) -> Result<()> {
        for envelope in envelopes {
            if let Some(ctrl) = decode_runtime_exec_control(envelope)? {
                match ctrl {
                    RuntimeExecControl::Activate(activate) => {
                        self.apply_route_control(&activate.route_key, true)?;
                    }
                    RuntimeExecControl::Deactivate(deactivate) => {
                        self.apply_route_control(&deactivate.route_key, false)?;
                    }
                }
                continue;
            }
            let Some(changed) = decode_source_resource_grants_changed(envelope)? else {
                continue;
            };
            if changed.source_kind != "mysql" {
                continue;
            }
            let config = self.config_with_resource_grants(&changed.resource_grants)?;
            self.control_state()?
                .replace_config_if_newer(changed.version, config)?;
        }
        Ok(())
    }
}

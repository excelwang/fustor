use std::sync::{Arc, OnceLock};
use std::time::Duration;

use crate::api::facade_status::{
    FacadePendingReason, SharedFacadePendingStatus, SharedFacadePendingStatusCell,
    shared_facade_pending_status_cell,
};
use crate::query::TreeGroupPayload;
use crate::query::{InternalQueryRequest, MaterializedQueryPayload, QueryNode, SubtreeStats};
use crate::runtime::execution_units;
use crate::runtime::routes::ROUTE_KEY_FACADE_CONTROL;
use crate::runtime::orchestration::{
    FacadeControlSignal, FacadeRuntimeUnit, split_app_control_signals,
};
use crate::runtime::unit_gate::RuntimeUnitGate;
use crate::workers::sink::{SinkFacade, SinkWorkerClientHandle};
use crate::workers::source::{SourceFacade, SourceWorkerClientHandle};
use crate::{FSMetaConfig, api, source};
use async_trait::async_trait;
use capanix_app_sdk::raw::{
    BoundaryContext, ChannelAttachReply, ChannelAttachRequest, ChannelBoundary,
    ChannelCommitRequest, ChannelCommitResult, ChannelIoSubset, ChannelKey, ChannelRecvRequest,
    ChannelSendRequest, StateBoundary,
};
use capanix_app_sdk::runtime::{
    ConfigValue, ControlEnvelope, KernelResultEnvelope, LogLevel, NodeId, RecvOpts,
    StateCellReadRequest, StateCellWatchRequest, StateCellWriteRequest, in_memory_state_boundary,
};
use capanix_app_sdk::{CnxError, Event, Result, RuntimeBoundary, RuntimeBoundaryApp};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::sink::SinkFileMeta;
#[cfg(test)]
use crate::source::config::SourceConfig;
use crate::source::config::{SinkExecutionMode, SourceExecutionMode};

// Canonical fs-meta app authoring flows through `app-sdk`; direct runtime-api use
// is confined to narrow infra seams such as boundary conversion helpers.

struct FacadeActivation {
    generation: u64,
    resource_ids: Vec<String>,
    handle: api::ApiServerHandle,
}

#[derive(Clone)]
#[cfg_attr(not(test), allow(dead_code))]
struct PendingFacadeActivation {
    route_key: String,
    generation: u64,
    resource_ids: Vec<String>,
    bound_scopes: Vec<capanix_route_proto::BoundScope>,
    group_ids: Vec<String>,
    runtime_managed: bool,
    runtime_exposure_confirmed: bool,
    resolved: api::config::ResolvedApiConfig,
}

pub(crate) fn shared_tokio_runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        let worker_threads = std::thread::available_parallelism()
            .map(|n| n.get().clamp(1, 2))
            .unwrap_or(1);
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .thread_name("fs-meta-shared-runtime")
            .enable_all()
            .build()
            .expect("build shared fs-meta tokio runtime")
    })
}


fn now_us() -> u64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => d.as_micros() as u64,
        Err(_) => 0,
    }
}

pub struct FSMetaApp {
    config: FSMetaConfig,
    node_id: NodeId,
    runtime_boundary: Option<Arc<dyn ChannelIoSubset>>,
    source: Arc<SourceFacade>,
    sink: Arc<SinkFacade>,
    pump_task: Mutex<Option<JoinHandle<()>>>,
    api_task: Arc<Mutex<Option<FacadeActivation>>>,
    pending_facade: Arc<Mutex<Option<PendingFacadeActivation>>>,
    facade_pending_status: SharedFacadePendingStatusCell,
    facade_gate: RuntimeUnitGate,
}

impl FSMetaApp {
    pub fn new(config: FSMetaConfig, node_id: NodeId) -> Result<Self> {
        Self::with_boundaries(config, node_id, None)
    }

    pub fn with_boundaries(
        config: FSMetaConfig,
        node_id: NodeId,
        boundary: Option<Arc<dyn ChannelIoSubset>>,
    ) -> Result<Self> {
        Self::with_boundaries_and_state(config, node_id, boundary, None, in_memory_state_boundary())
    }

    pub(crate) fn with_boundaries_and_state(
        config: FSMetaConfig,
        node_id: NodeId,
        boundary: Option<Arc<dyn ChannelIoSubset>>,
        ordinary_boundary: Option<Arc<dyn ChannelBoundary>>,
        state_boundary: Arc<dyn StateBoundary>,
    ) -> Result<Self> {
        let source_cfg = config.source.clone();
        let sink_source_cfg = config.source.clone();
        let source = match source_cfg.source_execution_mode {
            SourceExecutionMode::InProcess => Arc::new(SourceFacade::local(Arc::new(
                source::FSMetaSource::with_boundaries_and_state(
                    source_cfg,
                    node_id.clone(),
                    boundary.clone(),
                    state_boundary.clone(),
                )?,
            ))),
            SourceExecutionMode::WorkerProcess => match boundary.clone() {
                Some(channel_boundary) => {
                    let control_boundary = ordinary_boundary.clone().ok_or_else(|| {
                        CnxError::InvalidInput(
                            "source worker mode requires ordinary runtime-boundary injection"
                                .to_string(),
                        )
                    })?;
                    let pair = Arc::new(RuntimeBoundaryPair {
                        control: control_boundary,
                        data: channel_boundary.clone(),
                        state: state_boundary.clone(),
                    });
                    Arc::new(SourceFacade::worker(Arc::new(
                        SourceWorkerClientHandle::new(node_id.clone(), config.source.clone(), pair),
                    )))
                }
                None => {
                    return Err(CnxError::InvalidInput(
                        "source worker mode requires runtime-boundary injection".to_string(),
                    ));
                }
            },
        };
        let sink = match sink_source_cfg.sink_execution_mode {
            SinkExecutionMode::InProcess => Arc::new(SinkFacade::local(Arc::new(
                SinkFileMeta::with_boundaries_and_state(
                    node_id.clone(),
                    boundary.clone(),
                    state_boundary.clone(),
                    sink_source_cfg,
                )?,
            ))),
            SinkExecutionMode::WorkerProcess => match boundary.clone() {
                Some(channel_boundary) => {
                    let control_boundary = ordinary_boundary.clone().ok_or_else(|| {
                        CnxError::InvalidInput(
                            "sink worker mode requires ordinary runtime-boundary injection"
                                .to_string(),
                        )
                    })?;
                    let pair = Arc::new(RuntimeBoundaryPair {
                        control: control_boundary,
                        data: channel_boundary.clone(),
                        state: state_boundary.clone(),
                    });
                    Arc::new(SinkFacade::worker(Arc::new(SinkWorkerClientHandle::new(
                        node_id.clone(),
                        sink_source_cfg.clone(),
                        pair,
                    ))))
                }
                None => {
                    return Err(CnxError::InvalidInput(
                        "sink worker mode requires runtime-boundary injection".to_string(),
                    ));
                }
            },
        };
        Ok(Self {
            config,
            node_id,
            runtime_boundary: boundary,
            source,
            sink,
            pump_task: Mutex::new(None),
            api_task: Arc::new(Mutex::new(None)),
            pending_facade: Arc::new(Mutex::new(None)),
            facade_pending_status: shared_facade_pending_status_cell(),
            facade_gate: RuntimeUnitGate::new(
                "fs-meta",
                &[execution_units::FACADE_RUNTIME_UNIT_ID],
            ),
        })
    }

    pub async fn start(&self) -> Result<()> {
        if !self.config.api.enabled {
            return Err(CnxError::InvalidInput(
                "api.enabled must be true; fs-meta management API boundary is mandatory".into(),
            ));
        }

        let sink_is_worker = self.sink.is_worker();
        let source_is_worker = self.source.is_worker();

        if !sink_is_worker {
            self.sink.ensure_started()?;
        }

        let mut guard = self.pump_task.lock().await;
        if guard.is_none() && !source_is_worker {
            *guard = self
                .source
                .start(self.sink.clone(), self.runtime_boundary.clone())
                .await?;
        }
        drop(guard);

        Ok(())
    }

    fn facade_candidate_resource_ids(
        bound_scopes: &[capanix_route_proto::BoundScope],
    ) -> Vec<String> {
        let mut ids = std::collections::BTreeSet::new();
        for scope in bound_scopes {
            for resource_id in &scope.resource_ids {
                let trimmed = resource_id.trim();
                if !trimmed.is_empty() {
                    ids.insert(trimmed.to_string());
                }
            }
        }
        ids.into_iter().collect()
    }

    fn runtime_scoped_facade_group_ids(source: &SourceFacade, sink: &SinkFacade) -> Result<Vec<String>> {
        let mut source_groups = source.scheduled_source_group_ids()?.unwrap_or_default();
        let mut scan_groups = source.scheduled_scan_group_ids()?.unwrap_or_default();
        source_groups.extend(scan_groups);
        let mut sink_groups = sink.scheduled_group_ids()?.unwrap_or_default();
        if !source_groups.is_empty() && !sink_groups.is_empty() {
            return Ok(source_groups.intersection(&sink_groups).cloned().collect());
        }
        if !source_groups.is_empty() {
            return Ok(source_groups.into_iter().collect());
        }
        Ok(sink_groups.into_iter().collect())
    }

    fn facade_candidate_group_ids(
        source: &SourceFacade,
        sink: &SinkFacade,
        bound_scopes: &[capanix_route_proto::BoundScope],
    ) -> Result<Vec<String>> {
        let logical_root_ids = source
            .logical_roots_snapshot()?
            .into_iter()
            .map(|root| root.id)
            .collect::<std::collections::BTreeSet<_>>();
        let mut ids = std::collections::BTreeSet::new();
        for scope in bound_scopes {
            let scope_id = scope.scope_id.trim();
            if !scope_id.is_empty() && logical_root_ids.contains(scope_id) {
                ids.insert(scope_id.to_string());
            }
            for resource_id in &scope.resource_ids {
                let trimmed = resource_id.trim();
                if trimmed.is_empty() {
                    continue;
                }
                if let Some(group_id) = source.resolve_group_id_for_object_ref(trimmed)? {
                    ids.insert(group_id);
                }
            }
        }
        if !ids.is_empty() {
            return Ok(ids.into_iter().collect());
        }
        Self::runtime_scoped_facade_group_ids(source, sink)
    }

    #[cfg(test)]
    fn observation_candidate_group_ids(
        source: &SourceFacade,
        sink: &SinkFacade,
        pending: &PendingFacadeActivation,
    ) -> Result<std::collections::BTreeSet<String>> {
        if !pending.group_ids.is_empty() {
            return Ok(pending.group_ids.iter().cloned().collect());
        }
        Ok(Self::facade_candidate_group_ids(
            source,
            sink,
            &pending.bound_scopes,
        )?
        .into_iter()
        .collect())
    }

    #[cfg(test)]
    fn observation_eligible_for(
        source: &SourceFacade,
        sink: &SinkFacade,
        pending: &PendingFacadeActivation,
    ) -> Result<bool> {
        let source_status = source.status_snapshot()?;
        let sink_status = sink.status_snapshot()?;
        let candidate_groups = Self::observation_candidate_group_ids(source, sink, pending)?;

        if candidate_groups.is_empty() {
            return Ok(false);
        }

        if source_status
            .degraded_roots
            .iter()
            .any(|(root_id, _)| candidate_groups.contains(root_id))
        {
            return Ok(false);
        }

        let scan_groups = source_status
            .concrete_roots
            .iter()
            .filter(|root| {
                root.active
                    && root.is_group_primary
                    && root.scan_enabled
                    && candidate_groups.contains(&root.logical_root_id)
            })
            .map(|root| root.logical_root_id.clone())
            .collect::<std::collections::BTreeSet<_>>();
        let sink_groups = sink_status
            .groups
            .iter()
            .map(|group| (group.group_id.clone(), group))
            .collect::<std::collections::BTreeMap<_, _>>();

        for group_id in &candidate_groups {
            if let Some(group) = sink_groups.get(group_id)
                && group.overflow_pending_audit
            {
                return Ok(false);
            }
            if scan_groups.contains(group_id) {
                let Some(group) = sink_groups.get(group_id) else {
                    return Ok(false);
                };
                if !group.initial_audit_completed {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    async fn try_spawn_pending_facade(&self) -> Result<bool> {
        Self::try_spawn_pending_facade_from_parts(
            self.api_task.clone(),
            self.pending_facade.clone(),
            self.facade_pending_status.clone(),
            self.node_id.clone(),
            self.runtime_boundary.clone(),
            self.source.clone(),
            self.sink.clone(),
        )
        .await
    }

    async fn try_spawn_pending_facade_from_parts(
        api_task: Arc<Mutex<Option<FacadeActivation>>>,
        pending_facade: Arc<Mutex<Option<PendingFacadeActivation>>>,
        facade_pending_status: SharedFacadePendingStatusCell,
        node_id: NodeId,
        runtime_boundary: Option<Arc<dyn ChannelIoSubset>>,
        source: Arc<SourceFacade>,
        sink: Arc<SinkFacade>,
    ) -> Result<bool> {
        let Some(pending) = pending_facade.lock().await.clone() else {
            return Ok(false);
        };
        let replacing_existing = {
            let api_task_guard = api_task.lock().await;
            if let Some(current) = api_task_guard.as_ref()
                && current.generation == pending.generation
                && current.resource_ids == pending.resource_ids
            {
                drop(api_task_guard);
                let mut pending_guard = pending_facade.lock().await;
                if pending_guard.as_ref().is_some_and(|candidate| {
                    candidate.generation == pending.generation
                        && candidate.resource_ids == pending.resource_ids
                }) {
                    pending_guard.take();
                }
                Self::clear_pending_facade_status(&facade_pending_status);
                return Ok(true);
            }
            api_task_guard.is_some()
        };
        if replacing_existing && !pending.runtime_exposure_confirmed {
            return Ok(false);
        }
        // Cold start has no prior facade to retain, so runtime confirmation is
        // sufficient to bring up the process. Replacement also proceeds once
        // runtime confirms external exposure; materialized `/tree` and `/stats`
        // readiness is enforced at the query surface so `/on-demand-force-find`
        // can become available earlier.

        sink.ensure_started()?;

        eprintln!("fs_meta_runtime_app: spawning facade api server generation={} route_key={} resources={:?}", pending.generation, pending.route_key, pending.resource_ids);
        let handle = api::spawn(
            pending.resolved.clone(),
            node_id,
            runtime_boundary,
            source,
            sink,
            facade_pending_status.clone(),
        )
        .await?;

        let still_pending = {
            let pending_guard = pending_facade.lock().await;
            pending_guard.as_ref().is_some_and(|candidate| {
                candidate.generation == pending.generation
                    && candidate.resource_ids == pending.resource_ids
            })
        };
        if !still_pending {
            eprintln!("fs_meta_runtime_app: shutting down stale facade handle generation={} route_key={}", pending.generation, pending.route_key);
            handle.shutdown(Duration::from_secs(2)).await;
            return Ok(false);
        }

        let previous = api_task.lock().await.replace(FacadeActivation {
            generation: pending.generation,
            resource_ids: pending.resource_ids.clone(),
            handle,
        });
        let mut pending_guard = pending_facade.lock().await;
        if pending_guard.as_ref().is_some_and(|candidate| {
            candidate.generation == pending.generation
                && candidate.resource_ids == pending.resource_ids
        }) {
            pending_guard.take();
        }
        Self::clear_pending_facade_status(&facade_pending_status);
        drop(pending_guard);
        if let Some(current) = previous {
            eprintln!("fs_meta_runtime_app: shutting down previous active facade generation={}", current.generation);
            current.handle.shutdown(Duration::from_secs(2)).await;
        }
        Ok(true)
    }

    fn facade_retry_backoff(retry_attempts: u64) -> Duration {
        match retry_attempts {
            0 | 1 => Duration::ZERO,
            2 => Duration::from_secs(2),
            3 => Duration::from_secs(4),
            4 => Duration::from_secs(8),
            5 => Duration::from_secs(16),
            _ => Duration::from_secs(30),
        }
    }

    fn pending_status_matches(
        status: &SharedFacadePendingStatus,
        pending: &PendingFacadeActivation,
    ) -> bool {
        status.route_key == pending.route_key
            && status.generation == pending.generation
            && status.resource_ids == pending.resource_ids
    }

    fn clear_pending_facade_status(status_cell: &SharedFacadePendingStatusCell) {
        if let Ok(mut guard) = status_cell.write() {
            *guard = None;
        }
    }

    fn set_pending_facade_status_waiting(
        status_cell: &SharedFacadePendingStatusCell,
        pending: &PendingFacadeActivation,
    ) {
        let pending_since_us = status_cell
            .read()
            .ok()
            .and_then(|guard| {
                guard.as_ref().and_then(|status| {
                    Self::pending_status_matches(status, pending).then_some(status.pending_since_us)
                })
            })
            .unwrap_or_else(now_us);
        if let Ok(mut guard) = status_cell.write() {
            *guard = Some(SharedFacadePendingStatus {
                route_key: pending.route_key.clone(),
                generation: pending.generation,
                resource_ids: pending.resource_ids.clone(),
                runtime_managed: pending.runtime_managed,
                runtime_exposure_confirmed: pending.runtime_exposure_confirmed,
                reason: FacadePendingReason::AwaitingRuntimeExposure,
                retry_attempts: 0,
                pending_since_us,
                last_error: None,
                last_attempt_at_us: None,
                last_error_at_us: None,
                retry_backoff_ms: None,
                next_retry_at_us: None,
            });
        }
    }

    fn record_pending_facade_retry_error(
        status_cell: &SharedFacadePendingStatusCell,
        pending: &PendingFacadeActivation,
        err: &CnxError,
    ) {
        let now = now_us();
        let (retry_attempts, pending_since_us) = status_cell
            .read()
            .ok()
            .and_then(|guard| {
                guard.as_ref().and_then(|status| {
                    Self::pending_status_matches(status, pending).then_some((
                        status.retry_attempts.saturating_add(1),
                        status.pending_since_us,
                    ))
                })
            })
            .unwrap_or((1, now));
        let backoff = Self::facade_retry_backoff(retry_attempts);
        let next_retry_at_us = now.saturating_add(backoff.as_micros() as u64);
        if let Ok(mut guard) = status_cell.write() {
            *guard = Some(SharedFacadePendingStatus {
                route_key: pending.route_key.clone(),
                generation: pending.generation,
                resource_ids: pending.resource_ids.clone(),
                runtime_managed: pending.runtime_managed,
                runtime_exposure_confirmed: pending.runtime_exposure_confirmed,
                reason: FacadePendingReason::RetryingAfterError,
                retry_attempts,
                pending_since_us,
                last_error: Some(err.to_string()),
                last_attempt_at_us: Some(now),
                last_error_at_us: Some(now),
                retry_backoff_ms: Some(backoff.as_millis() as u64),
                next_retry_at_us: Some(next_retry_at_us),
            });
        }
    }

    fn pending_facade_retry_due(
        status_cell: &SharedFacadePendingStatusCell,
        pending: &PendingFacadeActivation,
    ) -> bool {
        let now = now_us();
        status_cell
            .read()
            .ok()
            .and_then(|guard| {
                guard.as_ref().and_then(|status| {
                    Self::pending_status_matches(status, pending).then_some(
                        status
                            .next_retry_at_us
                            .map_or(true, |deadline| deadline <= now),
                    )
                })
            })
            .unwrap_or(true)
    }

    async fn pending_facade_snapshot_for(
        &self,
        route_key: &str,
        generation: u64,
    ) -> Option<PendingFacadeActivation> {
        self.pending_facade
            .lock()
            .await
            .as_ref()
            .and_then(|pending| {
                (pending.route_key == route_key && pending.generation == generation)
                    .then_some(pending.clone())
            })
    }

    async fn retry_pending_facade(
        &self,
        route_key: &str,
        generation: u64,
        from_tick: bool,
    ) -> Result<()> {
        let Some(pending) = self
            .pending_facade_snapshot_for(route_key, generation)
            .await
        else {
            return Ok(());
        };
        let has_active = self.api_task.lock().await.is_some();
        if has_active && !pending.runtime_exposure_confirmed {
            Self::set_pending_facade_status_waiting(&self.facade_pending_status, &pending);
            return Ok(());
        }
        if from_tick && !Self::pending_facade_retry_due(&self.facade_pending_status, &pending) {
            return Ok(());
        }
        match self.try_spawn_pending_facade().await {
            Ok(_) => Ok(()),
            Err(err) => {
                Self::record_pending_facade_retry_error(
                    &self.facade_pending_status,
                    &pending,
                    &err,
                );
                log::warn!("fs-meta facade pending activation retry failed: {err}");
                Ok(())
            }
        }
    }

    async fn apply_facade_activate(
        &self,
        unit: FacadeRuntimeUnit,
        route_key: &str,
        generation: u64,
        bound_scopes: &[capanix_route_proto::BoundScope],
    ) -> Result<()> {
        // Facade activation is a runtime-owned generation/bind/run handoff carrier.
        // Trusted external observation remains subordinate to package-local
        // observation_eligible and projection catch-up; activation alone does
        // not imply current authoritative truth is already reflected outside.
        let unit_id = unit.unit_id();
        let accepted =
            self.facade_gate
                .apply_activate(unit_id, route_key, generation, bound_scopes)?;
        if !accepted {
            log::info!(
                "fs-meta facade: ignore stale activate unit={} generation={}",
                unit_id,
                generation
            );
            return Ok(());
        }
        let candidate_resource_ids = Self::facade_candidate_resource_ids(bound_scopes);
        let runtime_managed = self.runtime_boundary.is_some();
        let resolved = self
            .config
            .api
            .resolve_for_candidate_ids(&candidate_resource_ids)
            .ok_or_else(|| {
                CnxError::InvalidInput(format!(
                    "fs-meta facade activation requires locally announced facade resource among {:?}",
                    candidate_resource_ids
                ))
            })?;
        {
            let api_task = self.api_task.lock().await;
            if let Some(current) = api_task.as_ref()
                && current.generation == generation
                && current.resource_ids == candidate_resource_ids
            {
                drop(api_task);
                let mut pending = self.pending_facade.lock().await;
                if pending.as_ref().is_some_and(|candidate| {
                    candidate.generation == generation
                        && candidate.resource_ids == candidate_resource_ids
                }) {
                    pending.take();
                }
                return Ok(());
            }
        }
        let runtime_exposure_confirmed = !runtime_managed;
        let pending = PendingFacadeActivation {
            route_key: route_key.to_string(),
            generation,
            resource_ids: candidate_resource_ids,
            bound_scopes: bound_scopes.to_vec(),
            group_ids: Vec::new(),
            runtime_managed,
            runtime_exposure_confirmed,
            resolved,
        };
        *self.pending_facade.lock().await = Some(pending.clone());
        if !self.try_spawn_pending_facade().await? {
            Self::set_pending_facade_status_waiting(&self.facade_pending_status, &pending);
        }
        Ok(())
    }

    async fn shutdown_active_facade(&self) {
        eprintln!("fs_meta_runtime_app: shutdown_active_facade");
        if let Some(current) = self.api_task.lock().await.take() {
            eprintln!("fs_meta_runtime_app: shutting down previous active facade generation={}", current.generation);
            current.handle.shutdown(Duration::from_secs(2)).await;
        }
    }

    async fn apply_facade_deactivate(
        &self,
        unit: FacadeRuntimeUnit,
        route_key: &str,
        generation: u64,
    ) -> Result<()> {
        let unit_id = unit.unit_id();
        let accepted = self
            .facade_gate
            .apply_deactivate(unit_id, route_key, generation)?;
        if !accepted {
            log::info!(
                "fs-meta facade: ignore stale deactivate unit={} generation={}",
                unit_id,
                generation
            );
            return Ok(());
        }
        *self.pending_facade.lock().await = None;
        Self::clear_pending_facade_status(&self.facade_pending_status);
        self.shutdown_active_facade().await;
        Ok(())
    }

    fn accept_facade_tick(
        &self,
        unit: FacadeRuntimeUnit,
        route_key: &str,
        generation: u64,
    ) -> Result<bool> {
        self.facade_gate
            .accept_tick(unit.unit_id(), route_key, generation)
    }

    async fn confirm_pending_facade_exposure(&self, route_key: &str, generation: u64) -> bool {
        let mut pending_guard = self.pending_facade.lock().await;
        let Some(pending) = pending_guard.as_mut() else {
            return false;
        };
        if pending.route_key != route_key || pending.generation != generation {
            return false;
        }
        pending.runtime_exposure_confirmed = true;
        true
    }

    pub async fn query_tree(
        &self,
        params: &InternalQueryRequest,
    ) -> Result<std::collections::BTreeMap<String, TreeGroupPayload>> {
        let events = self.sink.materialized_query(params)?;
        let mut grouped = std::collections::BTreeMap::<String, TreeGroupPayload>::new();
        for event in &events {
            let payload = rmp_serde::from_slice::<MaterializedQueryPayload>(event.payload_bytes())
                .map_err(|e| CnxError::Internal(format!("decode tree response failed: {e}")))?;
            let MaterializedQueryPayload::Tree(response) = payload else {
                return Err(CnxError::Internal(
                    "unexpected stats payload for query_tree".into(),
                ));
            };
            grouped.insert(event.metadata().origin_id.0.clone(), response);
        }
        Ok(grouped)
    }

    pub async fn query_stats(&self, path: &[u8]) -> Result<SubtreeStats> {
        let events = self.sink.subtree_stats(path)?;
        let mut agg = SubtreeStats::default();
        for event in &events {
            let stats = rmp_serde::from_slice::<SubtreeStats>(event.payload_bytes())
                .map_err(|e| CnxError::Internal(format!("decode stats response failed: {e}")))?;
            agg.total_nodes += stats.total_nodes;
            agg.total_files += stats.total_files;
            agg.total_dirs += stats.total_dirs;
            agg.total_size += stats.total_size;
            agg.attested_count += stats.attested_count;
            agg.blind_spot_count += stats.blind_spot_count;
        }
        Ok(agg)
    }

    pub fn source_status_snapshot(&self) -> Result<crate::source::SourceStatusSnapshot> {
        self.source.status_snapshot()
    }

    pub fn sink_status_snapshot(&self) -> Result<crate::sink::SinkStatusSnapshot> {
        self.sink.status_snapshot()
    }

    pub async fn trigger_rescan_when_ready(&self) -> Result<()> {
        self.source.trigger_rescan_when_ready().await
    }

    pub fn query_node(&self, path: &[u8]) -> Result<Option<QueryNode>> {
        self.sink.query_node(path)
    }
}

pub(crate) struct RuntimeBoundaryPair {
    control: Arc<dyn ChannelBoundary>,
    data: Arc<dyn ChannelIoSubset>,
    state: Arc<dyn StateBoundary>,
}

impl ChannelBoundary for RuntimeBoundaryPair {
    fn channel_attach(
        &self,
        ctx: BoundaryContext,
        request: ChannelAttachRequest,
    ) -> Result<Vec<ChannelAttachReply>> {
        self.control.channel_attach(ctx, request)
    }

    fn log(&self, ctx: BoundaryContext, level: LogLevel, msg: &str) {
        self.control.log(ctx, level, msg)
    }

    fn report_app_failure(&self, reason: &str) {
        self.control.report_app_failure(reason);
    }

    fn app_manifest_config(&self) -> Option<std::collections::HashMap<String, ConfigValue>> {
        self.control.app_manifest_config()
    }
}

impl ChannelIoSubset for RuntimeBoundaryPair {
    fn channel_send(&self, ctx: BoundaryContext, request: ChannelSendRequest) -> Result<()> {
        self.data.channel_send(ctx, request)
    }

    fn channel_commit(
        &self,
        ctx: BoundaryContext,
        request: ChannelCommitRequest,
    ) -> Result<ChannelCommitResult> {
        self.data.channel_commit(ctx, request)
    }

    fn channel_recv(
        &self,
        ctx: BoundaryContext,
        request: ChannelRecvRequest,
    ) -> Result<Vec<Event>> {
        self.data.channel_recv(ctx, request)
    }

    fn channel_close(&self, ctx: BoundaryContext, channel: ChannelKey) -> Result<()> {
        self.data.channel_close(ctx, channel)
    }
}

impl StateBoundary for RuntimeBoundaryPair {
    fn statecell_read(
        &self,
        ctx: BoundaryContext,
        request: StateCellReadRequest,
    ) -> Result<KernelResultEnvelope> {
        self.state.statecell_read(ctx, request)
    }

    fn statecell_write(
        &self,
        ctx: BoundaryContext,
        request: StateCellWriteRequest,
    ) -> Result<KernelResultEnvelope> {
        self.state.statecell_write(ctx, request)
    }

    fn statecell_watch(
        &self,
        ctx: BoundaryContext,
        request: StateCellWatchRequest,
    ) -> Result<KernelResultEnvelope> {
        self.state.statecell_watch(ctx, request)
    }
}

#[async_trait]
impl RuntimeBoundaryApp for FSMetaApp {
    async fn start(&self) -> Result<()> {
        FSMetaApp::start(self).await
    }

    async fn send(&self, events: &[Event]) -> Result<()> {
        self.sink.send(events).await
    }

    async fn recv(&self, opts: RecvOpts) -> Result<Vec<Event>> {
        self.sink.recv(opts).await
    }

    async fn on_control_frame(&self, envelopes: &[ControlEnvelope]) -> Result<()> {
        let (source_signals, sink_signals, facade_signals) = split_app_control_signals(envelopes)?;
        for signal in facade_signals {
            match signal {
                FacadeControlSignal::Activate {
                    unit,
                    route_key,
                    generation,
                    bound_scopes,
                } => {
                    self.apply_facade_activate(unit, &route_key, generation, &bound_scopes)
                        .await?;
                }
                FacadeControlSignal::Deactivate {
                    unit,
                    route_key,
                    generation,
                } => {
                    self.apply_facade_deactivate(unit, &route_key, generation)
                        .await?;
                }
                FacadeControlSignal::Tick {
                    unit,
                    route_key,
                    generation,
                } => {
                    let accepted = self.accept_facade_tick(unit, &route_key, generation)?;
                    if !accepted {
                        log::info!(
                            "fs-meta facade: ignore stale/inactive tick unit={} generation={}",
                            unit.unit_id(),
                            generation
                        );
                    } else {
                        self.retry_pending_facade(&route_key, generation, true)
                            .await?;
                    }
                }
                FacadeControlSignal::ExposureConfirmed {
                    unit,
                    route_key,
                    generation,
                    confirmed_at_us: _confirmed_at_us,
                } => {
                    let accepted = self.accept_facade_tick(unit, &route_key, generation)?;
                    if !accepted {
                        log::info!(
                            "fs-meta facade: ignore stale/inactive exposure_confirmed unit={} generation={}",
                            unit.unit_id(),
                            generation
                        );
                    } else if self
                        .confirm_pending_facade_exposure(&route_key, generation)
                        .await
                    {
                        self.retry_pending_facade(&route_key, generation, false)
                            .await?;
                    }
                }
                FacadeControlSignal::RuntimeHostObjectGrantsChanged { .. }
                | FacadeControlSignal::Passthrough => {}
            }
        }
        if !source_signals.is_empty() {
            self.source
                .apply_orchestration_signals(&source_signals)
                .await?;
        }
        if !sink_signals.is_empty() {
            self.sink.apply_orchestration_signals(&sink_signals).await?;
        }
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        *self.pending_facade.lock().await = None;
        Self::clear_pending_facade_status(&self.facade_pending_status);
        self.shutdown_active_facade().await;
        self.source.close().await?;
        self.sink.close().await?;
        if let Some(handle) = self.pump_task.lock().await.take() {
            handle.abort();
        }
        Ok(())
    }
}

pub struct FSMetaRuntimeApp {
    inner: Option<Arc<FSMetaApp>>,
    init_error: Option<String>,
}

impl FSMetaRuntimeApp {
    fn init_error_message(err: CnxError) -> String {
        match err {
            CnxError::InvalidInput(msg) => msg,
            other => other.to_string(),
        }
    }

    fn inner_arc(&self) -> Result<Arc<FSMetaApp>> {
        self.inner.clone().ok_or_else(|| {
            CnxError::InvalidInput(
                self.init_error
                    .clone()
                    .unwrap_or_else(|| "fs-meta runtime init failed".to_string()),
            )
        })
    }

    fn runtime_local_host_ref(
        cfg: &std::collections::HashMap<String, ConfigValue>,
    ) -> Option<NodeId> {
        let runtime = match cfg.get("__cnx_runtime") {
            Some(ConfigValue::Map(map)) => map,
            _ => return None,
        };
        let local_host_ref = match runtime.get("local_host_ref") {
            Some(ConfigValue::String(v)) => v.trim(),
            _ => return None,
        };
        if local_host_ref.is_empty() {
            None
        } else {
            Some(NodeId(local_host_ref.to_string()))
        }
    }

    fn required_runtime_local_host_ref(
        cfg: &std::collections::HashMap<String, ConfigValue>,
    ) -> Result<NodeId> {
        Self::runtime_local_host_ref(cfg).ok_or_else(|| {
            CnxError::InvalidInput(
                "__cnx_runtime.local_host_ref is required for fs-meta local execution identity"
                    .to_string(),
            )
        })
    }

    fn build_from_runtime_boundaries(
        ordinary_boundary: Arc<dyn ChannelBoundary>,
        state_boundary: Arc<dyn StateBoundary>,
        data_boundary: Option<Arc<dyn ChannelIoSubset>>,
    ) -> Self {
        fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
            if let Some(msg) = payload.downcast_ref::<String>() {
                return msg.clone();
            }
            if let Some(msg) = payload.downcast_ref::<&'static str>() {
                return (*msg).to_string();
            }
            "unknown panic payload".to_string()
        }

        // Some embeddings still load fs-meta in-process, but that boundary is
        // non-isolating; constructor failures must stay on `init_error`
        // instead of crossing the ABI as a panic/abort path.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let manifest_cfg = match ordinary_boundary.app_manifest_config() {
                Some(cfg) => cfg,
                None => {
                    return Self {
                        inner: None,
                        init_error: Some(
                            "roots[] is required (missing app manifest config)".to_string(),
                        ),
                    };
                }
            };
            let node_id = match Self::required_runtime_local_host_ref(&manifest_cfg) {
                Ok(node_id) => node_id,
                Err(err) => {
                    return Self {
                        inner: None,
                        init_error: Some(Self::init_error_message(err)),
                    };
                }
            };
            let cfg = FSMetaConfig::from_manifest_config(&manifest_cfg);
            match cfg {
                Ok(cfg) => match FSMetaApp::with_boundaries_and_state(
                    cfg,
                    node_id,
                    data_boundary,
                    Some(ordinary_boundary),
                    state_boundary,
                ) {
                    Ok(inner) => Self {
                        inner: Some(Arc::new(inner)),
                        init_error: None,
                    },
                    Err(err) => {
                        let msg = Self::init_error_message(err);
                        log::error!("fs-meta runtime init failed (build app): {msg}");
                        Self {
                            inner: None,
                            init_error: Some(msg),
                        }
                    }
                },
                Err(err) => {
                    let msg = Self::init_error_message(err);
                    log::error!("fs-meta runtime init failed (manifest config): {msg}");
                    Self {
                        inner: None,
                        init_error: Some(msg),
                    }
                }
            }
        }));

        match result {
            Ok(app) => app,
            Err(payload) => {
                let msg = format!(
                    "fs-meta runtime init panicked during constructor: {}",
                    panic_message(payload)
                );
                log::error!("{msg}");
                Self {
                    inner: None,
                    init_error: Some(msg),
                }
            }
        }
    }

    pub fn new_without_io(boundary: Arc<dyn RuntimeBoundary>) -> Self {
        let ordinary_boundary: Arc<dyn ChannelBoundary> = boundary.clone();
        let state_boundary: Arc<dyn StateBoundary> = boundary;
        Self::build_from_runtime_boundaries(ordinary_boundary, state_boundary, None)
    }

    pub fn new(
        boundary: Arc<dyn RuntimeBoundary>,
        data_boundary: Arc<dyn ChannelIoSubset>,
    ) -> Self {
        let ordinary_boundary: Arc<dyn ChannelBoundary> = boundary.clone();
        let state_boundary: Arc<dyn StateBoundary> = boundary;
        Self::build_from_runtime_boundaries(ordinary_boundary, state_boundary, Some(data_boundary))
    }
}

#[async_trait]
impl RuntimeBoundaryApp for FSMetaRuntimeApp {
    async fn start(&self) -> Result<()> {
        let inner = self.inner_arc()?;
        if tokio::runtime::Handle::try_current().is_ok() {
            inner.start().await
        } else {
            shared_tokio_runtime().block_on(inner.start())
        }
    }

    async fn send(&self, events: &[Event]) -> Result<()> {
        let inner = self.inner_arc()?;
        if tokio::runtime::Handle::try_current().is_ok() {
            inner.send(events).await
        } else {
            shared_tokio_runtime().block_on(inner.send(events))
        }
    }

    async fn recv(&self, opts: RecvOpts) -> Result<Vec<Event>> {
        let inner = self.inner_arc()?;
        if tokio::runtime::Handle::try_current().is_ok() {
            inner.recv(opts).await
        } else {
            shared_tokio_runtime().block_on(inner.recv(opts))
        }
    }

    async fn on_control_frame(&self, envelopes: &[ControlEnvelope]) -> Result<()> {
        let inner = self.inner_arc()?;
        if tokio::runtime::Handle::try_current().is_ok() {
            inner.on_control_frame(envelopes).await
        } else {
            shared_tokio_runtime().block_on(inner.on_control_frame(envelopes))
        }
    }

    async fn close(&self) -> Result<()> {
        match self.inner_arc() {
            Ok(inner) => {
                if tokio::runtime::Handle::try_current().is_ok() {
                    inner.close().await
                } else {
                    shared_tokio_runtime().block_on(inner.close())
                }
            }
            Err(_) => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FSMetaConfig, api, query, source};
    use bytes::Bytes;
    use capanix_app_sdk::runtime::EventMetadata;
    use capanix_host_fs_types::{ControlEvent, FileMetaRecord, UnixStat};
    use capanix_route_proto::{
        ExecActivate, ExecControl, HostDescriptor, HostObjectGrant, HostObjectGrantState,
        HostObjectType, ObjectDescriptor, RuntimeHostObjectGrantsChanged, TrustedExposureConfirmed,
        WorkerTick, encode_exec_control_envelope,
        encode_runtime_host_object_grants_changed_envelope,
        encode_trusted_exposure_confirmed_envelope, encode_worker_tick_envelope,
    };
    use reqwest::Client;
    use serde_json::json;
    use std::fs;
    use std::net::TcpListener;
    use std::sync::{Arc, OnceLock};
    use std::time::Duration;
    use tempfile::tempdir;

    fn write_auth_files(dir: &tempfile::TempDir) -> (std::path::PathBuf, std::path::PathBuf) {
        let passwd = dir.path().join("fs-meta.passwd");
        let shadow = dir.path().join("fs-meta.shadow");
        fs::write(
            &passwd,
            "admin:1000:1000:fsmeta_management:/home/admin:/bin/bash:0\n",
        )
        .expect("write passwd");
        fs::write(&shadow, "admin:plain$admin:0\n").expect("write shadow");
        (passwd, shadow)
    }

    fn in_process_source_config() -> SourceConfig {
        SourceConfig {
            source_execution_mode: SourceExecutionMode::InProcess,
            sink_execution_mode: SinkExecutionMode::InProcess,
            ..SourceConfig::default()
        }
    }

    struct NoopBoundary;

    impl ChannelIoSubset for NoopBoundary {}

    fn activate_envelope(unit_id: &str) -> ControlEnvelope {
        encode_exec_control_envelope(&ExecControl::Activate(ExecActivate {
            route_key: ROUTE_KEY_FACADE_CONTROL.to_string(),
            worker_id: unit_id.to_string(),
            lease: None,
            generation: 1,
            expires_at_ms: 60_000,
            bound_scopes: Vec::new(),
        }))
        .expect("encode activate envelope")
    }

    fn activate_envelope_with_scopes(
        unit_id: &str,
        scope_id: &str,
        resource_ids: &[&str],
    ) -> ControlEnvelope {
        activate_envelope_with_generation_and_scopes(unit_id, scope_id, resource_ids, 1)
    }

    fn activate_envelope_with_generation_and_scopes(
        unit_id: &str,
        scope_id: &str,
        resource_ids: &[&str],
        generation: u64,
    ) -> ControlEnvelope {
        encode_exec_control_envelope(&ExecControl::Activate(ExecActivate {
            route_key: ROUTE_KEY_FACADE_CONTROL.to_string(),
            worker_id: unit_id.to_string(),
            lease: None,
            generation,
            expires_at_ms: 60_000,
            bound_scopes: vec![capanix_route_proto::BoundScope {
                scope_id: scope_id.to_string(),
                resource_ids: resource_ids.iter().map(|id| (*id).to_string()).collect(),
            }],
        }))
        .expect("encode activate envelope with scopes")
    }

    fn host_object_grants_changed_envelope(version: u64, mount_point: &str) -> ControlEnvelope {
        encode_runtime_host_object_grants_changed_envelope(&RuntimeHostObjectGrantsChanged {
            version,
            grants: vec![HostObjectGrant {
                object_ref: "single-app-node::root-1".to_string(),
                object_type: HostObjectType::MountRoot,
                interfaces: vec!["posix-fs".to_string(), "inotify".to_string()],
                host: HostDescriptor {
                    host_ref: "single-app-node".to_string(),
                    host_ip: "127.0.0.1".to_string(),
                    host_name: Some("single-app-node".to_string()),
                    site: None,
                    zone: None,
                    host_labels: Default::default(),
                },
                object: ObjectDescriptor {
                    mount_point: mount_point.to_string(),
                    fs_source: mount_point.to_string(),
                    fs_type: "nfs".to_string(),
                    mount_options: Vec::new(),
                },
                grant_state: HostObjectGrantState::Active,
            }],
        })
        .expect("encode runtime host object grants changed envelope")
    }

    #[allow(dead_code)]
    fn tick_envelope(unit_id: &str, generation: u64) -> ControlEnvelope {
        encode_worker_tick_envelope(&WorkerTick {
            route_key: ROUTE_KEY_FACADE_CONTROL.to_string(),
            worker_id: unit_id.to_string(),
            generation,
            at_ms: 1,
        })
        .expect("encode unit tick envelope")
    }

    #[allow(dead_code)]
    fn trusted_exposure_confirmed_envelope(unit_id: &str, generation: u64) -> ControlEnvelope {
        encode_trusted_exposure_confirmed_envelope(&TrustedExposureConfirmed {
            route_key: ROUTE_KEY_FACADE_CONTROL.to_string(),
            worker_id: unit_id.to_string(),
            generation,
            confirmed_at_us: 1,
        })
        .expect("encode trusted exposure confirmed envelope")
    }

    fn granted_mount_root(
        object_ref: &str,
        mount_point: &std::path::Path,
    ) -> source::config::GrantedMountRoot {
        source::config::GrantedMountRoot {
            object_ref: object_ref.to_string(),
            host_ref: "single-app-node".to_string(),
            host_ip: "127.0.0.1".to_string(),
            host_name: Some("single-app-node".to_string()),
            site: None,
            zone: None,
            host_labels: Default::default(),
            mount_point: mount_point.to_path_buf(),
            fs_source: mount_point.display().to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            interfaces: vec!["posix-fs".to_string(), "inotify".to_string()],
            active: true,
        }
    }

    fn reserve_bind_addr() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind temp listener");
        let addr = listener.local_addr().expect("local addr");
        drop(listener);
        addr.to_string()
    }

    fn facade_pending_status(app: &FSMetaApp) -> SharedFacadePendingStatus {
        app.facade_pending_status
            .read()
            .expect("read facade pending status")
            .clone()
            .expect("facade pending status present")
    }

    fn mk_source_event(origin: &str, path: &[u8], file_name: &[u8], ts: u64) -> Event {
        let record = FileMetaRecord::scan_update(
            path.to_vec(),
            file_name.to_vec(),
            UnixStat {
                is_dir: false,
                size: 1,
                mtime_us: ts,
                ctime_us: ts,
                dev: None,
                ino: None,
            },
            b"/".to_vec(),
            ts,
            false,
        );
        let payload = rmp_serde::to_vec_named(&record).expect("encode record");
        Event::new(
            EventMetadata {
                origin_id: NodeId(origin.to_string()),
                timestamp_us: ts,
                logical_ts: None,
                correlation_id: None,
                ingress_auth: None,
                trace: None,
            },
            Bytes::from(payload),
        )
    }

    fn mk_control_event(origin: &str, control: ControlEvent, ts: u64) -> Event {
        let payload = rmp_serde::to_vec_named(&control).expect("encode control event");
        Event::new(
            EventMetadata {
                origin_id: NodeId(origin.to_string()),
                timestamp_us: ts,
                logical_ts: None,
                correlation_id: None,
                ingress_auth: None,
                trace: None,
            },
            Bytes::from(payload),
        )
    }

    fn selector_mount_point(path: &str) -> ConfigValue {
        ConfigValue::Map(std::collections::HashMap::from([(
            "mount_point".to_string(),
            ConfigValue::String(path.to_string()),
        )]))
    }

    fn root_entry(path: &str) -> ConfigValue {
        ConfigValue::Map(std::collections::HashMap::from([(
            "selector".to_string(),
            selector_mount_point(path),
        )]))
    }

    fn root_entry_with_id(id: &str, path: &str) -> ConfigValue {
        ConfigValue::Map(std::collections::HashMap::from([
            ("id".to_string(), ConfigValue::String(id.to_string())),
            ("selector".to_string(), selector_mount_point(path)),
        ]))
    }

    fn worker_cfg(mode: &str) -> ConfigValue {
        ConfigValue::Map(std::collections::HashMap::from([(
            "mode".to_string(),
            ConfigValue::String(mode.to_string()),
        )]))
    }

    fn worker_cfg_with_bin(mode: &str, bin_path: &str) -> ConfigValue {
        ConfigValue::Map(std::collections::HashMap::from([
            ("mode".to_string(), ConfigValue::String(mode.to_string())),
            (
                "binary_path".to_string(),
                ConfigValue::String(bin_path.to_string()),
            ),
        ]))
    }

    #[tokio::test]
    async fn start_accepts_missing_roots_config_as_valid_deployed_state() {
        let _app = FSMetaApp::new(
            FSMetaConfig {
                source: in_process_source_config(),
                ..FSMetaConfig::default()
            },
            NodeId("single-app-node".into()),
        )
        .expect("empty roots should be accepted as valid deployed state");
    }

    #[tokio::test]
    async fn start_rejects_removed_unit_authority_state_config() {
        let cfg = std::collections::HashMap::from([
            (
                "roots".to_string(),
                ConfigValue::Array(vec![root_entry("/tmp")]),
            ),
            (
                "unit_authority_state_dir".to_string(),
                ConfigValue::String("/var/lib/capanix/fs-meta/statecell".to_string()),
            ),
        ]);
        let err = FSMetaConfig::from_manifest_config(&cfg)
            .expect_err("removed unit authority fields must fail-closed");
        assert!(
            err.to_string()
                .contains("unit_authority_state_carrier/unit_authority_state_dir are removed")
        );
    }

    #[tokio::test]
    async fn on_control_frame_routes_source_sink_and_facade_units() {
        let tmp = tempdir().expect("create temp dir");
        let (passwd_path, shadow_path) = write_auth_files(&tmp);
        let cfg = FSMetaConfig {
            source: SourceConfig {
                roots: vec![source::config::RootSpec::new("test-root", tmp.path())],
                ..in_process_source_config()
            },
            api: api::ApiConfig {
                facade_resource_id: "single-app-listener".to_string(),
                local_listener_resources: vec![api::config::ApiListenerResource {
                    resource_id: "single-app-listener".to_string(),
                    bind_addr: "127.0.0.1:0".to_string(),
                }],
                auth: api::ApiAuthConfig {
                    passwd_path,
                    shadow_path,
                    ..api::ApiAuthConfig::default()
                },
                ..api::ApiConfig::default()
            },
        };
        let app = FSMetaApp::new(cfg, NodeId("single-app-node".into())).expect("init app");
        match app
            .on_control_frame(&[
                activate_envelope("runtime.exec.source"),
                activate_envelope("runtime.exec.sink"),
                activate_envelope_with_scopes(
                    "runtime.exec.facade",
                    "single-app-listener",
                    &["single-app-listener"],
                ),
            ])
            .await
        {
            Ok(()) => {}
            Err(CnxError::InvalidInput(msg))
                if msg.contains("fs-meta api bind failed: Operation not permitted") =>
            {
                return;
            }
            Err(err) => {
                panic!("source/sink/facade unit controls must route without cross-rejection: {err}")
            }
        }
        let pending = app.pending_facade.lock().await.is_some();
        let active = app.api_task.lock().await.is_some();
        assert!(
            pending || active,
            "facade activation should either remain pending or spawn immediately"
        );
    }

    #[tokio::test]
    async fn standalone_facade_activation_uses_local_runtime_scope_fallback() {
        let tmp = tempdir().expect("create temp dir");
        let root_a = tmp.path().join("root-a");
        fs::create_dir_all(&root_a).expect("create root-a");
        fs::write(root_a.join("ready.txt"), "ready\n").expect("seed root-a");
        let (passwd_path, shadow_path) = write_auth_files(&tmp);
        let cfg = FSMetaConfig {
            source: SourceConfig {
                roots: vec![source::config::RootSpec::new("root-a", &root_a)],
                host_object_grants: vec![granted_mount_root("single-app-node::root-a-1", &root_a)],
                ..in_process_source_config()
            },
            api: api::ApiConfig {
                enabled: true,
                facade_resource_id: "single-app-listener".to_string(),
                local_listener_resources: vec![api::config::ApiListenerResource {
                    resource_id: "single-app-listener".to_string(),
                    bind_addr: "127.0.0.1:0".to_string(),
                }],
                auth: api::ApiAuthConfig {
                    passwd_path,
                    shadow_path,
                    ..api::ApiAuthConfig::default()
                },
            },
        };
        let app = FSMetaApp::new(cfg, NodeId("single-app-node".into())).expect("init app");
        app.start().await.expect("start app");
        match app
            .on_control_frame(&[activate_envelope_with_scopes(
                "runtime.exec.facade",
                "single-app-listener",
                &["single-app-listener"],
            )])
            .await
        {
            Ok(()) => {}
            Err(CnxError::InvalidInput(msg))
                if msg.contains("fs-meta api bind failed: Operation not permitted") =>
            {
                return;
            }
            Err(err) => {
                panic!("standalone facade activation should use local scope fallback: {err}")
            }
        }
        assert!(
            app.api_task.lock().await.is_some(),
            "standalone facade activation should spawn the API once local materialization is ready"
        );
        app.close().await.expect("close app");
    }

    #[tokio::test]
    async fn start_embedded_runtime_boundary_accepts_cold_start_before_initial_scan_materialization()
     {
        if cfg!(not(target_os = "linux")) {
            return;
        }

        let tmp = tempdir().expect("create temp dir");
        let root_a = tmp.path().join("root-a");
        fs::create_dir_all(&root_a).expect("create root-a");
        fs::write(root_a.join("ready.txt"), "ready\n").expect("seed root-a");
        let (passwd_path, shadow_path) = write_auth_files(&tmp);
        let cfg = FSMetaConfig {
            source: SourceConfig {
                roots: vec![source::config::RootSpec::new("root-a", &root_a)],
                host_object_grants: vec![granted_mount_root("single-app-node::root-a-1", &root_a)],
                ..in_process_source_config()
            },
            api: api::ApiConfig {
                enabled: true,
                facade_resource_id: "single-app-listener".to_string(),
                local_listener_resources: vec![api::config::ApiListenerResource {
                    resource_id: "single-app-listener".to_string(),
                    bind_addr: "127.0.0.1:0".to_string(),
                }],
                auth: api::ApiAuthConfig {
                    passwd_path,
                    shadow_path,
                    ..api::ApiAuthConfig::default()
                },
            },
        };
        let app = FSMetaApp::with_boundaries(
            cfg,
            NodeId("single-app-node".into()),
            Some(Arc::new(NoopBoundary)),
        )
        .expect("init app");
        tokio::time::timeout(Duration::from_secs(1), app.start())
            .await
            .expect("embedded runtime cold start should not wait for initial materialization")
            .expect("start app");
        app.close().await.expect("close app");
    }

    #[tokio::test]
    async fn facade_activate_keeps_prior_active_until_pending_generation_is_eligible() {
        let tmp = tempdir().expect("create temp dir");
        let (passwd_path, shadow_path) = write_auth_files(&tmp);
        let cfg = FSMetaConfig {
            source: SourceConfig {
                roots: vec![source::config::RootSpec::new("test-root", tmp.path())],
                host_object_grants: vec![granted_mount_root("single-app-node::root-1", tmp.path())],
                ..in_process_source_config()
            },
            api: api::ApiConfig {
                enabled: true,
                facade_resource_id: "single-app-listener".to_string(),
                local_listener_resources: vec![api::config::ApiListenerResource {
                    resource_id: "single-app-listener".to_string(),
                    bind_addr: "127.0.0.1:0".to_string(),
                }],
                auth: api::ApiAuthConfig {
                    passwd_path,
                    shadow_path,
                    ..api::ApiAuthConfig::default()
                },
            },
        };
        let app = FSMetaApp::with_boundaries(
            cfg,
            NodeId("single-app-node".into()),
            Some(Arc::new(NoopBoundary)),
        )
        .expect("init app");
        let existing = match api::spawn(
            app.config
                .api
                .resolve_for_candidate_ids(&["single-app-listener".to_string()])
                .expect("resolve facade config"),
            app.node_id.clone(),
            app.runtime_boundary.clone(),
            app.source.clone(),
            app.sink.clone(),
            app.facade_pending_status.clone(),
        )
        .await
        {
            Ok(handle) => handle,
            Err(CnxError::InvalidInput(msg))
                if msg.contains("fs-meta api bind failed: Operation not permitted") =>
            {
                return;
            }
            Err(err) => panic!("spawn active facade: {err}"),
        };
        *app.api_task.lock().await = Some(FacadeActivation {
            generation: 1,
            resource_ids: vec!["single-app-listener".to_string()],
            handle: existing,
        });

        app.apply_facade_activate(
            FacadeRuntimeUnit::Facade,
            ROUTE_KEY_FACADE_CONTROL,
            2,
            &[capanix_route_proto::BoundScope {
                scope_id: "test-root".to_string(),
                resource_ids: vec!["single-app-listener".to_string()],
            }],
        )
        .await
        .expect("apply facade activate");

        let active_guard = app.api_task.lock().await;
        let active = active_guard.as_ref().expect("prior active facade retained");
        assert_eq!(active.generation, 1);
        assert_eq!(active.resource_ids, vec!["single-app-listener".to_string()]);
        drop(active_guard);

        let pending = app
            .pending_facade
            .lock()
            .await
            .clone()
            .expect("pending facade recorded");
        assert_eq!(pending.generation, 2);
        assert_eq!(
            pending.resource_ids,
            vec!["single-app-listener".to_string()]
        );
        assert!(pending.group_ids.is_empty());
        assert_eq!(pending.route_key, ROUTE_KEY_FACADE_CONTROL);
        assert!(pending.runtime_managed);
        assert!(!pending.runtime_exposure_confirmed);
        app.close().await.expect("close fs-meta app");
    }

    #[tokio::test]
    async fn facade_cutover_replaces_previous_after_runtime_confirmation_even_before_tree_ready() {
        let tmp = tempdir().expect("create temp dir");
        let root = tmp.path().join("root-a");
        fs::create_dir_all(&root).expect("create root");
        let (passwd_path, shadow_path) = write_auth_files(&tmp);
        let cfg = FSMetaConfig {
            source: SourceConfig {
                roots: vec![source::config::RootSpec::new("test-root", &root)],
                host_object_grants: vec![granted_mount_root("single-app-node::root-1", &root)],
                ..in_process_source_config()
            },
            api: api::ApiConfig {
                enabled: true,
                facade_resource_id: "single-app-listener".to_string(),
                local_listener_resources: vec![api::config::ApiListenerResource {
                    resource_id: "single-app-listener".to_string(),
                    bind_addr: "127.0.0.1:0".to_string(),
                }],
                auth: api::ApiAuthConfig {
                    passwd_path,
                    shadow_path,
                    ..api::ApiAuthConfig::default()
                },
            },
        };
        let app = FSMetaApp::with_boundaries(
            cfg,
            NodeId("single-app-node".into()),
            Some(Arc::new(NoopBoundary)),
        )
        .expect("init app");
        let existing = match api::spawn(
            app.config
                .api
                .resolve_for_candidate_ids(&["single-app-listener".to_string()])
                .expect("resolve facade config"),
            app.node_id.clone(),
            app.runtime_boundary.clone(),
            app.source.clone(),
            app.sink.clone(),
            app.facade_pending_status.clone(),
        )
        .await
        {
            Ok(handle) => handle,
            Err(CnxError::InvalidInput(msg))
                if msg.contains("fs-meta api bind failed: Operation not permitted") =>
            {
                return;
            }
            Err(err) => panic!("spawn active facade: {err}"),
        };
        *app.api_task.lock().await = Some(FacadeActivation {
            generation: 1,
            resource_ids: vec!["single-app-listener".to_string()],
            handle: existing,
        });
        let readiness_pending = PendingFacadeActivation {
            route_key: ROUTE_KEY_FACADE_CONTROL.to_string(),
            generation: 2,
            resource_ids: vec!["single-app-listener".to_string()],
            bound_scopes: vec![capanix_route_proto::BoundScope {
                scope_id: "single-app-listener".to_string(),
                resource_ids: vec!["single-app-listener".to_string()],
            }],
            group_ids: Vec::new(),
            runtime_managed: true,
            runtime_exposure_confirmed: true,
            resolved: app
                .config
                .api
                .resolve_for_candidate_ids(&["single-app-listener".to_string()])
                .expect("resolve facade config"),
        };
        assert!(
            !FSMetaApp::observation_eligible_for(
                app.source.as_ref(),
                app.sink.as_ref(),
                &readiness_pending,
            )
            .expect("tree readiness still gated on initial audit"),
            "replacement facade should not imply materialized tree readiness"
        );
        *app.pending_facade.lock().await = Some(readiness_pending);
        assert!(
            app.try_spawn_pending_facade()
                .await
                .expect("runtime-confirmed replacement facade spawn"),
            "runtime confirmation should be enough to replace the facade even before tree readiness"
        );
        assert_eq!(
            app.api_task
                .lock()
                .await
                .as_ref()
                .expect("replacement facade active")
                .generation,
            2
        );
        assert!(app.pending_facade.lock().await.is_none());
        app.close().await.expect("close fs-meta app");
    }

    #[tokio::test]
    async fn facade_retry_waits_for_worker_tick_instead_of_background_polling() {
        let tmp = tempdir().expect("create temp dir");
        let bind_addr = reserve_bind_addr();
        let (passwd_path, shadow_path) = write_auth_files(&tmp);
        let cfg = FSMetaConfig {
            source: SourceConfig {
                roots: vec![source::config::RootSpec::new("test-root", tmp.path())],
                host_object_grants: vec![granted_mount_root("single-app-node::root-1", tmp.path())],
                ..in_process_source_config()
            },
            api: api::ApiConfig {
                enabled: true,
                facade_resource_id: "single-app-listener".to_string(),
                local_listener_resources: vec![api::config::ApiListenerResource {
                    resource_id: "single-app-listener".to_string(),
                    bind_addr: bind_addr.clone(),
                }],
                auth: api::ApiAuthConfig {
                    passwd_path,
                    shadow_path,
                    ..api::ApiAuthConfig::default()
                },
            },
        };
        let app = FSMetaApp::with_boundaries(
            cfg,
            NodeId("single-app-node".into()),
            Some(Arc::new(NoopBoundary)),
        )
        .expect("init app");
        let existing = match api::spawn(
            app.config
                .api
                .resolve_for_candidate_ids(&["single-app-listener".to_string()])
                .expect("resolve facade config"),
            app.node_id.clone(),
            app.runtime_boundary.clone(),
            app.source.clone(),
            app.sink.clone(),
            app.facade_pending_status.clone(),
        )
        .await
        {
            Ok(handle) => handle,
            Err(CnxError::InvalidInput(msg))
                if msg.contains("fs-meta api bind failed: Operation not permitted") =>
            {
                return;
            }
            Err(err) => panic!("spawn active facade: {err}"),
        };
        *app.api_task.lock().await = Some(FacadeActivation {
            generation: 1,
            resource_ids: vec!["single-app-listener".to_string()],
            handle: existing,
        });

        app.apply_facade_activate(
            FacadeRuntimeUnit::Facade,
            ROUTE_KEY_FACADE_CONTROL,
            2,
            &[capanix_route_proto::BoundScope {
                scope_id: "test-root".to_string(),
                resource_ids: vec!["single-app-listener".to_string()],
            }],
        )
        .await
        .expect("apply facade activate");

        let waiting = facade_pending_status(&app);
        assert_eq!(waiting.reason, FacadePendingReason::AwaitingRuntimeExposure);
        assert_eq!(waiting.retry_attempts, 0);

        app.on_control_frame(&[trusted_exposure_confirmed_envelope(
            execution_units::FACADE_RUNTIME_UNIT_ID,
            2,
        )])
        .await
        .expect("handle exposure confirmed");

        let after_confirm = facade_pending_status(&app);
        assert_eq!(
            after_confirm.reason,
            FacadePendingReason::RetryingAfterError
        );
        assert_eq!(after_confirm.retry_attempts, 1);
        assert!(
            after_confirm
                .last_error
                .as_deref()
                .is_some_and(|msg| msg.contains("fs-meta api bind failed")),
            "expected bind failure in pending status: {:?}",
            after_confirm.last_error
        );

        tokio::time::sleep(Duration::from_millis(350)).await;
        let without_tick = facade_pending_status(&app);
        assert_eq!(
            without_tick.retry_attempts, 1,
            "pending retry attempts must stay idle until another runtime pulse arrives"
        );

        app.on_control_frame(&[tick_envelope(execution_units::FACADE_RUNTIME_UNIT_ID, 2)])
            .await
            .expect("handle worker tick");

        let after_tick = facade_pending_status(&app);
        assert_eq!(after_tick.reason, FacadePendingReason::RetryingAfterError);
        assert_eq!(after_tick.retry_attempts, 2);
        app.close().await.expect("close fs-meta app");
    }

    #[tokio::test]
    async fn status_reports_facade_pending_retry_diagnostics() {
        let tmp = tempdir().expect("create temp dir");
        let bind_addr = reserve_bind_addr();
        let (passwd_path, shadow_path) = write_auth_files(&tmp);
        let cfg = FSMetaConfig {
            source: SourceConfig {
                roots: vec![source::config::RootSpec::new("test-root", tmp.path())],
                host_object_grants: vec![granted_mount_root("single-app-node::root-1", tmp.path())],
                ..in_process_source_config()
            },
            api: api::ApiConfig {
                enabled: true,
                facade_resource_id: "single-app-listener".to_string(),
                local_listener_resources: vec![api::config::ApiListenerResource {
                    resource_id: "single-app-listener".to_string(),
                    bind_addr: bind_addr.clone(),
                }],
                auth: api::ApiAuthConfig {
                    passwd_path,
                    shadow_path,
                    ..api::ApiAuthConfig::default()
                },
            },
        };
        let app = FSMetaApp::with_boundaries(
            cfg,
            NodeId("single-app-node".into()),
            Some(Arc::new(NoopBoundary)),
        )
        .expect("init app");
        let existing = match api::spawn(
            app.config
                .api
                .resolve_for_candidate_ids(&["single-app-listener".to_string()])
                .expect("resolve facade config"),
            app.node_id.clone(),
            app.runtime_boundary.clone(),
            app.source.clone(),
            app.sink.clone(),
            app.facade_pending_status.clone(),
        )
        .await
        {
            Ok(handle) => handle,
            Err(CnxError::InvalidInput(msg))
                if msg.contains("fs-meta api bind failed: Operation not permitted") =>
            {
                return;
            }
            Err(err) => panic!("spawn active facade: {err}"),
        };
        *app.api_task.lock().await = Some(FacadeActivation {
            generation: 1,
            resource_ids: vec!["single-app-listener".to_string()],
            handle: existing,
        });

        app.apply_facade_activate(
            FacadeRuntimeUnit::Facade,
            ROUTE_KEY_FACADE_CONTROL,
            2,
            &[capanix_route_proto::BoundScope {
                scope_id: "test-root".to_string(),
                resource_ids: vec!["single-app-listener".to_string()],
            }],
        )
        .await
        .expect("apply facade activate");
        app.on_control_frame(&[trusted_exposure_confirmed_envelope(
            execution_units::FACADE_RUNTIME_UNIT_ID,
            2,
        )])
        .await
        .expect("handle exposure confirmed");

        let client = Client::new();
        let login = client
            .post(format!("http://{bind_addr}/api/fs-meta/v1/session/login"))
            .json(&json!({"username":"admin","password":"admin"}))
            .send()
            .await
            .expect("login request");
        assert!(
            login.status().is_success(),
            "login failed: {}",
            login.status()
        );
        let login_body: serde_json::Value = login.json().await.expect("decode login");
        let token = login_body["token"].as_str().expect("token");
        let status = client
            .get(format!("http://{bind_addr}/api/fs-meta/v1/status"))
            .bearer_auth(token)
            .send()
            .await
            .expect("status request");
        assert!(
            status.status().is_success(),
            "status failed: {}",
            status.status()
        );
        let status_body: serde_json::Value = status.json().await.expect("decode status");
        assert_eq!(
            status_body["facade"]["pending"]["reason"],
            serde_json::Value::String("retrying_after_error".to_string())
        );
        assert_eq!(status_body["facade"]["pending"]["generation"], 2);
        assert_eq!(
            status_body["facade"]["pending"]["runtime_exposure_confirmed"],
            serde_json::Value::Bool(true)
        );
        assert!(
            status_body["facade"]["pending"]["retry_attempts"]
                .as_u64()
                .is_some_and(|attempts| attempts >= 1)
        );
        assert!(
            status_body["facade"]["pending"]["last_error"]
                .as_str()
                .is_some_and(|msg| msg.contains("fs-meta api bind failed"))
        );
        app.close().await.expect("close fs-meta app");
    }

    #[tokio::test]
    async fn runtime_managed_listener_only_facade_requires_generation_scoped_group_evidence() {
        let tmp = tempdir().expect("create temp dir");
        let root_a = tmp.path().join("root-a");
        fs::create_dir_all(&root_a).expect("create root-a");
        fs::write(root_a.join("ready.txt"), "ready\n").expect("seed root-a");
        let (passwd_path, shadow_path) = write_auth_files(&tmp);
        let cfg = FSMetaConfig {
            source: SourceConfig {
                roots: vec![source::config::RootSpec::new("root-a", &root_a)],
                host_object_grants: vec![granted_mount_root("single-app-node::root-a-1", &root_a)],
                ..in_process_source_config()
            },
            api: api::ApiConfig {
                enabled: true,
                facade_resource_id: "single-app-listener".to_string(),
                local_listener_resources: vec![api::config::ApiListenerResource {
                    resource_id: "single-app-listener".to_string(),
                    bind_addr: "127.0.0.1:0".to_string(),
                }],
                auth: api::ApiAuthConfig {
                    passwd_path,
                    shadow_path,
                    ..api::ApiAuthConfig::default()
                },
            },
        };
        let app = FSMetaApp::with_boundaries(
            cfg,
            NodeId("single-app-node".into()),
            Some(Arc::new(NoopBoundary)),
        )
        .expect("init app");

        let pending = PendingFacadeActivation {
            route_key: ROUTE_KEY_FACADE_CONTROL.to_string(),
            generation: 2,
            resource_ids: vec!["single-app-listener".to_string()],
            bound_scopes: vec![capanix_route_proto::BoundScope {
                scope_id: "single-app-listener".to_string(),
                resource_ids: vec!["single-app-listener".to_string()],
            }],
            group_ids: Vec::new(),
            runtime_managed: true,
            runtime_exposure_confirmed: false,
            resolved: app
                .config
                .api
                .resolve_for_candidate_ids(&["single-app-listener".to_string()])
                .expect("resolve facade config"),
        };
        assert!(
            !FSMetaApp::observation_eligible_for(app.source.as_ref(), app.sink.as_ref(), &pending)
                .expect("evaluate observation eligibility"),
            "runtime-managed facade gating must fail-closed when current generation group evidence is missing"
        );
        app.close().await.expect("close app");
    }

    #[tokio::test]
    async fn runtime_managed_cold_start_facade_starts_before_generation_scoped_group_evidence() {
        let tmp = tempdir().expect("create temp dir");
        let root_a = tmp.path().join("root-a");
        fs::create_dir_all(&root_a).expect("create root-a");
        fs::write(root_a.join("ready.txt"), "ready\n").expect("seed root-a");
        let (passwd_path, shadow_path) = write_auth_files(&tmp);
        let cfg = FSMetaConfig {
            source: SourceConfig {
                roots: vec![source::config::RootSpec::new("root-a", &root_a)],
                host_object_grants: vec![granted_mount_root("single-app-node::root-a-1", &root_a)],
                ..in_process_source_config()
            },
            api: api::ApiConfig {
                enabled: true,
                facade_resource_id: "single-app-listener".to_string(),
                local_listener_resources: vec![api::config::ApiListenerResource {
                    resource_id: "single-app-listener".to_string(),
                    bind_addr: "127.0.0.1:0".to_string(),
                }],
                auth: api::ApiAuthConfig {
                    passwd_path,
                    shadow_path,
                    ..api::ApiAuthConfig::default()
                },
            },
        };
        let app = FSMetaApp::with_boundaries(
            cfg,
            NodeId("single-app-node".into()),
            Some(Arc::new(NoopBoundary)),
        )
        .expect("init app");

        let pending = PendingFacadeActivation {
            route_key: ROUTE_KEY_FACADE_CONTROL.to_string(),
            generation: 2,
            resource_ids: vec!["single-app-listener".to_string()],
            bound_scopes: vec![capanix_route_proto::BoundScope {
                scope_id: "single-app-listener".to_string(),
                resource_ids: vec!["single-app-listener".to_string()],
            }],
            group_ids: Vec::new(),
            runtime_managed: true,
            runtime_exposure_confirmed: true,
            resolved: app
                .config
                .api
                .resolve_for_candidate_ids(&["single-app-listener".to_string()])
                .expect("resolve facade config"),
        };
        assert!(
            !FSMetaApp::observation_eligible_for(app.source.as_ref(), app.sink.as_ref(), &pending)
                .expect("evaluate observation eligibility"),
            "cold start fixture intentionally lacks generation-scoped observation evidence"
        );
        *app.pending_facade.lock().await = Some(pending);

        assert!(
            app.try_spawn_pending_facade()
                .await
                .expect("cold start facade spawn"),
            "cold-start runtime-managed facade should still come up"
        );
        assert!(
            app.api_task.lock().await.is_some(),
            "cold-start facade activation must own an HTTP task"
        );
        app.close().await.expect("close app");
    }

    #[tokio::test]
    async fn observation_eligibility_ignores_unbound_sink_overflow_for_listener_only_facade() {
        let tmp = tempdir().expect("create temp dir");
        let root_a = tmp.path().join("root-a");
        let root_b = tmp.path().join("root-b");
        fs::create_dir_all(&root_a).expect("create root-a");
        fs::create_dir_all(&root_b).expect("create root-b");
        let (passwd_path, shadow_path) = write_auth_files(&tmp);
        let cfg = FSMetaConfig {
            source: SourceConfig {
                roots: vec![
                    source::config::RootSpec::new("root-a", &root_a),
                    source::config::RootSpec::new("root-b", &root_b),
                ],
                host_object_grants: vec![
                    granted_mount_root("single-app-node::root-a-1", &root_a),
                    granted_mount_root("single-app-node::root-b-1", &root_b),
                ],
                ..in_process_source_config()
            },
            api: api::ApiConfig {
                enabled: true,
                facade_resource_id: "single-app-listener".to_string(),
                local_listener_resources: vec![api::config::ApiListenerResource {
                    resource_id: "single-app-listener".to_string(),
                    bind_addr: "127.0.0.1:0".to_string(),
                }],
                auth: api::ApiAuthConfig {
                    passwd_path,
                    shadow_path,
                    ..api::ApiAuthConfig::default()
                },
            },
        };
        let app = FSMetaApp::new(cfg, NodeId("single-app-node".into())).expect("init app");

        app.on_control_frame(&[activate_envelope_with_generation_and_scopes(
            "runtime.exec.source",
            "root-a",
            &[],
            2,
        )])
        .await
        .expect("activate scoped source generation");

        let source_groups = app
            .source
            .status_snapshot()
            .expect("source status")
            .concrete_roots
            .into_iter()
            .filter(|root| root.active)
            .map(|root| root.logical_root_id)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            source_groups,
            std::collections::BTreeSet::from(["root-a".to_string()])
        );

        app.sink
            .send(&[
                mk_source_event("single-app-node::root-a-1", b"/ready.txt", b"ready.txt", 10),
                mk_control_event(
                    "single-app-node::root-a-1",
                    ControlEvent::EpochEnd {
                        epoch_id: 0,
                        epoch_type: capanix_host_fs_types::EpochType::Audit,
                    },
                    11,
                ),
                mk_control_event("single-app-node::root-b-1", ControlEvent::WatchOverflow, 11),
            ])
            .await
            .expect("seed sink state");

        let sink_status = app.sink.status_snapshot().expect("sink status");
        assert!(
            sink_status
                .groups
                .iter()
                .any(|group| group.group_id == "root-b" && group.overflow_pending_audit),
            "unbound group should retain overflow evidence in sink status"
        );

        let pending = PendingFacadeActivation {
            route_key: ROUTE_KEY_FACADE_CONTROL.to_string(),
            generation: 2,
            resource_ids: vec!["single-app-listener".to_string()],
            bound_scopes: vec![capanix_route_proto::BoundScope {
                scope_id: "root-a".to_string(),
                resource_ids: vec!["single-app-listener".to_string()],
            }],
            group_ids: Vec::new(),
            runtime_managed: false,
            runtime_exposure_confirmed: true,
            resolved: app
                .config
                .api
                .resolve_for_candidate_ids(&["single-app-listener".to_string()])
                .expect("resolve facade config"),
        };
        assert!(
            FSMetaApp::observation_eligible_for(app.source.as_ref(), app.sink.as_ref(), &pending)
                .expect("evaluate observation eligibility"),
            "listener-only facade gating should use runtime-scoped source/sink groups instead of unrelated overflow on unbound groups"
        );
    }

    #[tokio::test]
    async fn on_control_frame_rejects_unknown_unit() {
        let tmp = tempdir().expect("create temp dir");
        let cfg = FSMetaConfig {
            source: SourceConfig {
                roots: vec![source::config::RootSpec::new("test-root", tmp.path())],
                ..in_process_source_config()
            },
            api: api::ApiConfig::default(),
        };
        let app = FSMetaApp::new(cfg, NodeId("single-app-node".into())).expect("init app");
        let err = app
            .on_control_frame(&[activate_envelope("runtime.exec.unknown")])
            .await
            .expect_err("unknown unit must be rejected");
        assert!(matches!(
            err,
            CnxError::NotSupported(msg) if msg.contains("unsupported worker_id")
        ));
    }

    #[tokio::test]
    async fn on_control_frame_rejects_batch_atomically_on_unknown_unit() {
        let tmp = tempdir().expect("create temp dir");
        let cfg = FSMetaConfig {
            source: SourceConfig {
                roots: vec![source::config::RootSpec::new("test-root", tmp.path())],
                ..in_process_source_config()
            },
            api: api::ApiConfig::default(),
        };
        let app = FSMetaApp::new(cfg, NodeId("single-app-node".into())).expect("init app");
        assert_eq!(
            app.source
                .host_object_grants_version_snapshot()
                .expect("snapshot version"),
            0
        );

        let err = app
            .on_control_frame(&[
                host_object_grants_changed_envelope(1, tmp.path().to_string_lossy().as_ref()),
                activate_envelope("runtime.exec.unknown"),
            ])
            .await
            .expect_err("batch with unknown unit must fail atomically");
        assert!(matches!(
            err,
            CnxError::NotSupported(msg) if msg.contains("unsupported worker_id")
        ));
        assert_eq!(
            app.source
                .host_object_grants_version_snapshot()
                .expect("snapshot version"),
            0,
            "batch failure must not apply partial shared control effects"
        );
    }

    #[tokio::test]
    async fn single_app_initial_scan_materializes_tree() {
        let tmp = tempdir().expect("create temp dir");
        fs::write(tmp.path().join("hello.txt"), b"world").expect("write seed file");
        let (passwd_path, shadow_path) = write_auth_files(&tmp);

        let cfg = FSMetaConfig {
            source: SourceConfig {
                roots: vec![source::config::RootSpec::new("test-root", tmp.path())],
                host_object_grants: vec![source::config::GrantedMountRoot {
                    object_ref: "single-app-node::root-1".to_string(),
                    host_ref: "single-app-node".to_string(),
                    host_ip: "127.0.0.1".to_string(),
                    host_name: Some("single-app-node".to_string()),
                    site: None,
                    zone: None,
                    host_labels: Default::default(),
                    mount_point: tmp.path().to_path_buf(),
                    fs_source: tmp.path().display().to_string(),
                    fs_type: "nfs".to_string(),
                    mount_options: Vec::new(),
                    interfaces: vec!["posix-fs".to_string(), "inotify".to_string()],
                    active: true,
                }],
                scan_workers: 1,
                batch_size: 128,
                max_scan_events: 4096,
                ..in_process_source_config()
            },
            api: api::ApiConfig {
                enabled: true,
                facade_resource_id: "single-app-listener".to_string(),
                local_listener_resources: vec![api::config::ApiListenerResource {
                    resource_id: "single-app-listener".to_string(),
                    bind_addr: "127.0.0.1:0".to_string(),
                }],
                auth: api::ApiAuthConfig {
                    passwd_path,
                    shadow_path,
                    ..api::ApiAuthConfig::default()
                },
            },
        };

        let app = FSMetaApp::new(cfg, NodeId("single-app-node".into())).expect("init app");
        if cfg!(target_os = "linux") {
            match app.start().await {
                Ok(()) => {}
                Err(CnxError::InvalidInput(msg))
                    if msg.contains("fs-meta api bind failed: Operation not permitted") =>
                {
                    return;
                }
                Err(err) => panic!("start app: {err}"),
            }
        } else {
            let err = app.start().await.expect_err("non-linux should fail fast");
            assert!(matches!(err, CnxError::NotSupported(_)));
            return;
        }

        tokio::time::sleep(Duration::from_millis(300)).await;

        let query = app
            .query_tree(&query::InternalQueryRequest::materialized(
                query::QueryOp::Tree,
                query::QueryScope::default(),
                Some(query::TreeQueryOptions::default()),
            ))
            .await
            .expect("query tree");
        let paths: Vec<Vec<u8>> = query
            .values()
            .flat_map(|group| group.entries.iter().map(|entry| entry.path.clone()))
            .collect();
        assert!(paths.iter().any(|path| path == b"/hello.txt"));

        app.close().await.expect("close app");
    }

    #[test]
    fn parses_manifest_config_multi_roots() {
        let cfg = std::collections::HashMap::from([(
            "roots".to_string(),
            ConfigValue::Array(vec![
                {
                    let mut row = match root_entry_with_id("nfs1-a", "/mnt/nfs1") {
                        ConfigValue::Map(map) => map,
                        _ => unreachable!(),
                    };
                    row.insert("watch".to_string(), ConfigValue::Bool(true));
                    row.insert("scan".to_string(), ConfigValue::Bool(true));
                    ConfigValue::Map(row)
                },
                root_entry_with_id("nfs2-b", "/mnt/nfs2"),
            ]),
        )]);
        let parsed = FSMetaConfig::from_manifest_config(&cfg).expect("parse roots config");
        assert_eq!(parsed.source.roots.len(), 2);
        assert_eq!(parsed.source.roots[0].id, "nfs1-a");
    }

    #[test]
    fn runtime_local_host_ref_from_runtime_metadata() {
        let cfg = std::collections::HashMap::from([(
            "__cnx_runtime".to_string(),
            ConfigValue::Map(std::collections::HashMap::from([(
                "local_host_ref".to_string(),
                ConfigValue::String("host://capanix-node-3".to_string()),
            )])),
        )]);
        let resolved = FSMetaRuntimeApp::runtime_local_host_ref(&cfg)
            .expect("local_host_ref should resolve from runtime metadata");
        assert_eq!(resolved.0, "host://capanix-node-3");
    }

    #[test]
    fn runtime_local_host_ref_rejects_empty_value() {
        let cfg = std::collections::HashMap::from([(
            "__cnx_runtime".to_string(),
            ConfigValue::Map(std::collections::HashMap::from([(
                "local_host_ref".to_string(),
                ConfigValue::String("   ".to_string()),
            )])),
        )]);
        assert!(
            FSMetaRuntimeApp::runtime_local_host_ref(&cfg).is_none(),
            "blank local_host_ref must not be accepted"
        );
    }

    #[test]
    fn required_runtime_local_host_ref_fails_closed_when_missing() {
        let cfg = std::collections::HashMap::from([(
            "__cnx_runtime".to_string(),
            ConfigValue::Map(std::collections::HashMap::new()),
        )]);
        let err = FSMetaRuntimeApp::required_runtime_local_host_ref(&cfg)
            .expect_err("missing local_host_ref must fail closed");
        assert!(
            err.to_string()
                .contains("__cnx_runtime.local_host_ref is required"),
            "error should explain required runtime local_host_ref"
        );
    }

    #[test]
    fn parses_source_tuning_fields_with_bounds() {
        let cfg = std::collections::HashMap::from([
            (
                "roots".to_string(),
                ConfigValue::Array(vec![root_entry("/mnt/nfs1")]),
            ),
            ("scan_workers".to_string(), ConfigValue::Int(999)),
            ("audit_interval_ms".to_string(), ConfigValue::Int(1)),
            ("throttle_interval_ms".to_string(), ConfigValue::Int(1)),
            ("sink_tombstone_ttl_ms".to_string(), ConfigValue::Int(1)),
            (
                "sink_tombstone_tolerance_us".to_string(),
                ConfigValue::Int(i64::MAX),
            ),
        ]);
        let parsed = FSMetaConfig::from_manifest_config(&cfg).expect("parse source tuning fields");
        assert_eq!(parsed.source.scan_workers, 16);
        assert_eq!(parsed.source.audit_interval, Duration::from_millis(5_000));
        assert_eq!(parsed.source.throttle_interval, Duration::from_millis(50));
        assert_eq!(
            parsed.source.sink_tombstone_ttl,
            Duration::from_millis(1_000)
        );
        assert_eq!(parsed.source.sink_tombstone_tolerance_us, 10_000_000);
    }

    #[test]
    fn rejects_removed_unit_authority_state_dir() {
        let cfg = std::collections::HashMap::from([
            (
                "roots".to_string(),
                ConfigValue::Array(vec![root_entry("/mnt/nfs1")]),
            ),
            (
                "unit_authority_state_dir".to_string(),
                ConfigValue::String("relative/statecell".to_string()),
            ),
        ]);
        let err = FSMetaConfig::from_manifest_config(&cfg)
            .expect_err("removed unit_authority_state_dir must fail");
        assert!(
            err.to_string()
                .contains("unit_authority_state_carrier/unit_authority_state_dir are removed")
        );
    }

    #[test]
    fn rejects_removed_unit_authority_state_carrier() {
        let cfg = std::collections::HashMap::from([
            (
                "roots".to_string(),
                ConfigValue::Array(vec![root_entry("/mnt/nfs1")]),
            ),
            (
                "unit_authority_state_carrier".to_string(),
                ConfigValue::String("external".to_string()),
            ),
        ]);
        let err = FSMetaConfig::from_manifest_config(&cfg)
            .expect_err("removed unit_authority_state_carrier must fail");
        assert!(
            err.to_string()
                .contains("unit_authority_state_carrier/unit_authority_state_dir are removed")
        );
    }

    #[test]
    fn parses_sink_execution_mode_worker_process() {
        let cfg = std::collections::HashMap::from([
            (
                "roots".to_string(),
                ConfigValue::Array(vec![root_entry("/mnt/nfs1")]),
            ),
            (
                "sink_execution_mode".to_string(),
                ConfigValue::String("worker-process".to_string()),
            ),
            (
                "sink_worker_bin_path".to_string(),
                ConfigValue::String("/usr/local/bin/fs_meta_sink_worker".to_string()),
            ),
        ]);
        let parsed =
            FSMetaConfig::from_manifest_config(&cfg).expect("parse worker-process sink mode");
        assert_eq!(
            parsed.source.sink_execution_mode,
            SinkExecutionMode::WorkerProcess
        );
        assert_eq!(
            parsed.source.sink_worker_bin_path,
            Some(std::path::PathBuf::from(
                "/usr/local/bin/fs_meta_sink_worker"
            ))
        );
    }

    #[test]
    fn parses_source_execution_mode_worker_process() {
        let cfg = std::collections::HashMap::from([
            (
                "roots".to_string(),
                ConfigValue::Array(vec![root_entry("/mnt/nfs1")]),
            ),
            (
                "source_execution_mode".to_string(),
                ConfigValue::String("worker-process".to_string()),
            ),
            (
                "source_worker_bin_path".to_string(),
                ConfigValue::String("/usr/local/bin/fs_meta_source_worker".to_string()),
            ),
        ]);
        let parsed =
            FSMetaConfig::from_manifest_config(&cfg).expect("parse worker-process source mode");
        assert_eq!(
            parsed.source.source_execution_mode,
            SourceExecutionMode::WorkerProcess
        );
        assert_eq!(
            parsed.source.source_worker_bin_path,
            Some(std::path::PathBuf::from(
                "/usr/local/bin/fs_meta_source_worker"
            ))
        );
    }

    #[test]
    fn rejects_unknown_sink_execution_mode() {
        let cfg = std::collections::HashMap::from([
            (
                "roots".to_string(),
                ConfigValue::Array(vec![root_entry("/mnt/nfs1")]),
            ),
            (
                "sink_execution_mode".to_string(),
                ConfigValue::String("sidecar".to_string()),
            ),
        ]);
        let err = FSMetaConfig::from_manifest_config(&cfg)
            .expect_err("unsupported sink execution mode must fail");
        assert!(err.to_string().contains("unsupported sink_execution_mode"));
    }

    #[test]
    fn rejects_unknown_source_execution_mode() {
        let cfg = std::collections::HashMap::from([
            (
                "roots".to_string(),
                ConfigValue::Array(vec![root_entry("/mnt/nfs1")]),
            ),
            (
                "source_execution_mode".to_string(),
                ConfigValue::String("sidecar".to_string()),
            ),
        ]);
        let err = FSMetaConfig::from_manifest_config(&cfg)
            .expect_err("unsupported source execution mode must fail");
        assert!(
            err.to_string()
                .contains("unsupported source_execution_mode")
        );
    }

    #[test]
    fn defaults_sink_worker_process_mode_to_standard_worker_binary() {
        let cfg = std::collections::HashMap::from([
            (
                "roots".to_string(),
                ConfigValue::Array(vec![root_entry("/mnt/nfs1")]),
            ),
            (
                "sink_execution_mode".to_string(),
                ConfigValue::String("worker-process".to_string()),
            ),
        ]);
        let parsed = FSMetaConfig::from_manifest_config(&cfg)
            .expect("worker-process mode should infer sink worker binary path");
        assert_eq!(
            parsed.source.sink_execution_mode,
            SinkExecutionMode::WorkerProcess
        );
        assert!(
            parsed
                .source
                .sink_worker_bin_path
                .as_ref()
                .is_some_and(|path| path.ends_with("fs_meta_sink_worker")),
            "sink worker baseline should infer the standard worker binary path"
        );
    }

    #[test]
    fn defaults_source_worker_process_mode_to_standard_worker_binary() {
        let cfg = std::collections::HashMap::from([
            (
                "roots".to_string(),
                ConfigValue::Array(vec![root_entry("/mnt/nfs1")]),
            ),
            (
                "source_execution_mode".to_string(),
                ConfigValue::String("worker-process".to_string()),
            ),
        ]);
        let parsed = FSMetaConfig::from_manifest_config(&cfg)
            .expect("worker-process mode should infer source worker binary path");
        assert_eq!(
            parsed.source.source_execution_mode,
            SourceExecutionMode::WorkerProcess
        );
        assert!(
            parsed
                .source
                .source_worker_bin_path
                .as_ref()
                .is_some_and(|path| path.ends_with("fs_meta_source_worker")),
            "source worker baseline should infer the standard worker binary path"
        );
    }

    #[test]
    fn parses_worker_oriented_mode_config_shape() {
        let cfg = std::collections::HashMap::from([
            (
                "roots".to_string(),
                ConfigValue::Array(vec![root_entry("/mnt/nfs1")]),
            ),
            (
                "workers".to_string(),
                ConfigValue::Map(std::collections::HashMap::from([
                    ("facade".to_string(), worker_cfg("embedded")),
                    (
                        "source".to_string(),
                        worker_cfg_with_bin("external", "/usr/local/bin/fs_meta_source_worker"),
                    ),
                    ("scan".to_string(), worker_cfg("external")),
                    ("sink".to_string(), worker_cfg("embedded")),
                ])),
            ),
        ]);
        let parsed =
            FSMetaConfig::from_manifest_config(&cfg).expect("parse worker-oriented config");
        assert_eq!(
            parsed.source.source_execution_mode,
            SourceExecutionMode::WorkerProcess
        );
        assert_eq!(
            parsed.source.sink_execution_mode,
            SinkExecutionMode::InProcess
        );
        assert_eq!(
            parsed.source.source_worker_bin_path,
            Some(std::path::PathBuf::from(
                "/usr/local/bin/fs_meta_source_worker"
            ))
        );
    }

    #[test]
    fn scan_worker_binary_path_can_back_shared_source_scan_realization() {
        let cfg = std::collections::HashMap::from([
            (
                "roots".to_string(),
                ConfigValue::Array(vec![root_entry("/mnt/nfs1")]),
            ),
            (
                "workers".to_string(),
                ConfigValue::Map(std::collections::HashMap::from([(
                    "scan".to_string(),
                    worker_cfg_with_bin("external", "/usr/local/bin/fs_meta_scan_worker"),
                )])),
            ),
        ]);
        let parsed = FSMetaConfig::from_manifest_config(&cfg)
            .expect("scan worker path should feed the shared source/scan realization");
        assert_eq!(
            parsed.source.source_execution_mode,
            SourceExecutionMode::WorkerProcess
        );
        assert_eq!(
            parsed.source.source_worker_bin_path,
            Some(std::path::PathBuf::from(
                "/usr/local/bin/fs_meta_scan_worker"
            ))
        );
    }

    #[test]
    fn rejects_external_facade_worker_mode() {
        let cfg = std::collections::HashMap::from([
            (
                "roots".to_string(),
                ConfigValue::Array(vec![root_entry("/mnt/nfs1")]),
            ),
            (
                "workers".to_string(),
                ConfigValue::Map(std::collections::HashMap::from([(
                    "facade".to_string(),
                    worker_cfg("external"),
                )])),
            ),
        ]);
        let err = FSMetaConfig::from_manifest_config(&cfg)
            .expect_err("facade-worker cannot yet move to external mode");
        assert!(
            err.to_string()
                .contains("workers.facade.mode=external is not supported")
        );
    }

    #[test]
    fn rejects_mismatched_source_and_scan_worker_modes() {
        let cfg = std::collections::HashMap::from([
            (
                "roots".to_string(),
                ConfigValue::Array(vec![root_entry("/mnt/nfs1")]),
            ),
            (
                "workers".to_string(),
                ConfigValue::Map(std::collections::HashMap::from([
                    ("source".to_string(), worker_cfg("embedded")),
                    ("scan".to_string(), worker_cfg("external")),
                ])),
            ),
        ]);
        let err = FSMetaConfig::from_manifest_config(&cfg)
            .expect_err("source/scan worker mode mismatch must fail");
        assert!(err.to_string().contains(
            "workers.source.mode and workers.scan.mode must match while source-worker and scan-worker still share one realization"
        ));
    }

    #[test]
    fn rejects_mismatched_source_and_scan_worker_binary_paths() {
        let cfg = std::collections::HashMap::from([
            (
                "roots".to_string(),
                ConfigValue::Array(vec![root_entry("/mnt/nfs1")]),
            ),
            (
                "workers".to_string(),
                ConfigValue::Map(std::collections::HashMap::from([
                    (
                        "source".to_string(),
                        worker_cfg_with_bin("external", "/usr/local/bin/fs_meta_source_worker"),
                    ),
                    (
                        "scan".to_string(),
                        worker_cfg_with_bin("external", "/usr/local/bin/fs_meta_scan_worker"),
                    ),
                ])),
            ),
        ]);
        let err = FSMetaConfig::from_manifest_config(&cfg)
            .expect_err("source/scan worker binary mismatch must fail");
        assert!(err.to_string().contains(
            "workers.source.binary_path and workers.scan.binary_path must match while source-worker and scan-worker still share one realization"
        ));
    }

    #[test]
    fn rejects_root_source_locator_field() {
        let cfg = std::collections::HashMap::from([(
            "roots".to_string(),
            ConfigValue::Array(vec![{
                let mut row = match root_entry_with_id("nfs1", "/mnt/nfs1") {
                    ConfigValue::Map(map) => map,
                    _ => unreachable!(),
                };
                row.insert(
                    "source_locator".to_string(),
                    ConfigValue::String("10.0.0.11".to_string()),
                );
                ConfigValue::Map(row)
            }]),
        )]);
        let err = FSMetaConfig::from_manifest_config(&cfg).expect_err("must reject source_locator");
        assert!(err.to_string().contains("source_locator is forbidden"));
    }

    #[test]
    fn derives_root_id_from_selector_mount_point_when_missing() {
        let cfg = std::collections::HashMap::from([(
            "roots".to_string(),
            ConfigValue::Array(vec![root_entry("/mnt/nfs1")]),
        )]);
        let parsed = FSMetaConfig::from_manifest_config(&cfg).expect("parse roots config");
        assert_eq!(parsed.source.roots[0].id, "mnt-nfs1");
    }

    #[test]
    fn rejects_root_path_field() {
        let cfg = std::collections::HashMap::from([(
            "root_path".to_string(),
            ConfigValue::String("/mnt/nfs1".to_string()),
        )]);
        let err = FSMetaConfig::from_manifest_config(&cfg).expect_err("must reject root_path");
        assert!(err.to_string().contains("root_path is forbidden"));
    }

    #[test]
    fn allows_missing_roots_field_as_empty_deployed_state() {
        let cfg = std::collections::HashMap::new();
        let parsed = FSMetaConfig::from_manifest_config(&cfg)
            .expect("missing roots should parse as empty deployed state");
        assert!(parsed.source.roots.is_empty());
    }

    #[test]
    fn parses_api_config_auth_fields() {
        let cfg = std::collections::HashMap::from([
            (
                "roots".to_string(),
                ConfigValue::Array(vec![root_entry("/mnt/nfs1")]),
            ),
            (
                "api".to_string(),
                ConfigValue::Map(std::collections::HashMap::from([
                    ("enabled".to_string(), ConfigValue::Bool(true)),
                    (
                        "facade_resource_id".to_string(),
                        ConfigValue::String("fs-meta-tcp-listener".to_string()),
                    ),
                    (
                        "auth".to_string(),
                        ConfigValue::Map(std::collections::HashMap::from([
                            (
                                "passwd_path".to_string(),
                                ConfigValue::String("/tmp/fs-meta.passwd".to_string()),
                            ),
                            (
                                "shadow_path".to_string(),
                                ConfigValue::String("/tmp/fs-meta.shadow".to_string()),
                            ),
                            ("session_ttl_secs".to_string(), ConfigValue::Int(900)),
                            (
                                "query_keys_path".to_string(),
                                ConfigValue::String("/tmp/fs-meta.query-keys.json".to_string()),
                            ),
                            (
                                "management_group".to_string(),
                                ConfigValue::String("fsmeta_management".to_string()),
                            ),
                        ])),
                    ),
                ])),
            ),
        ]);
        let cfg = {
            let mut cfg = cfg;
            cfg.insert(
                "__cnx_runtime".to_string(),
                ConfigValue::Map(std::collections::HashMap::from([
                    (
                        "local_host_ref".to_string(),
                        ConfigValue::String("host://capanix-node-3".to_string()),
                    ),
                    (
                        "announced_resources".to_string(),
                        ConfigValue::Array(vec![ConfigValue::Map(
                            std::collections::HashMap::from([
                                (
                                    "resource_id".to_string(),
                                    ConfigValue::String("fs-meta-tcp-listener".to_string()),
                                ),
                                (
                                    "node_id".to_string(),
                                    ConfigValue::String("host://capanix-node-3".to_string()),
                                ),
                                (
                                    "resource_kind".to_string(),
                                    ConfigValue::String("tcp_listener".to_string()),
                                ),
                                (
                                    "bind_addr".to_string(),
                                    ConfigValue::String("127.0.0.1:18080".to_string()),
                                ),
                            ]),
                        )]),
                    ),
                ])),
            );
            cfg
        };
        let parsed = FSMetaConfig::from_manifest_config(&cfg).expect("parse api config");
        assert!(parsed.api.enabled);
        assert_eq!(parsed.api.facade_resource_id, "fs-meta-tcp-listener");
        assert_eq!(parsed.api.local_listener_resources.len(), 1);
        assert_eq!(
            parsed.api.local_listener_resources[0].bind_addr,
            "127.0.0.1:18080"
        );
        assert_eq!(parsed.api.auth.session_ttl_secs, 900);
        assert_eq!(
            parsed.api.auth.passwd_path,
            std::path::PathBuf::from("/tmp/fs-meta.passwd")
        );
        assert_eq!(
            parsed.api.auth.query_keys_path,
            std::path::PathBuf::from("/tmp/fs-meta.query-keys.json")
        );
        assert_eq!(parsed.api.auth.management_group, "fsmeta_management");
    }

    #[test]
    fn rejects_api_disabled_in_manifest_config() {
        let cfg = std::collections::HashMap::from([
            (
                "roots".to_string(),
                ConfigValue::Array(vec![root_entry("/mnt/nfs1")]),
            ),
            (
                "api".to_string(),
                ConfigValue::Map(std::collections::HashMap::from([(
                    "enabled".to_string(),
                    ConfigValue::Bool(false),
                )])),
            ),
        ]);
        let err = FSMetaConfig::from_manifest_config(&cfg).expect_err("api must be mandatory");
        assert!(
            err.to_string()
                .contains("api.enabled must be true; fs-meta management API boundary is mandatory")
        );
    }
}

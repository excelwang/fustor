use std::sync::{Arc, Mutex, MutexGuard};

use capanix_app_sdk::Event;
use capanix_app_sdk::runtime::{EventMetadata, NodeId};
use data_view::{
    ActionExecutionContext, ActionResultSubmission, CreateStreamingSubscription,
    DataViewCommitOutcome, DataViewContractError, DataViewEngine, DataViewPersistence, DataViewRef,
    GraphDeltaRecord, ProjectionMode, RebaseStreamingSubscription, RetentionOutcome,
    RetentionPolicy, StreamingRefreshSubmission, StreamingSubscription, SystemIdGenerator,
};
use data_view_normalizers::register_builtin_normalizers;
use private_discovery::{
    DataViewRevisionSelector, DiscoveryFrontier, DiscoveryFrontierStrategy,
    PrivateDataDiscoveryRequest, PrivateDiscoveryOrchestrator, ScopedDataViewProjectionOutcome,
    ScopedDataViewProjectionRequest, ScopedDataViewProjector, ScopedDiscoveryGraph,
    ScopedProjectionAccess,
};
use serde::{Deserialize, Serialize};
use source_kit::{SourceErrorPayload, encode_msgpack};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildScopedDataViewRequest {
    pub revision_selector: DataViewRevisionSelector,
    pub access: ScopedProjectionAccess,
    pub discovery_request: PrivateDataDiscoveryRequest,
    pub now_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "kebab-case")]
pub enum DataViewRuntimeRequest {
    InspectDeclaredCapability { profile_id: String, producer_id: String, schema: data_view::SchemaVersionRef },
    InspectDeclaredOutcome { profile_id: String, idempotency_key: String },
    IssueDeclaredAdmission { profile_id: String, idempotency_key: String, now_ms: u64, ttl_ms: u64 },
    SubmitDeclaredResult { submission: data_view::DeclaredResultSubmission, now_ms: u64,
        current_policy_context_digest: String, current_grant_epoch: u64 },
    GetDataViewHead,
    GetGraphDelta {
        delta_ref: String,
    },
    GetStreamingSubscription {
        subscription_id: String,
    },
    BuildScopedDataView {
        request: BuildScopedDataViewRequest,
    },
    IssueVisibleActionContext {
        request: BuildScopedDataViewRequest,
        action_id: String,
        policy_context_digest: String,
        grant_epoch: u64,
        issued_at_ms: u64,
        expires_at_ms: u64,
    },
    SubmitActionResult {
        submission: ActionResultSubmission,
        now_ms: u64,
        current_policy_context_digest: String,
    },
    CreateStreamingSubscription {
        request: CreateStreamingSubscription,
    },
    PauseStreamingSubscription {
        subscription_id: String,
    },
    RevokeStreamingSubscription {
        subscription_id: String,
    },
    RebaseStreamingSubscription {
        request: RebaseStreamingSubscription,
    },
    SubmitStreamingRefresh {
        submission: StreamingRefreshSubmission,
        now_ms: u64,
        current_policy_context_digest: String,
        current_grant_epoch: u64,
    },
    ApplyRetention {
        policy: RetentionPolicy,
    },
}

impl DataViewRuntimeRequest {
    pub fn is_query_operation(&self) -> bool {
        matches!(
            self,
            Self::GetDataViewHead
                | Self::InspectDeclaredCapability { .. }
                | Self::InspectDeclaredOutcome { .. }
                | Self::GetGraphDelta { .. }
                | Self::GetStreamingSubscription { .. }
                | Self::BuildScopedDataView { .. }
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RuntimeLane {
    Any,
    Query,
    Command,
}

impl RuntimeLane {
    fn accepts(self, request: &DataViewRuntimeRequest) -> bool {
        match self {
            Self::Any => true,
            Self::Query => request.is_query_operation(),
            Self::Command => !request.is_query_operation(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload", rename_all = "kebab-case")]
pub enum DataViewRuntimePayload {
    DeclaredCapability(Option<data_view::DeclaredResultCapability>),
    DeclaredOutcome(Option<DataViewCommitOutcome>),
    DeclaredAdmission(data_view::DeclaredResultAdmissionContext),
    DataViewRef(DataViewRef),
    GraphDelta(GraphDeltaRecord),
    ScopedDataView(ScopedDataViewProjectionOutcome),
    ActionExecutionContext(ActionExecutionContext),
    Commit(DataViewCommitOutcome),
    StreamingSubscription(StreamingSubscription),
    Retention(RetentionOutcome),
    Error(SourceErrorPayload),
}

#[derive(Clone)]
pub struct DataViewRuntimeService {
    engine: Arc<Mutex<DataViewEngine>>,
    projector: Arc<Mutex<ScopedDataViewProjector>>,
    discovery: PrivateDiscoveryOrchestrator,
    origin_id: NodeId,
}

impl DataViewRuntimeService {
    /// Owner configuration, not a public route for callers to grant themselves rights.
    pub fn register_declared_policy(&self, policy: data_view::DeclaredResultPolicy) -> Result<(), DataViewContractError> {
        let mut engine = self.engine()?;
        if !engine.normalizer_registry_mut().contains(&policy.schema) {
            engine.register_normalizer(Arc::new(data_view::DeclaredManifestNormalizer::new(policy.schema.clone())))?;
        }
        engine.register_declared_policy(policy)
    }
    pub fn new(
        data_view_id: impl Into<String>,
        origin_id: NodeId,
    ) -> Result<Self, DataViewContractError> {
        Self::from_engine(DataViewEngine::new(data_view_id)?, origin_id)
    }

    pub fn with_persistence(
        data_view_id: impl Into<String>,
        origin_id: NodeId,
        persistence: Arc<dyn DataViewPersistence>,
    ) -> Result<Self, DataViewContractError> {
        let engine = DataViewEngine::with_persistence(
            data_view_id,
            Arc::new(SystemIdGenerator),
            persistence,
        )?;
        Self::from_engine(engine, origin_id)
    }

    fn from_engine(
        mut engine: DataViewEngine,
        origin_id: NodeId,
    ) -> Result<Self, DataViewContractError> {
        register_builtin_normalizers(engine.normalizer_registry_mut())?;
        Ok(Self {
            engine: Arc::new(Mutex::new(engine)),
            projector: Arc::new(Mutex::new(ScopedDataViewProjector)),
            discovery: PrivateDiscoveryOrchestrator::new(),
            origin_id,
        })
    }

    pub fn handle_events(&self, events: Vec<Event>) -> Vec<Event> {
        events
            .into_iter()
            .map(|event| self.handle_event(event))
            .collect()
    }

    pub fn handle_query_events(&self, events: Vec<Event>) -> Vec<Event> {
        events
            .into_iter()
            .map(|event| self.handle_event_on_lane(event, RuntimeLane::Query))
            .collect()
    }

    pub fn handle_command_events(&self, events: Vec<Event>) -> Vec<Event> {
        events
            .into_iter()
            .map(|event| self.handle_event_on_lane(event, RuntimeLane::Command))
            .collect()
    }

    pub fn handle_event(&self, event: Event) -> Event {
        self.handle_event_on_lane(event, RuntimeLane::Any)
    }

    fn handle_event_on_lane(&self, event: Event, lane: RuntimeLane) -> Event {
        let correlation_id = event.metadata().correlation_id;
        let payload = match self.handle_event_payload(event.payload_bytes(), lane) {
            Ok(payload) => payload,
            Err(error) => DataViewRuntimePayload::Error(SourceErrorPayload::new(
                "data-view-request-rejected",
                error.to_string(),
                matches!(
                    error,
                    DataViewContractError::Expired
                        | DataViewContractError::RequiresRevalidation
                        | DataViewContractError::PolicyContextMismatch
                        | DataViewContractError::GrantEpochMismatch
                ),
            )),
        };
        self.reply_event(payload, correlation_id)
    }

    fn handle_event_payload(
        &self,
        payload: &[u8],
        lane: RuntimeLane,
    ) -> Result<DataViewRuntimePayload, DataViewContractError> {
        let request: DataViewRuntimeRequest = rmp_serde::from_slice(payload)
            .map_err(|_| DataViewContractError::InvalidContract("runtime request"))?;
        if !lane.accepts(&request) {
            return Err(DataViewContractError::InvalidContract(
                "runtime route operation class",
            ));
        }
        match request {
            DataViewRuntimeRequest::InspectDeclaredCapability { profile_id, producer_id, schema } => {
                Ok(DataViewRuntimePayload::DeclaredCapability(self.engine()?.inspect_declared_capability(&profile_id, &producer_id, &schema)?))
            }
            DataViewRuntimeRequest::InspectDeclaredOutcome { profile_id, idempotency_key } => {
                Ok(DataViewRuntimePayload::DeclaredOutcome(self.engine()?.inspect_declared_outcome(&profile_id, &idempotency_key)?))
            }
            DataViewRuntimeRequest::GetDataViewHead => Ok(DataViewRuntimePayload::DataViewRef(
                self.engine()?.head_ref(),
            )),
            DataViewRuntimeRequest::GetGraphDelta { delta_ref } => {
                let engine = self.engine()?;
                let delta = engine
                    .store()
                    .graph_delta(&delta_ref)
                    .cloned()
                    .ok_or(DataViewContractError::NotFound("graph delta"))?;
                Ok(DataViewRuntimePayload::GraphDelta(delta))
            }
            DataViewRuntimeRequest::GetStreamingSubscription { subscription_id } => {
                let engine = self.engine()?;
                let subscription = engine
                    .streaming_registry()
                    .subscription(&subscription_id)
                    .cloned()
                    .ok_or(DataViewContractError::NotFound("subscription"))?;
                Ok(DataViewRuntimePayload::StreamingSubscription(subscription))
            }
            DataViewRuntimeRequest::BuildScopedDataView { request } => {
                let mut engine = self.engine()?;
                let mut projector = self.projector()?;
                Ok(DataViewRuntimePayload::ScopedDataView(self.build_scoped(
                    &mut engine,
                    &mut projector,
                    request,
                )?))
            }
            DataViewRuntimeRequest::IssueVisibleActionContext {
                request,
                action_id,
                policy_context_digest,
                grant_epoch,
                issued_at_ms,
                expires_at_ms,
            } => {
                let access = request.access.clone();
                let mut engine = self.engine()?;
                let mut projector = self.projector()?;
                let outcome = self.build_scoped(&mut engine, &mut projector, request)?;
                let ScopedDataViewProjectionOutcome::Projected(view) = outcome else {
                    return Err(DataViewContractError::NotFound("scoped revision"));
                };
                let context = projector.issue_action_context(
                    &mut engine,
                    &view,
                    &access,
                    &action_id,
                    policy_context_digest,
                    grant_epoch,
                    issued_at_ms,
                    expires_at_ms,
                )?;
                Ok(DataViewRuntimePayload::ActionExecutionContext(context))
            }
            DataViewRuntimeRequest::IssueDeclaredAdmission { profile_id, idempotency_key, now_ms, ttl_ms } => {
                Ok(DataViewRuntimePayload::DeclaredAdmission(self.engine()?.issue_declared_admission(&profile_id, &idempotency_key, now_ms, ttl_ms)?))
            }
            DataViewRuntimeRequest::SubmitDeclaredResult { submission, now_ms, current_policy_context_digest, current_grant_epoch } => {
                Ok(DataViewRuntimePayload::Commit(self.engine()?.submit_declared_result(submission, now_ms, &current_policy_context_digest, current_grant_epoch)))
            }
            DataViewRuntimeRequest::SubmitActionResult {
                submission,
                now_ms,
                current_policy_context_digest,
            } => Ok(DataViewRuntimePayload::Commit(
                self.engine()?.submit_action_result(
                    submission,
                    now_ms,
                    &current_policy_context_digest,
                ),
            )),
            DataViewRuntimeRequest::CreateStreamingSubscription { request } => {
                Ok(DataViewRuntimePayload::StreamingSubscription(
                    self.engine()?.create_streaming_subscription(request)?,
                ))
            }
            DataViewRuntimeRequest::PauseStreamingSubscription { subscription_id } => {
                Ok(DataViewRuntimePayload::StreamingSubscription(
                    self.engine()?
                        .pause_streaming_subscription(&subscription_id)?,
                ))
            }
            DataViewRuntimeRequest::RevokeStreamingSubscription { subscription_id } => {
                Ok(DataViewRuntimePayload::StreamingSubscription(
                    self.engine()?
                        .revoke_streaming_subscription(&subscription_id)?,
                ))
            }
            DataViewRuntimeRequest::RebaseStreamingSubscription { request } => {
                Ok(DataViewRuntimePayload::StreamingSubscription(
                    self.engine()?.rebase_streaming_subscription(request)?,
                ))
            }
            DataViewRuntimeRequest::SubmitStreamingRefresh {
                submission,
                now_ms,
                current_policy_context_digest,
                current_grant_epoch,
            } => Ok(DataViewRuntimePayload::Commit(
                self.engine()?.submit_stream_refresh(
                    submission,
                    now_ms,
                    &current_policy_context_digest,
                    current_grant_epoch,
                ),
            )),
            DataViewRuntimeRequest::ApplyRetention { policy } => Ok(
                DataViewRuntimePayload::Retention(self.engine()?.apply_retention(&policy)?),
            ),
        }
    }

    fn build_scoped(
        &self,
        engine: &mut DataViewEngine,
        projector: &mut ScopedDataViewProjector,
        mut request: BuildScopedDataViewRequest,
    ) -> Result<ScopedDataViewProjectionOutcome, DataViewContractError> {
        let preliminary = projector.project(
            engine,
            ScopedDataViewProjectionRequest {
                revision_selector: request.revision_selector.clone(),
                access: request.access.clone(),
                scoped_discovery_graph: empty_graph(request.now_ms),
                discovery_frontier: empty_frontier(),
            },
        )?;
        let ScopedDataViewProjectionOutcome::Projected(preliminary_view) = preliminary else {
            return Ok(ScopedDataViewProjectionOutcome::RevisionNotAvailable);
        };
        request.discovery_request.scoped_data_view = Some(*preliminary_view);
        let discovery = self
            .discovery
            .discover(request.discovery_request, request.now_ms);
        let final_projection = projector.project(
            engine,
            ScopedDataViewProjectionRequest {
                revision_selector: request.revision_selector,
                access: request.access,
                scoped_discovery_graph: discovery.scoped_discovery_graph,
                discovery_frontier: discovery.discovery_frontier,
            },
        )?;
        if let ScopedDataViewProjectionOutcome::Projected(view) = &final_projection
            && view.projection_mode == ProjectionMode::CurrentActionable
        {
            for ranked in &view.discovery_frontier.ranked_actions {
                engine.register_action(ranked.action.clone())?;
            }
        }
        Ok(final_projection)
    }

    fn engine(&self) -> Result<MutexGuard<'_, DataViewEngine>, DataViewContractError> {
        self.engine
            .lock()
            .map_err(|_| DataViewContractError::PersistenceFailed)
    }

    fn projector(&self) -> Result<MutexGuard<'_, ScopedDataViewProjector>, DataViewContractError> {
        self.projector
            .lock()
            .map_err(|_| DataViewContractError::PersistenceFailed)
    }

    fn reply_event(&self, payload: DataViewRuntimePayload, correlation_id: Option<u64>) -> Event {
        let bytes = encode_msgpack(&payload).unwrap_or_else(|_| Vec::new().into());
        Event::new(
            EventMetadata {
                origin_id: self.origin_id.clone(),
                timestamp_us: now_us(),
                logical_ts: None,
                correlation_id,
                ingress_auth: None,
                trace: None,
            },
            bytes,
        )
    }
}

fn empty_graph(now_ms: u64) -> ScopedDiscoveryGraph {
    ScopedDiscoveryGraph {
        resource_nodes: Vec::new(),
        action_nodes: Vec::new(),
        visibility_edges: Vec::new(),
        executable_edges: Vec::new(),
        expansion_edges: Vec::new(),
        source_authority_refs: Vec::new(),
        policy_constraints: Vec::new(),
        freshness_constraints: Vec::new(),
        expires_at_ms: now_ms,
    }
}

fn empty_frontier() -> DiscoveryFrontier {
    DiscoveryFrontier {
        strategy: DiscoveryFrontierStrategy::BalancedBreadthThenDepth,
        ranked_actions: Vec::new(),
        skipped_branches: Vec::new(),
        explanation: "empty DataView frontier".into(),
    }
}

fn now_us() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_micros() as u64)
        .unwrap_or(0)
}

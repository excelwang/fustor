use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use ring::digest::{SHA256, digest};
use serde::{Deserialize, Serialize};

use crate::{
    ActionExecutionContext, BoundedSourceAction, CreateStreamingSubscription,
    DATA_VIEW_WIRE_VERSION, DataViewContractError, DataViewRef, IdGenerator, ObservationPath,
    ProjectionMode, StreamingRefreshOperation, StreamingRefreshSubmission, StreamingSubscription,
    StreamingSubscriptionStatus, SystemIdGenerator, TrackedResultRef,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ActionExecutionState {
    Issued,
    Committed {
        submission_id: String,
        outcome_ref: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RegisteredExecution {
    context: ActionExecutionContext,
    state: ActionExecutionState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct BoundedActionRegistrySnapshot {
    actions: BTreeMap<String, BoundedSourceAction>,
    executions: BTreeMap<String, RegisteredExecution>,
}

#[derive(Clone)]
pub struct BoundedActionRegistry {
    id_generator: Arc<dyn IdGenerator>,
    actions: BTreeMap<String, BoundedSourceAction>,
    executions: BTreeMap<String, RegisteredExecution>,
}

impl Default for BoundedActionRegistry {
    fn default() -> Self {
        Self::new(Arc::new(SystemIdGenerator))
    }
}

impl BoundedActionRegistry {
    pub fn new(id_generator: Arc<dyn IdGenerator>) -> Self {
        Self {
            id_generator,
            actions: BTreeMap::new(),
            executions: BTreeMap::new(),
        }
    }

    pub(crate) fn snapshot(&self) -> BoundedActionRegistrySnapshot {
        BoundedActionRegistrySnapshot {
            actions: self.actions.clone(),
            executions: self.executions.clone(),
        }
    }

    pub(crate) fn from_snapshot(
        id_generator: Arc<dyn IdGenerator>,
        snapshot: BoundedActionRegistrySnapshot,
    ) -> Self {
        Self {
            id_generator,
            actions: snapshot.actions,
            executions: snapshot.executions,
        }
    }

    pub fn register_action(
        &mut self,
        action: BoundedSourceAction,
    ) -> Result<(), DataViewContractError> {
        action.validate()?;
        if let Some(existing) = self.actions.get(&action.action_id) {
            return if existing == &action {
                Ok(())
            } else {
                Err(DataViewContractError::AlreadyExists("action"))
            };
        }
        self.actions.insert(action.action_id.clone(), action);
        Ok(())
    }

    pub fn action(&self, action_id: &str) -> Option<&BoundedSourceAction> {
        self.actions.get(action_id)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn issue_context(
        &mut self,
        action_id: &str,
        base_revision: DataViewRef,
        projection_mode: ProjectionMode,
        policy_context_digest: impl Into<String>,
        grant_epoch: u64,
        issued_at_ms: u64,
        expires_at_ms: u64,
    ) -> Result<ActionExecutionContext, DataViewContractError> {
        if projection_mode == ProjectionMode::HistoricalReplay {
            return Err(DataViewContractError::HistoricalReplayIsReadOnly);
        }
        if expires_at_ms <= issued_at_ms {
            return Err(DataViewContractError::InvalidContract("expires_at_ms"));
        }
        let action = self
            .actions
            .get(action_id)
            .ok_or(DataViewContractError::NotFound("action"))?;
        let execution_id = self.id_generator.next_id("execution")?;
        let idempotency_key = self.id_generator.next_id("idempotency")?;
        let context = ActionExecutionContext {
            wire_version: DATA_VIEW_WIRE_VERSION,
            action_id: action_id.to_string(),
            execution_id: execution_id.clone(),
            base_revision,
            action_contract_digest: action_contract_digest(action)?,
            policy_context_digest: policy_context_digest.into(),
            grant_epoch,
            issued_at_ms,
            expires_at_ms,
            idempotency_key,
        };
        self.executions.insert(
            execution_id,
            RegisteredExecution {
                context: context.clone(),
                state: ActionExecutionState::Issued,
            },
        );
        Ok(context)
    }

    pub fn resolve_execution(
        &self,
        execution_id: &str,
        now_ms: u64,
    ) -> Result<(&BoundedSourceAction, &ActionExecutionContext), DataViewContractError> {
        let execution = self
            .executions
            .get(execution_id)
            .ok_or(DataViewContractError::NotFound("execution"))?;
        if execution.context.expires_at_ms <= now_ms {
            return Err(DataViewContractError::Expired);
        }
        let action = self
            .actions
            .get(&execution.context.action_id)
            .ok_or(DataViewContractError::NotFound("action"))?;
        Ok((action, &execution.context))
    }

    pub fn execution_state(&self, execution_id: &str) -> Option<&ActionExecutionState> {
        self.executions
            .get(execution_id)
            .map(|record| &record.state)
    }

    pub fn mark_committed(
        &mut self,
        execution_id: &str,
        submission_id: &str,
        outcome_ref: &str,
    ) -> Result<ActionExecutionState, DataViewContractError> {
        let execution = self
            .executions
            .get_mut(execution_id)
            .ok_or(DataViewContractError::NotFound("execution"))?;
        match &execution.state {
            ActionExecutionState::Issued => {
                execution.state = ActionExecutionState::Committed {
                    submission_id: submission_id.to_string(),
                    outcome_ref: outcome_ref.to_string(),
                };
                Ok(execution.state.clone())
            }
            ActionExecutionState::Committed {
                submission_id: committed_submission,
                ..
            } if committed_submission == submission_id => Ok(execution.state.clone()),
            ActionExecutionState::Committed { .. } => {
                Err(DataViewContractError::AlreadyExists("execution commit"))
            }
        }
    }
}

pub fn action_contract_digest(
    action: &BoundedSourceAction,
) -> Result<String, DataViewContractError> {
    let encoded =
        serde_json::to_vec(action).map_err(|_| DataViewContractError::SerializationFailed)?;
    let hash = digest(&SHA256, &encoded);
    let mut output = String::with_capacity(hash.as_ref().len() * 2);
    for byte in hash.as_ref() {
        use std::fmt::Write;
        write!(&mut output, "{byte:02x}")
            .map_err(|_| DataViewContractError::SerializationFailed)?;
    }
    Ok(output)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamBatchDisposition {
    New,
    Duplicate { outcome_ref: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ProcessedStreamBatch {
    sequence: u64,
    submission_id: String,
    resulting_watermark: String,
    outcome_ref: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ProcessedStreamBatchSnapshot {
    subscription_id: String,
    batch_id: String,
    batch: ProcessedStreamBatch,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct StreamingSubscriptionRegistrySnapshot {
    subscriptions: BTreeMap<String, StreamingSubscription>,
    processed_batches: Vec<ProcessedStreamBatchSnapshot>,
}

#[derive(Clone)]
pub struct StreamingSubscriptionRegistry {
    id_generator: Arc<dyn IdGenerator>,
    subscriptions: BTreeMap<String, StreamingSubscription>,
    processed_batches: BTreeMap<(String, String), ProcessedStreamBatch>,
}

impl Default for StreamingSubscriptionRegistry {
    fn default() -> Self {
        Self::new(Arc::new(SystemIdGenerator))
    }
}

impl StreamingSubscriptionRegistry {
    pub fn new(id_generator: Arc<dyn IdGenerator>) -> Self {
        Self {
            id_generator,
            subscriptions: BTreeMap::new(),
            processed_batches: BTreeMap::new(),
        }
    }

    pub(crate) fn snapshot(&self) -> StreamingSubscriptionRegistrySnapshot {
        StreamingSubscriptionRegistrySnapshot {
            subscriptions: self.subscriptions.clone(),
            processed_batches: self
                .processed_batches
                .iter()
                .map(
                    |((subscription_id, batch_id), batch)| ProcessedStreamBatchSnapshot {
                        subscription_id: subscription_id.clone(),
                        batch_id: batch_id.clone(),
                        batch: batch.clone(),
                    },
                )
                .collect(),
        }
    }

    pub(crate) fn from_snapshot(
        id_generator: Arc<dyn IdGenerator>,
        snapshot: StreamingSubscriptionRegistrySnapshot,
    ) -> Self {
        Self {
            id_generator,
            subscriptions: snapshot.subscriptions,
            processed_batches: snapshot
                .processed_batches
                .into_iter()
                .map(|record| ((record.subscription_id, record.batch_id), record.batch))
                .collect(),
        }
    }

    pub fn create(
        &mut self,
        request: CreateStreamingSubscription,
        existing_results: &BTreeSet<TrackedResultRef>,
    ) -> Result<StreamingSubscription, DataViewContractError> {
        validate_subscription_request(&request, existing_results)?;
        let subscription_id = self.id_generator.next_id("subscription")?;
        let subscription = StreamingSubscription {
            wire_version: request.wire_version,
            subscription_id: subscription_id.clone(),
            tracked_result_refs: request.tracked_result_refs,
            source_authority_ref: request.source_authority_ref,
            observation_path: request.observation_path,
            allowed_schema_versions: request.allowed_schema_versions,
            source_watermark: request.source_watermark,
            next_sequence: 1,
            max_events_per_batch: request.max_events_per_batch,
            max_bytes_per_batch: request.max_bytes_per_batch,
            max_batches_per_window: request.max_batches_per_window,
            batch_window_ms: request.batch_window_ms,
            max_in_flight: request.max_in_flight,
            window_started_at_ms: request.lease_issued_at_ms,
            committed_batches_in_window: 0,
            in_flight: 0,
            lease_issued_at_ms: request.lease_issued_at_ms,
            lease_expires_at_ms: request.lease_expires_at_ms,
            policy_context_digest: request.policy_context_digest,
            grant_epoch: request.grant_epoch,
            status: StreamingSubscriptionStatus::Active,
        };
        self.subscriptions
            .insert(subscription_id, subscription.clone());
        Ok(subscription)
    }

    pub fn subscription(&self, subscription_id: &str) -> Option<&StreamingSubscription> {
        self.subscriptions.get(subscription_id)
    }

    pub fn validate_batch(
        &mut self,
        submission: &StreamingRefreshSubmission,
        now_ms: u64,
        policy_context_digest: &str,
        grant_epoch: u64,
    ) -> Result<StreamBatchDisposition, DataViewContractError> {
        if submission.wire_version != DATA_VIEW_WIRE_VERSION {
            return Err(DataViewContractError::InvalidContract("wire_version"));
        }
        let batch_key = (
            submission.subscription_id.clone(),
            submission.batch_id.clone(),
        );
        if let Some(batch) = self.processed_batches.get(&batch_key) {
            if batch.sequence == submission.sequence
                && batch.submission_id == submission.submission_id
                && batch.resulting_watermark == submission.resulting_watermark
            {
                return Ok(StreamBatchDisposition::Duplicate {
                    outcome_ref: batch.outcome_ref.clone(),
                });
            }
            return Err(DataViewContractError::BatchConflict);
        }

        let subscription = self
            .subscriptions
            .get_mut(&submission.subscription_id)
            .ok_or(DataViewContractError::NotFound("subscription"))?;
        ensure_subscription_active(subscription, now_ms, policy_context_digest, grant_epoch)?;
        if !subscription
            .allowed_schema_versions
            .contains(&submission.schema)
        {
            subscription.status = StreamingSubscriptionStatus::RequiresRevalidation;
            return Err(DataViewContractError::UnsupportedSchema);
        }
        if submission.actual_events != submission.refreshes.len() as u64
            || submission.actual_events > subscription.max_events_per_batch
        {
            return Err(DataViewContractError::LimitExceeded("events"));
        }
        if submission.actual_bytes > subscription.max_bytes_per_batch {
            return Err(DataViewContractError::LimitExceeded("bytes"));
        }
        let payload_bytes = submission
            .refreshes
            .iter()
            .map(|entry| {
                entry
                    .payload
                    .as_ref()
                    .map_or(0, |payload| payload.len() as u64)
            })
            .sum::<u64>();
        if payload_bytes != submission.actual_bytes
            || submission
                .refreshes
                .iter()
                .any(|entry| match entry.operation {
                    StreamingRefreshOperation::Refresh => entry.payload.is_none(),
                    StreamingRefreshOperation::Invalidate | StreamingRefreshOperation::Delete => {
                        entry.payload.is_some()
                    }
                })
        {
            return Err(DataViewContractError::InvalidContract(
                "stream refresh payload",
            ));
        }
        if submission.sequence != subscription.next_sequence {
            subscription.status = StreamingSubscriptionStatus::RequiresRevalidation;
            return Err(DataViewContractError::SequenceGap);
        }
        if submission.previous_watermark != subscription.source_watermark {
            subscription.status = StreamingSubscriptionStatus::RequiresRevalidation;
            return Err(DataViewContractError::WatermarkGap);
        }
        if submission.refreshes.iter().any(|entry| {
            !subscription
                .tracked_result_refs
                .contains(&entry.tracked_result_ref)
        }) {
            return Err(DataViewContractError::TrackedResultUnavailable);
        }
        validate_projection_evidence(
            &subscription.observation_path,
            submission.projection_evidence.as_ref(),
        )?;
        if now_ms.saturating_sub(subscription.window_started_at_ms) >= subscription.batch_window_ms
        {
            subscription.window_started_at_ms = now_ms;
            subscription.committed_batches_in_window = 0;
        }
        if subscription.committed_batches_in_window >= subscription.max_batches_per_window {
            return Err(DataViewContractError::LimitExceeded("batches-per-window"));
        }
        if subscription.in_flight >= subscription.max_in_flight {
            return Err(DataViewContractError::LimitExceeded("in-flight"));
        }
        subscription.in_flight += 1;
        Ok(StreamBatchDisposition::New)
    }

    pub fn commit_batch(
        &mut self,
        submission: &StreamingRefreshSubmission,
        outcome_ref: &str,
    ) -> Result<(), DataViewContractError> {
        let subscription = self
            .subscriptions
            .get_mut(&submission.subscription_id)
            .ok_or(DataViewContractError::NotFound("subscription"))?;
        if subscription.next_sequence != submission.sequence
            || subscription.source_watermark != submission.previous_watermark
        {
            return Err(DataViewContractError::RequiresRevalidation);
        }
        subscription.next_sequence += 1;
        subscription.source_watermark = submission.resulting_watermark.clone();
        subscription.in_flight = subscription.in_flight.saturating_sub(1);
        subscription.committed_batches_in_window += 1;
        self.processed_batches.insert(
            (
                submission.subscription_id.clone(),
                submission.batch_id.clone(),
            ),
            ProcessedStreamBatch {
                sequence: submission.sequence,
                submission_id: submission.submission_id.clone(),
                resulting_watermark: submission.resulting_watermark.clone(),
                outcome_ref: outcome_ref.to_string(),
            },
        );
        Ok(())
    }

    pub fn abort_batch(&mut self, subscription_id: &str) {
        if let Some(subscription) = self.subscriptions.get_mut(subscription_id) {
            subscription.in_flight = subscription.in_flight.saturating_sub(1);
        }
    }

    pub fn pause(
        &mut self,
        subscription_id: &str,
    ) -> Result<StreamingSubscription, DataViewContractError> {
        let subscription = self
            .subscriptions
            .get_mut(subscription_id)
            .ok_or(DataViewContractError::NotFound("subscription"))?;
        if subscription.status == StreamingSubscriptionStatus::Revoked {
            return Err(DataViewContractError::Revoked);
        }
        subscription.status = StreamingSubscriptionStatus::Paused;
        Ok(subscription.clone())
    }

    pub fn revoke(
        &mut self,
        subscription_id: &str,
    ) -> Result<StreamingSubscription, DataViewContractError> {
        let subscription = self
            .subscriptions
            .get_mut(subscription_id)
            .ok_or(DataViewContractError::NotFound("subscription"))?;
        subscription.status = StreamingSubscriptionStatus::Revoked;
        Ok(subscription.clone())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn rebase(
        &mut self,
        subscription_id: &str,
        tracked_result_refs: BTreeSet<TrackedResultRef>,
        existing_results: &BTreeSet<TrackedResultRef>,
        source_watermark: impl Into<String>,
        policy_context_digest: impl Into<String>,
        grant_epoch: u64,
        lease_issued_at_ms: u64,
        lease_expires_at_ms: u64,
    ) -> Result<StreamingSubscription, DataViewContractError> {
        if tracked_result_refs.is_empty()
            || !tracked_result_refs.is_subset(existing_results)
            || lease_expires_at_ms <= lease_issued_at_ms
        {
            return Err(DataViewContractError::TrackedResultUnavailable);
        }
        let subscription = self
            .subscriptions
            .get_mut(subscription_id)
            .ok_or(DataViewContractError::NotFound("subscription"))?;
        if subscription.status == StreamingSubscriptionStatus::Revoked {
            return Err(DataViewContractError::Revoked);
        }
        subscription.tracked_result_refs = tracked_result_refs;
        subscription.source_watermark = source_watermark.into();
        subscription.policy_context_digest = policy_context_digest.into();
        subscription.grant_epoch = grant_epoch;
        subscription.lease_issued_at_ms = lease_issued_at_ms;
        subscription.lease_expires_at_ms = lease_expires_at_ms;
        subscription.window_started_at_ms = lease_issued_at_ms;
        subscription.committed_batches_in_window = 0;
        subscription.in_flight = 0;
        subscription.status = StreamingSubscriptionStatus::Active;
        Ok(subscription.clone())
    }
}

fn validate_subscription_request(
    request: &CreateStreamingSubscription,
    existing_results: &BTreeSet<TrackedResultRef>,
) -> Result<(), DataViewContractError> {
    if request.wire_version != DATA_VIEW_WIRE_VERSION {
        return Err(DataViewContractError::InvalidContract("wire_version"));
    }
    if request.tracked_result_refs.is_empty()
        || !request.tracked_result_refs.is_subset(existing_results)
    {
        return Err(DataViewContractError::TrackedResultUnavailable);
    }
    if request.allowed_schema_versions.is_empty() {
        return Err(DataViewContractError::InvalidContract(
            "allowed_schema_versions",
        ));
    }
    for schema in &request.allowed_schema_versions {
        schema.validate()?;
    }
    if request.max_events_per_batch == 0 {
        return Err(DataViewContractError::InvalidContract(
            "max_events_per_batch",
        ));
    }
    if request.max_bytes_per_batch == 0 {
        return Err(DataViewContractError::InvalidContract(
            "max_bytes_per_batch",
        ));
    }
    if request.max_batches_per_window == 0 {
        return Err(DataViewContractError::InvalidContract(
            "max_batches_per_window",
        ));
    }
    if request.batch_window_ms == 0 {
        return Err(DataViewContractError::InvalidContract("batch_window_ms"));
    }
    if request.max_in_flight == 0 {
        return Err(DataViewContractError::InvalidContract("max_in_flight"));
    }
    if request.lease_expires_at_ms <= request.lease_issued_at_ms {
        return Err(DataViewContractError::InvalidContract(
            "lease_expires_at_ms",
        ));
    }
    if let Some(projection) = request.observation_path.projection()
        && projection.source_authority_ref != request.source_authority_ref
    {
        return Err(DataViewContractError::ObservationPathMismatch);
    }
    Ok(())
}

fn ensure_subscription_active(
    subscription: &mut StreamingSubscription,
    now_ms: u64,
    policy_context_digest: &str,
    grant_epoch: u64,
) -> Result<(), DataViewContractError> {
    match subscription.status {
        StreamingSubscriptionStatus::Active => {}
        StreamingSubscriptionStatus::Paused => return Err(DataViewContractError::Paused),
        StreamingSubscriptionStatus::Expired => return Err(DataViewContractError::Expired),
        StreamingSubscriptionStatus::Revoked => return Err(DataViewContractError::Revoked),
        StreamingSubscriptionStatus::RequiresRevalidation => {
            return Err(DataViewContractError::RequiresRevalidation);
        }
    }
    if subscription.lease_expires_at_ms <= now_ms {
        subscription.status = StreamingSubscriptionStatus::Expired;
        return Err(DataViewContractError::Expired);
    }
    if subscription.policy_context_digest != policy_context_digest {
        subscription.status = StreamingSubscriptionStatus::RequiresRevalidation;
        return Err(DataViewContractError::PolicyContextMismatch);
    }
    if subscription.grant_epoch != grant_epoch {
        subscription.status = StreamingSubscriptionStatus::RequiresRevalidation;
        return Err(DataViewContractError::GrantEpochMismatch);
    }
    Ok(())
}

fn validate_projection_evidence(
    path: &ObservationPath,
    evidence: Option<&crate::ProjectionApplyEvidence>,
) -> Result<(), DataViewContractError> {
    match (path, evidence) {
        (ObservationPath::Direct, None) => Ok(()),
        (ObservationPath::Direct, Some(_)) => {
            Err(DataViewContractError::ProjectionEvidenceUnexpected)
        }
        (ObservationPath::ProjectionBacked { .. }, None) => {
            Err(DataViewContractError::ProjectionEvidenceRequired)
        }
        (ObservationPath::ProjectionBacked { projection }, Some(evidence))
            if projection == &evidence.projection =>
        {
            Ok(())
        }
        (ObservationPath::ProjectionBacked { .. }, Some(_)) => {
            Err(DataViewContractError::ObservationPathMismatch)
        }
    }
}

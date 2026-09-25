use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::{
    ActionExecutionContext, ActionExecutionState, ActionResultSubmission, BoundedActionRegistry,
    BoundedActionRegistrySnapshot, BoundedSourceAction, CreateStreamingSubscription,
    DATA_VIEW_WIRE_VERSION, DataViewCommitOutcome, DataViewCommitStatus, DataViewContractError,
    DataViewPersistence, DataViewRef, DataViewStore, FreshnessState, IdGenerator, ObservationPath,
    ProducerKind, ProjectionMode, RebaseStreamingSubscription, ResultNormalizer,
    ResultNormalizerRegistry, RetentionOutcome, RetentionPolicy, StreamBatchDisposition,
    StreamingRefreshOperation, StreamingRefreshSubmission, StreamingSubscription,
    StreamingSubscriptionRegistry, StreamingSubscriptionRegistrySnapshot, SystemIdGenerator,
    TrackedResultRef, action_contract_digest,
};

const ENGINE_CONTROL_STATE_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedEngineControlState {
    version: u16,
    action_registry: BoundedActionRegistrySnapshot,
    streaming_registry: StreamingSubscriptionRegistrySnapshot,
    outcomes: BTreeMap<String, DataViewCommitOutcome>,
    #[serde(default)]
    declared_admissions: BTreeMap<String, crate::DeclaredResultAdmissionContext>,
    #[serde(default)]
    declared_outcomes: BTreeMap<String, (String, DataViewCommitOutcome)>,
}

impl Default for PersistedEngineControlState {
    fn default() -> Self {
        Self {
            version: ENGINE_CONTROL_STATE_VERSION,
            action_registry: BoundedActionRegistrySnapshot::default(),
            streaming_registry: StreamingSubscriptionRegistrySnapshot::default(),
            outcomes: BTreeMap::new(),
            declared_admissions: BTreeMap::new(),
            declared_outcomes: BTreeMap::new(),
        }
    }
}

pub struct DataViewEngine {
    action_registry: BoundedActionRegistry,
    streaming_registry: StreamingSubscriptionRegistry,
    normalizer_registry: ResultNormalizerRegistry,
    store: DataViewStore,
    outcomes: BTreeMap<String, DataViewCommitOutcome>,
    declared_policies: BTreeMap<String, crate::DeclaredResultPolicy>,
    declared_admissions: BTreeMap<String, crate::DeclaredResultAdmissionContext>,
    declared_outcomes: BTreeMap<String, (String, DataViewCommitOutcome)>,
}

impl DataViewEngine {
    pub fn new(data_view_id: impl Into<String>) -> Result<Self, DataViewContractError> {
        Self::with_id_generator(data_view_id, Arc::new(SystemIdGenerator))
    }

    pub fn with_id_generator(
        data_view_id: impl Into<String>,
        id_generator: Arc<dyn IdGenerator>,
    ) -> Result<Self, DataViewContractError> {
        let store = DataViewStore::with_id_generator(data_view_id, id_generator.clone())?;
        Self::from_store(store, id_generator)
    }

    pub fn with_persistence(
        data_view_id: impl Into<String>,
        id_generator: Arc<dyn IdGenerator>,
        persistence: Arc<dyn DataViewPersistence>,
    ) -> Result<Self, DataViewContractError> {
        let store =
            DataViewStore::with_persistence(data_view_id, id_generator.clone(), persistence)?;
        Self::from_store(store, id_generator)
    }

    fn from_store(
        store: DataViewStore,
        id_generator: Arc<dyn IdGenerator>,
    ) -> Result<Self, DataViewContractError> {
        let control = if store.control_state().is_empty() {
            PersistedEngineControlState::default()
        } else {
            serde_json::from_slice::<PersistedEngineControlState>(store.control_state())
                .map_err(|_| DataViewContractError::PersistenceFailed)?
        };
        if control.version != ENGINE_CONTROL_STATE_VERSION {
            return Err(DataViewContractError::PersistenceFailed);
        }
        Ok(Self {
            action_registry: BoundedActionRegistry::from_snapshot(
                id_generator.clone(),
                control.action_registry,
            ),
            streaming_registry: StreamingSubscriptionRegistry::from_snapshot(
                id_generator,
                control.streaming_registry,
            ),
            normalizer_registry: ResultNormalizerRegistry::default(),
            store,
            outcomes: control.outcomes,
            declared_policies: BTreeMap::new(),
            declared_admissions: control.declared_admissions,
            declared_outcomes: control.declared_outcomes,
        })
    }

    pub fn head_ref(&self) -> DataViewRef {
        self.store.head_ref()
    }

    pub fn register_declared_policy(&mut self, policy: crate::DeclaredResultPolicy) -> Result<(), DataViewContractError> {
        policy.validate()?;
        if self.declared_policies.contains_key(&policy.profile_id) { return Err(DataViewContractError::AlreadyExists("declared policy")); }
        self.declared_policies.insert(policy.profile_id.clone(), policy);
        Ok(())
    }

    /// Current owner policy only. This does not issue an admission or reserve a write.
    pub fn inspect_declared_capability(&self, profile_id: &str, producer_id: &str, schema: &crate::SchemaVersionRef) -> Result<Option<crate::DeclaredResultCapability>, DataViewContractError> {
        if profile_id.is_empty() || profile_id.len() > 256 || producer_id.is_empty() || producer_id.len() > 256
            || schema.schema_ref.len() > 4096 || schema.schema_version.len() > 256 {
            return Err(DataViewContractError::InvalidContract("declared capability identity"));
        }
        schema.validate()?;
        let Some(policy) = self.declared_policies.get(profile_id) else { return Ok(None); };
        if policy.producer_id != producer_id || policy.schema != *schema || !self.normalizer_registry.contains(schema) {
            return Ok(None);
        }
        Ok(Some(crate::DeclaredResultCapability {
            data_view_id: self.store.head_ref().data_view_id,
            policy: policy.clone(),
            policy_digest: crate::declared::declared_digest(policy)?,
        }))
    }

    /// Historical acceptance only, not current result visibility or a new grant.
    pub fn inspect_declared_outcome(&self, profile_id: &str, idempotency_key: &str) -> Result<Option<DataViewCommitOutcome>, DataViewContractError> {
        if profile_id.is_empty() || profile_id.len() > 256 || idempotency_key.is_empty() || idempotency_key.len() > 256 {
            return Err(DataViewContractError::InvalidContract("declared outcome identity"));
        }
        let Some(policy) = self.declared_policies.get(profile_id) else { return Ok(None); };
        let contexts: Vec<_> = self.declared_admissions.values().filter(|context|
            context.policy.profile_id == policy.profile_id && context.policy.producer_id == policy.producer_id
                && context.idempotency_key == idempotency_key).collect();
        // A later issue under a changed policy cannot relabel an older outcome.
        if contexts.is_empty() || contexts.iter().any(|context| context.policy != *policy) {
            return Ok(None);
        }
        let key = crate::declared::declared_digest(&(policy.profile_id.clone(), policy.producer_id.clone(), idempotency_key.to_string()))?;
        Ok(self.declared_outcomes.get(&key).map(|(_, outcome)| outcome.clone()))
    }

    pub fn issue_declared_admission(&mut self, profile_id: &str, idempotency_key: &str, now_ms: u64, ttl_ms: u64) -> Result<crate::DeclaredResultAdmissionContext, DataViewContractError> {
        let policy = self.declared_policies.get(profile_id).ok_or(DataViewContractError::NotFound("declared policy"))?.clone();
        if idempotency_key.is_empty() || idempotency_key.len() > 256 || ttl_ms == 0 || ttl_ms > policy.max_ttl_ms { return Err(DataViewContractError::InvalidContract("declared admission limits")); }
        let outcome_key = crate::declared::declared_digest(&(policy.profile_id.clone(), policy.producer_id.clone(), idempotency_key.to_string()))?;
        if let Some(existing) = self.declared_admissions.values().find(|context| context.policy == policy && context.idempotency_key == idempotency_key
            && (context.expires_at_ms > now_ms || self.declared_outcomes.contains_key(&outcome_key))) { return Ok(existing.clone()); }
        let contract_digest = crate::declared::declared_digest(&policy)?;
        let context = crate::DeclaredResultAdmissionContext { admission_id: SystemIdGenerator.next_id("admission")?, admission_contract_digest: contract_digest,
            policy, base_revision: self.head_ref(), issued_at_ms: now_ms, expires_at_ms: now_ms.checked_add(ttl_ms).ok_or(DataViewContractError::InvalidContract("expires_at"))?, idempotency_key: idempotency_key.into() };
        if self.declared_admissions.len() >= 10000 { return Err(DataViewContractError::LimitExceeded("declared admission registry")); }
        let mut control: PersistedEngineControlState = serde_json::from_slice(&self.encode_control_state(&self.action_registry, &self.streaming_registry, &self.outcomes)?).map_err(|_| DataViewContractError::SerializationFailed)?;
        control.declared_admissions.insert(context.admission_id.clone(), context.clone());
        self.store.persist_control_state(serde_json::to_vec(&control).map_err(|_| DataViewContractError::SerializationFailed)?)?;
        self.declared_admissions = control.declared_admissions;
        Ok(context)
    }

    pub fn submit_declared_result(&mut self, submission: crate::DeclaredResultSubmission, now_ms: u64,
        current_policy_context_digest: &str, current_grant_epoch: u64) -> DataViewCommitOutcome {
        let head = self.head_ref();
        match self.try_submit_declared_result(&submission, now_ms, current_policy_context_digest, current_grant_epoch) {
            Ok(outcome) => outcome, Err(error) => rejection_outcome(&submission.submission_id, head, error),
        }
    }

    fn try_submit_declared_result(&mut self, submission: &crate::DeclaredResultSubmission, now_ms: u64,
        current_policy_context_digest: &str, current_grant_epoch: u64) -> Result<DataViewCommitOutcome, DataViewContractError> {
        let admission = &submission.admission;
        let policy = self.declared_policies.get(&admission.policy.profile_id).ok_or(DataViewContractError::NotFound("declared policy"))?;
        if admission.policy != *policy || self.declared_admissions.get(&admission.admission_id) != Some(admission)
            || admission.admission_contract_digest != crate::declared::declared_digest(policy)? { return Err(DataViewContractError::ExecutionContextMismatch); }
        if policy.policy_context_digest != current_policy_context_digest { return Err(DataViewContractError::PolicyContextMismatch); }
        if policy.grant_epoch != current_grant_epoch { return Err(DataViewContractError::GrantEpochMismatch); }
        let key = crate::declared::declared_digest(&(policy.profile_id.clone(), policy.producer_id.clone(), admission.idempotency_key.clone()))?;
        let fingerprint = crate::declared::declared_digest(submission)?;
        if let Some((previous, outcome)) = self.declared_outcomes.get(&key) {
            return if previous == &fingerprint { Ok(outcome.clone()) } else { Err(DataViewContractError::AlreadyExists("declared idempotency key")) };
        }
        if now_ms < admission.issued_at_ms || now_ms >= admission.expires_at_ms { return Err(DataViewContractError::Expired); }
        if self.head_ref() != admission.base_revision { return Err(DataViewContractError::RequiresRevalidation); }
        crate::declared::validate_declared_manifest(submission, now_ms)?;
        let normalizer = self.normalizer_registry.resolve(&submission.manifest.schema)?.clone();
        let normalized = normalizer.normalize_declared(submission)?;
        let prepared = self.store.prepare_declared_commit(submission, normalized)?;
        let outcome = prepared.outcome.clone();
        let mut control: PersistedEngineControlState = serde_json::from_slice(&self.encode_control_state(&self.action_registry, &self.streaming_registry, &self.outcomes)?).map_err(|_| DataViewContractError::SerializationFailed)?;
        control.declared_outcomes.insert(key, (fingerprint, outcome.clone()));
        self.store.apply_prepared_commit_with_control(prepared, serde_json::to_vec(&control).map_err(|_| DataViewContractError::SerializationFailed)?)?;
        self.declared_outcomes = control.declared_outcomes;
        Ok(outcome)
    }

    pub fn store(&self) -> &DataViewStore {
        &self.store
    }

    pub fn action_registry(&self) -> &BoundedActionRegistry {
        &self.action_registry
    }

    pub fn streaming_registry(&self) -> &StreamingSubscriptionRegistry {
        &self.streaming_registry
    }

    pub fn register_normalizer(
        &mut self,
        normalizer: Arc<dyn ResultNormalizer>,
    ) -> Result<(), DataViewContractError> {
        self.normalizer_registry.register(normalizer)
    }

    pub fn normalizer_registry_mut(&mut self) -> &mut ResultNormalizerRegistry {
        &mut self.normalizer_registry
    }

    pub fn register_action(
        &mut self,
        action: BoundedSourceAction,
    ) -> Result<(), DataViewContractError> {
        let mut next = self.action_registry.clone();
        next.register_action(action)?;
        if next.snapshot() != self.action_registry.snapshot() {
            let control =
                self.encode_control_state(&next, &self.streaming_registry, &self.outcomes)?;
            self.store.persist_control_state(control)?;
            self.action_registry = next;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn issue_action_context(
        &mut self,
        action_id: &str,
        base_revision: DataViewRef,
        projection_mode: ProjectionMode,
        policy_context_digest: impl Into<String>,
        grant_epoch: u64,
        issued_at_ms: u64,
        expires_at_ms: u64,
    ) -> Result<ActionExecutionContext, DataViewContractError> {
        let mut next = self.action_registry.clone();
        let context = next.issue_context(
            action_id,
            base_revision,
            projection_mode,
            policy_context_digest,
            grant_epoch,
            issued_at_ms,
            expires_at_ms,
        )?;
        let control = self.encode_control_state(&next, &self.streaming_registry, &self.outcomes)?;
        self.store.persist_control_state(control)?;
        self.action_registry = next;
        Ok(context)
    }

    pub fn create_streaming_subscription(
        &mut self,
        request: CreateStreamingSubscription,
    ) -> Result<StreamingSubscription, DataViewContractError> {
        let tracked = self.store.tracked_result_refs();
        let mut next = self.streaming_registry.clone();
        let subscription = next.create(request, &tracked)?;
        let control = self.encode_control_state(&self.action_registry, &next, &self.outcomes)?;
        self.store.persist_control_state(control)?;
        self.streaming_registry = next;
        Ok(subscription)
    }

    pub fn pause_streaming_subscription(
        &mut self,
        subscription_id: &str,
    ) -> Result<StreamingSubscription, DataViewContractError> {
        let mut next = self.streaming_registry.clone();
        let subscription = next.pause(subscription_id)?;
        let control = self.encode_control_state(&self.action_registry, &next, &self.outcomes)?;
        self.store.persist_control_state(control)?;
        self.streaming_registry = next;
        Ok(subscription)
    }

    pub fn revoke_streaming_subscription(
        &mut self,
        subscription_id: &str,
    ) -> Result<StreamingSubscription, DataViewContractError> {
        let mut next = self.streaming_registry.clone();
        let subscription = next.revoke(subscription_id)?;
        let control = self.encode_control_state(&self.action_registry, &next, &self.outcomes)?;
        self.store.persist_control_state(control)?;
        self.streaming_registry = next;
        Ok(subscription)
    }

    pub fn rebase_streaming_subscription(
        &mut self,
        request: RebaseStreamingSubscription,
    ) -> Result<StreamingSubscription, DataViewContractError> {
        if request.wire_version != DATA_VIEW_WIRE_VERSION
            || request.subscription_id.trim().is_empty()
        {
            return Err(DataViewContractError::InvalidContract(
                "streaming subscription rebase",
            ));
        }
        let existing = self.store.tracked_result_refs();
        let mut next = self.streaming_registry.clone();
        let subscription = next.rebase(
            &request.subscription_id,
            request.tracked_result_refs,
            &existing,
            request.source_watermark,
            request.policy_context_digest,
            request.grant_epoch,
            request.lease_issued_at_ms,
            request.lease_expires_at_ms,
        )?;
        let control = self.encode_control_state(&self.action_registry, &next, &self.outcomes)?;
        self.store.persist_control_state(control)?;
        self.streaming_registry = next;
        Ok(subscription)
    }

    pub fn resolve_scoped_revision_alias(
        &self,
        public_ref: &DataViewRef,
        subject_scope_id: &str,
    ) -> Option<DataViewRef> {
        self.store
            .resolve_scoped_revision_alias(public_ref, subject_scope_id)
    }

    pub fn get_or_create_scoped_revision_alias(
        &mut self,
        subject_scope_id: &str,
        visible_digest: &str,
        internal_ref: &DataViewRef,
    ) -> Result<DataViewRef, DataViewContractError> {
        self.store.get_or_create_scoped_revision_alias(
            subject_scope_id,
            visible_digest,
            internal_ref,
        )
    }

    pub fn apply_retention(
        &mut self,
        policy: &RetentionPolicy,
    ) -> Result<RetentionOutcome, DataViewContractError> {
        self.store.apply_retention(policy)
    }

    pub fn submit_action_result(
        &mut self,
        submission: ActionResultSubmission,
        now_ms: u64,
        current_policy_context_digest: &str,
    ) -> DataViewCommitOutcome {
        let head_before = self.store.head_ref();
        match self.try_submit_action_result(
            submission.clone(),
            now_ms,
            current_policy_context_digest,
        ) {
            Ok(outcome) => outcome,
            Err(error) => rejection_outcome(&submission.submission_id, head_before, error),
        }
    }

    pub fn submit_stream_refresh(
        &mut self,
        submission: StreamingRefreshSubmission,
        now_ms: u64,
        current_policy_context_digest: &str,
        current_grant_epoch: u64,
    ) -> DataViewCommitOutcome {
        let head_before = self.store.head_ref();
        match self.try_submit_stream_refresh(
            submission.clone(),
            now_ms,
            current_policy_context_digest,
            current_grant_epoch,
        ) {
            Ok(outcome) => outcome,
            Err(error) => rejection_outcome(&submission.submission_id, head_before, error),
        }
    }

    fn try_submit_action_result(
        &mut self,
        submission: ActionResultSubmission,
        now_ms: u64,
        current_policy_context_digest: &str,
    ) -> Result<DataViewCommitOutcome, DataViewContractError> {
        if let Some(ActionExecutionState::Committed {
            submission_id,
            outcome_ref,
        }) = self
            .action_registry
            .execution_state(&submission.execution.execution_id)
        {
            if submission_id == &submission.submission_id {
                return self
                    .outcomes
                    .get(outcome_ref)
                    .cloned()
                    .ok_or(DataViewContractError::NotFound("commit outcome"));
            }
            return Err(DataViewContractError::AlreadyExists("execution commit"));
        }

        let (action, issued_context) = self
            .action_registry
            .resolve_execution(&submission.execution.execution_id, now_ms)?;
        let action = action.clone();
        let issued_context = issued_context.clone();
        validate_action_submission(
            &action,
            &issued_context,
            &submission,
            current_policy_context_digest,
        )?;
        let normalizer = self
            .normalizer_registry
            .resolve(&submission.schema)?
            .clone();
        let normalized = normalizer.normalize_action(&submission)?;
        let prepared = self.store.prepare_action_commit(&submission, normalized)?;
        let outcome_ref = prepared
            .outcome
            .delta_ref
            .clone()
            .ok_or(DataViewContractError::InvalidContract("delta_ref"))?;
        let outcome = prepared.outcome.clone();
        let mut next_actions = self.action_registry.clone();
        next_actions.mark_committed(
            &submission.execution.execution_id,
            &submission.submission_id,
            &outcome_ref,
        )?;
        let mut next_outcomes = self.outcomes.clone();
        next_outcomes.insert(outcome_ref, outcome.clone());
        let control =
            self.encode_control_state(&next_actions, &self.streaming_registry, &next_outcomes)?;
        self.store
            .apply_prepared_commit_with_control(prepared, control)?;
        self.action_registry = next_actions;
        self.outcomes = next_outcomes;
        Ok(outcome)
    }

    fn try_submit_stream_refresh(
        &mut self,
        submission: StreamingRefreshSubmission,
        now_ms: u64,
        current_policy_context_digest: &str,
        current_grant_epoch: u64,
    ) -> Result<DataViewCommitOutcome, DataViewContractError> {
        let mut next_streaming = self.streaming_registry.clone();
        let disposition = match next_streaming.validate_batch(
            &submission,
            now_ms,
            current_policy_context_digest,
            current_grant_epoch,
        ) {
            Ok(disposition) => disposition,
            Err(error) => {
                if next_streaming.snapshot() != self.streaming_registry.snapshot() {
                    let control = self.encode_control_state(
                        &self.action_registry,
                        &next_streaming,
                        &self.outcomes,
                    )?;
                    self.store.persist_control_state(control)?;
                    self.streaming_registry = next_streaming;
                }
                return Err(error);
            }
        };
        match disposition {
            StreamBatchDisposition::Duplicate { outcome_ref } => {
                return self
                    .outcomes
                    .get(&outcome_ref)
                    .cloned()
                    .ok_or(DataViewContractError::NotFound("stream outcome"));
            }
            StreamBatchDisposition::New => {}
        }
        let subscription = next_streaming
            .subscription(&submission.subscription_id)
            .cloned()
            .ok_or(DataViewContractError::NotFound("subscription"))?;
        let normalizer = self
            .normalizer_registry
            .resolve(&submission.schema)?
            .clone();
        let mut normalized_refreshes = BTreeMap::<TrackedResultRef, _>::new();
        for entry in &submission.refreshes {
            if entry.source_revision.trim().is_empty()
                || entry.native_anchors.is_empty()
                || entry.evidence_refs.is_empty()
            {
                return Err(DataViewContractError::InvalidContract(
                    "stream refresh evidence",
                ));
            }
            if entry.operation == StreamingRefreshOperation::Refresh {
                let normalized = normalizer.normalize_stream_refresh(
                    &subscription.source_authority_ref,
                    &entry.tracked_result_ref,
                    entry,
                )?;
                if normalized_refreshes
                    .insert(entry.tracked_result_ref.clone(), normalized)
                    .is_some()
                {
                    return Err(DataViewContractError::InvalidContract(
                        "duplicate tracked result refresh",
                    ));
                }
            }
        }
        let prepared =
            self.store
                .prepare_stream_commit(&subscription, &submission, normalized_refreshes)?;
        let outcome_ref = prepared
            .outcome
            .delta_ref
            .clone()
            .ok_or(DataViewContractError::InvalidContract("delta_ref"))?;
        let outcome = prepared.outcome.clone();
        next_streaming.commit_batch(&submission, &outcome_ref)?;
        let mut next_outcomes = self.outcomes.clone();
        next_outcomes.insert(outcome_ref, outcome.clone());
        let control =
            self.encode_control_state(&self.action_registry, &next_streaming, &next_outcomes)?;
        self.store
            .apply_prepared_commit_with_control(prepared, control)?;
        self.streaming_registry = next_streaming;
        self.outcomes = next_outcomes;
        Ok(outcome)
    }

    fn encode_control_state(
        &self,
        action_registry: &BoundedActionRegistry,
        streaming_registry: &StreamingSubscriptionRegistry,
        outcomes: &BTreeMap<String, DataViewCommitOutcome>,
    ) -> Result<Vec<u8>, DataViewContractError> {
        serde_json::to_vec(&PersistedEngineControlState {
            version: ENGINE_CONTROL_STATE_VERSION,
            action_registry: action_registry.snapshot(),
            streaming_registry: streaming_registry.snapshot(),
            outcomes: outcomes.clone(),
            declared_admissions: self.declared_admissions.clone(),
            declared_outcomes: self.declared_outcomes.clone(),
        })
        .map_err(|_| DataViewContractError::SerializationFailed)
    }
}

fn validate_action_submission(
    action: &BoundedSourceAction,
    issued_context: &ActionExecutionContext,
    submission: &ActionResultSubmission,
    current_policy_context_digest: &str,
) -> Result<(), DataViewContractError> {
    if submission.wire_version != DATA_VIEW_WIRE_VERSION
        || submission.execution.wire_version != DATA_VIEW_WIRE_VERSION
    {
        return Err(DataViewContractError::InvalidContract("wire_version"));
    }
    if &submission.execution != issued_context
        || submission.execution.action_id != action.action_id
        || submission.execution.action_contract_digest != action_contract_digest(action)?
    {
        return Err(DataViewContractError::ExecutionContextMismatch);
    }
    if submission.execution.policy_context_digest != current_policy_context_digest {
        return Err(DataViewContractError::PolicyContextMismatch);
    }
    if submission.execution.grant_epoch != submission.grant_epoch {
        return Err(DataViewContractError::GrantEpochMismatch);
    }
    if action.source_authority_ref != submission.source_authority_ref {
        return Err(DataViewContractError::SourceAuthorityMismatch);
    }
    submission.schema.validate()?;
    if submission.submission_id.trim().is_empty()
        || submission.authority_object_ref.trim().is_empty()
        || submission.producer_id.trim().is_empty()
        || submission.source_revision.trim().is_empty()
        || submission.native_anchors.is_empty()
        || submission.evidence_refs.is_empty()
    {
        return Err(DataViewContractError::InvalidContract(
            "source result evidence",
        ));
    }
    if submission.actual_bytes != submission.payload.len() as u64
        || submission.actual_rows > action.max_rows
        || submission.actual_bytes > action.max_bytes
        || submission.actual_calls > action.max_calls
        || submission.actual_fanout > action.max_fanout
        || submission.actual_latency_ms > action.max_latency_ms
        || submission.actual_cost_units > action.max_cost_units
    {
        return Err(DataViewContractError::LimitExceeded("action"));
    }
    if submission.actual_calls == 0 {
        return Err(DataViewContractError::InvalidContract("actual_calls"));
    }
    if submission
        .continuation
        .as_ref()
        .is_some_and(|continuation| continuation.is_empty())
        || matches!(action.continuation_policy, crate::ContinuationPolicy::None)
            && submission.continuation.is_some()
    {
        return Err(DataViewContractError::InvalidContract("continuation"));
    }
    if matches!(
        submission.freshness,
        FreshnessState::RequiresRevalidation | FreshnessState::Invalidated
    ) {
        return Err(DataViewContractError::FreshnessRequiresRevalidation);
    }
    match (&action.observation_path, &submission.projection_evidence) {
        (ObservationPath::Direct, None) if submission.producer_kind == ProducerKind::Source => {}
        (ObservationPath::Direct, Some(_)) => {
            return Err(DataViewContractError::ProjectionEvidenceUnexpected);
        }
        (ObservationPath::Direct, None) => {
            return Err(DataViewContractError::ObservationPathMismatch);
        }
        (ObservationPath::ProjectionBacked { .. }, None) => {
            return Err(DataViewContractError::ProjectionEvidenceRequired);
        }
        (ObservationPath::ProjectionBacked { projection }, Some(evidence)) => {
            if submission.producer_kind != ProducerKind::SourceProjection
                || &evidence.projection != projection
                || evidence.projection.source_authority_ref != submission.source_authority_ref
                || evidence.source_revision != submission.source_revision
                || evidence.evidence_refs.is_empty()
                || matches!(
                    evidence.freshness,
                    FreshnessState::RequiresRevalidation | FreshnessState::Invalidated
                )
            {
                return Err(DataViewContractError::ObservationPathMismatch);
            }
        }
    }
    Ok(())
}

fn rejection_outcome(
    submission_id: &str,
    data_view: DataViewRef,
    error: DataViewContractError,
) -> DataViewCommitOutcome {
    let (status, residual_code, retryable) = match error {
        DataViewContractError::Expired
        | DataViewContractError::PolicyContextMismatch
        | DataViewContractError::GrantEpochMismatch
        | DataViewContractError::FreshnessRequiresRevalidation
        | DataViewContractError::RequiresRevalidation
        | DataViewContractError::SequenceGap
        | DataViewContractError::WatermarkGap => (
            DataViewCommitStatus::RequiresRevalidation,
            "requires-revalidation",
            true,
        ),
        DataViewContractError::UnsupportedSchema => {
            (DataViewCommitStatus::Rejected, "unsupported-schema", false)
        }
        DataViewContractError::LimitExceeded(_) => (
            DataViewCommitStatus::Rejected,
            "bounded-limit-exceeded",
            false,
        ),
        _ => (DataViewCommitStatus::Rejected, "submission-rejected", false),
    };
    DataViewCommitOutcome {
        submission_id: submission_id.to_string(),
        status,
        data_view,
        delta_ref: None,
        conflict_version_refs: Vec::new(),
        residual_code: Some(residual_code.to_string()),
        retryable,
    }
}

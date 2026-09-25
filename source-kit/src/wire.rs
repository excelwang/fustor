use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use capanix_app_sdk::{CnxError, Result};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const SOURCE_WIRE_VERSION: u16 = 2;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceExecutionLimits {
    pub max_rows: u64,
    pub max_bytes: u64,
    pub max_calls: u64,
    pub max_fanout: u64,
    pub max_latency_ms: u64,
    pub max_cost_units: u64,
}

impl SourceExecutionLimits {
    pub fn validate(&self) -> Result<()> {
        if self.max_rows == 0 {
            return Err(CnxError::InvalidInput("max_rows must be positive".into()));
        }
        if self.max_bytes == 0 {
            return Err(CnxError::InvalidInput("max_bytes must be positive".into()));
        }
        if self.max_calls == 0 {
            return Err(CnxError::InvalidInput("max_calls must be positive".into()));
        }
        if self.max_fanout == 0 {
            return Err(CnxError::InvalidInput("max_fanout must be positive".into()));
        }
        if self.max_latency_ms == 0 {
            return Err(CnxError::InvalidInput(
                "max_latency_ms must be positive".into(),
            ));
        }
        if self.max_cost_units == 0 {
            return Err(CnxError::InvalidInput(
                "max_cost_units must be positive".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceExecutionContext {
    pub request_id: String,
    pub grant_epoch: u64,
    pub issued_at_ms: u64,
    pub deadline_ms: u64,
    pub limits: SourceExecutionLimits,
}

impl SourceExecutionContext {
    pub fn one_shot(
        request_id: impl Into<String>,
        grant_epoch: u64,
        issued_at_ms: u64,
        deadline_ms: u64,
        max_rows: u64,
        max_bytes: u64,
    ) -> Self {
        Self {
            request_id: request_id.into(),
            grant_epoch,
            issued_at_ms,
            deadline_ms,
            limits: SourceExecutionLimits {
                max_rows,
                max_bytes,
                max_calls: 1,
                max_fanout: 1,
                max_latency_ms: deadline_ms.saturating_sub(issued_at_ms),
                max_cost_units: 1,
            },
        }
    }

    pub fn validate(&self, now_ms: u64) -> Result<()> {
        if self.request_id.trim().is_empty() {
            return Err(CnxError::InvalidInput(
                "source request_id must be non-empty".into(),
            ));
        }
        if self.grant_epoch == 0 {
            return Err(CnxError::InvalidInput(
                "source grant_epoch must be positive".into(),
            ));
        }
        if self.deadline_ms <= self.issued_at_ms {
            return Err(CnxError::InvalidInput(
                "source deadline_ms must be after issued_at_ms".into(),
            ));
        }
        self.limits.validate()?;
        if self.issued_at_ms > now_ms {
            return Err(CnxError::InvalidInput(
                "source issued_at_ms must not be in the future".into(),
            ));
        }
        if self.limits.max_latency_ms > self.deadline_ms - self.issued_at_ms {
            return Err(CnxError::InvalidInput(
                "max_latency_ms exceeds source execution deadline window".into(),
            ));
        }
        if now_ms > self.deadline_ms {
            return Err(CnxError::Timeout);
        }
        Ok(())
    }
}

pub fn source_operation_timeout(
    context: Option<&SourceExecutionContext>,
    now_ms: u64,
) -> Result<Option<Duration>> {
    let Some(context) = context else {
        return Ok(None);
    };
    context.validate(now_ms)?;
    let deadline_remaining_ms = context.deadline_ms.saturating_sub(now_ms);
    let elapsed_ms = now_ms.saturating_sub(context.issued_at_ms);
    let latency_remaining_ms = context.limits.max_latency_ms.saturating_sub(elapsed_ms);
    let remaining_ms = deadline_remaining_ms.min(latency_remaining_ms);
    if remaining_ms == 0 {
        return Err(CnxError::Timeout);
    }
    Ok(Some(Duration::from_millis(remaining_ms)))
}

#[derive(Debug, Clone)]
pub struct SourceOperationBudget {
    started: Instant,
    timeout: Option<Duration>,
    max_bytes: Option<u64>,
    consumed_bytes: u64,
    max_calls: Option<u64>,
    max_fanout: Option<u64>,
    max_cost_units: Option<u64>,
    calls: u64,
    fanout: u64,
    cost_units: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceOperationAccounting {
    pub calls: u64,
    pub fanout: u64,
    pub cost_units: u64,
}

impl SourceOperationAccounting {
    pub const fn one_call() -> Self {
        Self {
            calls: 1,
            fanout: 1,
            cost_units: 1,
        }
    }
}

impl SourceOperationBudget {
    pub fn from_context(context: Option<&SourceExecutionContext>, now_ms: u64) -> Result<Self> {
        let timeout = source_operation_timeout(context, now_ms)?;
        let mut budget = Self::new(timeout, context.map(|context| context.limits.max_bytes));
        if let Some(context) = context {
            budget.max_calls = Some(context.limits.max_calls);
            budget.max_fanout = Some(context.limits.max_fanout);
            budget.max_cost_units = Some(context.limits.max_cost_units);
        }
        Ok(budget)
    }

    pub fn new(timeout: Option<Duration>, max_bytes: Option<u64>) -> Self {
        Self {
            started: Instant::now(),
            timeout,
            max_bytes,
            consumed_bytes: 0,
            max_calls: None,
            max_fanout: None,
            max_cost_units: None,
            calls: 0,
            fanout: 0,
            cost_units: 0,
        }
    }

    pub fn unbounded() -> Self {
        Self::new(None, None)
    }

    pub fn check_deadline(&self) -> Result<()> {
        if self
            .timeout
            .is_some_and(|timeout| self.started.elapsed() >= timeout)
        {
            Err(CnxError::Timeout)
        } else {
            Ok(())
        }
    }

    pub fn remaining_timeout(&self) -> Result<Option<Duration>> {
        self.check_deadline()?;
        Ok(self
            .timeout
            .map(|timeout| timeout.saturating_sub(self.started.elapsed())))
    }

    pub fn consume_bytes(&mut self, bytes: u64) -> Result<()> {
        self.check_deadline()?;
        let consumed = self.consumed_bytes.checked_add(bytes).ok_or_else(|| {
            CnxError::ResourceExhausted("source operation byte accounting overflow".into())
        })?;
        if self.max_bytes.is_some_and(|max_bytes| consumed > max_bytes) {
            return Err(CnxError::ResourceExhausted(
                "source operation max_bytes exhausted".into(),
            ));
        }
        self.consumed_bytes = consumed;
        Ok(())
    }

    pub fn remaining_bytes(&self) -> Option<u64> {
        self.max_bytes
            .map(|max_bytes| max_bytes.saturating_sub(self.consumed_bytes))
    }

    pub fn consumed_bytes(&self) -> u64 {
        self.consumed_bytes
    }

    pub fn observe_fanout(&mut self, fanout: u64) -> Result<()> {
        self.check_deadline()?;
        if self
            .max_fanout
            .is_some_and(|max_fanout| fanout > max_fanout)
        {
            return Err(CnxError::ResourceExhausted(
                "source operation max_fanout exhausted".into(),
            ));
        }
        self.fanout = self.fanout.max(fanout);
        Ok(())
    }

    pub fn consume_call(&mut self, cost_units: u64) -> Result<()> {
        self.check_deadline()?;
        let calls = self.calls.checked_add(1).ok_or_else(|| {
            CnxError::ResourceExhausted("source operation call accounting overflow".into())
        })?;
        let cost = self.cost_units.checked_add(cost_units).ok_or_else(|| {
            CnxError::ResourceExhausted("source operation cost accounting overflow".into())
        })?;
        if self.max_calls.is_some_and(|max_calls| calls > max_calls) {
            return Err(CnxError::ResourceExhausted(
                "source operation max_calls exhausted".into(),
            ));
        }
        if self
            .max_cost_units
            .is_some_and(|max_cost_units| cost > max_cost_units)
        {
            return Err(CnxError::ResourceExhausted(
                "source operation max_cost_units exhausted".into(),
            ));
        }
        self.calls = calls;
        self.cost_units = cost;
        Ok(())
    }

    pub fn ensure_call_capacity(&self, calls: u64, cost_units: u64) -> Result<()> {
        self.check_deadline()?;
        let projected_calls = self.calls.checked_add(calls).ok_or_else(|| {
            CnxError::ResourceExhausted("source operation call accounting overflow".into())
        })?;
        let projected_cost = self.cost_units.checked_add(cost_units).ok_or_else(|| {
            CnxError::ResourceExhausted("source operation cost accounting overflow".into())
        })?;
        if self
            .max_calls
            .is_some_and(|max_calls| projected_calls > max_calls)
        {
            return Err(CnxError::ResourceExhausted(
                "source operation max_calls exhausted".into(),
            ));
        }
        if self
            .max_cost_units
            .is_some_and(|max_cost_units| projected_cost > max_cost_units)
        {
            return Err(CnxError::ResourceExhausted(
                "source operation max_cost_units exhausted".into(),
            ));
        }
        Ok(())
    }

    pub fn accounting(&self) -> SourceOperationAccounting {
        SourceOperationAccounting {
            calls: self.calls,
            fanout: self.fanout,
            cost_units: self.cost_units,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceExecutionEnvelope<T> {
    pub wire_version: u16,
    pub context: SourceExecutionContext,
    pub request: T,
}

impl<T> SourceExecutionEnvelope<T> {
    pub fn new(context: SourceExecutionContext, request: T) -> Self {
        Self {
            wire_version: SOURCE_WIRE_VERSION,
            context,
            request,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceNativeAnchor {
    pub anchor_kind: String,
    pub native_ref: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_pointer: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_version: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SourceContinuationState {
    Terminal,
    ContinuationAvailable,
    SessionClosed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceActualUsage {
    pub rows: u64,
    pub bytes: u64,
    pub calls: u64,
    pub fanout: u64,
    pub latency_ms: u64,
    pub cost_units: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceObservationReceipt {
    pub source_kind: String,
    pub authority_object_ref: String,
    pub operation: String,
    pub source_revision: String,
    #[serde(default)]
    pub native_anchors: Vec<SourceNativeAnchor>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    pub observed_at_ms: u64,
    pub grant_epoch: u64,
    pub continuation_state: SourceContinuationState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation_digest: Option<String>,
    pub actual: SourceActualUsage,
    pub payload_digest: String,
    pub receipt_digest: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceResponseEnvelope<T> {
    pub wire_version: u16,
    pub request_id: String,
    pub payload: T,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt: Option<SourceObservationReceipt>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourcePayloadObservation {
    pub authority_object_ref: String,
    pub native_anchors: Vec<SourceNativeAnchor>,
    pub grant_epoch: Option<u64>,
    pub continuation_state: SourceContinuationState,
    pub continuation: Option<Vec<u8>>,
    pub actual_rows: u64,
}

pub trait SourceRequestMetadata {
    fn source_kind(&self) -> &'static str;
    fn authority_object_ref(&self) -> &str;
    fn operation_name(&self) -> &'static str;
    fn materializable(&self) -> bool;
    fn closes_session(&self) -> bool {
        false
    }
}

pub trait SourcePayloadMetadata {
    fn source_observation(&self) -> Option<SourcePayloadObservation>;

    fn source_native_revision(&self) -> Option<&str> {
        None
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DecodedSourceRequest<T> {
    pub request: T,
    pub context: Option<SourceExecutionContext>,
}

impl<T> DecodedSourceRequest<T> {
    pub fn is_versioned(&self) -> bool {
        self.context.is_some()
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum SourceRequestWire<T> {
    Envelope(SourceExecutionEnvelope<T>),
    Legacy(T),
}

#[derive(Deserialize)]
#[serde(untagged)]
enum SourceResponseWire<T> {
    Envelope(Box<SourceResponseEnvelope<T>>),
    Legacy(T),
}

#[derive(Debug, Clone, PartialEq)]
pub enum DecodedSourceResponse<T> {
    Envelope(Box<SourceResponseEnvelope<T>>),
    Legacy(T),
}

pub fn encode_source_request<T: Serialize>(
    envelope: &SourceExecutionEnvelope<T>,
) -> Result<Vec<u8>> {
    rmp_serde::to_vec_named(envelope)
        .map_err(|err| CnxError::InvalidInput(format!("encode source request envelope: {err}")))
}

pub fn decode_source_request<T: DeserializeOwned>(
    payload: &[u8],
    now_ms: u64,
) -> Result<DecodedSourceRequest<T>> {
    let wire = rmp_serde::from_slice::<SourceRequestWire<T>>(payload)
        .map_err(|err| CnxError::InvalidInput(format!("decode source request: {err}")))?;
    match wire {
        SourceRequestWire::Envelope(envelope) => {
            if envelope.wire_version != SOURCE_WIRE_VERSION {
                return Err(CnxError::ProtocolViolation(format!(
                    "unsupported source wire_version {}",
                    envelope.wire_version
                )));
            }
            envelope.context.validate(now_ms)?;
            Ok(DecodedSourceRequest {
                request: envelope.request,
                context: Some(envelope.context),
            })
        }
        SourceRequestWire::Legacy(request) => Ok(DecodedSourceRequest {
            request,
            context: None,
        }),
    }
}

pub fn decode_source_response<T: DeserializeOwned + Serialize + SourcePayloadMetadata>(
    payload: &[u8],
) -> Result<DecodedSourceResponse<T>> {
    let wire = rmp_serde::from_slice::<SourceResponseWire<T>>(payload)
        .map_err(|err| CnxError::InvalidInput(format!("decode source response: {err}")))?;
    match wire {
        SourceResponseWire::Envelope(envelope) => {
            if envelope.wire_version != SOURCE_WIRE_VERSION {
                return Err(CnxError::ProtocolViolation(format!(
                    "unsupported source response wire_version {}",
                    envelope.wire_version
                )));
            }
            if let Some(receipt) = &envelope.receipt {
                validate_receipt_integrity(
                    payload,
                    &envelope.request_id,
                    &envelope.payload,
                    receipt,
                )?;
            }
            Ok(DecodedSourceResponse::Envelope(envelope))
        }
        SourceResponseWire::Legacy(payload) => Ok(DecodedSourceResponse::Legacy(payload)),
    }
}

pub fn encode_source_response<R, P>(
    decoded: &DecodedSourceRequest<R>,
    payload: P,
    elapsed_ms: u64,
    observed_at_ms: u64,
) -> Result<Vec<u8>>
where
    R: SourceRequestMetadata,
    P: SourcePayloadMetadata + Serialize,
{
    encode_source_response_with_accounting(
        decoded,
        payload,
        elapsed_ms,
        observed_at_ms,
        SourceOperationAccounting::one_call(),
    )
}

pub fn encode_source_response_with_accounting<R, P>(
    decoded: &DecodedSourceRequest<R>,
    payload: P,
    elapsed_ms: u64,
    observed_at_ms: u64,
    accounting: SourceOperationAccounting,
) -> Result<Vec<u8>>
where
    R: SourceRequestMetadata,
    P: SourcePayloadMetadata + Serialize,
{
    let Some(context) = decoded.context.as_ref() else {
        return rmp_serde::to_vec_named(&payload)
            .map_err(|err| CnxError::Internal(format!("encode legacy source response: {err}")));
    };
    context.limits.validate()?;
    let observation = payload.source_observation();
    let receipt = match (decoded.request.materializable(), observation) {
        (true, Some(observation)) => Some(build_receipt(
            &decoded.request,
            context,
            &payload,
            observation,
            elapsed_ms,
            observed_at_ms,
            accounting,
        )?),
        (true, None) | (false, _) => None,
    };
    let mut envelope = SourceResponseEnvelope {
        wire_version: SOURCE_WIRE_VERSION,
        request_id: context.request_id.clone(),
        payload,
        receipt,
    };
    encode_response_fixed_point(&mut envelope, &context.limits)
}

pub fn encode_source_result<R, P, F>(
    decoded: &DecodedSourceRequest<R>,
    result: Result<P>,
    elapsed_ms: u64,
    observed_at_ms: u64,
    error_payload: F,
) -> Result<Vec<u8>>
where
    R: SourceRequestMetadata,
    P: SourcePayloadMetadata + Serialize + Clone,
    F: Fn(CnxError) -> P,
{
    let payload = match result {
        Ok(payload) => payload,
        Err(error) => error_payload(error),
    };
    match encode_source_response(decoded, payload, elapsed_ms, observed_at_ms) {
        Ok(encoded) => Ok(encoded),
        Err(error) => {
            encode_source_response(decoded, error_payload(error), elapsed_ms, observed_at_ms)
        }
    }
}

pub fn encode_accounted_source_result<R, P, F>(
    decoded: &DecodedSourceRequest<R>,
    result: Result<(P, SourceOperationAccounting)>,
    elapsed_ms: u64,
    observed_at_ms: u64,
    error_payload: F,
) -> Result<Vec<u8>>
where
    R: SourceRequestMetadata,
    P: SourcePayloadMetadata + Serialize + Clone,
    F: Fn(CnxError) -> P,
{
    let (payload, accounting) = match result {
        Ok(result) => result,
        Err(error) => (
            error_payload(error),
            SourceOperationAccounting {
                calls: 0,
                fanout: 0,
                cost_units: 0,
            },
        ),
    };
    match encode_source_response_with_accounting(
        decoded,
        payload,
        elapsed_ms,
        observed_at_ms,
        accounting,
    ) {
        Ok(encoded) => Ok(encoded),
        Err(error) => encode_source_response_with_accounting(
            decoded,
            error_payload(error),
            elapsed_ms,
            observed_at_ms,
            accounting,
        ),
    }
}

pub fn validate_source_request_limit(
    context: Option<&SourceExecutionContext>,
    requested: Option<usize>,
) -> Result<()> {
    let (Some(context), Some(requested)) = (context, requested) else {
        return Ok(());
    };
    let requested = u64::try_from(requested)
        .map_err(|_| CnxError::InvalidInput("source request limit is too large".into()))?;
    if requested > context.limits.max_rows {
        return Err(CnxError::InvalidInput(format!(
            "source request limit={requested} exceeds max_rows={}",
            context.limits.max_rows
        )));
    }
    Ok(())
}

pub fn validate_source_grant_epoch(
    context: Option<&SourceExecutionContext>,
    current_grant_epoch: Option<u64>,
    source_kind: &str,
) -> Result<()> {
    let Some(context) = context else {
        return Ok(());
    };
    if current_grant_epoch != Some(context.grant_epoch) {
        return Err(CnxError::AccessDenied(format!(
            "{source_kind} source grant epoch changed before execution"
        )));
    }
    Ok(())
}

fn build_receipt<R: SourceRequestMetadata, P: Serialize + SourcePayloadMetadata>(
    request: &R,
    context: &SourceExecutionContext,
    payload: &P,
    observation: SourcePayloadObservation,
    elapsed_ms: u64,
    observed_at_ms: u64,
    accounting: SourceOperationAccounting,
) -> Result<SourceObservationReceipt> {
    if observation.authority_object_ref != request.authority_object_ref() {
        return Err(CnxError::ProtocolViolation(
            "source response authority object does not match request".into(),
        ));
    }
    let grant_epoch = observation.grant_epoch.unwrap_or(context.grant_epoch);
    if grant_epoch != context.grant_epoch {
        return Err(CnxError::AccessDenied(
            "source response grant_epoch does not match execution context".into(),
        ));
    }
    if observed_at_ms < context.issued_at_ms || observed_at_ms > context.deadline_ms {
        return Err(CnxError::Timeout);
    }
    let payload_digest = payload_digest_for(payload)?;
    let source_revision = source_revision_for(
        request.source_kind(),
        request.authority_object_ref(),
        request.operation_name(),
        payload,
        payload.source_native_revision(),
    )?;
    let continuation_state = if request.closes_session() {
        SourceContinuationState::SessionClosed
    } else {
        observation.continuation_state
    };
    let continuation_digest = observation
        .continuation
        .as_deref()
        .map(source_continuation_digest);
    let evidence_ref = evidence_ref_for(
        &context.request_id,
        &source_revision,
        grant_epoch,
        observed_at_ms,
        continuation_state,
        continuation_digest.as_deref(),
        &observation.native_anchors,
    )?;
    let actual = SourceActualUsage {
        rows: observation.actual_rows,
        bytes: 0,
        calls: accounting.calls,
        fanout: accounting.fanout,
        latency_ms: elapsed_ms,
        cost_units: accounting.cost_units,
    };
    validate_actual_usage(&actual, &context.limits)?;
    Ok(SourceObservationReceipt {
        source_kind: request.source_kind().into(),
        authority_object_ref: observation.authority_object_ref,
        operation: request.operation_name().into(),
        source_revision,
        native_anchors: observation.native_anchors,
        evidence_refs: vec![evidence_ref],
        observed_at_ms,
        grant_epoch,
        continuation_state,
        continuation_digest,
        actual,
        payload_digest,
        receipt_digest: String::new(),
    })
}

fn encode_response_fixed_point<T: Serialize>(
    envelope: &mut SourceResponseEnvelope<T>,
    limits: &SourceExecutionLimits,
) -> Result<Vec<u8>> {
    for _ in 0..8 {
        if let Some(receipt) = envelope.receipt.as_ref() {
            let digest = receipt_digest_for(&envelope.request_id, &envelope.payload, receipt)?;
            if let Some(receipt) = envelope.receipt.as_mut() {
                receipt.receipt_digest = digest;
            }
        }
        let encoded = rmp_serde::to_vec_named(&*envelope)
            .map_err(|err| CnxError::Internal(format!("encode source response envelope: {err}")))?;
        let encoded_len = encoded.len() as u64;
        let Some(receipt) = envelope.receipt.as_mut() else {
            return Ok(encoded);
        };
        if receipt.actual.bytes == encoded_len {
            validate_actual_usage(&receipt.actual, limits)?;
            return Ok(encoded);
        }
        receipt.actual.bytes = encoded_len;
    }
    Err(CnxError::Internal(
        "source response actual_bytes did not converge".into(),
    ))
}

fn validate_actual_usage(actual: &SourceActualUsage, limits: &SourceExecutionLimits) -> Result<()> {
    for (name, actual, limit) in [
        ("rows", actual.rows, limits.max_rows),
        ("bytes", actual.bytes, limits.max_bytes),
        ("calls", actual.calls, limits.max_calls),
        ("fanout", actual.fanout, limits.max_fanout),
        ("latency_ms", actual.latency_ms, limits.max_latency_ms),
        ("cost_units", actual.cost_units, limits.max_cost_units),
    ] {
        if actual > limit {
            return Err(CnxError::ResourceExhausted(format!(
                "source result actual_{name}={actual} exceeds max_{name}={limit}"
            )));
        }
    }
    Ok(())
}

fn validate_receipt_integrity<T: Serialize + SourcePayloadMetadata>(
    encoded: &[u8],
    request_id: &str,
    payload: &T,
    receipt: &SourceObservationReceipt,
) -> Result<()> {
    if request_id.trim().is_empty()
        || receipt.source_kind.trim().is_empty()
        || receipt.authority_object_ref.trim().is_empty()
        || receipt.operation.trim().is_empty()
        || receipt.source_revision.trim().is_empty()
        || receipt.native_anchors.is_empty()
        || receipt.evidence_refs.len() != 1
        || receipt.observed_at_ms == 0
        || receipt.grant_epoch == 0
        || receipt.payload_digest.trim().is_empty()
        || receipt.receipt_digest.trim().is_empty()
    {
        return Err(CnxError::ProtocolViolation(
            "source observation receipt is incomplete".into(),
        ));
    }
    if receipt
        .native_anchors
        .iter()
        .any(|anchor| anchor.anchor_kind.trim().is_empty() || anchor.native_ref.trim().is_empty())
        || receipt
            .evidence_refs
            .iter()
            .any(|evidence| evidence.trim().is_empty())
    {
        return Err(CnxError::ProtocolViolation(
            "source observation receipt contains an incomplete anchor or evidence ref".into(),
        ));
    }
    if receipt.actual.bytes != encoded.len() as u64 {
        return Err(CnxError::ProtocolViolation(
            "source observation receipt actual_bytes mismatch".into(),
        ));
    }
    let expected_payload_digest = payload_digest_for(payload)?;
    if receipt.payload_digest != expected_payload_digest {
        return Err(CnxError::ProtocolViolation(
            "source observation receipt payload digest mismatch".into(),
        ));
    }
    let expected_revision = source_revision_for(
        &receipt.source_kind,
        &receipt.authority_object_ref,
        &receipt.operation,
        payload,
        payload.source_native_revision(),
    )?;
    if receipt.source_revision != expected_revision {
        return Err(CnxError::ProtocolViolation(
            "source observation receipt payload revision mismatch".into(),
        ));
    }
    let expected_evidence = evidence_ref_for(
        request_id,
        &receipt.source_revision,
        receipt.grant_epoch,
        receipt.observed_at_ms,
        receipt.continuation_state,
        receipt.continuation_digest.as_deref(),
        &receipt.native_anchors,
    )?;
    if receipt.evidence_refs != [expected_evidence] {
        return Err(CnxError::ProtocolViolation(
            "source observation receipt evidence binding mismatch".into(),
        ));
    }
    let expected_receipt_digest = receipt_digest_for(request_id, payload, receipt)?;
    if receipt.receipt_digest != expected_receipt_digest {
        return Err(CnxError::ProtocolViolation(
            "source observation receipt integrity digest mismatch".into(),
        ));
    }
    Ok(())
}

fn payload_digest_for<T: Serialize>(payload: &T) -> Result<String> {
    let payload_bytes = rmp_serde::to_vec_named(payload)
        .map_err(|err| CnxError::Internal(format!("encode source payload for digest: {err}")))?;
    Ok(format!(
        "source-payload:{}",
        digest_parts(&[&payload_bytes])
    ))
}

pub fn source_continuation_digest(continuation: &[u8]) -> String {
    format!("source-continuation:{}", digest_parts(&[continuation]))
}

#[derive(Serialize)]
struct ReceiptIntegrityMaterial<'a> {
    request_id: &'a str,
    source_kind: &'a str,
    authority_object_ref: &'a str,
    operation: &'a str,
    source_revision: &'a str,
    native_anchors: &'a [SourceNativeAnchor],
    evidence_refs: &'a [String],
    observed_at_ms: u64,
    grant_epoch: u64,
    continuation_state: SourceContinuationState,
    continuation_digest: Option<&'a str>,
    actual: &'a SourceActualUsage,
    payload_digest: &'a str,
}

fn receipt_digest_for<T: Serialize>(
    request_id: &str,
    payload: &T,
    receipt: &SourceObservationReceipt,
) -> Result<String> {
    let payload_digest = payload_digest_for(payload)?;
    if receipt.payload_digest != payload_digest {
        return Err(CnxError::ProtocolViolation(
            "source observation receipt payload digest mismatch".into(),
        ));
    }
    let material = ReceiptIntegrityMaterial {
        request_id,
        source_kind: &receipt.source_kind,
        authority_object_ref: &receipt.authority_object_ref,
        operation: &receipt.operation,
        source_revision: &receipt.source_revision,
        native_anchors: &receipt.native_anchors,
        evidence_refs: &receipt.evidence_refs,
        observed_at_ms: receipt.observed_at_ms,
        grant_epoch: receipt.grant_epoch,
        continuation_state: receipt.continuation_state,
        continuation_digest: receipt.continuation_digest.as_deref(),
        actual: &receipt.actual,
        payload_digest: &receipt.payload_digest,
    };
    let encoded = rmp_serde::to_vec_named(&material).map_err(|err| {
        CnxError::Internal(format!("encode source receipt integrity material: {err}"))
    })?;
    Ok(format!("source-receipt:{}", digest_parts(&[&encoded])))
}

fn source_revision_for<T: Serialize>(
    source_kind: &str,
    authority_object_ref: &str,
    operation: &str,
    payload: &T,
    native_revision: Option<&str>,
) -> Result<String> {
    if let Some(native_revision) = native_revision {
        if native_revision.trim().is_empty() {
            return Err(CnxError::ProtocolViolation(
                "source-native revision must be non-empty".into(),
            ));
        }
        return Ok(native_revision.to_string());
    }
    let payload_bytes = rmp_serde::to_vec_named(payload)
        .map_err(|err| CnxError::Internal(format!("encode source payload for receipt: {err}")))?;
    Ok(format!(
        "source-revision:{}",
        digest_parts(&[
            source_kind.as_bytes(),
            authority_object_ref.as_bytes(),
            operation.as_bytes(),
            &payload_bytes,
        ])
    ))
}

pub fn source_native_revision_digest(kind: &str, parts: &[&[u8]]) -> String {
    format!("{kind}:{}", digest_parts(parts))
}

fn evidence_ref_for(
    request_id: &str,
    source_revision: &str,
    grant_epoch: u64,
    observed_at_ms: u64,
    continuation_state: SourceContinuationState,
    continuation_digest: Option<&str>,
    native_anchors: &[SourceNativeAnchor],
) -> Result<String> {
    let anchors = rmp_serde::to_vec_named(native_anchors)
        .map_err(|err| CnxError::Internal(format!("encode source anchors for receipt: {err}")))?;
    let continuation = match continuation_state {
        SourceContinuationState::Terminal => b"terminal".as_slice(),
        SourceContinuationState::ContinuationAvailable => b"continuation-available".as_slice(),
        SourceContinuationState::SessionClosed => b"session-closed".as_slice(),
    };
    Ok(format!(
        "source-evidence:{}",
        digest_parts(&[
            request_id.as_bytes(),
            source_revision.as_bytes(),
            &grant_epoch.to_be_bytes(),
            &observed_at_ms.to_be_bytes(),
            continuation,
            continuation_digest.unwrap_or_default().as_bytes(),
            &anchors,
        ])
    ))
}

fn digest_parts(parts: &[&[u8]]) -> String {
    let mut digest = Sha256::new();
    for part in parts {
        digest.update((part.len() as u64).to_be_bytes());
        digest.update(part);
    }
    let bytes = digest.finalize();
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push_str(&format!("{byte:02x}"));
    }
    encoded
}

pub fn source_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct TestRequest {
        object_ref: String,
    }

    impl SourceRequestMetadata for TestRequest {
        fn source_kind(&self) -> &'static str {
            "test"
        }

        fn authority_object_ref(&self) -> &str {
            &self.object_ref
        }

        fn operation_name(&self) -> &'static str {
            "snapshot-page"
        }

        fn materializable(&self) -> bool {
            true
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct TestPayload {
        object_ref: String,
        rows: Vec<String>,
    }

    impl SourcePayloadMetadata for TestPayload {
        fn source_observation(&self) -> Option<SourcePayloadObservation> {
            Some(SourcePayloadObservation {
                authority_object_ref: self.object_ref.clone(),
                native_anchors: vec![SourceNativeAnchor {
                    anchor_kind: "test-scope".into(),
                    native_ref: self.object_ref.clone(),
                    native_pointer: None,
                    native_version: None,
                }],
                grant_epoch: Some(7),
                continuation_state: SourceContinuationState::Terminal,
                continuation: None,
                actual_rows: self.rows.len() as u64,
            })
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct NativeRevisionPayload {
        object_ref: String,
        source_revision: String,
    }

    impl SourcePayloadMetadata for NativeRevisionPayload {
        fn source_observation(&self) -> Option<SourcePayloadObservation> {
            Some(SourcePayloadObservation {
                authority_object_ref: self.object_ref.clone(),
                native_anchors: vec![SourceNativeAnchor {
                    anchor_kind: "test-scope".into(),
                    native_ref: self.object_ref.clone(),
                    native_pointer: None,
                    native_version: Some(self.source_revision.clone()),
                }],
                grant_epoch: Some(7),
                continuation_state: SourceContinuationState::Terminal,
                continuation: None,
                actual_rows: 0,
            })
        }

        fn source_native_revision(&self) -> Option<&str> {
            Some(self.source_revision.as_str())
        }
    }

    fn context() -> SourceExecutionContext {
        SourceExecutionContext {
            request_id: "request:1".into(),
            grant_epoch: 7,
            issued_at_ms: 100,
            deadline_ms: 1_100,
            limits: SourceExecutionLimits {
                max_rows: 10,
                max_bytes: 8_192,
                max_calls: 1,
                max_fanout: 1,
                max_latency_ms: 1_000,
                max_cost_units: 1,
            },
        }
    }

    #[test]
    fn versioned_source_roundtrip_preserves_receipt_and_exact_wire_bytes() {
        let request = SourceExecutionEnvelope::new(
            context(),
            TestRequest {
                object_ref: "source:test".into(),
            },
        );
        let request_bytes = encode_source_request(&request).expect("encode request");
        let decoded = decode_source_request::<TestRequest>(&request_bytes, 200).expect("decode");
        let response = encode_source_response(
            &decoded,
            TestPayload {
                object_ref: "source:test".into(),
                rows: vec!["a".into(), "b".into()],
            },
            5,
            205,
        )
        .expect("encode response");
        let DecodedSourceResponse::Envelope(response_envelope) =
            decode_source_response::<TestPayload>(&response).expect("decode response")
        else {
            panic!("expected versioned response");
        };
        let receipt = response_envelope.receipt.expect("receipt");
        assert_eq!(receipt.actual.rows, 2);
        assert_eq!(receipt.actual.bytes, response.len() as u64);
        assert_eq!(receipt.grant_epoch, 7);
        assert!(receipt.source_revision.starts_with("source-revision:"));
        assert!(receipt.evidence_refs[0].starts_with("source-evidence:"));
    }

    #[test]
    fn native_source_revision_is_preserved_exactly_and_empty_revision_fails_closed() {
        let request = SourceExecutionEnvelope::new(
            context(),
            TestRequest {
                object_ref: "source:test".into(),
            },
        );
        let request_bytes = encode_source_request(&request).expect("encode request");
        let decoded =
            decode_source_request::<TestRequest>(&request_bytes, 200).expect("decode request");
        let response = encode_source_response(
            &decoded,
            NativeRevisionPayload {
                object_ref: "source:test".into(),
                source_revision: "native-revision:42".into(),
            },
            5,
            205,
        )
        .expect("encode native revision response");
        let DecodedSourceResponse::Envelope(envelope) =
            decode_source_response::<NativeRevisionPayload>(&response).expect("decode response")
        else {
            panic!("expected versioned response");
        };
        assert_eq!(
            envelope.receipt.expect("receipt").source_revision,
            "native-revision:42"
        );

        let error = encode_source_response(
            &decoded,
            NativeRevisionPayload {
                object_ref: "source:test".into(),
                source_revision: String::new(),
            },
            5,
            205,
        )
        .expect_err("empty native revision must fail closed");
        assert!(matches!(error, CnxError::ProtocolViolation(_)));
    }

    #[test]
    fn source_execution_limits_fail_closed() {
        let request = SourceExecutionEnvelope::new(
            SourceExecutionContext {
                limits: SourceExecutionLimits {
                    max_rows: 1,
                    ..context().limits
                },
                ..context()
            },
            TestRequest {
                object_ref: "source:test".into(),
            },
        );
        let bytes = encode_source_request(&request).expect("encode request");
        let decoded = decode_source_request::<TestRequest>(&bytes, 200).expect("decode");
        let error = encode_source_response(
            &decoded,
            TestPayload {
                object_ref: "source:test".into(),
                rows: vec!["a".into(), "b".into()],
            },
            5,
            205,
        )
        .expect_err("row limit must fail");
        assert!(error.to_string().contains("actual_rows=2"));
    }

    #[test]
    fn accounted_response_binds_real_calls_fanout_and_cost() {
        let execution_context = SourceExecutionContext {
            limits: SourceExecutionLimits {
                max_calls: 2,
                max_cost_units: 2,
                ..context().limits
            },
            ..context()
        };
        let request = SourceExecutionEnvelope::new(
            execution_context,
            TestRequest {
                object_ref: "source:test".into(),
            },
        );
        let encoded = encode_source_request(&request).expect("request");
        let decoded = decode_source_request::<TestRequest>(&encoded, 200).expect("decode");
        let response = encode_source_response_with_accounting(
            &decoded,
            TestPayload {
                object_ref: "source:test".into(),
                rows: vec!["a".into()],
            },
            5,
            205,
            SourceOperationAccounting {
                calls: 2,
                fanout: 1,
                cost_units: 2,
            },
        )
        .expect("accounted response");
        let DecodedSourceResponse::Envelope(response) =
            decode_source_response::<TestPayload>(&response).expect("decode response")
        else {
            panic!("expected envelope");
        };
        let receipt = response.receipt.expect("receipt");
        assert_eq!(receipt.actual.calls, 2);
        assert_eq!(receipt.actual.fanout, 1);
        assert_eq!(receipt.actual.cost_units, 2);
    }

    #[test]
    fn expired_source_request_is_rejected_before_execution() {
        let request = SourceExecutionEnvelope::new(
            context(),
            TestRequest {
                object_ref: "source:test".into(),
            },
        );
        let bytes = encode_source_request(&request).expect("encode request");
        assert!(matches!(
            decode_source_request::<TestRequest>(&bytes, 1_101),
            Err(CnxError::Timeout)
        ));
    }

    #[test]
    fn operation_timeout_uses_the_stricter_deadline_or_latency_budget() {
        let mut bounded = context();
        bounded.limits.max_latency_ms = 250;
        assert_eq!(
            source_operation_timeout(Some(&bounded), 200).expect("remaining timeout"),
            Some(Duration::from_millis(150))
        );
        assert!(matches!(
            source_operation_timeout(Some(&bounded), 350),
            Err(CnxError::Timeout)
        ));

        bounded.issued_at_ms = 400;
        assert!(matches!(
            source_operation_timeout(Some(&bounded), 350),
            Err(CnxError::InvalidInput(_))
        ));
        assert_eq!(
            source_operation_timeout(None, 350).expect("legacy timeout"),
            None
        );
    }

    #[test]
    fn legacy_source_wire_remains_explicitly_distinguishable() {
        let request = TestRequest {
            object_ref: "source:test".into(),
        };
        let bytes = rmp_serde::to_vec_named(&request).expect("encode legacy");
        let decoded = decode_source_request::<TestRequest>(&bytes, 200).expect("decode legacy");
        assert!(!decoded.is_versioned());
    }

    #[test]
    fn response_decode_rejects_payload_and_receipt_tampering() {
        let request = SourceExecutionEnvelope::new(
            context(),
            TestRequest {
                object_ref: "source:test".into(),
            },
        );
        let request_bytes = encode_source_request(&request).expect("encode request");
        let decoded = decode_source_request::<TestRequest>(&request_bytes, 200).expect("decode");
        let encoded = encode_source_response(
            &decoded,
            TestPayload {
                object_ref: "source:test".into(),
                rows: vec!["a".into()],
            },
            5,
            205,
        )
        .expect("encode response");
        let mut envelope: SourceResponseEnvelope<TestPayload> =
            rmp_serde::from_slice(&encoded).expect("decode raw response");

        envelope.payload.rows[0] = "b".into();
        let tampered = rmp_serde::to_vec_named(&envelope).expect("encode tampered payload");
        assert!(matches!(
            decode_source_response::<TestPayload>(&tampered),
            Err(CnxError::ProtocolViolation(_))
        ));

        envelope.payload.rows[0] = "a".into();
        envelope.receipt.as_mut().unwrap().native_anchors[0].native_ref = "source:tesu".into();
        let tampered = rmp_serde::to_vec_named(&envelope).expect("encode tampered receipt");
        assert!(matches!(
            decode_source_response::<TestPayload>(&tampered),
            Err(CnxError::ProtocolViolation(_))
        ));
    }
}

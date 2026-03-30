use std::sync::{Arc, RwLock};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FacadePendingReason {
    AwaitingRuntimeExposure,
    AwaitingObservationEligibility,
    RetryingAfterError,
}

impl FacadePendingReason {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::AwaitingRuntimeExposure => "awaiting_runtime_exposure",
            Self::AwaitingObservationEligibility => "awaiting_observation_eligibility",
            Self::RetryingAfterError => "retrying_after_error",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SharedFacadePendingStatus {
    pub(crate) route_key: String,
    pub(crate) generation: u64,
    pub(crate) resource_ids: Vec<String>,
    pub(crate) runtime_managed: bool,
    pub(crate) runtime_exposure_confirmed: bool,
    pub(crate) reason: FacadePendingReason,
    pub(crate) retry_attempts: u64,
    pub(crate) pending_since_us: u64,
    pub(crate) last_error: Option<String>,
    pub(crate) last_attempt_at_us: Option<u64>,
    pub(crate) last_error_at_us: Option<u64>,
    pub(crate) retry_backoff_ms: Option<u64>,
    pub(crate) next_retry_at_us: Option<u64>,
}

pub(crate) type SharedFacadePendingStatusCell = Arc<RwLock<Option<SharedFacadePendingStatus>>>;

pub(crate) fn shared_facade_pending_status_cell() -> SharedFacadePendingStatusCell {
    Arc::new(RwLock::new(None))
}

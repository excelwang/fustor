use crate::domain_state::FacadeServiceState;
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
pub(crate) type SharedFacadeServiceStateCell = Arc<RwLock<FacadeServiceState>>;

#[derive(Debug, Clone)]
pub(crate) struct PublishedFacadeStatusSnapshot {
    pub(crate) state: FacadeServiceState,
    pub(crate) pending: Option<SharedFacadePendingStatus>,
}

#[derive(Clone)]
pub(crate) struct PublishedFacadeStatusReader {
    facade_service_state: SharedFacadeServiceStateCell,
    facade_pending_status: SharedFacadePendingStatusCell,
}

impl PublishedFacadeStatusReader {
    pub(crate) fn new(
        facade_service_state: SharedFacadeServiceStateCell,
        facade_pending_status: SharedFacadePendingStatusCell,
    ) -> Self {
        Self {
            facade_service_state,
            facade_pending_status,
        }
    }

    pub(crate) fn snapshot(&self) -> PublishedFacadeStatusSnapshot {
        read_published_facade_status(&self.facade_service_state, &self.facade_pending_status)
    }
}

pub(crate) fn shared_facade_pending_status_cell() -> SharedFacadePendingStatusCell {
    Arc::new(RwLock::new(None))
}

pub(crate) fn shared_facade_service_state_cell() -> SharedFacadeServiceStateCell {
    Arc::new(RwLock::new(FacadeServiceState::Unavailable))
}

pub(crate) fn read_published_facade_status(
    facade_service_state: &SharedFacadeServiceStateCell,
    facade_pending_status: &SharedFacadePendingStatusCell,
) -> PublishedFacadeStatusSnapshot {
    PublishedFacadeStatusSnapshot {
        state: *facade_service_state
            .read()
            .expect("read facade service state"),
        pending: facade_pending_status
            .read()
            .ok()
            .and_then(|pending| pending.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_facade_service_state_cell_defaults_to_unavailable() {
        assert_eq!(
            *shared_facade_service_state_cell()
                .read()
                .expect("read facade service state"),
            FacadeServiceState::Unavailable
        );
    }

    #[test]
    fn read_published_facade_status_returns_state_and_pending_snapshot() {
        let facade_service_state = shared_facade_service_state_cell();
        let facade_pending_status = shared_facade_pending_status_cell();
        *facade_service_state
            .write()
            .expect("write facade service state") = FacadeServiceState::Pending;
        *facade_pending_status
            .write()
            .expect("write facade pending status") = Some(SharedFacadePendingStatus {
            route_key: "route".to_string(),
            generation: 2,
            resource_ids: vec!["listener-a".to_string()],
            runtime_managed: true,
            runtime_exposure_confirmed: false,
            reason: FacadePendingReason::AwaitingRuntimeExposure,
            retry_attempts: 0,
            pending_since_us: 11,
            last_error: None,
            last_attempt_at_us: None,
            last_error_at_us: None,
            retry_backoff_ms: None,
            next_retry_at_us: None,
        });

        let snapshot = read_published_facade_status(&facade_service_state, &facade_pending_status);

        assert_eq!(snapshot.state, FacadeServiceState::Pending);
        let pending = snapshot.pending.expect("pending");
        assert_eq!(pending.route_key, "route");
        assert_eq!(pending.generation, 2);
        assert_eq!(pending.reason, FacadePendingReason::AwaitingRuntimeExposure);
    }

    #[test]
    fn published_facade_status_reader_returns_state_and_pending_snapshot() {
        let facade_service_state = shared_facade_service_state_cell();
        let facade_pending_status = shared_facade_pending_status_cell();
        *facade_service_state
            .write()
            .expect("write facade service state") = FacadeServiceState::Serving;
        *facade_pending_status
            .write()
            .expect("write facade pending status") = Some(SharedFacadePendingStatus {
            route_key: "route".to_string(),
            generation: 3,
            resource_ids: vec!["listener-b".to_string()],
            runtime_managed: true,
            runtime_exposure_confirmed: true,
            reason: FacadePendingReason::RetryingAfterError,
            retry_attempts: 2,
            pending_since_us: 22,
            last_error: Some("bind failed".to_string()),
            last_attempt_at_us: Some(30),
            last_error_at_us: Some(29),
            retry_backoff_ms: Some(250),
            next_retry_at_us: Some(280),
        });

        let snapshot = PublishedFacadeStatusReader::new(
            facade_service_state.clone(),
            facade_pending_status.clone(),
        )
        .snapshot();

        assert_eq!(snapshot.state, FacadeServiceState::Serving);
        let pending = snapshot.pending.expect("pending");
        assert_eq!(pending.route_key, "route");
        assert_eq!(pending.generation, 3);
        assert_eq!(pending.reason, FacadePendingReason::RetryingAfterError);
    }
}

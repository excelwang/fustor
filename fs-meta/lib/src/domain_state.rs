use capanix_managed_state_sdk::{ObservationState, ObservationStatus};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GroupServiceState {
    NotSelected,
    SelectedPending,
    ServingTrusted,
    ServingDegraded,
    Retiring,
    Retired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum QueryObservationState {
    Unavailable,
    FreshOnly,
    MaterializedUntrusted,
    TrustedMaterialized,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FacadeServiceState {
    Unavailable,
    Pending,
    Serving,
    Degraded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RolloutGenerationState {
    CatchUp,
    Eligible,
    Cutover,
    Drain,
    Retire,
    Stable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NodeParticipationState {
    Absent,
    Joining,
    Serving,
    Degraded,
    Retiring,
    Retired,
}

impl GroupServiceState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotSelected => "not-selected",
            Self::SelectedPending => "selected-pending",
            Self::ServingTrusted => "serving-trusted",
            Self::ServingDegraded => "serving-degraded",
            Self::Retiring => "retiring",
            Self::Retired => "retired",
        }
    }
}

impl QueryObservationState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unavailable => "unavailable",
            Self::FreshOnly => "fresh-only",
            Self::MaterializedUntrusted => "materialized-untrusted",
            Self::TrustedMaterialized => "trusted-materialized",
        }
    }
}

impl FacadeServiceState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unavailable => "unavailable",
            Self::Pending => "pending",
            Self::Serving => "serving",
            Self::Degraded => "degraded",
        }
    }
}

impl RolloutGenerationState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CatchUp => "catch-up",
            Self::Eligible => "eligible",
            Self::Cutover => "cutover",
            Self::Drain => "drain",
            Self::Retire => "retire",
            Self::Stable => "stable",
        }
    }
}

impl NodeParticipationState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Absent => "absent",
            Self::Joining => "joining",
            Self::Serving => "serving",
            Self::Degraded => "degraded",
            Self::Retiring => "retiring",
            Self::Retired => "retired",
        }
    }
}

impl From<ObservationState> for QueryObservationState {
    fn from(value: ObservationState) -> Self {
        match value {
            ObservationState::FreshOnly => Self::FreshOnly,
            ObservationState::MaterializedUntrusted => Self::MaterializedUntrusted,
            ObservationState::TrustedMaterialized => Self::TrustedMaterialized,
        }
    }
}

impl From<&ObservationStatus> for QueryObservationState {
    fn from(value: &ObservationStatus) -> Self {
        value.state.into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_observation_state_tracks_managed_state_sdk_names() {
        assert_eq!(
            QueryObservationState::from(&ObservationStatus::fresh_only()).as_str(),
            "fresh-only"
        );
        assert_eq!(
            QueryObservationState::from(&ObservationStatus::default()).as_str(),
            "materialized-untrusted"
        );
        assert_eq!(
            QueryObservationState::from(&ObservationStatus::trusted_materialized()).as_str(),
            "trusted-materialized"
        );
    }
}

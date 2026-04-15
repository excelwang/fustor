use capanix_managed_state_sdk::{ObservationState, ObservationStatus};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum GroupServiceState {
    NotSelected,
    SelectedPending,
    ServingTrusted,
    ServingDegraded,
    Retiring,
    Retired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum QueryObservationState {
    Unavailable,
    FreshOnly,
    MaterializedUntrusted,
    TrustedMaterialized,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum FacadeServiceState {
    Unavailable,
    Pending,
    Serving,
    Degraded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum RolloutGenerationState {
    CatchUp,
    Eligible,
    Cutover,
    Drain,
    Retire,
    Stable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum NodeParticipationState {
    Absent,
    Joining,
    Serving,
    Degraded,
    Retiring,
    Retired,
}

impl QueryObservationState {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Unavailable => "unavailable",
            Self::FreshOnly => "fresh-only",
            Self::MaterializedUntrusted => "materialized-untrusted",
            Self::TrustedMaterialized => "trusted-materialized",
        }
    }
}

impl FacadeServiceState {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Unavailable => "unavailable",
            Self::Pending => "pending",
            Self::Serving => "serving",
            Self::Degraded => "degraded",
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

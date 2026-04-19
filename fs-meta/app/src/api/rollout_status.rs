use crate::domain_state::RolloutGenerationState;
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PublishedRolloutStatusSnapshot {
    pub(crate) state: RolloutGenerationState,
    pub(crate) serving_generation: Option<u64>,
    pub(crate) candidate_generation: Option<u64>,
    pub(crate) retiring_generation: Option<u64>,
}

pub(crate) type SharedRolloutStatusCell = Arc<RwLock<PublishedRolloutStatusSnapshot>>;

#[derive(Clone)]
pub(crate) struct PublishedRolloutStatusReader {
    rollout_status: SharedRolloutStatusCell,
}

impl PublishedRolloutStatusReader {
    pub(crate) fn new(rollout_status: SharedRolloutStatusCell) -> Self {
        Self { rollout_status }
    }

    pub(crate) fn snapshot(&self) -> PublishedRolloutStatusSnapshot {
        read_published_rollout_status(&self.rollout_status)
    }
}

pub(crate) fn shared_rollout_status_cell() -> SharedRolloutStatusCell {
    Arc::new(RwLock::new(PublishedRolloutStatusSnapshot {
        state: RolloutGenerationState::CatchUp,
        serving_generation: None,
        candidate_generation: None,
        retiring_generation: None,
    }))
}

pub(crate) fn read_published_rollout_status(
    rollout_status: &SharedRolloutStatusCell,
) -> PublishedRolloutStatusSnapshot {
    *rollout_status.read().expect("read rollout status")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_rollout_status_defaults_to_catch_up() {
        assert_eq!(
            shared_rollout_status_cell()
                .read()
                .expect("read rollout status")
                .state,
            RolloutGenerationState::CatchUp
        );
    }

    #[test]
    fn rollout_status_reader_returns_snapshot() {
        let rollout_status = shared_rollout_status_cell();
        *rollout_status.write().expect("write rollout status") = PublishedRolloutStatusSnapshot {
            state: RolloutGenerationState::Stable,
            serving_generation: Some(7),
            candidate_generation: None,
            retiring_generation: None,
        };

        let snapshot = PublishedRolloutStatusReader::new(rollout_status).snapshot();
        assert_eq!(snapshot.state, RolloutGenerationState::Stable);
        assert_eq!(snapshot.serving_generation, Some(7));
    }
}

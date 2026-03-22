use serde::{Deserialize, Serialize};

pub use capanix_managed_state_sdk::{ObservationState, ObservationStatus, ReadClass};

use crate::query::reliability::GroupReliability;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum StabilityState {
    #[default]
    NotEvaluated,
    Stable,
    Unstable,
    Unknown,
    Degraded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TreeStability {
    pub mode: crate::shared_types::query::StabilityMode,
    pub state: StabilityState,
    pub quiet_window_ms: Option<u64>,
    pub observed_quiet_for_ms: Option<u64>,
    pub remaining_ms: Option<u64>,
    #[serde(default)]
    pub blocked_reasons: Vec<String>,
}

impl TreeStability {
    pub fn not_evaluated() -> Self {
        Self {
            mode: crate::shared_types::query::StabilityMode::None,
            state: StabilityState::NotEvaluated,
            quiet_window_ms: None,
            observed_quiet_for_ms: None,
            remaining_ms: None,
            blocked_reasons: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum PageOrder {
    #[default]
    PathLex,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TreePageRoot {
    #[serde(with = "serde_bytes")]
    pub path: Vec<u8>,
    pub size: u64,
    pub modified_time_us: u64,
    pub is_dir: bool,
    pub exists: bool,
    pub has_children: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TreePageEntry {
    #[serde(with = "serde_bytes")]
    pub path: Vec<u8>,
    pub depth: usize,
    pub size: u64,
    pub modified_time_us: u64,
    pub is_dir: bool,
    pub has_children: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TreeGroupPayload {
    pub reliability: GroupReliability,
    pub stability: TreeStability,
    pub root: TreePageRoot,
    pub entries: Vec<TreePageEntry>,
}

use serde::{Deserialize, Serialize};

use crate::query::reliability::GroupReliability;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum ReadClass {
    Fresh,
    Materialized,
    #[default]
    TrustedMaterialized,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum ObservationState {
    FreshOnly,
    #[default]
    MaterializedUntrusted,
    TrustedMaterialized,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ObservationStatus {
    pub state: ObservationState,
    #[serde(default)]
    pub reasons: Vec<String>,
}

impl ObservationStatus {
    pub fn fresh_only() -> Self {
        Self {
            state: ObservationState::FreshOnly,
            reasons: Vec::new(),
        }
    }

    pub fn trusted_materialized() -> Self {
        Self {
            state: ObservationState::TrustedMaterialized,
            reasons: Vec::new(),
        }
    }
}

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
    pub mode: capanix_host_fs_types::query::StabilityMode,
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
            mode: capanix_host_fs_types::query::StabilityMode::None,
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

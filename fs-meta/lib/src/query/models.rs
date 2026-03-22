use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryNode {
    #[serde(with = "serde_bytes")]
    pub path: Vec<u8>,
    #[serde(with = "serde_bytes")]
    pub file_name: Vec<u8>,
    pub size: u64,
    pub modified_time_us: u64,
    pub is_dir: bool,
    pub monitoring_attested: bool,
    pub is_suspect: bool,
    pub is_blind_spot: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SubtreeStats {
    pub total_nodes: u64,
    pub total_files: u64,
    pub total_dirs: u64,
    pub total_size: u64,
    pub attested_count: u64,
    pub blind_spot_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_file_mtime_us: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HealthStats {
    pub live_nodes: u64,
    pub tombstoned_count: u64,
    pub attested_count: u64,
    pub suspect_count: u64,
    pub blind_spot_count: u64,
    pub shadow_time_us: u64,
}

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlEvent {
    EpochStart {
        epoch_id: u64,
        epoch_type: EpochType,
    },
    EpochEnd {
        epoch_id: u64,
        epoch_type: EpochType,
    },
    WatchOverflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EpochType {
    Audit,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlCommand {
    OnDemand {
        #[serde(with = "serde_bytes")]
        path: Vec<u8>,
        recursive: bool,
    },
    Audit,
}

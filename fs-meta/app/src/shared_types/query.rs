use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum StabilityMode {
    #[default]
    None,
    QuietWindow,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum UnreliableReason {
    Unattested = 0,
    SuspectNodes = 1,
    BlindSpotsDetected = 2,
    WatchOverflowPendingAudit = 3,
}

impl std::fmt::Display for UnreliableReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UnreliableReason::WatchOverflowPendingAudit => {
                write!(f, "watch overflow pending audit")
            }
            UnreliableReason::BlindSpotsDetected => write!(f, "blind spots detected"),
            UnreliableReason::SuspectNodes => write!(f, "suspect nodes detected"),
            UnreliableReason::Unattested => write!(f, "unattested nodes"),
        }
    }
}

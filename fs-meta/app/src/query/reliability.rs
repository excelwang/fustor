use capanix_host_fs_types::query::UnreliableReason;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct GroupReliability {
    pub reliable: bool,
    pub unreliable_reason: Option<UnreliableReason>,
}

impl GroupReliability {
    pub fn from_reason(unreliable_reason: Option<UnreliableReason>) -> Self {
        Self {
            reliable: unreliable_reason.is_none(),
            unreliable_reason,
        }
    }
}

#[derive(Debug, Default)]
pub struct ReliabilityAccumulator {
    highest_reason: Option<UnreliableReason>,
}

impl ReliabilityAccumulator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn observe_flags(
        &mut self,
        monitoring_attested: bool,
        is_suspect: bool,
        is_blind_spot: bool,
    ) {
        if !monitoring_attested {
            self.observe_reason(UnreliableReason::Unattested);
        }
        if is_suspect {
            self.observe_reason(UnreliableReason::SuspectNodes);
        }
        if is_blind_spot {
            self.observe_reason(UnreliableReason::BlindSpotsDetected);
        }
    }

    pub fn observe_overflow(&mut self, overflow_pending_audit: bool) {
        if overflow_pending_audit {
            self.observe_reason(UnreliableReason::WatchOverflowPendingAudit);
        }
    }

    pub fn observe_reason(&mut self, reason: UnreliableReason) {
        if self
            .highest_reason
            .as_ref()
            .is_none_or(|current| *current < reason)
        {
            self.highest_reason = Some(reason);
        }
    }

    pub fn finish(self) -> GroupReliability {
        GroupReliability::from_reason(self.highest_reason)
    }
}

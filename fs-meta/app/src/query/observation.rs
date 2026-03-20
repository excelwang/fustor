use std::collections::{BTreeMap, BTreeSet};

use crate::query::tree::{ObservationState, ObservationStatus};
use crate::sink::SinkStatusSnapshot;
use crate::source::SourceStatusSnapshot;

#[derive(Debug, Clone, Default)]
pub struct ObservationEvidence {
    pub candidate_groups: BTreeSet<String>,
    pub initial_audit_groups: BTreeSet<String>,
    pub degraded_groups: BTreeSet<String>,
    pub overflow_pending_groups: BTreeSet<String>,
    pub allow_empty_candidate: bool,
}

pub fn evaluate_observation_status(evidence: &ObservationEvidence) -> ObservationStatus {
    if evidence.candidate_groups.is_empty() {
        return if evidence.allow_empty_candidate {
            ObservationStatus::trusted_materialized()
        } else {
            ObservationStatus {
                state: ObservationState::MaterializedUntrusted,
                reasons: vec!["no-candidate-groups".to_string()],
            }
        };
    }

    let mut pending_initial_audit = Vec::<String>::new();
    let mut overflow_pending = Vec::<String>::new();
    let mut degraded = Vec::<String>::new();

    for group_id in &evidence.candidate_groups {
        if evidence.degraded_groups.contains(group_id) {
            degraded.push(group_id.clone());
        }
        if evidence.overflow_pending_groups.contains(group_id) {
            overflow_pending.push(group_id.clone());
        }
        if evidence.initial_audit_groups.contains(group_id) {
            pending_initial_audit.push(group_id.clone());
        }
    }

    if degraded.is_empty() && overflow_pending.is_empty() && pending_initial_audit.is_empty() {
        return ObservationStatus::trusted_materialized();
    }

    let mut reasons = Vec::new();
    if !pending_initial_audit.is_empty() {
        reasons.push(format!(
            "initial audit incomplete for groups [{}]",
            pending_initial_audit.join(", ")
        ));
    }
    if !overflow_pending.is_empty() {
        reasons.push(format!(
            "overflow audit pending for groups [{}]",
            overflow_pending.join(", ")
        ));
    }
    if !degraded.is_empty() {
        reasons.push(format!(
            "degraded coverage for groups [{}]",
            degraded.join(", ")
        ));
    }

    ObservationStatus {
        state: ObservationState::MaterializedUntrusted,
        reasons,
    }
}

pub fn materialized_query_observation_evidence(
    source_status: &SourceStatusSnapshot,
    sink_status: &SinkStatusSnapshot,
) -> ObservationEvidence {
    let sink_groups = sink_status
        .groups
        .iter()
        .map(|group| (group.group_id.as_str(), group))
        .collect::<BTreeMap<_, _>>();

    let mut candidate_groups = source_status
        .concrete_roots
        .iter()
        .filter(|root| root.active && root.is_group_primary && root.scan_enabled)
        .map(|root| root.logical_root_id.clone())
        .collect::<BTreeSet<_>>();
    for root in &source_status.logical_roots {
        if root.matched_grants > 0 && root.active_members > 0 {
            candidate_groups.insert(root.root_id.clone());
        }
    }

    let degraded_groups = source_status
        .degraded_roots
        .iter()
        .map(|(root_id, _)| root_id.clone())
        .collect::<BTreeSet<_>>();

    let mut initial_audit_groups = BTreeSet::new();
    let mut overflow_pending_groups = BTreeSet::new();
    for group_id in &candidate_groups {
        let Some(group) = sink_groups.get(group_id.as_str()) else {
            initial_audit_groups.insert(group_id.clone());
            continue;
        };
        if !group.initial_audit_completed {
            initial_audit_groups.insert(group_id.clone());
        }
        if group.overflow_pending_audit {
            overflow_pending_groups.insert(group_id.clone());
        }
    }

    ObservationEvidence {
        candidate_groups,
        initial_audit_groups,
        degraded_groups,
        overflow_pending_groups,
        allow_empty_candidate: true,
    }
}

pub fn candidate_group_observation_evidence(
    source_status: &SourceStatusSnapshot,
    sink_status: &SinkStatusSnapshot,
    candidate_groups: &BTreeSet<String>,
) -> ObservationEvidence {
    let sink_groups = sink_status
        .groups
        .iter()
        .map(|group| (group.group_id.as_str(), group))
        .collect::<BTreeMap<_, _>>();
    let degraded_groups = source_status
        .degraded_roots
        .iter()
        .map(|(root_id, _)| root_id.clone())
        .collect::<BTreeSet<_>>();
    let scan_groups = source_status
        .concrete_roots
        .iter()
        .filter(|root| {
            root.active
                && root.is_group_primary
                && root.scan_enabled
                && candidate_groups.contains(&root.logical_root_id)
        })
        .map(|root| root.logical_root_id.clone())
        .collect::<BTreeSet<_>>();

    let mut initial_audit_groups = BTreeSet::new();
    let mut overflow_pending_groups = BTreeSet::new();
    for group_id in candidate_groups {
        if let Some(group) = sink_groups.get(group_id.as_str()) {
            if group.overflow_pending_audit {
                overflow_pending_groups.insert(group_id.clone());
            }
        }
        if scan_groups.contains(group_id) {
            let Some(group) = sink_groups.get(group_id.as_str()) else {
                initial_audit_groups.insert(group_id.clone());
                continue;
            };
            if !group.initial_audit_completed {
                initial_audit_groups.insert(group_id.clone());
            }
        }
    }

    ObservationEvidence {
        candidate_groups: candidate_groups.clone(),
        initial_audit_groups,
        degraded_groups,
        overflow_pending_groups,
        allow_empty_candidate: false,
    }
}

pub fn trusted_materialized_not_ready_message(status: &ObservationStatus) -> String {
    if status.reasons.is_empty() {
        return "trusted-materialized reads remain unavailable until package-local materialized observation evidence is trusted".to_string();
    }
    format!(
        "trusted-materialized reads remain unavailable until package-local materialized observation evidence is trusted: {}",
        status.reasons.join("; ")
    )
}

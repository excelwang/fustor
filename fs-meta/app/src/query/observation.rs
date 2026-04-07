use std::collections::{BTreeMap, BTreeSet};

use crate::sink::SinkStatusSnapshot;
use crate::source::SourceStatusSnapshot;
pub use capanix_managed_state_sdk::{
    ObservationEvidence, ObservationTrustPolicy, evaluate_observation_status,
    trusted_materialized_not_ready_message,
};

pub fn materialized_query_observation_evidence(
    source_status: &SourceStatusSnapshot,
    sink_status: &SinkStatusSnapshot,
) -> ObservationEvidence {
    let sink_groups = sink_status
        .groups
        .iter()
        .map(|group| (group.group_id.as_str(), group))
        .collect::<BTreeMap<_, _>>();

    let concrete_candidate_groups = source_status
        .concrete_roots
        .iter()
        .filter(|root| root.active && root.is_group_primary && root.scan_enabled)
        .map(|root| root.logical_root_id.clone())
        .collect::<BTreeSet<_>>();
    let mut candidate_groups = concrete_candidate_groups.clone();
    for root in &source_status.logical_roots {
        if root.matched_grants > 0
            && root.active_members > 0
            && (concrete_candidate_groups.is_empty()
                || concrete_candidate_groups.contains(&root.root_id))
        {
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
    }
}

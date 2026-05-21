use std::collections::{BTreeMap, BTreeSet};

use crate::sink::SinkStatusSnapshot;
use crate::source::SourceStatusSnapshot;
pub use capanix_managed_state_sdk::{
    ObservationEvidence, ObservationState, ObservationStatus, ObservationTrustPolicy,
    evaluate_observation_status, trusted_materialized_not_ready_message,
};

fn concrete_root_counts_as_materialized_candidate(
    root: &crate::source::SourceConcreteRootHealthSnapshot,
) -> bool {
    root.active
        && root.is_group_primary
        && root.scan_enabled
        && !root.status.starts_with("waiting_for_root:")
}

pub fn source_concrete_root_completed_current_audit(
    root: &crate::source::SourceConcreteRootHealthSnapshot,
) -> bool {
    root.last_audit_completed_at_us
        .is_some_and(|completed_at_us| {
            let started_at_us = root.last_audit_started_at_us.unwrap_or_default();
            let requested_at_us = root.last_rescan_requested_at_us.unwrap_or_default();
            completed_at_us >= started_at_us && completed_at_us >= requested_at_us
        })
}

fn source_concrete_root_reports_implicit_current_audit(
    root: &crate::source::SourceConcreteRootHealthSnapshot,
) -> bool {
    matches!(root.status.as_str(), "ready" | "ok")
        && root.coverage_mode == "audit_only"
        && !root.rescan_pending
        && root.last_rescan_requested_at_us.is_none()
        && root.last_audit_started_at_us.is_none()
        && root.last_audit_completed_at_us.is_none()
}

pub fn source_concrete_root_has_current_owner_evidence(
    root: &crate::source::SourceConcreteRootHealthSnapshot,
) -> bool {
    concrete_root_counts_as_materialized_candidate(root)
        && (source_concrete_root_completed_current_audit(root)
            || source_concrete_root_reports_implicit_current_audit(root))
}

pub fn source_concrete_root_needs_current_owner_evidence(
    root: &crate::source::SourceConcreteRootHealthSnapshot,
) -> bool {
    concrete_root_counts_as_materialized_candidate(root)
        && !source_concrete_root_completed_current_audit(root)
}

fn source_concrete_root_blocks_materialized_observation(
    root: &crate::source::SourceConcreteRootHealthSnapshot,
) -> bool {
    source_concrete_root_needs_current_owner_evidence(root)
}

fn source_logical_root_reports_service_ready(
    root: &crate::source::SourceLogicalRootHealthSnapshot,
) -> bool {
    matches!(root.status.as_str(), "ready" | "ok")
        && root.matched_grants > 0
        && root.active_members > 0
}

fn source_groups_with_current_owner_evidence(
    source_status: &SourceStatusSnapshot,
) -> BTreeSet<String> {
    let concrete_observed_scan_groups = source_status
        .concrete_roots
        .iter()
        .filter(|root| root.active && root.scan_enabled)
        .map(|root| root.logical_root_id.clone())
        .collect::<BTreeSet<_>>();
    let mut groups = source_status
        .concrete_roots
        .iter()
        .filter(|root| source_concrete_root_has_current_owner_evidence(root))
        .map(|root| root.logical_root_id.clone())
        .collect::<BTreeSet<_>>();
    groups.extend(
        source_status
            .logical_roots
            .iter()
            .filter(|root| {
                !concrete_observed_scan_groups.contains(&root.root_id)
                    && source_logical_root_reports_service_ready(root)
            })
            .map(|root| root.root_id.clone()),
    );
    groups
}

fn group_has_local_sink_presence(
    group_id: &str,
    sink_groups: &BTreeMap<&str, &crate::sink::SinkGroupStatusSnapshot>,
    scheduled_groups: &BTreeSet<String>,
) -> bool {
    scheduled_groups.contains(group_id)
        || sink_groups
            .get(group_id)
            .is_some_and(|group| sink_group_has_materialized_presence(group))
}

fn sink_group_has_materialized_presence(group: &crate::sink::SinkGroupStatusSnapshot) -> bool {
    let has_bound_primary = !group.primary_object_ref.is_empty()
        && group.primary_object_ref != "unassigned"
        && group.primary_object_ref != group.group_id;
    has_bound_primary
        || group.live_nodes > 0
        || group.total_nodes > 0
        || matches!(
            group.normalized_readiness(),
            crate::sink::GroupReadinessState::Ready
        )
}

fn sink_group_blocks_materialized_observation(
    group: &crate::sink::SinkGroupStatusSnapshot,
) -> bool {
    !matches!(
        group.normalized_readiness(),
        crate::sink::GroupReadinessState::Ready
    )
}

fn source_logical_root_degrades_materialized_observation(
    root: &crate::source::SourceLogicalRootHealthSnapshot,
) -> bool {
    if root.matched_grants == 0 || root.active_members == 0 {
        return false;
    }
    let status_head = root
        .status
        .split_once(':')
        .map(|(head, _)| head)
        .unwrap_or(root.status.as_str());
    status_head != "ready"
        && (root.status.contains("degraded")
            || root.status.contains("error")
            || root.status.contains("failed")
            || root.status.contains("overflow"))
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
    let scheduled_groups = sink_status
        .scheduled_groups_by_node
        .values()
        .flat_map(|groups| groups.iter().cloned())
        .collect::<BTreeSet<_>>();

    let concrete_candidate_groups = source_status
        .concrete_roots
        .iter()
        .filter(|root| {
            concrete_root_counts_as_materialized_candidate(root)
                && group_has_local_sink_presence(
                    &root.logical_root_id,
                    &sink_groups,
                    &scheduled_groups,
                )
        })
        .map(|root| root.logical_root_id.clone())
        .collect::<BTreeSet<_>>();
    let mut candidate_groups = concrete_candidate_groups.clone();
    for root in &source_status.logical_roots {
        if root.matched_grants > 0
            && root.active_members > 0
            && group_has_local_sink_presence(&root.root_id, &sink_groups, &scheduled_groups)
        {
            candidate_groups.insert(root.root_id.clone());
        }
    }

    let mut degraded_groups = source_status
        .degraded_roots
        .iter()
        .filter(|(root_id, _)| candidate_groups.contains(root_id))
        .map(|(root_id, _)| root_id.clone())
        .collect::<BTreeSet<_>>();
    degraded_groups.extend(
        source_status
            .logical_roots
            .iter()
            .filter(|root| source_logical_root_degrades_materialized_observation(root))
            .map(|root| root.root_id.clone()),
    );
    let source_groups_with_current_owner_evidence =
        source_groups_with_current_owner_evidence(source_status);
    let source_initial_audit_groups = source_status
        .concrete_roots
        .iter()
        .filter(|root| {
            source_concrete_root_blocks_materialized_observation(root)
                && !source_groups_with_current_owner_evidence.contains(&root.logical_root_id)
                && group_has_local_sink_presence(
                    &root.logical_root_id,
                    &sink_groups,
                    &scheduled_groups,
                )
        })
        .map(|root| root.logical_root_id.clone())
        .collect::<BTreeSet<_>>();

    let mut initial_audit_groups = BTreeSet::new();
    let mut overflow_pending_groups = BTreeSet::new();
    for group_id in &candidate_groups {
        let Some(group) = sink_groups.get(group_id.as_str()) else {
            initial_audit_groups.insert(group_id.clone());
            continue;
        };
        if sink_group_blocks_materialized_observation(group) {
            initial_audit_groups.insert(group_id.clone());
        }
        if group.overflow_pending_materialization {
            overflow_pending_groups.insert(group_id.clone());
        }
    }
    initial_audit_groups.extend(source_initial_audit_groups);

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
    let mut degraded_groups = source_status
        .degraded_roots
        .iter()
        .filter(|(root_id, _)| candidate_groups.contains(root_id))
        .map(|(root_id, _)| root_id.clone())
        .collect::<BTreeSet<_>>();
    degraded_groups.extend(
        source_status
            .logical_roots
            .iter()
            .filter(|root| {
                candidate_groups.contains(&root.root_id)
                    && source_logical_root_degrades_materialized_observation(root)
            })
            .map(|root| root.root_id.clone()),
    );
    let scheduled_groups = sink_status
        .scheduled_groups_by_node
        .values()
        .flat_map(|groups| groups.iter().cloned())
        .collect::<BTreeSet<_>>();
    let scan_groups = source_status
        .concrete_roots
        .iter()
        .filter(|root| {
            concrete_root_counts_as_materialized_candidate(root)
                && candidate_groups.contains(&root.logical_root_id)
                && group_has_local_sink_presence(
                    &root.logical_root_id,
                    &sink_groups,
                    &scheduled_groups,
                )
        })
        .map(|root| root.logical_root_id.clone())
        .collect::<BTreeSet<_>>();
    let source_groups_with_current_owner_evidence =
        source_groups_with_current_owner_evidence(source_status);
    let source_initial_audit_groups = source_status
        .concrete_roots
        .iter()
        .filter(|root| {
            source_concrete_root_blocks_materialized_observation(root)
                && !source_groups_with_current_owner_evidence.contains(&root.logical_root_id)
                && candidate_groups.contains(&root.logical_root_id)
                && group_has_local_sink_presence(
                    &root.logical_root_id,
                    &sink_groups,
                    &scheduled_groups,
                )
        })
        .map(|root| root.logical_root_id.clone())
        .collect::<BTreeSet<_>>();

    let mut initial_audit_groups = BTreeSet::new();
    let mut overflow_pending_groups = BTreeSet::new();
    for group_id in candidate_groups {
        if let Some(group) = sink_groups.get(group_id.as_str()) {
            if group.overflow_pending_materialization {
                overflow_pending_groups.insert(group_id.clone());
            }
        }
        if scan_groups.contains(group_id) {
            let Some(group) = sink_groups.get(group_id.as_str()) else {
                initial_audit_groups.insert(group_id.clone());
                continue;
            };
            if sink_group_blocks_materialized_observation(group) {
                initial_audit_groups.insert(group_id.clone());
            }
        }
    }
    initial_audit_groups.extend(source_initial_audit_groups);

    ObservationEvidence {
        candidate_groups: candidate_groups.clone(),
        initial_audit_groups,
        degraded_groups,
        overflow_pending_groups,
    }
}

pub fn source_status_covers_readiness_groups(
    source_status: &SourceStatusSnapshot,
    readiness_groups: &BTreeSet<String>,
) -> bool {
    let source_groups = source_groups_with_current_owner_evidence(source_status);
    readiness_groups.is_subset(&source_groups)
}

pub fn materialized_scheduled_group_ids(snapshot: &SinkStatusSnapshot) -> BTreeSet<String> {
    let mut groups = snapshot
        .scheduled_groups_by_node
        .values()
        .flat_map(|groups| groups.iter().cloned())
        .collect::<BTreeSet<_>>();
    groups.extend(snapshot.groups.iter().map(|group| group.group_id.clone()));
    groups
}

pub fn sink_group_readiness_reports_live_materialized_group(
    group: &crate::sink::SinkGroupStatusSnapshot,
) -> bool {
    group.materialized_service_live_ready()
}

pub fn sink_status_covers_ready_readiness_groups(
    sink_status: &SinkStatusSnapshot,
    readiness_groups: &BTreeSet<String>,
) -> bool {
    let scheduled_groups = materialized_scheduled_group_ids(sink_status);
    if !readiness_groups.is_subset(&scheduled_groups) {
        return false;
    }
    let ready_groups = sink_status
        .groups
        .iter()
        .filter(|group| sink_group_readiness_reports_live_materialized_group(group))
        .map(|group| group.group_id.clone())
        .collect::<BTreeSet<_>>();
    readiness_groups.is_subset(&ready_groups)
}

pub fn materialized_observation_status_for_readiness_groups(
    source_status: &SourceStatusSnapshot,
    sink_status: &SinkStatusSnapshot,
    readiness_groups: &BTreeSet<String>,
) -> ObservationStatus {
    if readiness_groups.is_empty() {
        return materialized_observation_status(source_status, sink_status);
    }
    let mut evidence =
        candidate_group_observation_evidence(source_status, sink_status, readiness_groups);
    let source_groups = source_groups_with_current_owner_evidence(source_status);
    for missing_group in readiness_groups.difference(&source_groups) {
        evidence.initial_audit_groups.insert(missing_group.clone());
    }
    let scheduled_groups = materialized_scheduled_group_ids(sink_status);
    let ready_groups = sink_status
        .groups
        .iter()
        .filter(|group| sink_group_readiness_reports_live_materialized_group(group))
        .map(|group| group.group_id.clone())
        .collect::<BTreeSet<_>>();
    for missing_group in readiness_groups.difference(&scheduled_groups) {
        evidence.initial_audit_groups.insert(missing_group.clone());
    }
    for missing_group in readiness_groups.difference(&ready_groups) {
        evidence.initial_audit_groups.insert(missing_group.clone());
    }
    evaluate_observation_status(&evidence, ObservationTrustPolicy::materialized_query())
}

pub fn materialized_status_cache_is_ready(
    source_status: &SourceStatusSnapshot,
    sink_status: &SinkStatusSnapshot,
    readiness_groups: &BTreeSet<String>,
) -> bool {
    !readiness_groups.is_empty()
        && source_status_covers_readiness_groups(source_status, readiness_groups)
        && sink_status_covers_ready_readiness_groups(sink_status, readiness_groups)
        && materialized_observation_status_for_readiness_groups(
            source_status,
            sink_status,
            readiness_groups,
        )
        .state
            == ObservationState::TrustedMaterialized
}

pub fn trusted_materialized_status_covers_readiness_groups(
    source_status: &SourceStatusSnapshot,
    sink_status: &SinkStatusSnapshot,
    readiness_groups: &BTreeSet<String>,
) -> bool {
    if readiness_groups.is_empty() {
        return materialized_observation_status(source_status, sink_status).state
            == ObservationState::TrustedMaterialized;
    }
    materialized_status_cache_is_ready(source_status, sink_status, readiness_groups)
}

fn materialized_observation_status(
    source_status: &SourceStatusSnapshot,
    sink_status: &SinkStatusSnapshot,
) -> ObservationStatus {
    evaluate_observation_status(
        &materialized_query_observation_evidence(source_status, sink_status),
        ObservationTrustPolicy::materialized_query(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::SinkGroupStatusSnapshot;
    use crate::source::{SourceConcreteRootHealthSnapshot, SourceLogicalRootHealthSnapshot};
    use capanix_managed_state_sdk::ObservationState;

    fn concrete_root(
        logical_root_id: &str,
        is_group_primary: bool,
    ) -> SourceConcreteRootHealthSnapshot {
        SourceConcreteRootHealthSnapshot {
            root_key: format!("{logical_root_id}@test"),
            logical_root_id: logical_root_id.to_string(),
            object_ref: format!("node-x::{logical_root_id}"),
            status: "running".to_string(),
            coverage_mode: "realtime_hotset_plus_audit".to_string(),
            watch_enabled: true,
            scan_enabled: true,
            is_group_primary,
            active: true,
            watch_lru_capacity: 65536,
            audit_interval_ms: 300000,
            overflow_count: 0,
            overflow_pending: false,
            rescan_pending: false,
            last_rescan_requested_at_us: None,
            last_rescan_reason: None,
            last_error: None,
            last_audit_started_at_us: Some(10),
            last_audit_completed_at_us: Some(20),
            last_audit_duration_ms: Some(10),
            emitted_batch_count: 0,
            emitted_event_count: 0,
            emitted_control_event_count: 0,
            emitted_data_event_count: 0,
            emitted_path_capture_target: None,
            emitted_path_event_count: 0,
            last_emitted_at_us: None,
            last_emitted_origins: Vec::new(),
            forwarded_batch_count: 0,
            forwarded_event_count: 0,
            forwarded_path_event_count: 0,
            last_forwarded_at_us: None,
            last_forwarded_origins: Vec::new(),
            current_revision: None,
            current_stream_generation: None,
            candidate_revision: None,
            candidate_stream_generation: None,
            candidate_status: None,
            draining_revision: None,
            draining_stream_generation: None,
            draining_status: None,
        }
    }

    fn sink_group(group_id: &str) -> SinkGroupStatusSnapshot {
        SinkGroupStatusSnapshot {
            group_id: group_id.to_string(),
            primary_object_ref: "unassigned".to_string(),
            total_nodes: 0,
            live_nodes: 0,
            tombstoned_count: 0,
            attested_count: 0,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 0,
            shadow_lag_us: 0,
            overflow_pending_materialization: false,

            readiness: crate::sink::GroupReadinessState::PendingMaterialization,
            materialized_revision: 1,
            estimated_heap_bytes: 0,
        }
    }

    fn ready_sink_group(group_id: &str) -> SinkGroupStatusSnapshot {
        SinkGroupStatusSnapshot {
            group_id: group_id.to_string(),
            primary_object_ref: format!("node-a::{group_id}"),
            total_nodes: 10,
            live_nodes: 10,
            tombstoned_count: 0,
            attested_count: 10,
            suspect_count: 0,
            blind_spot_count: 0,
            shadow_time_us: 1,
            shadow_lag_us: 0,
            overflow_pending_materialization: false,
            readiness: crate::sink::GroupReadinessState::Ready,
            materialized_revision: 1,
            estimated_heap_bytes: 1,
        }
    }

    #[test]
    fn materialized_query_observation_evidence_keeps_ready_sink_group_with_unattested_nodes_available()
     {
        let source_status = SourceStatusSnapshot {
            logical_roots: vec![SourceLogicalRootHealthSnapshot {
                root_id: "nfs1".to_string(),
                status: "ready".to_string(),
                active_members: 3,
                matched_grants: 3,
                coverage_mode: "realtime_hotset_plus_audit".to_string(),
            }],
            concrete_roots: vec![concrete_root("nfs1", true)],
            ..SourceStatusSnapshot::default()
        };
        let mut group = ready_sink_group("nfs1");
        group.attested_count = 9;
        let sink_status = SinkStatusSnapshot {
            groups: vec![group],
            ..SinkStatusSnapshot::default()
        };

        let evidence = materialized_query_observation_evidence(&source_status, &sink_status);
        assert_eq!(
            evidence.candidate_groups,
            BTreeSet::from(["nfs1".to_string()])
        );
        assert!(evidence.degraded_groups.is_empty());
        let status =
            evaluate_observation_status(&evidence, ObservationTrustPolicy::materialized_query());
        assert_eq!(status.state, ObservationState::TrustedMaterialized);
    }

    #[test]
    fn materialized_query_observation_evidence_keeps_ready_sink_group_trusted_with_blind_spots() {
        let source_status = SourceStatusSnapshot {
            logical_roots: vec![SourceLogicalRootHealthSnapshot {
                root_id: "nfs1".to_string(),
                status: "ready".to_string(),
                active_members: 3,
                matched_grants: 3,
                coverage_mode: "realtime_hotset_plus_audit".to_string(),
            }],
            concrete_roots: vec![concrete_root("nfs1", true)],
            ..SourceStatusSnapshot::default()
        };
        let mut group = ready_sink_group("nfs1");
        group.blind_spot_count = 1;
        let sink_status = SinkStatusSnapshot {
            groups: vec![group],
            ..SinkStatusSnapshot::default()
        };

        let evidence = materialized_query_observation_evidence(&source_status, &sink_status);
        assert_eq!(
            evidence.candidate_groups,
            BTreeSet::from(["nfs1".to_string()])
        );
        assert!(evidence.initial_audit_groups.is_empty());
        assert!(evidence.degraded_groups.is_empty());
        let status =
            evaluate_observation_status(&evidence, ObservationTrustPolicy::materialized_query());
        assert_eq!(status.state, ObservationState::TrustedMaterialized);
    }

    #[test]
    fn materialized_query_observation_evidence_ignores_logical_only_groups_without_local_sink_schedule()
     {
        let source_status = SourceStatusSnapshot {
            logical_roots: vec![
                SourceLogicalRootHealthSnapshot {
                    root_id: "nfs1".to_string(),
                    status: "ready".to_string(),
                    active_members: 3,
                    matched_grants: 3,
                    coverage_mode: "realtime_hotset_plus_audit".to_string(),
                },
                SourceLogicalRootHealthSnapshot {
                    root_id: "nfs2".to_string(),
                    status: "ready".to_string(),
                    active_members: 3,
                    matched_grants: 3,
                    coverage_mode: "realtime_hotset_plus_audit".to_string(),
                },
            ],
            concrete_roots: vec![concrete_root("nfs1", false), concrete_root("nfs2", false)],
            ..SourceStatusSnapshot::default()
        };
        let sink_status = SinkStatusSnapshot {
            groups: vec![sink_group("nfs1"), sink_group("nfs2")],
            scheduled_groups_by_node: BTreeMap::new(),
            ..SinkStatusSnapshot::default()
        };

        let evidence = materialized_query_observation_evidence(&source_status, &sink_status);
        assert!(
            evidence.candidate_groups.is_empty(),
            "logical-only groups without any local sink schedule must not keep package-local materialized observation untrusted: {evidence:?}"
        );
        assert!(
            evidence.initial_audit_groups.is_empty(),
            "logical-only groups without any local sink schedule must not be reported as initial-audit blockers: {evidence:?}"
        );
    }

    #[test]
    fn materialized_query_observation_evidence_keeps_logical_only_groups_when_local_sink_schedule_exists()
     {
        let source_status = SourceStatusSnapshot {
            logical_roots: vec![SourceLogicalRootHealthSnapshot {
                root_id: "nfs1".to_string(),
                status: "ready".to_string(),
                active_members: 3,
                matched_grants: 3,
                coverage_mode: "realtime_hotset_plus_audit".to_string(),
            }],
            concrete_roots: vec![concrete_root("nfs1", false)],
            ..SourceStatusSnapshot::default()
        };
        let sink_status = SinkStatusSnapshot {
            groups: vec![sink_group("nfs1")],
            scheduled_groups_by_node: BTreeMap::from([(
                "node-d".to_string(),
                vec!["nfs1".to_string()],
            )]),
            ..SinkStatusSnapshot::default()
        };

        let evidence = materialized_query_observation_evidence(&source_status, &sink_status);
        assert_eq!(
            evidence.candidate_groups,
            BTreeSet::from(["nfs1".to_string()]),
            "logical-only groups should still count when local sink scheduling exists: {evidence:?}"
        );
        assert_eq!(
            evidence.initial_audit_groups,
            BTreeSet::from(["nfs1".to_string()]),
            "scheduled logical-only groups must still block on initial audit until materialized: {evidence:?}"
        );
    }

    #[test]
    fn materialized_query_observation_evidence_keeps_every_scheduled_logical_group_even_when_some_groups_have_primary_scan_evidence()
     {
        let source_status = SourceStatusSnapshot {
            logical_roots: vec![
                SourceLogicalRootHealthSnapshot {
                    root_id: "nfs1".to_string(),
                    status: "ready".to_string(),
                    active_members: 3,
                    matched_grants: 3,
                    coverage_mode: "realtime_hotset_plus_audit".to_string(),
                },
                SourceLogicalRootHealthSnapshot {
                    root_id: "nfs2".to_string(),
                    status: "ready".to_string(),
                    active_members: 3,
                    matched_grants: 3,
                    coverage_mode: "realtime_hotset_plus_audit".to_string(),
                },
            ],
            concrete_roots: vec![concrete_root("nfs2", true)],
            ..SourceStatusSnapshot::default()
        };
        let sink_status = SinkStatusSnapshot {
            groups: vec![sink_group("nfs1"), sink_group("nfs2")],
            scheduled_groups_by_node: BTreeMap::from([(
                "node-d".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            )]),
            ..SinkStatusSnapshot::default()
        };

        let evidence = materialized_query_observation_evidence(&source_status, &sink_status);
        assert_eq!(
            evidence.candidate_groups,
            BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]),
            "trusted observation must include every scheduled logical root, not only roots with concrete primary scan evidence: {evidence:?}"
        );
        assert_eq!(
            evidence.initial_audit_groups,
            BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]),
            "every scheduled non-ready materialized root must block trusted-materialized reads: {evidence:?}"
        );
    }

    #[test]
    fn materialized_query_observation_evidence_ignores_waiting_for_root_primary_scan_groups_without_local_sink_schedule()
     {
        let mut waiting_for_root = concrete_root("nfs1", true);
        waiting_for_root.status =
            "waiting_for_root: No such file or directory (os error 2)".to_string();
        waiting_for_root.last_error = Some("No such file or directory (os error 2)".to_string());
        let source_status = SourceStatusSnapshot {
            logical_roots: vec![SourceLogicalRootHealthSnapshot {
                root_id: "nfs1".to_string(),
                status: "ready".to_string(),
                active_members: 3,
                matched_grants: 3,
                coverage_mode: "realtime_hotset_plus_audit".to_string(),
            }],
            concrete_roots: vec![waiting_for_root],
            ..SourceStatusSnapshot::default()
        };
        let sink_status = SinkStatusSnapshot::default();

        let evidence = materialized_query_observation_evidence(&source_status, &sink_status);
        assert!(
            evidence.candidate_groups.is_empty(),
            "primary scan groups whose concrete roots are still waiting_for_root and have no local sink schedule must not keep package-local materialized observation untrusted: {evidence:?}"
        );
        assert!(
            evidence.initial_audit_groups.is_empty(),
            "waiting_for_root primary scan groups without any local sink schedule must not be reported as initial-audit blockers: {evidence:?}"
        );
    }

    #[test]
    fn candidate_group_observation_evidence_ignores_waiting_for_root_primary_scan_groups_without_local_sink_schedule()
     {
        let mut waiting_for_root = concrete_root("nfs1", true);
        waiting_for_root.status =
            "waiting_for_root: No such file or directory (os error 2)".to_string();
        waiting_for_root.last_error = Some("No such file or directory (os error 2)".to_string());
        let source_status = SourceStatusSnapshot {
            concrete_roots: vec![waiting_for_root],
            ..SourceStatusSnapshot::default()
        };
        let sink_status = SinkStatusSnapshot::default();
        let candidate_groups = BTreeSet::from(["nfs1".to_string()]);

        let evidence =
            candidate_group_observation_evidence(&source_status, &sink_status, &candidate_groups);
        assert!(
            evidence.initial_audit_groups.is_empty(),
            "waiting_for_root primary scan groups without local sink schedule must not stay in candidate-group initial-audit blockers: {evidence:?}"
        );
    }

    #[test]
    fn materialized_query_observation_evidence_ignores_running_primary_scan_groups_without_local_sink_schedule()
     {
        let source_status = SourceStatusSnapshot {
            logical_roots: vec![SourceLogicalRootHealthSnapshot {
                root_id: "nfs1".to_string(),
                status: "ready".to_string(),
                active_members: 3,
                matched_grants: 3,
                coverage_mode: "realtime_hotset_plus_audit".to_string(),
            }],
            concrete_roots: vec![concrete_root("nfs1", true)],
            ..SourceStatusSnapshot::default()
        };
        let sink_status = SinkStatusSnapshot::default();

        let evidence = materialized_query_observation_evidence(&source_status, &sink_status);
        assert!(
            evidence.candidate_groups.is_empty(),
            "primary scan groups without any local sink schedule must not keep package-local materialized observation untrusted: {evidence:?}"
        );
        assert!(
            evidence.initial_audit_groups.is_empty(),
            "primary scan groups without any local sink schedule must not be reported as initial-audit blockers: {evidence:?}"
        );
    }

    #[test]
    fn candidate_group_observation_evidence_ignores_running_primary_scan_groups_without_local_sink_schedule()
     {
        let source_status = SourceStatusSnapshot {
            concrete_roots: vec![concrete_root("nfs1", true)],
            ..SourceStatusSnapshot::default()
        };
        let sink_status = SinkStatusSnapshot::default();
        let candidate_groups = BTreeSet::from(["nfs1".to_string()]);

        let evidence =
            candidate_group_observation_evidence(&source_status, &sink_status, &candidate_groups);
        assert!(
            evidence.initial_audit_groups.is_empty(),
            "primary scan groups without local sink schedule must not stay in candidate-group initial-audit blockers: {evidence:?}"
        );
    }

    #[test]
    fn materialized_query_observation_evidence_prefers_exported_readiness_over_legacy_initial_audit_bool()
     {
        let source_status = SourceStatusSnapshot {
            logical_roots: vec![SourceLogicalRootHealthSnapshot {
                root_id: "nfs1".to_string(),
                status: "ready".to_string(),
                active_members: 3,
                matched_grants: 3,
                coverage_mode: "realtime_hotset_plus_audit".to_string(),
            }],
            concrete_roots: vec![concrete_root("nfs1", true)],
            ..SourceStatusSnapshot::default()
        };
        let sink_status = SinkStatusSnapshot {
            groups: vec![SinkGroupStatusSnapshot {
                group_id: "nfs1".to_string(),
                primary_object_ref: "node-a::nfs1".to_string(),
                total_nodes: 3,
                live_nodes: 3,
                tombstoned_count: 0,
                attested_count: 0,
                suspect_count: 0,
                blind_spot_count: 0,
                shadow_time_us: 10,
                shadow_lag_us: 0,
                overflow_pending_materialization: false,

                readiness: crate::sink::GroupReadinessState::Ready,
                materialized_revision: 7,
                estimated_heap_bytes: 0,
            }],
            scheduled_groups_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["nfs1".to_string()],
            )]),
            ..SinkStatusSnapshot::default()
        };

        let evidence = materialized_query_observation_evidence(&source_status, &sink_status);
        assert!(
            evidence.initial_audit_groups.is_empty(),
            "query observation must trust exported sink readiness over stale legacy initial_audit_completed=false: {evidence:?}"
        );
    }

    #[test]
    fn materialized_query_observation_evidence_uses_normalized_sink_readiness() {
        let source_status = SourceStatusSnapshot {
            logical_roots: vec![SourceLogicalRootHealthSnapshot {
                root_id: "nfs1".to_string(),
                status: "ready".to_string(),
                active_members: 3,
                matched_grants: 3,
                coverage_mode: "realtime_hotset_plus_audit".to_string(),
            }],
            concrete_roots: vec![concrete_root("nfs1", true)],
            ..SourceStatusSnapshot::default()
        };
        let sink_status = SinkStatusSnapshot {
            groups: vec![SinkGroupStatusSnapshot {
                group_id: "nfs1".to_string(),
                primary_object_ref: "node-a::nfs1".to_string(),
                total_nodes: 3,
                live_nodes: 3,
                tombstoned_count: 0,
                attested_count: 0,
                suspect_count: 0,
                blind_spot_count: 0,
                shadow_time_us: 10,
                shadow_lag_us: 0,
                overflow_pending_materialization: false,
                readiness: crate::sink::GroupReadinessState::WaitingForMaterializedRoot,
                materialized_revision: 7,
                estimated_heap_bytes: 0,
            }],
            scheduled_groups_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["nfs1".to_string()],
            )]),
            ..SinkStatusSnapshot::default()
        };

        let evidence = materialized_query_observation_evidence(&source_status, &sink_status);
        assert!(
            evidence.initial_audit_groups.is_empty(),
            "query observation must use normalized sink readiness so live materialized groups are not downgraded to initial-audit blockers: {evidence:?}"
        );
    }

    #[test]
    fn candidate_group_observation_evidence_prefers_exported_readiness_over_legacy_initial_audit_bool()
     {
        let source_status = SourceStatusSnapshot {
            concrete_roots: vec![concrete_root("nfs1", true)],
            ..SourceStatusSnapshot::default()
        };
        let sink_status = SinkStatusSnapshot {
            groups: vec![SinkGroupStatusSnapshot {
                group_id: "nfs1".to_string(),
                primary_object_ref: "node-a::nfs1".to_string(),
                total_nodes: 3,
                live_nodes: 3,
                tombstoned_count: 0,
                attested_count: 0,
                suspect_count: 0,
                blind_spot_count: 0,
                shadow_time_us: 10,
                shadow_lag_us: 0,
                overflow_pending_materialization: false,

                readiness: crate::sink::GroupReadinessState::Ready,
                materialized_revision: 7,
                estimated_heap_bytes: 0,
            }],
            ..SinkStatusSnapshot::default()
        };
        let candidate_groups = BTreeSet::from(["nfs1".to_string()]);

        let evidence =
            candidate_group_observation_evidence(&source_status, &sink_status, &candidate_groups);
        assert!(
            evidence.initial_audit_groups.is_empty(),
            "candidate-group observation must trust exported sink readiness over stale legacy initial_audit_completed=false: {evidence:?}"
        );
    }

    #[test]
    fn candidate_group_observation_evidence_uses_normalized_sink_readiness() {
        let source_status = SourceStatusSnapshot {
            concrete_roots: vec![concrete_root("nfs1", true)],
            ..SourceStatusSnapshot::default()
        };
        let sink_status = SinkStatusSnapshot {
            groups: vec![SinkGroupStatusSnapshot {
                group_id: "nfs1".to_string(),
                primary_object_ref: "node-a::nfs1".to_string(),
                total_nodes: 3,
                live_nodes: 3,
                tombstoned_count: 0,
                attested_count: 0,
                suspect_count: 0,
                blind_spot_count: 0,
                shadow_time_us: 10,
                shadow_lag_us: 0,
                overflow_pending_materialization: false,
                readiness: crate::sink::GroupReadinessState::WaitingForMaterializedRoot,
                materialized_revision: 7,
                estimated_heap_bytes: 0,
            }],
            scheduled_groups_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["nfs1".to_string()],
            )]),
            ..SinkStatusSnapshot::default()
        };
        let candidate_groups = BTreeSet::from(["nfs1".to_string()]);

        let evidence =
            candidate_group_observation_evidence(&source_status, &sink_status, &candidate_groups);
        assert!(
            evidence.initial_audit_groups.is_empty(),
            "candidate-group observation must use normalized sink readiness so live materialized groups are not downgraded to initial-audit blockers: {evidence:?}"
        );
    }

    #[test]
    fn materialized_status_cache_is_not_ready_while_primary_rescan_is_pending() {
        let mut root = concrete_root("nfs1", true);
        root.rescan_pending = true;
        root.last_rescan_requested_at_us = Some(30);
        let source_status = SourceStatusSnapshot {
            logical_roots: vec![SourceLogicalRootHealthSnapshot {
                root_id: "nfs1".to_string(),
                status: "ready".to_string(),
                active_members: 3,
                matched_grants: 3,
                coverage_mode: "realtime_hotset_plus_audit".to_string(),
            }],
            concrete_roots: vec![root],
            ..SourceStatusSnapshot::default()
        };
        let sink_status = SinkStatusSnapshot {
            groups: vec![ready_sink_group("nfs1")],
            scheduled_groups_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["nfs1".to_string()],
            )]),
            ..SinkStatusSnapshot::default()
        };
        let readiness_groups = BTreeSet::from(["nfs1".to_string()]);

        let evidence =
            candidate_group_observation_evidence(&source_status, &sink_status, &readiness_groups);
        assert_eq!(
            evidence.initial_audit_groups,
            BTreeSet::from(["nfs1".to_string()])
        );
        assert!(!materialized_status_cache_is_ready(
            &source_status,
            &sink_status,
            &readiness_groups,
        ));
    }

    #[test]
    fn readiness_group_observation_rejects_missing_source_coverage() {
        let source_status = SourceStatusSnapshot::default();
        let sink_status = SinkStatusSnapshot {
            groups: vec![ready_sink_group("nfs1")],
            scheduled_groups_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["nfs1".to_string()],
            )]),
            ..SinkStatusSnapshot::default()
        };
        let readiness_groups = BTreeSet::from(["nfs1".to_string()]);

        let status = materialized_observation_status_for_readiness_groups(
            &source_status,
            &sink_status,
            &readiness_groups,
        );

        assert_eq!(status.state, ObservationState::MaterializedUntrusted);
        assert!(!materialized_status_cache_is_ready(
            &source_status,
            &sink_status,
            &readiness_groups,
        ));
    }

    #[test]
    fn readiness_group_observation_rejects_missing_ready_sink_coverage() {
        let source_status = SourceStatusSnapshot {
            logical_roots: vec![SourceLogicalRootHealthSnapshot {
                root_id: "nfs1".to_string(),
                status: "ready".to_string(),
                active_members: 3,
                matched_grants: 3,
                coverage_mode: "realtime_hotset_plus_audit".to_string(),
            }],
            concrete_roots: vec![concrete_root("nfs1", true)],
            ..SourceStatusSnapshot::default()
        };
        let sink_status = SinkStatusSnapshot::default();
        let readiness_groups = BTreeSet::from(["nfs1".to_string()]);

        let status = materialized_observation_status_for_readiness_groups(
            &source_status,
            &sink_status,
            &readiness_groups,
        );

        assert_eq!(status.state, ObservationState::MaterializedUntrusted);
        assert!(!materialized_status_cache_is_ready(
            &source_status,
            &sink_status,
            &readiness_groups,
        ));
    }

    #[test]
    fn readiness_group_observation_rejects_primary_without_completed_current_audit() {
        let mut root = concrete_root("nfs1", true);
        root.last_audit_started_at_us = None;
        root.last_audit_completed_at_us = None;
        root.last_audit_duration_ms = None;
        let source_status = SourceStatusSnapshot {
            logical_roots: vec![SourceLogicalRootHealthSnapshot {
                root_id: "nfs1".to_string(),
                status: "ready".to_string(),
                active_members: 3,
                matched_grants: 3,
                coverage_mode: "realtime_hotset_plus_audit".to_string(),
            }],
            concrete_roots: vec![root],
            ..SourceStatusSnapshot::default()
        };
        let sink_status = SinkStatusSnapshot {
            groups: vec![ready_sink_group("nfs1")],
            scheduled_groups_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["nfs1".to_string()],
            )]),
            ..SinkStatusSnapshot::default()
        };
        let readiness_groups = BTreeSet::from(["nfs1".to_string()]);

        let status = materialized_observation_status_for_readiness_groups(
            &source_status,
            &sink_status,
            &readiness_groups,
        );

        assert_eq!(status.state, ObservationState::MaterializedUntrusted);
        assert!(!materialized_status_cache_is_ready(
            &source_status,
            &sink_status,
            &readiness_groups,
        ));
    }

    #[test]
    fn readiness_group_observation_accepts_ready_empty_sink_coverage() {
        let source_status = SourceStatusSnapshot {
            logical_roots: vec![SourceLogicalRootHealthSnapshot {
                root_id: "nfs1".to_string(),
                status: "ready".to_string(),
                active_members: 3,
                matched_grants: 3,
                coverage_mode: "realtime_hotset_plus_audit".to_string(),
            }],
            concrete_roots: vec![concrete_root("nfs1", true)],
            ..SourceStatusSnapshot::default()
        };
        let mut group = ready_sink_group("nfs1");
        group.total_nodes = 0;
        let sink_status = SinkStatusSnapshot {
            groups: vec![group],
            scheduled_groups_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["nfs1".to_string()],
            )]),
            ..SinkStatusSnapshot::default()
        };
        let readiness_groups = BTreeSet::from(["nfs1".to_string()]);

        let status = materialized_observation_status_for_readiness_groups(
            &source_status,
            &sink_status,
            &readiness_groups,
        );

        assert_eq!(status.state, ObservationState::TrustedMaterialized);
        assert!(materialized_status_cache_is_ready(
            &source_status,
            &sink_status,
            &readiness_groups,
        ));
    }

    #[test]
    fn completed_audit_satisfies_stale_rescan_pending_evidence() {
        let mut root = concrete_root("nfs1", true);
        root.rescan_pending = true;
        root.last_rescan_requested_at_us = Some(10);
        root.last_audit_started_at_us = Some(11);
        root.last_audit_completed_at_us = Some(20);
        let source_status = SourceStatusSnapshot {
            logical_roots: vec![SourceLogicalRootHealthSnapshot {
                root_id: "nfs1".to_string(),
                status: "ready".to_string(),
                active_members: 3,
                matched_grants: 3,
                coverage_mode: "realtime_hotset_plus_audit".to_string(),
            }],
            concrete_roots: vec![root],
            ..SourceStatusSnapshot::default()
        };
        let sink_status = SinkStatusSnapshot {
            groups: vec![ready_sink_group("nfs1")],
            scheduled_groups_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["nfs1".to_string()],
            )]),
            ..SinkStatusSnapshot::default()
        };
        let readiness_groups = BTreeSet::from(["nfs1".to_string()]);

        let evidence =
            candidate_group_observation_evidence(&source_status, &sink_status, &readiness_groups);
        assert!(evidence.initial_audit_groups.is_empty());
        assert!(materialized_status_cache_is_ready(
            &source_status,
            &sink_status,
            &readiness_groups,
        ));
    }

    #[test]
    fn readiness_group_observation_uses_group_level_current_audit_evidence() {
        let completed_primary = concrete_root("nfs1", true);
        let mut redundant_pending_primary = concrete_root("nfs1", true);
        redundant_pending_primary.root_key = "nfs1@node-b".to_string();
        redundant_pending_primary.object_ref = "node-b::nfs1".to_string();
        redundant_pending_primary.rescan_pending = true;
        redundant_pending_primary.last_rescan_requested_at_us = Some(30);
        redundant_pending_primary.last_audit_started_at_us = None;
        redundant_pending_primary.last_audit_completed_at_us = None;
        redundant_pending_primary.last_audit_duration_ms = None;
        let source_status = SourceStatusSnapshot {
            logical_roots: vec![SourceLogicalRootHealthSnapshot {
                root_id: "nfs1".to_string(),
                status: "ready".to_string(),
                active_members: 3,
                matched_grants: 3,
                coverage_mode: "realtime_hotset_plus_audit".to_string(),
            }],
            concrete_roots: vec![completed_primary, redundant_pending_primary],
            ..SourceStatusSnapshot::default()
        };
        let sink_status = SinkStatusSnapshot {
            groups: vec![ready_sink_group("nfs1")],
            scheduled_groups_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["nfs1".to_string()],
            )]),
            ..SinkStatusSnapshot::default()
        };
        let readiness_groups = BTreeSet::from(["nfs1".to_string()]);

        let candidate_evidence =
            candidate_group_observation_evidence(&source_status, &sink_status, &readiness_groups);
        assert!(
            candidate_evidence.initial_audit_groups.is_empty(),
            "a redundant pending concrete primary must not block a group that already has current owner audit evidence: {candidate_evidence:?}"
        );
        let materialized_evidence =
            materialized_query_observation_evidence(&source_status, &sink_status);
        assert!(
            materialized_evidence.initial_audit_groups.is_empty(),
            "materialized query observation must use the same group-level source evidence: {materialized_evidence:?}"
        );
        assert_eq!(
            materialized_observation_status_for_readiness_groups(
                &source_status,
                &sink_status,
                &readiness_groups,
            )
            .state,
            ObservationState::TrustedMaterialized,
        );
        assert!(materialized_status_cache_is_ready(
            &source_status,
            &sink_status,
            &readiness_groups,
        ));
    }

    #[test]
    fn materialized_status_cache_is_not_ready_while_primary_audit_is_inflight() {
        let mut root = concrete_root("nfs1", true);
        root.last_audit_started_at_us = Some(30);
        root.last_audit_completed_at_us = None;
        let source_status = SourceStatusSnapshot {
            logical_roots: vec![SourceLogicalRootHealthSnapshot {
                root_id: "nfs1".to_string(),
                status: "ready".to_string(),
                active_members: 3,
                matched_grants: 3,
                coverage_mode: "realtime_hotset_plus_audit".to_string(),
            }],
            concrete_roots: vec![root],
            ..SourceStatusSnapshot::default()
        };
        let sink_status = SinkStatusSnapshot {
            groups: vec![ready_sink_group("nfs1")],
            scheduled_groups_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["nfs1".to_string()],
            )]),
            ..SinkStatusSnapshot::default()
        };
        let readiness_groups = BTreeSet::from(["nfs1".to_string()]);

        let evidence = materialized_query_observation_evidence(&source_status, &sink_status);
        assert_eq!(
            evidence.initial_audit_groups,
            BTreeSet::from(["nfs1".to_string()])
        );
        assert!(!materialized_status_cache_is_ready(
            &source_status,
            &sink_status,
            &readiness_groups,
        ));
    }

    #[test]
    fn non_primary_rescan_pending_does_not_satisfy_materialized_readiness() {
        let mut root = concrete_root("nfs1", false);
        root.rescan_pending = true;
        let source_status = SourceStatusSnapshot {
            logical_roots: vec![SourceLogicalRootHealthSnapshot {
                root_id: "nfs1".to_string(),
                status: "ready".to_string(),
                active_members: 3,
                matched_grants: 3,
                coverage_mode: "realtime_hotset_plus_audit".to_string(),
            }],
            concrete_roots: vec![root],
            ..SourceStatusSnapshot::default()
        };
        let sink_status = SinkStatusSnapshot {
            groups: vec![ready_sink_group("nfs1")],
            scheduled_groups_by_node: BTreeMap::from([(
                "node-a".to_string(),
                vec!["nfs1".to_string()],
            )]),
            ..SinkStatusSnapshot::default()
        };
        let readiness_groups = BTreeSet::from(["nfs1".to_string()]);

        assert!(!materialized_status_cache_is_ready(
            &source_status,
            &sink_status,
            &readiness_groups,
        ));
    }
}

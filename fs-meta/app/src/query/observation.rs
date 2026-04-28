use std::collections::{BTreeMap, BTreeSet};

use crate::sink::SinkStatusSnapshot;
use crate::source::SourceStatusSnapshot;
pub use capanix_managed_state_sdk::{
    ObservationEvidence, ObservationTrustPolicy, evaluate_observation_status,
    trusted_materialized_not_ready_message,
};

fn concrete_root_counts_as_materialized_candidate(
    root: &crate::source::SourceConcreteRootHealthSnapshot,
) -> bool {
    root.active
        && root.is_group_primary
        && root.scan_enabled
        && !root.status.starts_with("waiting_for_root:")
}

fn group_has_local_sink_presence(
    group_id: &str,
    sink_groups: &BTreeMap<&str, &crate::sink::SinkGroupStatusSnapshot>,
    scheduled_groups: &BTreeSet<String>,
) -> bool {
    sink_groups.contains_key(group_id) || scheduled_groups.contains(group_id)
}

fn sink_group_blocks_materialized_observation(
    group: &crate::sink::SinkGroupStatusSnapshot,
) -> bool {
    !matches!(group.readiness, crate::sink::GroupReadinessState::Ready)
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
        if sink_group_blocks_materialized_observation(group) {
            initial_audit_groups.insert(group_id.clone());
        }
        if group.overflow_pending_materialization {
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

    ObservationEvidence {
        candidate_groups: candidate_groups.clone(),
        initial_audit_groups,
        degraded_groups,
        overflow_pending_groups,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::SinkGroupStatusSnapshot;
    use crate::source::{SourceConcreteRootHealthSnapshot, SourceLogicalRootHealthSnapshot};

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
            last_rescan_reason: None,
            last_error: None,
            last_audit_started_at_us: None,
            last_audit_completed_at_us: None,
            last_audit_duration_ms: None,
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
}

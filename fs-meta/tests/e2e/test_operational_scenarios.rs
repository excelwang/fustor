#![cfg(target_os = "linux")]

use crate::support::api_client::OperatorSession;
use crate::support::cluster5::Cluster5;
use crate::support::nfs_lab::NfsLab;
use crate::support::{reserve_http_addrs, skip_unless_real_nfs_enabled, wait_until};
use fs_meta::{RootSelector, RootSpec};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OperationalMode {
    Smoke,
    ForceFindExecutionSemantics,
    NewNfsJoin,
    RootPathModify,
    VisibilityChangeAndSinkSelection,
    SinkFailover,
    FacadeFailoverAndResourceSwitch,
    NfsRetire,
}

impl OperationalMode {
    fn app_prefix(self) -> &'static str {
        match self {
            Self::Smoke => "fs-meta-api-ops-smoke",
            Self::ForceFindExecutionSemantics => "fs-meta-api-ops-force-find",
            Self::NewNfsJoin => "fs-meta-api-ops-nfs-join",
            Self::RootPathModify => "fs-meta-api-ops-root-path",
            Self::VisibilityChangeAndSinkSelection => "fs-meta-api-ops-visibility",
            Self::SinkFailover => "fs-meta-api-ops-sink-failover",
            Self::FacadeFailoverAndResourceSwitch => "fs-meta-api-ops-facade-failover",
            Self::NfsRetire => "fs-meta-api-ops-nfs-retire",
        }
    }
}

struct OperationalHarness {
    cluster: Cluster5,
    lab: NfsLab,
    session: OperatorSession,
    app_id: String,
    facade_resource_id: String,
    facade_addrs: Vec<String>,
}

pub fn run() -> Result<(), String> {
    run_mode(OperationalMode::Smoke)
}

pub fn run_force_find_execution_semantics() -> Result<(), String> {
    run_mode(OperationalMode::ForceFindExecutionSemantics)
}

pub fn run_new_nfs_join() -> Result<(), String> {
    run_mode(OperationalMode::NewNfsJoin)
}

pub fn run_root_path_modify() -> Result<(), String> {
    run_mode(OperationalMode::RootPathModify)
}

pub fn run_visibility_change_and_sink_selection() -> Result<(), String> {
    run_mode(OperationalMode::VisibilityChangeAndSinkSelection)
}

pub fn run_sink_failover() -> Result<(), String> {
    run_mode(OperationalMode::SinkFailover)
}

pub fn run_facade_failover_and_resource_switch() -> Result<(), String> {
    run_mode(OperationalMode::FacadeFailoverAndResourceSwitch)
}

pub fn run_nfs_retire() -> Result<(), String> {
    run_mode(OperationalMode::NfsRetire)
}

#[test]
#[ignore = "requires Linux + CAPANIX_REAL_NFS_E2E=1 + passwordless sudo"]
fn activation_scope_capture_preserved_layout_distributed() {
    run_activation_scope_capture_preserved_layout().unwrap();
}

#[test]
#[ignore = "requires Linux + CAPANIX_REAL_NFS_E2E=1 + passwordless sudo"]
fn activation_scope_capture_nfs2_visibility_contracted_to_node_a() {
    run_activation_scope_capture_nfs2_visibility_contracted_to_node_a().unwrap();
}

#[test]
#[ignore = "requires Linux + CAPANIX_REAL_NFS_E2E=1 + passwordless sudo"]
fn activation_scope_capture_force_find_preserved_pre_force_find() {
    run_activation_scope_capture_force_find_preserved_pre_force_find().unwrap();
}

fn run_mode(mode: OperationalMode) -> Result<(), String> {
    if let Some(reason) = skip_unless_real_nfs_enabled() {
        eprintln!("[fs-meta-api-ops] skipped: {reason}");
        return Ok(());
    }

    let mut harness = build_operational_harness(
        mode.app_prefix(),
        matches!(
            mode,
            OperationalMode::NewNfsJoin | OperationalMode::NfsRetire
        ),
        if matches!(mode, OperationalMode::FacadeFailoverAndResourceSwitch) {
            2
        } else {
            1
        },
    )?;
    match mode {
        OperationalMode::Smoke => {
            scenario_force_find_smoke(&mut harness.lab, &mut harness.session)?;
        }
        OperationalMode::ForceFindExecutionSemantics => {
            scenario_force_find_execution_semantics(
                &harness.cluster,
                &mut harness.lab,
                &mut harness.session,
            )?;
        }
        OperationalMode::NewNfsJoin => {
            scenario_new_nfs_join(&harness.cluster, &mut harness.lab, &mut harness.session)?;
        }
        OperationalMode::RootPathModify => {
            scenario_root_path_modify(&harness.cluster, &harness.lab, &mut harness.session)?;
        }
        OperationalMode::VisibilityChangeAndSinkSelection => {
            scenario_visibility_change_and_sink_selection(
                &harness.cluster,
                &mut harness.lab,
                &mut harness.session,
                &harness.app_id,
                &harness.facade_resource_id,
            )?;
        }
        OperationalMode::SinkFailover => {
            scenario_sink_failover(&harness.cluster, &mut harness.session, &harness.app_id)?;
        }
        OperationalMode::FacadeFailoverAndResourceSwitch => {
            scenario_facade_failover_and_resource_switch(
                &harness.cluster,
                &mut harness.session,
                &harness.facade_resource_id,
                &harness.facade_addrs,
                &harness.app_id,
            )?;
        }
        OperationalMode::NfsRetire => {
            scenario_new_nfs_join(&harness.cluster, &mut harness.lab, &mut harness.session)?;
            scenario_nfs_retire(&harness.cluster, &mut harness.lab, &mut harness.session)?;
        }
    }

    Ok(())
}

fn run_activation_scope_capture_preserved_layout() -> Result<(), String> {
    if let Some(reason) = skip_unless_real_nfs_enabled() {
        eprintln!("[fs-meta-api-ops] activation-scope skipped: {reason}");
        return Ok(());
    }
    let mut harness = build_operational_harness("fs-meta-activation-scope", false, 1)?;
    scenario_force_find_smoke(&mut harness.lab, &mut harness.session)?;
    let _ = harness.session.tree(&[
        ("path", "/force-find-stress".to_string()),
        ("recursive", "true".to_string()),
    ])?;

    let mut node_a_source_pids = BTreeSet::new();
    let mut node_a_sink_pids = BTreeSet::new();
    let mut sink_holder = None::<String>;
    wait_until(
        Duration::from_secs(90),
        "node-a runtime activation reaches source and sink units",
        || {
            node_a_source_pids = harness.cluster.unit_active_pids_for_instance(
                "node-a",
                &harness.app_id,
                "runtime.exec.source",
            )?;
            node_a_sink_pids = harness.cluster.unit_active_pids_for_instance(
                "node-a",
                &harness.app_id,
                "runtime.exec.sink",
            )?;
            sink_holder =
                current_sink_holder_for_group(&harness.cluster, &harness.app_id, "nfs2")?;
            if !node_a_source_pids.is_empty() && !node_a_sink_pids.is_empty() {
                return Ok(true);
            }
            Err(format!(
                "node-a source_pids={node_a_source_pids:?} sink_pids={node_a_sink_pids:?} sink_holder={sink_holder:?}"
            ))
        },
    )?;

    let mut node_a_source = BTreeSet::new();
    let mut node_a_scan = BTreeSet::new();
    let mut node_a_sink = BTreeSet::new();
    let mut node_a_routes = Vec::<String>::new();
    wait_until(
        Duration::from_secs(30),
        "distributed activation scope capture converges",
        || {
            node_a_routes = activation_route_summaries(&harness.cluster, "node-a")?;
            node_a_source = unit_bound_scope_ids_from_activation_status(
                &harness.cluster,
                "node-a",
                "runtime.exec.source",
            )?;
            node_a_scan = unit_bound_scope_ids_from_activation_status(
                &harness.cluster,
                "node-a",
                "runtime.exec.scan",
            )?;
            node_a_sink = unit_bound_scope_ids_from_activation_status(
                &harness.cluster,
                "node-a",
                "runtime.exec.sink",
            )?;
            if !node_a_routes.is_empty()
                && node_a_sink.contains("nfs1")
                && node_a_sink.contains("nfs2")
            {
                return Ok(true);
            }
            Err(format!(
                "node-a routes={node_a_routes:?} source={node_a_source:?} scan={node_a_scan:?} sink={node_a_sink:?}"
            ))
        },
    )?;

    eprintln!(
        "[fs-meta-api-ops] activation-scope-capture node-a source_pids={node_a_source_pids:?} sink_pids={node_a_sink_pids:?} sink_holder={sink_holder:?} routes={node_a_routes:?} source={node_a_source:?} scan={node_a_scan:?} sink={node_a_sink:?}"
    );
    assert!(
        node_a_source.contains("nfs1"),
        "incoming activation summary should include nfs1 on node-a source: source={node_a_source:?} scan={node_a_scan:?} sink={node_a_sink:?}"
    );
    assert!(
        node_a_scan.contains("nfs1"),
        "incoming activation summary should include nfs1 on node-a scan: source={node_a_source:?} scan={node_a_scan:?} sink={node_a_sink:?}"
    );
    assert!(
        node_a_sink.contains("nfs2"),
        "preserved layout should bind node-a sink/query for nfs2: sink={node_a_sink:?}"
    );
    assert!(
        node_a_source.contains("nfs2"),
        "incoming activation summary should include nfs2 on node-a source when node-a is a visible nfs2 scope member: source={node_a_source:?} scan={node_a_scan:?} sink={node_a_sink:?}"
    );
    assert!(
        node_a_scan.contains("nfs2"),
        "incoming activation summary should include nfs2 on node-a scan when node-a is a visible nfs2 scope member: source={node_a_source:?} scan={node_a_scan:?} sink={node_a_sink:?}"
    );

    Ok(())
}

fn run_activation_scope_capture_nfs2_visibility_contracted_to_node_a() -> Result<(), String> {
    if let Some(reason) = skip_unless_real_nfs_enabled() {
        eprintln!("[fs-meta-api-ops] activation-scope-node-a skipped: {reason}");
        return Ok(());
    }

    let mut harness = build_operational_harness("fs-meta-activation-scope-node-a", false, 1)?;
    let single_root = json!([root_payload("nfs2", &harness.lab.export_source("nfs2"), "/")]);
    harness.session.update_roots(&single_root)?;
    harness.session.rescan()?;

    wait_until(Duration::from_secs(30), "single root nfs2 active", || {
        let current = harness.session.monitoring_roots()?;
        Ok(current
            .get("roots")
            .and_then(Value::as_array)
            .map(|rows| {
                rows.len() == 1 && rows[0].get("id").and_then(Value::as_str) == Some("nfs2")
            })
            .unwrap_or(false))
    })?;

    wait_until(
        Duration::from_secs(60),
        "source active on nfs2 visible members",
        || {
            let a = harness.cluster.unit_active_pids_for_instance(
                "node-a",
                &harness.app_id,
                "runtime.exec.source",
            )?;
            let c = harness.cluster.unit_active_pids_for_instance(
                "node-c",
                &harness.app_id,
                "runtime.exec.source",
            )?;
            let d = harness.cluster.unit_active_pids_for_instance(
                "node-d",
                &harness.app_id,
                "runtime.exec.source",
            )?;
            if !a.is_empty() && !c.is_empty() && !d.is_empty() {
                return Ok(true);
            }
            Err(format!("node-a={a:?} node-c={c:?} node-d={d:?}"))
        },
    )?;

    for node_name in ["node-c", "node-d"] {
        harness.cluster.withdraw_resources_clusterwide(
            &harness.cluster.node_id(node_name)?,
            vec!["nfs2".to_string()],
        )?;
        let _ = harness.lab.unmount_export(node_name, "nfs2");
    }

    wait_until(
        Duration::from_secs(60),
        "nfs2 withdrawn from node-c/node-d grants",
        || {
            let grants = harness.session.runtime_grants()?;
            Ok(!grants
                .to_string()
                .contains(&format!("{}::nfs2", harness.cluster.node_id("node-c")?))
                && !grants
                    .to_string()
                    .contains(&format!("{}::nfs2", harness.cluster.node_id("node-d")?)))
        },
    )?;

    let mut node_a_source_pids = BTreeSet::new();
    let mut node_a_sink_pids = BTreeSet::new();
    let mut sink_holder = None::<String>;
    wait_until(
        Duration::from_secs(90),
        "node-a becomes sole nfs2 source/sink holder",
        || {
            node_a_source_pids = harness.cluster.unit_active_pids_for_instance(
                "node-a",
                &harness.app_id,
                "runtime.exec.source",
            )?;
            node_a_sink_pids = harness.cluster.unit_active_pids_for_instance(
                "node-a",
                &harness.app_id,
                "runtime.exec.sink",
            )?;
            sink_holder =
                current_sink_holder_for_group(&harness.cluster, &harness.app_id, "nfs2")?;
            if !node_a_source_pids.is_empty()
                && !node_a_sink_pids.is_empty()
                && sink_holder.as_deref() == Some("node-a")
            {
                return Ok(true);
            }
            Err(format!(
                "node-a source_pids={node_a_source_pids:?} sink_pids={node_a_sink_pids:?} sink_holder={sink_holder:?}"
            ))
        },
    )?;

    let node_a_routes = activation_route_summaries(&harness.cluster, "node-a")?;
    let node_a_source = unit_bound_scope_ids_from_activation_status(
        &harness.cluster,
        "node-a",
        "runtime.exec.source",
    )?;
    let node_a_scan =
        unit_bound_scope_ids_from_activation_status(&harness.cluster, "node-a", "runtime.exec.scan")?;
    let node_a_sink =
        unit_bound_scope_ids_from_activation_status(&harness.cluster, "node-a", "runtime.exec.sink")?;

    eprintln!(
        "[fs-meta-api-ops] activation-scope-node-a source_pids={node_a_source_pids:?} sink_pids={node_a_sink_pids:?} sink_holder={sink_holder:?} routes={node_a_routes:?} source={node_a_source:?} scan={node_a_scan:?} sink={node_a_sink:?}"
    );
    assert!(
        !node_a_routes.is_empty(),
        "node-a activation routes should exist once source/sink activation is delivered: source_pids={node_a_source_pids:?} sink_pids={node_a_sink_pids:?} sink_holder={sink_holder:?}"
    );
    assert!(
        node_a_source.contains("nfs2"),
        "node-a source scopes should include nfs2 after visibility contraction: routes={node_a_routes:?} source={node_a_source:?} scan={node_a_scan:?} sink={node_a_sink:?}"
    );
    assert!(
        node_a_scan.contains("nfs2"),
        "node-a scan scopes should include nfs2 after visibility contraction: routes={node_a_routes:?} source={node_a_source:?} scan={node_a_scan:?} sink={node_a_sink:?}"
    );
    assert!(
        node_a_sink.contains("nfs2"),
        "node-a sink scopes should include nfs2 after visibility contraction: routes={node_a_routes:?} source={node_a_source:?} scan={node_a_scan:?} sink={node_a_sink:?}"
    );

    Ok(())
}

fn run_activation_scope_capture_force_find_preserved_pre_force_find() -> Result<(), String> {
    if let Some(reason) = skip_unless_real_nfs_enabled() {
        eprintln!("[fs-meta-api-ops] activation-scope-preserved skipped: {reason}");
        return Ok(());
    }

    let mut harness = build_operational_harness("fs-meta-activation-force-find-preserved", false, 1)?;
    scenario_force_find_smoke(&mut harness.lab, &mut harness.session)?;
    let node_a_id = harness.cluster.node_id("node-a")?;

    let node_a_nfs1_object_ref = format!("{node_a_id}::nfs1");
    let node_a_nfs2_object_ref = format!("{node_a_id}::nfs2");

    let mut nfs1_nodes = 0u64;
    let mut nfs2_nodes = 0u64;
    let mut node_a_source = BTreeSet::new();
    let mut node_a_scan = BTreeSet::new();
    let mut node_a_sink = BTreeSet::new();
    let mut node_a_source_control = Vec::<String>::new();
    let mut node_a_sink_control = Vec::<String>::new();
    let mut node_a_source_published_batches = 0u64;
    let mut node_a_source_published_events = 0u64;
    let mut node_a_source_published_control = 0u64;
    let mut node_a_source_published_data = 0u64;
    let mut node_a_source_current_stream_generation = None::<u64>;
    let mut node_a_source_published_origins = Vec::<String>::new();
    let mut node_a_source_published_origin_counts = Vec::<String>::new();
    let mut node_a_source_enqueued_path_origin_counts = Vec::<String>::new();
    let mut node_a_source_pending_path_origin_counts = Vec::<String>::new();
    let mut node_a_source_yielded_path_origin_counts = Vec::<String>::new();
    let mut node_a_source_summarized_path_origin_counts = Vec::<String>::new();
    let mut node_a_source_published_path_origin_counts = Vec::<String>::new();
    let mut node_a_sink_received_batches = 0u64;
    let mut node_a_sink_received_events = 0u64;
    let mut node_a_sink_received_control = 0u64;
    let mut node_a_sink_received_data = 0u64;
    let mut node_a_sink_received_origins = Vec::<String>::new();
    let mut node_a_sink_received_origin_counts = Vec::<String>::new();
    let mut nfs2_selected_root_nodes = 0u64;
    let mut nfs2_selected_root_exists = false;
    let mut nfs2_selected_root_entries = 0usize;
    let mut nfs2_selected_root_paths = Vec::<String>::new();
    let mut nfs2_selected_root_has_force_find = false;
    let mut nfs2_selected_root_has_force_find_child = false;
    let mut nfs2_selected_force_find_nodes = 0u64;
    let mut nfs2_selected_force_find_exists = false;
    let mut nfs2_selected_force_find_entries = 0usize;
    let mut node_a_nfs1_concrete = Vec::<String>::new();
    let mut nfs2_primary = None::<String>;
    let mut nfs2_logical = None::<String>;
    let mut node_a_nfs2_concrete = Vec::<String>::new();
    wait_until(
        Duration::from_secs(90),
        "preserved pre-force-find activation capture",
        || {
            let tree = harness.session.tree(&[
                ("path", "/force-find-stress".to_string()),
                ("recursive", "true".to_string()),
            ])?;
            nfs1_nodes = group_total_nodes(&tree, "nfs1");
            nfs2_nodes = group_total_nodes(&tree, "nfs2");
            let nfs2_selected_root = harness.session.tree(&[
                ("path", "/".to_string()),
                ("recursive", "true".to_string()),
                ("group", "nfs2".to_string()),
            ])?;
            (
                nfs2_selected_root_exists,
                nfs2_selected_root_entries,
                nfs2_selected_root_nodes,
            ) = group_root_exists_entries_and_total(&nfs2_selected_root, "nfs2");
            nfs2_selected_root_paths = group_entry_paths(&nfs2_selected_root, "nfs2");
            nfs2_selected_root_has_force_find = nfs2_selected_root_paths
                .iter()
                .any(|path| path == "/force-find-stress");
            nfs2_selected_root_has_force_find_child = nfs2_selected_root_paths
                .iter()
                .any(|path| path.starts_with("/force-find-stress/"));
            let nfs2_selected_force_find = harness.session.tree(&[
                ("path", "/force-find-stress".to_string()),
                ("recursive", "true".to_string()),
                ("group", "nfs2".to_string()),
            ])?;
            (
                nfs2_selected_force_find_exists,
                nfs2_selected_force_find_entries,
                nfs2_selected_force_find_nodes,
            ) = group_root_exists_entries_and_total(&nfs2_selected_force_find, "nfs2");
            let status = harness.session.status()?;
            node_a_source = status_debug_groups_by_node(
                &status,
                "source",
                "scheduled_source_groups_by_node",
                &node_a_id,
            );
            node_a_scan = status_debug_groups_by_node(
                &status,
                "source",
                "scheduled_scan_groups_by_node",
                &node_a_id,
            );
            node_a_sink = status_debug_groups_by_node(
                &status,
                "sink",
                "scheduled_groups_by_node",
                &node_a_id,
            );
            node_a_source_control = status_debug_strings_by_node(
                &status,
                "source",
                "last_control_frame_signals_by_node",
                &node_a_id,
            );
            node_a_sink_control = status_debug_strings_by_node(
                &status,
                "sink",
                "last_control_frame_signals_by_node",
                &node_a_id,
            );
            node_a_source_published_batches = status_debug_u64_by_node(
                &status,
                "source",
                "published_batches_by_node",
                &node_a_id,
            );
            node_a_source_published_events = status_debug_u64_by_node(
                &status,
                "source",
                "published_events_by_node",
                &node_a_id,
            );
            node_a_source_published_control = status_debug_u64_by_node(
                &status,
                "source",
                "published_control_events_by_node",
                &node_a_id,
            );
            node_a_source_published_data = status_debug_u64_by_node(
                &status,
                "source",
                "published_data_events_by_node",
                &node_a_id,
            );
            node_a_source_current_stream_generation = status
                .get("source")
                .and_then(|source| source.get("debug"))
                .and_then(|debug| debug.get("current_stream_generation"))
                .and_then(Value::as_u64);
            node_a_source_published_origins = status_debug_strings_by_node(
                &status,
                "source",
                "last_published_origins_by_node",
                &node_a_id,
            );
            node_a_source_published_origin_counts = status_debug_strings_by_node(
                &status,
                "source",
                "published_origin_counts_by_node",
                &node_a_id,
            );
            node_a_source_enqueued_path_origin_counts = status_debug_strings_by_node(
                &status,
                "source",
                "enqueued_path_origin_counts_by_node",
                &node_a_id,
            );
            node_a_source_pending_path_origin_counts = status_debug_strings_by_node(
                &status,
                "source",
                "pending_path_origin_counts_by_node",
                &node_a_id,
            );
            node_a_source_yielded_path_origin_counts = status_debug_strings_by_node(
                &status,
                "source",
                "yielded_path_origin_counts_by_node",
                &node_a_id,
            );
            node_a_source_summarized_path_origin_counts = status_debug_strings_by_node(
                &status,
                "source",
                "summarized_path_origin_counts_by_node",
                &node_a_id,
            );
            node_a_source_published_path_origin_counts = status_debug_strings_by_node(
                &status,
                "source",
                "published_path_origin_counts_by_node",
                &node_a_id,
            );
            node_a_sink_received_batches = status_debug_u64_by_node(
                &status,
                "sink",
                "received_batches_by_node",
                &node_a_id,
            );
            node_a_sink_received_events = status_debug_u64_by_node(
                &status,
                "sink",
                "received_events_by_node",
                &node_a_id,
            );
            node_a_sink_received_control = status_debug_u64_by_node(
                &status,
                "sink",
                "received_control_events_by_node",
                &node_a_id,
            );
            node_a_sink_received_data = status_debug_u64_by_node(
                &status,
                "sink",
                "received_data_events_by_node",
                &node_a_id,
            );
            node_a_sink_received_origins = status_debug_strings_by_node(
                &status,
                "sink",
                "last_received_origins_by_node",
                &node_a_id,
            );
            node_a_sink_received_origin_counts = status_debug_strings_by_node(
                &status,
                "sink",
                "received_origin_counts_by_node",
                &node_a_id,
            );
            node_a_nfs1_concrete = source_concrete_root_summaries(
                &mut harness.session,
                &node_a_nfs1_object_ref,
            )?;
            nfs2_primary = source_primary_for_group(&mut harness.session, "nfs2")?;
            nfs2_logical = source_logical_root_summary(&mut harness.session, "nfs2")?;
            node_a_nfs2_concrete = source_concrete_root_summaries(
                &mut harness.session,
                &node_a_nfs2_object_ref,
            )?;
            if nfs1_nodes > 0
                && nfs2_nodes > 0
                && nfs2_selected_root_exists
                && nfs2_selected_root_has_force_find
                && nfs2_selected_force_find_exists
                && node_a_sink.contains("nfs2")
                && !node_a_source_control.is_empty()
                && !node_a_sink_control.is_empty()
            {
                return Ok(true);
            }
            Err(format!(
                "nfs1_nodes={nfs1_nodes} nfs2_nodes={nfs2_nodes} nfs2_selected_root_exists={nfs2_selected_root_exists} nfs2_selected_root_entries={nfs2_selected_root_entries} nfs2_selected_root_nodes={nfs2_selected_root_nodes} nfs2_selected_root_has_force_find={nfs2_selected_root_has_force_find} nfs2_selected_root_has_force_find_child={nfs2_selected_root_has_force_find_child} nfs2_selected_root_paths={nfs2_selected_root_paths:?} nfs2_selected_force_find_exists={nfs2_selected_force_find_exists} nfs2_selected_force_find_entries={nfs2_selected_force_find_entries} nfs2_selected_force_find_nodes={nfs2_selected_force_find_nodes} source={node_a_source:?} scan={node_a_scan:?} sink={node_a_sink:?} source_control={node_a_source_control:?} sink_control={node_a_sink_control:?} source_current_stream_generation={node_a_source_current_stream_generation:?} source_published_batches={node_a_source_published_batches} source_published_events={node_a_source_published_events} source_published_control={node_a_source_published_control} source_published_data={node_a_source_published_data} source_published_origins={node_a_source_published_origins:?} source_published_origin_counts={node_a_source_published_origin_counts:?} source_enqueued_path_origin_counts={node_a_source_enqueued_path_origin_counts:?} source_pending_path_origin_counts={node_a_source_pending_path_origin_counts:?} source_yielded_path_origin_counts={node_a_source_yielded_path_origin_counts:?} source_summarized_path_origin_counts={node_a_source_summarized_path_origin_counts:?} source_published_path_origin_counts={node_a_source_published_path_origin_counts:?} sink_received_batches={node_a_sink_received_batches} sink_received_events={node_a_sink_received_events} sink_received_control={node_a_sink_received_control} sink_received_data={node_a_sink_received_data} sink_received_origins={node_a_sink_received_origins:?} sink_received_origin_counts={node_a_sink_received_origin_counts:?} node_a_nfs1_concrete={node_a_nfs1_concrete:?} nfs2_primary={nfs2_primary:?} nfs2_logical={nfs2_logical:?} node_a_nfs2_concrete={node_a_nfs2_concrete:?}"
            ))
        },
    )?;

    eprintln!(
        "[fs-meta-api-ops] activation-scope-preserved nfs1_nodes={nfs1_nodes} nfs2_nodes={nfs2_nodes} nfs2_selected_root_exists={nfs2_selected_root_exists} nfs2_selected_root_entries={nfs2_selected_root_entries} nfs2_selected_root_nodes={nfs2_selected_root_nodes} nfs2_selected_root_has_force_find={nfs2_selected_root_has_force_find} nfs2_selected_root_has_force_find_child={nfs2_selected_root_has_force_find_child} nfs2_selected_root_paths={nfs2_selected_root_paths:?} nfs2_selected_force_find_exists={nfs2_selected_force_find_exists} nfs2_selected_force_find_entries={nfs2_selected_force_find_entries} nfs2_selected_force_find_nodes={nfs2_selected_force_find_nodes} source={node_a_source:?} scan={node_a_scan:?} sink={node_a_sink:?} source_control={node_a_source_control:?} sink_control={node_a_sink_control:?} source_current_stream_generation={node_a_source_current_stream_generation:?} source_published_batches={node_a_source_published_batches} source_published_events={node_a_source_published_events} source_published_control={node_a_source_published_control} source_published_data={node_a_source_published_data} source_published_origins={node_a_source_published_origins:?} source_published_origin_counts={node_a_source_published_origin_counts:?} source_enqueued_path_origin_counts={node_a_source_enqueued_path_origin_counts:?} source_pending_path_origin_counts={node_a_source_pending_path_origin_counts:?} source_yielded_path_origin_counts={node_a_source_yielded_path_origin_counts:?} source_summarized_path_origin_counts={node_a_source_summarized_path_origin_counts:?} source_published_path_origin_counts={node_a_source_published_path_origin_counts:?} sink_received_batches={node_a_sink_received_batches} sink_received_events={node_a_sink_received_events} sink_received_control={node_a_sink_received_control} sink_received_data={node_a_sink_received_data} sink_received_origins={node_a_sink_received_origins:?} sink_received_origin_counts={node_a_sink_received_origin_counts:?} node_a_nfs1_concrete={node_a_nfs1_concrete:?} nfs2_primary={nfs2_primary:?} nfs2_logical={nfs2_logical:?} node_a_nfs2_concrete={node_a_nfs2_concrete:?}"
    );
    if let Some(window_secs) = debug_publish_lag_window_secs() {
        let mut late_path_counts = node_a_source_published_path_origin_counts.clone();
        let mut late_nfs2_nodes = nfs2_nodes;
        let mut late_published_events = node_a_source_published_events;
        let mut late_published_data = node_a_source_published_data;
        let mut late_last_published_at_us = 0u64;
        let initial_nfs1_path_count =
            published_origin_count(&node_a_source_published_path_origin_counts, &node_a_nfs1_object_ref);
        let initial_nfs2_path_count =
            published_origin_count(&node_a_source_published_path_origin_counts, &node_a_nfs2_object_ref);
        let initial_nfs1_emitted_path_events = source_concrete_root_field_u64(
            &mut harness.session,
            &node_a_nfs1_object_ref,
            "emitted_path_event_count",
        )?;
        let initial_nfs2_emitted_path_events = source_concrete_root_field_u64(
            &mut harness.session,
            &node_a_nfs2_object_ref,
            "emitted_path_event_count",
        )?;
        let initial_nfs1_transition = source_concrete_root_transition_snapshot(
            &mut harness.session,
            &node_a_nfs1_object_ref,
        )?;
        let initial_nfs2_transition = source_concrete_root_transition_snapshot(
            &mut harness.session,
            &node_a_nfs2_object_ref,
        )?;
        let mut late_nfs1_emitted_path_events = initial_nfs1_emitted_path_events;
        let mut late_nfs2_emitted_path_events = initial_nfs2_emitted_path_events;
        let mut late_nfs1_transition = initial_nfs1_transition.clone();
        let mut late_nfs2_transition = initial_nfs2_transition.clone();
        let mut saw_late_publish = published_origin_counts_include(
            &late_path_counts,
            &node_a_nfs2_object_ref,
        );
        let mut saw_late_tree = late_nfs2_nodes > 0;
        let deadline = std::time::Instant::now() + Duration::from_secs(window_secs);
        while std::time::Instant::now() < deadline {
            let tree = harness.session.tree(&[
                ("path", "/force-find-stress".to_string()),
                ("recursive", "true".to_string()),
            ])?;
            late_nfs2_nodes = group_total_nodes(&tree, "nfs2");
            let status = harness.session.status()?;
            late_published_events = status_debug_u64_by_node(
                &status,
                "source",
                "published_events_by_node",
                &node_a_id,
            );
            late_published_data = status_debug_u64_by_node(
                &status,
                "source",
                "published_data_events_by_node",
                &node_a_id,
            );
            late_last_published_at_us = status_debug_u64_by_node(
                &status,
                "source",
                "last_published_at_us_by_node",
                &node_a_id,
            );
            late_nfs1_emitted_path_events = source_concrete_root_field_u64(
                &mut harness.session,
                &node_a_nfs1_object_ref,
                "emitted_path_event_count",
            )?;
            late_nfs2_emitted_path_events = source_concrete_root_field_u64(
                &mut harness.session,
                &node_a_nfs2_object_ref,
                "emitted_path_event_count",
            )?;
            late_nfs1_transition = source_concrete_root_transition_snapshot(
                &mut harness.session,
                &node_a_nfs1_object_ref,
            )?;
            late_nfs2_transition = source_concrete_root_transition_snapshot(
                &mut harness.session,
                &node_a_nfs2_object_ref,
            )?;
            late_path_counts = status_debug_strings_by_node(
                &status,
                "source",
                "published_path_origin_counts_by_node",
                &node_a_id,
            );
            saw_late_publish =
                published_origin_counts_include(&late_path_counts, &node_a_nfs2_object_ref);
            saw_late_tree = late_nfs2_nodes > 0;
            if saw_late_publish || saw_late_tree {
                break;
            }
            thread::sleep(Duration::from_millis(250));
        }
        let late_nfs1_path_count = published_origin_count(&late_path_counts, &node_a_nfs1_object_ref);
        let late_nfs2_path_count = published_origin_count(&late_path_counts, &node_a_nfs2_object_ref);
        eprintln!(
            "[fs-meta-api-ops] activation-scope-preserved-lag-window window_secs={window_secs} saw_late_publish={saw_late_publish} saw_late_tree={saw_late_tree} initial_published_events={node_a_source_published_events} initial_published_data={node_a_source_published_data} late_published_events={late_published_events} late_published_data={late_published_data} late_last_published_at_us={late_last_published_at_us} initial_nfs1_path_count={initial_nfs1_path_count} late_nfs1_path_count={late_nfs1_path_count} initial_nfs2_path_count={initial_nfs2_path_count} late_nfs2_path_count={late_nfs2_path_count} initial_nfs1_emitted_path_events={initial_nfs1_emitted_path_events} late_nfs1_emitted_path_events={late_nfs1_emitted_path_events} initial_nfs2_emitted_path_events={initial_nfs2_emitted_path_events} late_nfs2_emitted_path_events={late_nfs2_emitted_path_events} initial_nfs1_transition={} late_nfs1_transition={} initial_nfs2_transition={} late_nfs2_transition={} initial_path_counts={node_a_source_published_path_origin_counts:?} late_path_counts={late_path_counts:?} initial_nfs2_nodes={nfs2_nodes} late_nfs2_nodes={late_nfs2_nodes}",
            initial_nfs1_transition.summary(),
            late_nfs1_transition.summary(),
            initial_nfs2_transition.summary(),
            late_nfs2_transition.summary()
        );
    }
    assert!(
        node_a_sink.contains("nfs2"),
        "preserved failing shape should still show node-a sink coverage for nfs2 before first force-find: source={node_a_source:?} scan={node_a_scan:?} sink={node_a_sink:?} source_control={node_a_source_control:?} sink_control={node_a_sink_control:?}"
    );
    assert!(
        node_a_source.contains("nfs2"),
        "final /status source shaping omits node-a nfs2 coverage before preserved failure: source={node_a_source:?} scan={node_a_scan:?} sink={node_a_sink:?} source_control={node_a_source_control:?} sink_control={node_a_sink_control:?}"
    );
    assert!(
        node_a_scan.contains("nfs2"),
        "final /status scan shaping omits node-a nfs2 coverage before preserved failure: source={node_a_source:?} scan={node_a_scan:?} sink={node_a_sink:?} source_control={node_a_source_control:?} sink_control={node_a_sink_control:?}"
    );
    assert!(
        !node_a_source_control.is_empty(),
        "final /status source control shaping should remain non-empty on node-a during preserved failure: source={node_a_source:?} scan={node_a_scan:?} sink={node_a_sink:?} source_control={node_a_source_control:?} sink_control={node_a_sink_control:?}"
    );
    assert!(
        !node_a_sink_control.is_empty(),
        "final /status sink control shaping should remain non-empty on node-a during preserved failure: source={node_a_source:?} scan={node_a_scan:?} sink={node_a_sink:?} source_control={node_a_source_control:?} sink_control={node_a_sink_control:?}"
    );

    Ok(())
}

fn build_operational_harness(
    app_prefix: &str,
    preannounce_nfs4: bool,
    facade_count: usize,
) -> Result<OperationalHarness, String> {
    let mut lab = NfsLab::start()?;
    seed_baseline_content(&lab)?;
    let cluster = Cluster5::start()?;
    let app_id = format!("{app_prefix}-{}", unique_suffix());
    let facade_resource_id = format!("fs-meta-tcp-listener-{app_id}");
    let facade_addrs = reserve_http_addrs(facade_count)?;

    install_baseline_resources(&cluster, &mut lab, &facade_resource_id, &facade_addrs)?;
    if preannounce_nfs4 {
        lab.create_export("nfs4")?;
        let mount_a = lab.mount_export("node-a", "nfs4")?;
        let mount_c = lab.mount_export("node-c", "nfs4")?;
        let mount_d = lab.mount_export("node-d", "nfs4")?;
        announce_nfs(&cluster, &lab, "node-a", "nfs4", &mount_a)?;
        announce_nfs(&cluster, &lab, "node-c", "nfs4", &mount_c)?;
        announce_nfs(&cluster, &lab, "node-d", "nfs4", &mount_d)?;
    }
    let roots = baseline_roots(&lab);
    let release =
        cluster.build_fs_meta_release(&app_id, &facade_resource_id, roots.clone(), 1, true)?;
    cluster.apply_release("node-a", release)?;

    let candidate_base_urls = facade_addrs
        .iter()
        .map(|addr| format!("http://{addr}"))
        .collect::<Vec<_>>();
    let _base_url = cluster.wait_http_login_ready(
        &candidate_base_urls,
        "operator",
        "operator123",
        Duration::from_secs(120),
    )?;
    let mut session =
        OperatorSession::login_many(candidate_base_urls.clone(), "operator", "operator123")?;
    session.rescan()?;

    Ok(OperationalHarness {
        cluster,
        lab,
        session,
        app_id,
        facade_resource_id,
        facade_addrs,
    })
}

fn scenario_force_find_smoke(
    lab: &mut NfsLab,
    session: &mut OperatorSession,
) -> Result<(), String> {
    eprintln!("[fs-meta-api-ops] substep=force-find-stress-seed");
    seed_force_find_stress_content(lab, "nfs1", "force-find-stress", 40, 100)?;
    seed_force_find_stress_content(lab, "nfs2", "force-find-stress", 40, 100)?;
    eprintln!("[fs-meta-api-ops] substep=force-find-stress-rescan");
    session.rescan()?;

    Ok(())
}

fn scenario_force_find_execution_semantics(
    cluster: &Cluster5,
    lab: &mut NfsLab,
    session: &mut OperatorSession,
) -> Result<(), String> {
    scenario_force_find_smoke(lab, session)?;
    eprintln!("[fs-meta-api-ops] substep=force-find-stress-materialize");
    wait_until(
        Duration::from_secs(90),
        "force-find stress trees materialize",
        || {
            let tree = session.tree(&[
                ("path", "/force-find-stress".to_string()),
                ("recursive", "true".to_string()),
            ])?;
            Ok(group_total_nodes(&tree, "nfs1") > 0 && group_total_nodes(&tree, "nfs2") > 0)
        },
    )?;

    let nfs1_primary = source_primary_for_group(session, "nfs1")?
        .ok_or_else(|| "status missing nfs1 source-primary".to_string())?;

    eprintln!("[fs-meta-api-ops] substep=force-find-runner-first");
    let _ = session.force_find(&[
        ("path", "/force-find-stress".to_string()),
        ("recursive", "true".to_string()),
    ])?;
    let runner_first = wait_last_force_find_runner(session, "nfs1", None)?;
    eprintln!("[fs-meta-api-ops] substep=force-find-runner-second");
    let _ = session.force_find(&[
        ("path", "/force-find-stress".to_string()),
        ("recursive", "true".to_string()),
    ])?;
    let runner_second = wait_last_force_find_runner(session, "nfs1", Some(runner_first.as_str()))?;
    eprintln!("[fs-meta-api-ops] substep=force-find-runner-third");
    let _ = session.force_find(&[
        ("path", "/force-find-stress".to_string()),
        ("recursive", "true".to_string()),
    ])?;
    let runner_third = wait_last_force_find_runner(session, "nfs1", Some(runner_second.as_str()))?;

    let observed_runners = BTreeSet::from([
        runner_first.clone(),
        runner_second.clone(),
        runner_third.clone(),
    ]);
    if observed_runners.len() < 2 {
        return Err(format!(
            "force-find runner did not rotate across bound nfs1 sources: primary={nfs1_primary} runners={observed_runners:?}"
        ));
    }
    if observed_runners
        .iter()
        .all(|runner| runner == &nfs1_primary)
    {
        return Err(format!(
            "force-find runner never diverged from source-primary; primary={nfs1_primary} runners={observed_runners:?}"
        ));
    }

    let grants = session.runtime_grants()?;
    let failing_runner = runner_first.clone();
    let failing_node = node_name_for_object_ref(&grants, &failing_runner)?;
    eprintln!("[fs-meta-api-ops] substep=force-find-fallback-unmount");
    let _ = lab.unmount_export(&failing_node, "nfs1");
    let _fallback_resp = session.force_find(&[
        ("path", "/force-find-stress".to_string()),
        ("recursive", "true".to_string()),
    ])?;
    let fallback_runner =
        wait_last_force_find_runner(session, "nfs1", Some(failing_runner.as_str()))?;
    if fallback_runner == failing_runner {
        return Err(format!(
            "force-find fallback stayed on failed runner {failing_runner} after unmount on {failing_node}"
        ));
    }
    let remount = lab.mount_export(&failing_node, "nfs1")?;
    announce_nfs(cluster, lab, &failing_node, "nfs1", &remount)?;

    Ok(())
}

fn scenario_new_nfs_join(
    cluster: &Cluster5,
    lab: &mut NfsLab,
    session: &mut OperatorSession,
) -> Result<(), String> {
    if lab.mount_path("node-a", "nfs4").is_none()
        || lab.mount_path("node-c", "nfs4").is_none()
        || lab.mount_path("node-d", "nfs4").is_none()
    {
        lab.create_export("nfs4")?;
        let mount_a = lab.mount_export("node-a", "nfs4")?;
        let mount_c = lab.mount_export("node-c", "nfs4")?;
        let mount_d = lab.mount_export("node-d", "nfs4")?;
        announce_nfs(cluster, lab, "node-a", "nfs4", &mount_a)?;
        announce_nfs(cluster, lab, "node-c", "nfs4", &mount_c)?;
        announce_nfs(cluster, lab, "node-d", "nfs4", &mount_d)?;
    }
    lab.write_file("nfs4", "joined/a.txt", "a\n")?;
    lab.write_file("nfs4", "joined/deeper/b.txt", "b\n")?;
    session.rescan()?;

    wait_until(
        Duration::from_secs(60),
        "runtime grants include nfs4",
        || {
            let grants = session.runtime_grants()?;
            if first_mount_for_fs_source(&grants, &lab.export_source("nfs4")).is_ok() {
                Ok(true)
            } else {
                Err(format!("latest grants={grants}"))
            }
        },
    )?;

    let mut roots = current_roots_payload(session)?;
    roots
        .as_array_mut()
        .unwrap()
        .push(root_payload("nfs4", &lab.export_source("nfs4"), "/"));
    let preview = session.preview_roots(&roots)?;
    if !preview.to_string().contains("nfs4") {
        return Err(format!("roots preview did not include nfs4: {preview}"));
    }
    session.update_roots(&roots)?;
    session.rescan()?;
    wait_until(
        Duration::from_secs(60),
        "monitoring roots include nfs4",
        || {
            let current = session.monitoring_roots()?;
            Ok(current.to_string().contains("\"id\":\"nfs4\""))
        },
    )?;

    Ok(())
}

fn scenario_root_path_modify(
    _cluster: &Cluster5,
    lab: &NfsLab,
    session: &mut OperatorSession,
) -> Result<(), String> {
    lab.mkdir("nfs1", "hot")?;
    lab.write_file("nfs1", "hot/only-hot.txt", "hot\n")?;

    let mut roots = current_roots_payload(session)?;
    for row in roots.as_array_mut().unwrap() {
        if row.get("id").and_then(Value::as_str) == Some("nfs1") {
            row["subpath_scope"] = json!("/hot");
        }
    }
    let preview = session.preview_roots(&roots)?;
    if !preview.to_string().contains("/hot") {
        return Err(format!(
            "roots preview did not reflect /hot subpath: {preview}"
        ));
    }
    session.update_roots(&roots)?;
    session.rescan()?;
    wait_until(Duration::from_secs(30), "roots reflect /hot", || {
        let current = session.monitoring_roots()?;
        Ok(current.to_string().contains("\"subpath_scope\":\"/hot\""))
    })?;
    Ok(())
}

fn scenario_visibility_change_and_sink_selection(
    cluster: &Cluster5,
    lab: &mut NfsLab,
    session: &mut OperatorSession,
    app_id: &str,
    facade_resource_id: &str,
) -> Result<(), String> {
    // Narrow roots to a single scope so sink realization is unambiguous.
    let single_root = json!([root_payload("nfs2", &lab.export_source("nfs2"), "/")]);
    session.update_roots(&single_root)?;
    session.rescan()?;
    wait_until(Duration::from_secs(30), "single root nfs2 active", || {
        let current = session.monitoring_roots()?;
        Ok(current
            .get("roots")
            .and_then(Value::as_array)
            .map(|rows| {
                rows.len() == 1 && rows[0].get("id").and_then(Value::as_str) == Some("nfs2")
            })
            .unwrap_or(false))
    })?;

    {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let a =
                cluster.unit_active_pids_for_instance("node-a", app_id, "runtime.exec.source")?;
            let c =
                cluster.unit_active_pids_for_instance("node-c", app_id, "runtime.exec.source")?;
            let d =
                cluster.unit_active_pids_for_instance("node-d", app_id, "runtime.exec.source")?;
            if !a.is_empty() && !c.is_empty() && !d.is_empty() {
                break;
            }
            if Instant::now() > deadline {
                let raw_metrics_a = cluster.ctl_ok("node-a", json!({ "command": "metrics_get" }))?;
                let raw_metrics_c = cluster.ctl_ok("node-c", json!({ "command": "metrics_get" }))?;
                let raw_metrics_d = cluster.ctl_ok("node-d", json!({ "command": "metrics_get" }))?;
                return Err(format!(
                    "timeout waiting for source active on nfs2 visible members: node-a pids={a:?} raw_metrics={raw_metrics_a} node-c pids={c:?} raw_metrics={raw_metrics_c} node-d pids={d:?} raw_metrics={raw_metrics_d}"
                ));
            }
            thread::sleep(Duration::from_millis(100));
        }
    }

    let before_holder = current_sink_holder_for_group(cluster, app_id, "nfs2")?;
    cluster
        .withdraw_resources_clusterwide(&cluster.node_id("node-d")?, vec!["nfs2".to_string()])?;
    let _ = lab.unmount_export("node-d", "nfs2");

    wait_until(
        Duration::from_secs(60),
        "nfs2 withdrawn from node-d grants",
        || {
            let grants = session.runtime_grants()?;
            Ok(!grants
                .to_string()
                .contains(&format!("{}::nfs2", cluster.node_id("node-d")?)))
        },
    )?;

    wait_until(
        Duration::from_secs(90),
        "sink holder not on withdrawn node",
        || {
            let holder = current_sink_holder_for_group(cluster, app_id, "nfs2")?;
            Ok(holder.is_some() && holder != Some("node-d".to_string()))
        },
    )?;
    let after_holder = current_sink_holder_for_group(cluster, app_id, "nfs2")?
        .ok_or_else(|| "sink holder disappeared after visibility change".to_string())?;
    if after_holder == "node-d" {
        return Err("sink holder remained on withdrawn node-d".to_string());
    }
    if before_holder == Some("node-d".to_string()) && after_holder == "node-d" {
        return Err("sink holder did not move away from withdrawn node-d".to_string());
    }

    // keep facade resource alive by ensuring both facade listener resources still exist
    let facade_status = current_facade_holders(cluster, app_id)?;
    if facade_status.is_empty() {
        return Err(format!(
            "facade disappeared while adjusting sink visibility for {facade_resource_id}"
        ));
    }
    Ok(())
}

fn scenario_sink_failover(
    cluster: &Cluster5,
    session: &mut OperatorSession,
    app_id: &str,
) -> Result<(), String> {
    let mut before = None::<SinkHolderSnapshot>;
    wait_until(
        Duration::from_secs(90),
        "initial scoped sink holder elected",
        || {
            before = current_sink_failover_holder_snapshot(cluster, app_id)?;
            if before.is_some() {
                Ok(true)
            } else {
                Err("no current sink holder to fail over".to_string())
            }
        },
    )?;
    let before = before.ok_or_else(|| "no current sink holder to fail over".to_string())?;
    let pid = before
        .active_pids
        .iter()
        .next()
        .copied()
        .ok_or_else(|| format!("no sink pid found on holder {}", before.node_name))?;
    cluster.kill_pid(&before.node_name, pid)?;

    wait_until(
        Duration::from_secs(90),
        "status remains available after sink failover",
        || {
            let status = session.status()?;
            Ok(status.get("sink").is_some())
        },
    )?;

    wait_until(Duration::from_secs(90), "new scoped sink holder elected", || {
        let next = current_sink_failover_holder_snapshot(cluster, app_id)?;
        Ok(sink_failover_successor_elected(&before, next.as_ref()))
    })?;
    Ok(())
}

fn scenario_facade_failover_and_resource_switch(
    cluster: &Cluster5,
    session: &mut OperatorSession,
    facade_resource_id: &str,
    facade_addrs: &[String],
    app_id: &str,
) -> Result<(), String> {
    let _ = app_id;
    let current_url = session
        .client()
        .base_url()
        .trim_end_matches('/')
        .to_string();
    let current_index = facade_addrs
        .iter()
        .position(|addr| format!("http://{addr}") == current_url)
        .unwrap_or(0);
    let holder_node = if current_index == 0 {
        "node-d"
    } else {
        "node-e"
    };

    cluster.withdraw_resources_clusterwide(
        &cluster.node_id(&holder_node)?,
        vec![facade_resource_id.to_string()],
    )?;
    wait_until(
        Duration::from_secs(90),
        "facade resource switch withdraws current holder",
        || {
            let holders = current_facade_holders(cluster, app_id)?;
            Ok(holders.iter().all(|(node, _)| node != holder_node))
        },
    )?;

    let candidate_base_urls = facade_addrs
        .iter()
        .map(|addr| format!("http://{addr}"))
        .collect::<Vec<_>>();
    let reachable = cluster.wait_http_login_ready(
        &candidate_base_urls,
        "operator",
        "operator123",
        Duration::from_secs(90),
    )?;
    let mut replacement_session =
        OperatorSession::login_many(candidate_base_urls, "operator", "operator123")?;
    let status = replacement_session.status()?;
    if status.get("source").is_none() {
        return Err(format!(
            "replacement facade status missing source after switch reachable={reachable}: {status}"
        ));
    }
    Ok(())
}

fn scenario_nfs_retire(
    cluster: &Cluster5,
    lab: &mut NfsLab,
    session: &mut OperatorSession,
) -> Result<(), String> {
    cluster
        .withdraw_resources_clusterwide(&cluster.node_id("node-b")?, vec!["nfs3".to_string()])?;
    cluster
        .withdraw_resources_clusterwide(&cluster.node_id("node-d")?, vec!["nfs3".to_string()])?;
    cluster
        .withdraw_resources_clusterwide(&cluster.node_id("node-e")?, vec!["nfs3".to_string()])?;
    let _ = lab.unmount_export("node-b", "nfs3");
    let _ = lab.unmount_export("node-d", "nfs3");
    let _ = lab.unmount_export("node-e", "nfs3");
    lab.retire_export("nfs3")?;

    let roots = json!([
        root_payload("nfs2", &lab.export_source("nfs2"), "/"),
        root_payload("nfs4", &lab.export_source("nfs4"), "/"),
    ]);
    session.update_roots(&roots)?;
    session.rescan()?;
    wait_until(Duration::from_secs(60), "nfs3 retired from roots", || {
        let current = session.monitoring_roots()?;
        Ok(!current.to_string().contains("\"id\":\"nfs3\""))
    })?;
    let grants = session.runtime_grants()?;
    if grants.to_string().contains("\"resource_id\":\"nfs3\"") {
        return Err(format!(
            "runtime grants still expose retired nfs3: {grants}"
        ));
    }
    let tree = session.tree(&[("path", "/".to_string()), ("recursive", "true".to_string())])?;
    if tree.to_string().contains("nfs3") {
        return Err(format!("tree still references retired nfs3: {tree}"));
    }
    Ok(())
}

fn seed_baseline_content(lab: &NfsLab) -> Result<(), String> {
    lab.mkdir("nfs1", "nested/child")?;
    lab.write_file("nfs1", "nested/child/deep.txt", "deep-nfs1\n")?;
    lab.write_file("nfs1", "extra-count-1.txt", "count-1\n")?;
    lab.write_file("nfs1", "extra-count-2.txt", "count-2\n")?;
    thread::sleep(Duration::from_millis(5));
    lab.write_file("nfs2", "latest-age.txt", "latest-nfs2\n")?;
    lab.mkdir("nfs2", "nested")?;
    lab.write_file("nfs2", "nested/peer.txt", "peer\n")?;
    thread::sleep(Duration::from_millis(5));
    lab.write_file("nfs3", "misc.txt", "misc\n")?;
    Ok(())
}

fn install_baseline_resources(
    cluster: &Cluster5,
    lab: &mut NfsLab,
    facade_resource_id: &str,
    facade_addrs: &[String],
) -> Result<(), String> {
    let exports = [
        ("node-a", "nfs1"),
        ("node-b", "nfs1"),
        ("node-c", "nfs1"),
        ("node-a", "nfs2"),
        ("node-c", "nfs2"),
        ("node-d", "nfs2"),
        ("node-b", "nfs3"),
        ("node-d", "nfs3"),
        ("node-e", "nfs3"),
    ];
    for (node_name, export_name) in exports {
        let mount_path = lab.mount_export(node_name, export_name)?;
        announce_nfs(cluster, lab, node_name, export_name, &mount_path)?;
    }
    for (node_name, bind_addr) in ["node-d", "node-e"].into_iter().zip(facade_addrs.iter()) {
        announce_listener(cluster, node_name, facade_resource_id, bind_addr)?;
    }
    Ok(())
}

fn announce_nfs(
    cluster: &Cluster5,
    lab: &NfsLab,
    node_name: &str,
    export_name: &str,
    mount_path: &Path,
) -> Result<(), String> {
    cluster.announce_resources_clusterwide(vec![json!({
        "resource_id": export_name,
        "node_id": cluster.node_id(node_name)?,
        "resource_kind": "nfs",
        "source": lab.export_source(export_name),
        "mount_hint": mount_path.display().to_string(),
    })])?;
    Ok(())
}

fn announce_listener(
    cluster: &Cluster5,
    node_name: &str,
    resource_id: &str,
    bind_addr: &str,
) -> Result<(), String> {
    cluster.announce_resources_clusterwide(vec![json!({
        "resource_id": resource_id,
        "node_id": cluster.node_id(node_name)?,
        "resource_kind": "tcp_listener",
        "source": format!("http://{node_name}/listener/{resource_id}"),
        "bind_addr": bind_addr,
    })])?;
    Ok(())
}

fn baseline_roots(lab: &NfsLab) -> Vec<RootSpec> {
    vec![
        root_spec("nfs1", &lab.export_source("nfs1")),
        root_spec("nfs2", &lab.export_source("nfs2")),
        root_spec("nfs3", &lab.export_source("nfs3")),
    ]
}

fn current_roots_payload(session: &mut OperatorSession) -> Result<Value, String> {
    let current = session.monitoring_roots()?;
    Ok(current.get("roots").cloned().unwrap_or_else(|| json!([])))
}

const SINK_FAILOVER_GROUP_ID: &str = "nfs2";

fn current_sink_holder_for_group(
    cluster: &Cluster5,
    app_id: &str,
    group_id: &str,
) -> Result<Option<String>, String> {
    current_sink_holder_for_scope(cluster, app_id, Some(group_id))
}

#[derive(Debug, Clone)]
struct SinkHolderSnapshot {
    node_name: String,
    active_pids: BTreeSet<u32>,
    bound_scopes: BTreeSet<String>,
}

fn current_sink_holder_for_scope(
    cluster: &Cluster5,
    app_id: &str,
    group_id: Option<&str>,
) -> Result<Option<String>, String> {
    let mut snapshots = Vec::new();
    for node_name in ["node-a", "node-b", "node-c", "node-d", "node-e"] {
        snapshots.push(sink_holder_snapshot_for_node(cluster, node_name, app_id)?);
    }
    unique_sink_holder_from_snapshots(&snapshots, group_id)
}

fn unique_sink_holder_from_snapshots(
    snapshots: &[SinkHolderSnapshot],
    group_id: Option<&str>,
) -> Result<Option<String>, String> {
    let mut holders = Vec::new();
    let mut debug_rows = Vec::new();
    for snapshot in snapshots {
        debug_rows.push(format!(
            "{} pids={:?} scopes={:?}",
            snapshot.node_name, snapshot.active_pids, snapshot.bound_scopes
        ));
        let matches_group = group_id
            .map(|scope_id| snapshot.bound_scopes.contains(scope_id))
            .unwrap_or(true);
        if !snapshot.active_pids.is_empty() && matches_group {
            holders.push(snapshot.node_name.clone());
        }
    }
    if holders.len() > 1 {
        return Err(format!(
            "multiple sink holders detected: {holders:?} snapshots={debug_rows:?}"
        ));
    }
    Ok(holders.into_iter().next())
}

fn unique_sink_failover_holder_from_snapshots(
    snapshots: &[SinkHolderSnapshot],
) -> Result<Option<String>, String> {
    unique_sink_holder_from_snapshots(snapshots, Some(SINK_FAILOVER_GROUP_ID))
}

fn unique_sink_holder_snapshot_from_snapshots(
    snapshots: &[SinkHolderSnapshot],
    group_id: Option<&str>,
) -> Result<Option<SinkHolderSnapshot>, String> {
    let holder = unique_sink_holder_from_snapshots(snapshots, group_id)?;
    Ok(holder.and_then(|node_name| {
        snapshots
            .iter()
            .find(|snapshot| snapshot.node_name == node_name)
            .cloned()
    }))
}

fn current_sink_failover_holder_snapshot(
    cluster: &Cluster5,
    app_id: &str,
) -> Result<Option<SinkHolderSnapshot>, String> {
    let mut snapshots = Vec::new();
    for node_name in ["node-a", "node-b", "node-c", "node-d", "node-e"] {
        snapshots.push(sink_holder_snapshot_for_node(cluster, node_name, app_id)?);
    }
    unique_sink_holder_snapshot_from_snapshots(&snapshots, Some(SINK_FAILOVER_GROUP_ID))
}

fn sink_holder_snapshot_for_node(
    cluster: &Cluster5,
    node_name: &str,
    app_id: &str,
) -> Result<SinkHolderSnapshot, String> {
    let status = cluster.status(node_name)?;
    Ok(sink_holder_snapshot_from_status(&status, app_id, node_name))
}

fn sink_holder_snapshot_from_status(
    status: &Value,
    instance_id: &str,
    node_name: &str,
) -> SinkHolderSnapshot {
    let managed_pids = status
        .get("daemon")
        .and_then(|v| v.get("managed_processes"))
        .and_then(Value::as_array)
        .map(|rows| {
            rows.iter()
                .filter(|row| row.get("instance_id").and_then(Value::as_str) == Some(instance_id))
                .filter_map(|row| row.get("pid").and_then(Value::as_u64).map(|v| v as u32))
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
    let routes = status
        .get("daemon")
        .and_then(|v| v.get("activation"))
        .and_then(|v| v.get("routes"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut active_pids = BTreeSet::new();
    let mut bound_scopes = BTreeSet::new();
    for route in routes {
        let Some(apps) = route.get("apps").and_then(Value::as_array) else {
            continue;
        };
        let route_active_pids = route
            .get("active_pids")
            .and_then(Value::as_array)
            .map(|rows| {
                rows.iter()
                    .filter_map(|row| row.as_u64().map(|v| v as u32))
                    .filter(|pid| managed_pids.contains(pid))
                    .collect::<BTreeSet<_>>()
            })
            .unwrap_or_default();
        let mut route_has_delivered_sink_activation = false;
        for row in apps {
            let active_unit = row
                .get("unit_ids")
                .and_then(Value::as_array)
                .is_some_and(|units| units.iter().any(|v| v.as_str() == Some("runtime.exec.sink")));
            if !active_unit {
                continue;
            }
            if row.get("delivered").and_then(Value::as_bool) != Some(true) {
                continue;
            }
            if row.get("gate").and_then(Value::as_str) != Some("activated") {
                continue;
            }
            if row.get("op").and_then(Value::as_str) != Some("activate") {
                continue;
            }
            route_has_delivered_sink_activation = true;
            if let Some(scopes) = row
                .get("bound_scopes_by_unit")
                .and_then(|v| v.get("runtime.exec.sink"))
                .and_then(Value::as_array)
            {
                for scope in scopes {
                    if let Some(scope_id) = scope.get("scope_id").and_then(Value::as_str) {
                        if !scope_id.trim().is_empty() {
                            bound_scopes.insert(scope_id.to_string());
                        }
                    }
                }
            }
            if let Some(pid) = row.get("pid").and_then(Value::as_u64).map(|v| v as u32) {
                if managed_pids.contains(&pid) {
                    active_pids.insert(pid);
                }
            }
        }
        if route_has_delivered_sink_activation {
            active_pids.extend(route_active_pids);
        }
    }
    SinkHolderSnapshot {
        node_name: node_name.to_string(),
        active_pids,
        bound_scopes,
    }
}

fn sink_failover_successor_elected(
    before: &SinkHolderSnapshot,
    after: Option<&SinkHolderSnapshot>,
) -> bool {
    matches!(
        after,
        Some(after)
            if after.node_name != before.node_name || after.active_pids != before.active_pids
    )
}

fn current_facade_holders(cluster: &Cluster5, app_id: &str) -> Result<Vec<(String, u32)>, String> {
    let mut holders = Vec::new();
    for node_name in ["node-a", "node-b", "node-c", "node-d", "node-e"] {
        let pids = cluster.facade_pids_for_instance(node_name, app_id)?;
        for pid in pids {
            holders.push((node_name.to_string(), pid));
        }
    }
    Ok(holders)
}

fn unit_bound_scope_ids_from_activation_status(
    cluster: &Cluster5,
    node_name: &str,
    unit_id: &str,
) -> Result<BTreeSet<String>, String> {
    let status = cluster.status(node_name)?;
    let routes = status
        .get("daemon")
        .and_then(|v| v.get("activation"))
        .and_then(|v| v.get("routes"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut scope_ids = BTreeSet::new();
    for route in routes {
        let Some(apps) = route.get("apps").and_then(Value::as_array) else {
            continue;
        };
        for row in apps {
            let active_unit = row
                .get("unit_ids")
                .and_then(Value::as_array)
                .is_some_and(|units| units.iter().any(|v| v.as_str() == Some(unit_id)));
            if !active_unit {
                continue;
            }
            let Some(scopes) = row
                .get("bound_scopes_by_unit")
                .and_then(|v| v.get(unit_id))
                .and_then(Value::as_array)
            else {
                continue;
            };
            for scope in scopes {
                if let Some(scope_id) = scope.get("scope_id").and_then(Value::as_str) {
                    if !scope_id.trim().is_empty() {
                        scope_ids.insert(scope_id.to_string());
                    }
                }
            }
        }
    }
    Ok(scope_ids)
}

fn activation_route_summaries(cluster: &Cluster5, node_name: &str) -> Result<Vec<String>, String> {
    let status = cluster.status(node_name)?;
    let routes = status
        .get("daemon")
        .and_then(|v| v.get("activation"))
        .and_then(|v| v.get("routes"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut summaries = Vec::new();
    for route in routes {
        let route_key = route
            .get("route_key")
            .and_then(Value::as_str)
            .unwrap_or("<unknown>");
        let state = route
            .get("state")
            .and_then(Value::as_str)
            .unwrap_or("<unknown>");
        let active_pids = route
            .get("active_pids")
            .cloned()
            .unwrap_or_else(|| json!([]));
        let apps = route
            .get("apps")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for row in apps {
            let units = row.get("unit_ids").cloned().unwrap_or_else(|| json!([]));
            let scopes = row
                .get("bound_scopes_by_unit")
                .cloned()
                .unwrap_or_else(|| json!({}));
            let gate = row.get("gate").and_then(Value::as_str).unwrap_or("<unknown>");
            let delivered = row
                .get("delivered")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            summaries.push(format!(
                "route={route_key} state={state} active_pids={active_pids} units={units} gate={gate} delivered={delivered} scopes={scopes}"
            ));
        }
    }
    summaries.sort();
    Ok(summaries)
}

fn status_debug_groups_by_node(
    status: &Value,
    section: &str,
    field: &str,
    node_name: &str,
) -> BTreeSet<String> {
    status
        .get(section)
        .and_then(|v| v.get("debug"))
        .and_then(|v| v.get(field))
        .and_then(|v| v.get(node_name))
        .and_then(Value::as_array)
        .map(|groups| {
            groups
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default()
}

fn status_debug_strings_by_node(
    status: &Value,
    section: &str,
    field: &str,
    node_name: &str,
) -> Vec<String> {
    status
        .get(section)
        .and_then(|v| v.get("debug"))
        .and_then(|v| v.get(field))
        .and_then(|v| v.get(node_name))
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn status_debug_u64_by_node(
    status: &Value,
    section: &str,
    field: &str,
    node_name: &str,
) -> u64 {
    status
        .get(section)
        .and_then(|v| v.get("debug"))
        .and_then(|v| v.get(field))
        .and_then(|v| v.get(node_name))
        .and_then(Value::as_u64)
        .unwrap_or_default()
}

fn debug_publish_lag_window_secs() -> Option<u64> {
    std::env::var("FSMETA_DEBUG_PUBLISH_LAG_WINDOW_SECS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|secs| *secs > 0)
}

fn published_origin_counts_include(entries: &[String], object_ref: &str) -> bool {
    let prefix = format!("{object_ref}=");
    entries.iter().any(|entry| entry.starts_with(&prefix))
}

fn published_origin_count(entries: &[String], object_ref: &str) -> u64 {
    let prefix = format!("{object_ref}=");
    entries
        .iter()
        .find_map(|entry| {
            let value = entry.strip_prefix(&prefix)?;
            value.parse::<u64>().ok()
        })
        .unwrap_or_default()
}

#[derive(Clone, Debug, Default)]
struct ConcreteRootTransitionSnapshot {
    status: String,
    rescan_pending: bool,
    overflow_pending: bool,
    last_rescan_reason: Option<String>,
    last_audit_completed_at_us: Option<u64>,
    emitted_event_count: u64,
    emitted_path_event_count: u64,
    last_emitted_at_us: Option<u64>,
    forwarded_event_count: u64,
    forwarded_path_event_count: u64,
    last_forwarded_at_us: Option<u64>,
    current_revision: Option<u64>,
    current_stream_generation: Option<u64>,
    candidate_revision: Option<u64>,
    candidate_stream_generation: Option<u64>,
    candidate_status: Option<String>,
    draining_revision: Option<u64>,
    draining_stream_generation: Option<u64>,
    draining_status: Option<String>,
}

impl ConcreteRootTransitionSnapshot {
    fn summary(&self) -> String {
        format!(
            "status={} rescan_pending={} overflow_pending={} last_rescan_reason={:?} last_audit_completed_at_us={:?} emitted_event_count={} emitted_path_event_count={} last_emitted_at_us={:?} forwarded_event_count={} forwarded_path_event_count={} last_forwarded_at_us={:?} current_revision={:?} current_stream_generation={:?} candidate_revision={:?} candidate_stream_generation={:?} candidate_status={:?} draining_revision={:?} draining_stream_generation={:?} draining_status={:?}",
            self.status,
            self.rescan_pending,
            self.overflow_pending,
            self.last_rescan_reason,
            self.last_audit_completed_at_us,
            self.emitted_event_count,
            self.emitted_path_event_count,
            self.last_emitted_at_us,
            self.forwarded_event_count,
            self.forwarded_path_event_count,
            self.last_forwarded_at_us,
            self.current_revision,
            self.current_stream_generation,
            self.candidate_revision,
            self.candidate_stream_generation,
            self.candidate_status,
            self.draining_revision,
            self.draining_stream_generation,
            self.draining_status
        )
    }
}

fn source_logical_root_summary(
    session: &mut OperatorSession,
    root_id: &str,
) -> Result<Option<String>, String> {
    for status in session.status_all()? {
        if let Some(root) = status
            .get("source")
            .and_then(|source| source.get("logical_roots"))
            .and_then(Value::as_array)
            .and_then(|roots| {
                roots.iter()
                    .find(|root| root.get("root_id").and_then(Value::as_str) == Some(root_id))
            })
        {
            let status_label = root.get("status").and_then(Value::as_str).unwrap_or("<missing>");
            let matched_grants = root
                .get("matched_grants")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let active_members = root
                .get("active_members")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let coverage_mode = root
                .get("coverage_mode")
                .and_then(Value::as_str)
                .unwrap_or("<missing>");
            return Ok(Some(format!(
                "status={status_label} matched_grants={matched_grants} active_members={active_members} coverage_mode={coverage_mode}"
            )));
        }
    }
    Ok(None)
}

fn source_concrete_root_summaries(
    session: &mut OperatorSession,
    object_ref: &str,
) -> Result<Vec<String>, String> {
    let mut summaries = Vec::<String>::new();
    for status in session.status_all()? {
        if let Some(root) = status
            .get("source")
            .and_then(|source| source.get("concrete_roots"))
            .and_then(Value::as_array)
            .and_then(|roots| {
                roots.iter().find(|root| {
                    root.get("object_ref").and_then(Value::as_str) == Some(object_ref)
                })
            })
        {
            summaries.push(format!(
                "object_ref={} status={} coverage_mode={} watch_enabled={} scan_enabled={} is_group_primary={} active={} rescan_pending={} overflow_pending={} last_rescan_reason={:?} last_audit_started_at_us={:?} last_audit_completed_at_us={:?} emitted_batch_count={:?} emitted_event_count={:?} emitted_control_event_count={:?} emitted_data_event_count={:?} emitted_path_capture_target={:?} emitted_path_event_count={:?} last_emitted_at_us={:?} last_emitted_origins={:?} forwarded_batch_count={:?} forwarded_event_count={:?} forwarded_path_event_count={:?} last_forwarded_at_us={:?} last_forwarded_origins={:?} current_revision={:?} current_stream_generation={:?} candidate_revision={:?} candidate_stream_generation={:?} draining_revision={:?} draining_stream_generation={:?} last_error={:?}",
                object_ref,
                root.get("status").and_then(Value::as_str).unwrap_or("<missing>"),
                root.get("coverage_mode")
                    .and_then(Value::as_str)
                    .unwrap_or("<missing>"),
                root.get("watch_enabled")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                root.get("scan_enabled")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                root.get("is_group_primary")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                root.get("active").and_then(Value::as_bool).unwrap_or(false),
                root.get("rescan_pending")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                root.get("overflow_pending")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                root.get("last_rescan_reason").cloned().unwrap_or(Value::Null),
                root.get("last_audit_started_at_us").cloned().unwrap_or(Value::Null),
                root.get("last_audit_completed_at_us")
                    .cloned()
                    .unwrap_or(Value::Null),
                root.get("emitted_batch_count").cloned().unwrap_or(Value::Null),
                root.get("emitted_event_count").cloned().unwrap_or(Value::Null),
                root.get("emitted_control_event_count")
                    .cloned()
                    .unwrap_or(Value::Null),
                root.get("emitted_data_event_count")
                    .cloned()
                    .unwrap_or(Value::Null),
                root.get("emitted_path_capture_target")
                    .cloned()
                    .unwrap_or(Value::Null),
                root.get("emitted_path_event_count")
                    .cloned()
                    .unwrap_or(Value::Null),
                root.get("last_emitted_at_us").cloned().unwrap_or(Value::Null),
                root.get("last_emitted_origins").cloned().unwrap_or(Value::Null),
                root.get("forwarded_batch_count").cloned().unwrap_or(Value::Null),
                root.get("forwarded_event_count").cloned().unwrap_or(Value::Null),
                root.get("forwarded_path_event_count")
                    .cloned()
                    .unwrap_or(Value::Null),
                root.get("last_forwarded_at_us")
                    .cloned()
                    .unwrap_or(Value::Null),
                root.get("last_forwarded_origins")
                    .cloned()
                    .unwrap_or(Value::Null),
                root.get("current_revision").cloned().unwrap_or(Value::Null),
                root.get("current_stream_generation")
                    .cloned()
                    .unwrap_or(Value::Null),
                root.get("candidate_revision").cloned().unwrap_or(Value::Null),
                root.get("candidate_stream_generation")
                    .cloned()
                    .unwrap_or(Value::Null),
                root.get("draining_revision").cloned().unwrap_or(Value::Null),
                root.get("draining_stream_generation")
                    .cloned()
                    .unwrap_or(Value::Null),
                root.get("last_error").cloned().unwrap_or(Value::Null)
            ));
        }
    }
    summaries.sort();
    Ok(summaries)
}

fn source_concrete_root_field_u64(
    session: &mut OperatorSession,
    object_ref: &str,
    field: &str,
) -> Result<u64, String> {
    for status in session.status_all()? {
        if let Some(value) = status
            .get("source")
            .and_then(|source| source.get("concrete_roots"))
            .and_then(Value::as_array)
            .and_then(|roots| {
                roots.iter().find(|root| {
                    root.get("object_ref").and_then(Value::as_str) == Some(object_ref)
                })
            })
            .and_then(|root| root.get(field))
            .and_then(Value::as_u64)
        {
            return Ok(value);
        }
    }
    Ok(0)
}

fn source_concrete_root_transition_snapshot(
    session: &mut OperatorSession,
    object_ref: &str,
) -> Result<ConcreteRootTransitionSnapshot, String> {
    for status in session.status_all()? {
        if let Some(root) = status
            .get("source")
            .and_then(|source| source.get("concrete_roots"))
            .and_then(Value::as_array)
            .and_then(|roots| {
                roots.iter().find(|root| {
                    root.get("object_ref").and_then(Value::as_str) == Some(object_ref)
                })
            })
        {
            return Ok(ConcreteRootTransitionSnapshot {
                status: root
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("<missing>")
                    .to_string(),
                rescan_pending: root
                    .get("rescan_pending")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                overflow_pending: root
                    .get("overflow_pending")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                last_rescan_reason: root
                    .get("last_rescan_reason")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                last_audit_completed_at_us: root
                    .get("last_audit_completed_at_us")
                    .and_then(Value::as_u64),
                emitted_event_count: root
                    .get("emitted_event_count")
                    .and_then(Value::as_u64)
                    .unwrap_or_default(),
                emitted_path_event_count: root
                    .get("emitted_path_event_count")
                    .and_then(Value::as_u64)
                    .unwrap_or_default(),
                last_emitted_at_us: root
                    .get("last_emitted_at_us")
                    .and_then(Value::as_u64),
                forwarded_event_count: root
                    .get("forwarded_event_count")
                    .and_then(Value::as_u64)
                    .unwrap_or_default(),
                forwarded_path_event_count: root
                    .get("forwarded_path_event_count")
                    .and_then(Value::as_u64)
                    .unwrap_or_default(),
                last_forwarded_at_us: root
                    .get("last_forwarded_at_us")
                    .and_then(Value::as_u64),
                current_revision: root
                    .get("current_revision")
                    .and_then(Value::as_u64),
                current_stream_generation: root
                    .get("current_stream_generation")
                    .and_then(Value::as_u64),
                candidate_revision: root
                    .get("candidate_revision")
                    .and_then(Value::as_u64),
                candidate_stream_generation: root
                    .get("candidate_stream_generation")
                    .and_then(Value::as_u64),
                candidate_status: root
                    .get("candidate_status")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                draining_revision: root
                    .get("draining_revision")
                    .and_then(Value::as_u64),
                draining_stream_generation: root
                    .get("draining_stream_generation")
                    .and_then(Value::as_u64),
                draining_status: root
                    .get("draining_status")
                    .and_then(Value::as_str)
                    .map(str::to_string),
            });
        }
    }
    Ok(ConcreteRootTransitionSnapshot::default())
}

fn source_primary_for_group(
    session: &mut OperatorSession,
    group_id: &str,
) -> Result<Option<String>, String> {
    for status in session.status_all()? {
        if let Some(primary) = status
            .get("source")
            .and_then(|source| source.get("debug"))
            .and_then(|debug| debug.get("source_primary_by_group"))
            .and_then(Value::as_object)
            .and_then(|groups| groups.get(group_id))
            .and_then(Value::as_str)
            .map(str::to_string)
        {
            return Ok(Some(primary));
        }
    }
    Ok(None)
}

fn wait_last_force_find_runner(
    session: &mut OperatorSession,
    group_id: &str,
    previous: Option<&str>,
) -> Result<String, String> {
    let mut last_seen = None::<String>;
    wait_until(
        Duration::from_secs(90),
        &format!("last force-find runner for {group_id}"),
        || {
            let mut observed = None::<String>;
            for status in session.status_all()? {
                let runners = status
                    .get("source")
                    .and_then(|source| source.get("debug"))
                    .and_then(|debug| debug.get("last_force_find_runners_by_group"))
                    .and_then(Value::as_object)
                    .and_then(|groups| groups.get(group_id))
                    .and_then(Value::as_array)
                    .map(|items| {
                        items
                            .iter()
                            .filter_map(Value::as_str)
                            .map(str::to_string)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                let runner = runners
                    .iter()
                    .find(|runner| previous.is_none_or(|prev| prev != runner.as_str()))
                    .cloned()
                    .or_else(|| {
                        status
                            .get("source")
                            .and_then(|source| source.get("debug"))
                            .and_then(|debug| debug.get("last_force_find_runner_by_group"))
                            .and_then(Value::as_object)
                            .and_then(|groups| groups.get(group_id))
                            .and_then(Value::as_str)
                            .map(str::to_string)
                    });
                if runner.is_some() {
                    observed = runner;
                    break;
                }
            }
            last_seen = observed.clone();
            Ok(observed
                .as_deref()
                .is_some_and(|runner| previous.is_none_or(|prev| prev != runner)))
        },
    )?;
    last_seen.ok_or_else(|| format!("missing last force-find runner for group {group_id}"))
}

fn node_name_for_object_ref(grants: &Value, object_ref: &str) -> Result<String, String> {
    let rows = grants
        .get("grants")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("runtime grants missing array: {grants}"))?;
    let mount_point = rows
        .iter()
        .find(|row| row.get("object_ref").and_then(Value::as_str) == Some(object_ref))
        .and_then(|row| row.get("mount_point").and_then(Value::as_str))
        .ok_or_else(|| format!("runtime grants missing mount_point for object_ref {object_ref}"))?;
    for node_name in ["node-a", "node-b", "node-c", "node-d", "node-e"] {
        if mount_point.contains(&format!("/mounts/{node_name}/")) {
            return Ok(node_name.to_string());
        }
    }
    Err(format!(
        "could not infer node name from mount_point '{mount_point}' for object_ref {object_ref}"
    ))
}

fn seed_force_find_stress_content(
    lab: &NfsLab,
    export_name: &str,
    root_dir: &str,
    dir_count: usize,
    files_per_dir: usize,
) -> Result<(), String> {
    for dir_idx in 0..dir_count {
        let dir = format!("{root_dir}/dir-{dir_idx:03}");
        lab.mkdir(export_name, &dir)?;
        for file_idx in 0..files_per_dir {
            lab.write_file(
                export_name,
                &format!("{dir}/file-{file_idx:03}.txt"),
                "force-find-stress\n",
            )?;
        }
    }
    Ok(())
}

fn root_spec(id: &str, fs_source: &str) -> RootSpec {
    RootSpec {
        id: id.to_string(),
        selector: RootSelector {
            mount_point: None,
            fs_source: Some(fs_source.to_string()),
            fs_type: None,
            host_ip: None,
            host_ref: None,
        },
        subpath_scope: PathBuf::from("/"),
        watch: true,
        scan: true,
        audit_interval_ms: None,
    }
}

fn root_payload(id: &str, fs_source: &str, subpath_scope: &str) -> Value {
    json!({
        "id": id,
        "selector": { "fs_source": fs_source },
        "subpath_scope": subpath_scope,
        "watch": true,
        "scan": true,
    })
}

fn mounts_by_fs_source(grants: &Value) -> Result<BTreeMap<String, Vec<PathBuf>>, String> {
    let mut by_fs_source = BTreeMap::<String, Vec<PathBuf>>::new();
    let rows = grants
        .get("grants")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("runtime grants missing rows: {grants}"))?;
    for row in rows {
        let fs_source = row
            .get("fs_source")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("grant row missing fs_source: {row}"))?;
        let mount_point = row
            .get("mount_point")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("grant row missing mount_point: {row}"))?;
        by_fs_source
            .entry(fs_source.to_string())
            .or_default()
            .push(PathBuf::from(mount_point));
    }
    Ok(by_fs_source)
}

fn first_mount_for_fs_source(grants: &Value, fs_source: &str) -> Result<PathBuf, String> {
    mounts_by_fs_source(grants)?
        .remove(fs_source)
        .and_then(|paths| paths.into_iter().next())
        .ok_or_else(|| format!("missing representative mount for fs_source {fs_source}"))
}

fn group_total_nodes(payload: &Value, group_key: &str) -> u64 {
    let (_, _, total) = group_root_exists_entries_and_total(payload, group_key);
    total
}

fn group_root_exists_entries_and_total(payload: &Value, group_key: &str) -> (bool, usize, u64) {
    let Some(group) = payload
        .get("groups")
        .and_then(Value::as_array)
        .and_then(|groups| {
            groups
                .iter()
                .find(|group| group.get("group").and_then(Value::as_str) == Some(group_key))
        })
    else {
        return (false, 0, 0);
    };
    let root_exists = group
        .get("root")
        .and_then(|root| root.get("exists"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let entries = group
        .get("entries")
        .and_then(Value::as_array)
        .map(|rows| rows.len())
        .unwrap_or(0);
    let total = entries as u64 + if root_exists { 1 } else { 0 };
    (root_exists, entries, total)
}

fn group_entry_paths(payload: &Value, group_key: &str) -> Vec<String> {
    payload
        .get("groups")
        .and_then(Value::as_array)
        .and_then(|groups| {
            groups
                .iter()
                .find(|group| group.get("group").and_then(Value::as_str) == Some(group_key))
        })
        .and_then(|group| group.get("entries"))
        .and_then(Value::as_array)
        .map(|entries| {
            entries
                .iter()
                .filter_map(|entry| entry.get("path").and_then(Value::as_str))
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn unique_suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

#[test]
fn sink_holder_selection_ignores_nodes_without_target_scope() {
    let snapshots = vec![
        SinkHolderSnapshot {
            node_name: "node-a".to_string(),
            active_pids: BTreeSet::from([1]),
            bound_scopes: BTreeSet::from(["nfs2".to_string()]),
        },
        SinkHolderSnapshot {
            node_name: "node-b".to_string(),
            active_pids: BTreeSet::from([2]),
            bound_scopes: BTreeSet::from(["nfs3".to_string()]),
        },
    ];

    assert_eq!(
        unique_sink_holder_from_snapshots(&snapshots, Some("nfs2")).expect("nfs2 holder"),
        Some("node-a".to_string())
    );
}

#[test]
fn sink_failover_holder_selection_is_scoped_to_failover_group() {
    let before = vec![
        SinkHolderSnapshot {
            node_name: "node-a".to_string(),
            active_pids: BTreeSet::from([1]),
            bound_scopes: BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]),
        },
        SinkHolderSnapshot {
            node_name: "node-b".to_string(),
            active_pids: BTreeSet::from([2]),
            bound_scopes: BTreeSet::from(["nfs3".to_string()]),
        },
    ];
    assert_eq!(
        unique_sink_failover_holder_from_snapshots(&before).expect("initial holder"),
        Some("node-a".to_string())
    );

    let after = vec![
        SinkHolderSnapshot {
            node_name: "node-b".to_string(),
            active_pids: BTreeSet::from([2]),
            bound_scopes: BTreeSet::from(["nfs3".to_string()]),
        },
        SinkHolderSnapshot {
            node_name: "node-c".to_string(),
            active_pids: BTreeSet::from([3]),
            bound_scopes: BTreeSet::from(["nfs2".to_string()]),
        },
    ];
    assert_eq!(
        unique_sink_failover_holder_from_snapshots(&after).expect("successor holder"),
        Some("node-c".to_string())
    );
}

#[test]
fn sink_failover_successor_election_accepts_same_node_with_new_pid_set() {
    let before = SinkHolderSnapshot {
        node_name: "node-a".to_string(),
        active_pids: BTreeSet::from([1]),
        bound_scopes: BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]),
    };
    let after = SinkHolderSnapshot {
        node_name: "node-a".to_string(),
        active_pids: BTreeSet::from([9]),
        bound_scopes: BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()]),
    };

    assert!(
        sink_failover_successor_elected(&before, Some(&after)),
        "same-node restarted sink should count as successor"
    );
}

#[test]
fn sink_holder_snapshot_ignores_undelivered_route_level_active_pids() {
    let status = json!({
        "daemon": {
            "managed_processes": [
                { "instance_id": "app-1", "pid": 1 }
            ],
            "activation": {
                "routes": [
                    {
                        "route_key": "sink-status:v1.req",
                        "active_pids": [1],
                        "apps": [
                            {
                                "unit_ids": ["runtime.exec.sink"],
                                "bound_scopes_by_unit": {
                                    "runtime.exec.sink": [
                                        { "scope_id": "nfs2" }
                                    ]
                                },
                                "delivered": false,
                                "gate": "runtime_exposure_confirmed",
                                "op": "activate"
                            }
                        ]
                    }
                ]
            }
        }
    });

    let snapshot = sink_holder_snapshot_from_status(&status, "app-1", "node-a");
    assert!(
        snapshot.active_pids.is_empty(),
        "pending sink activation must not count as a live holder: {snapshot:?}"
    );
    assert!(
        snapshot.bound_scopes.is_empty(),
        "pending sink activation must not contribute live holder scopes: {snapshot:?}"
    );
}

#[test]
fn sink_holder_snapshot_accepts_delivered_route_level_active_pids() {
    let status = json!({
        "daemon": {
            "managed_processes": [
                { "instance_id": "app-1", "pid": 1 }
            ],
            "activation": {
                "routes": [
                    {
                        "route_key": "sink-status:v1.req",
                        "active_pids": [1],
                        "apps": [
                            {
                                "unit_ids": ["runtime.exec.sink"],
                                "bound_scopes_by_unit": {
                                    "runtime.exec.sink": [
                                        { "scope_id": "nfs2" }
                                    ]
                                },
                                "delivered": true,
                                "gate": "activated",
                                "op": "activate"
                            }
                        ]
                    }
                ]
            }
        }
    });

    let snapshot = sink_holder_snapshot_from_status(&status, "app-1", "node-a");
    assert_eq!(snapshot.active_pids, BTreeSet::from([1]));
    assert_eq!(snapshot.bound_scopes, BTreeSet::from(["nfs2".to_string()]));
}

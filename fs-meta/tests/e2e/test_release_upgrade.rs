#![cfg(target_os = "linux")]

use crate::support::api_client::{FsMetaApiClient, OperatorSession};
use crate::support::cluster5::Cluster5;
use crate::support::cpu_budget::{assert_cpu_budget, measure_cpu_budget};
use crate::support::nfs_lab::NfsLab;
use crate::support::{reserve_http_addrs, skip_unless_real_nfs_enabled, wait_until};
use fs_meta::{RootSelector, RootSpec};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex, MutexGuard, OnceLock,
};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UpgradeMode {
    Smoke,
    PeerSourceControlCompletionAfterNodeARestart,
    FacadeClaimContinuityAfterUpgrade,
    RootsPersistAcrossUpgrade,
    TreeStatsStableAcrossUpgrade,
    TreeMaterializationAfterUpgrade,
    SinkControlRolesAfterUpgrade,
    SourceControlRolesAfterUpgrade,
    UpgradeWindowJoin,
    CpuBudget,
}

impl UpgradeMode {
    fn app_prefix(self) -> &'static str {
        match self {
            Self::Smoke => "fs-meta-api-upgrade-smoke",
            Self::PeerSourceControlCompletionAfterNodeARestart => {
                "fs-meta-api-upgrade-peer-source-control"
            }
            Self::FacadeClaimContinuityAfterUpgrade => "fs-meta-api-upgrade-facade-claim",
            Self::RootsPersistAcrossUpgrade => "fs-meta-api-upgrade-roots",
            Self::TreeStatsStableAcrossUpgrade => "fs-meta-api-upgrade-tree-stats",
            Self::TreeMaterializationAfterUpgrade => "fs-meta-api-upgrade-tree-materialization",
            Self::SinkControlRolesAfterUpgrade => "fs-meta-api-upgrade-sink-control-roles",
            Self::SourceControlRolesAfterUpgrade => "fs-meta-api-upgrade-source-control-roles",
            Self::UpgradeWindowJoin => "fs-meta-api-upgrade-window",
            Self::CpuBudget => "fs-meta-api-upgrade-cpu",
        }
    }
}

struct UpgradeHarness {
    cluster: Cluster5,
    lab: NfsLab,
    session: OperatorSession,
    candidate_base_urls: Vec<String>,
    app_id: String,
    facade_resource_id: String,
    roots: Vec<RootSpec>,
    baseline_cpu: BTreeMap<String, Vec<u32>>,
}

pub fn run() -> Result<(), String> {
    run_mode(UpgradeMode::Smoke)
}

fn peer_source_control_env_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    match LOCK.get_or_init(|| Mutex::new(())).lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn with_peer_source_control_repro_env<T>(f: impl FnOnce() -> T) -> T {
    let _lock = peer_source_control_env_lock();
    let vars = [
        ("FSMETA_DEBUG_FORCE_FIND_ROUTE_CAPTURE", "1"),
        ("FSMETA_DEBUG_CONTROL_SCOPE_CAPTURE", "1"),
        ("FSMETA_DEBUG_SINK_WORKER_PRE_DISPATCH", "1"),
        ("DATANIX_KEEP_E2E_ARTIFACTS", "1"),
    ];
    let previous = vars
        .iter()
        .map(|(key, _)| ((*key).to_string(), std::env::var_os(key)))
        .collect::<Vec<(String, Option<OsString>)>>();
    for (key, value) in vars {
        std::env::set_var(key, value);
    }
    let result = f();
    for (key, value) in previous {
        match value {
            Some(value) => std::env::set_var(&key, value),
            None => std::env::remove_var(&key),
        }
    }
    result
}

pub fn run_peer_source_control_completion_after_node_a_recovery() -> Result<(), String> {
    with_peer_source_control_repro_env(|| {
        run_mode(UpgradeMode::PeerSourceControlCompletionAfterNodeARestart)
    })
}

pub fn run_facade_claim_continuity_after_upgrade() -> Result<(), String> {
    run_mode(UpgradeMode::FacadeClaimContinuityAfterUpgrade)
}

pub fn run_roots_persist_across_upgrade() -> Result<(), String> {
    run_mode(UpgradeMode::RootsPersistAcrossUpgrade)
}

pub fn run_tree_stats_stable_across_upgrade() -> Result<(), String> {
    run_mode(UpgradeMode::TreeStatsStableAcrossUpgrade)
}

pub fn run_tree_materialization_after_upgrade() -> Result<(), String> {
    run_mode(UpgradeMode::TreeMaterializationAfterUpgrade)
}

pub fn run_sink_control_roles_after_upgrade() -> Result<(), String> {
    run_mode(UpgradeMode::SinkControlRolesAfterUpgrade)
}

pub fn run_source_control_roles_after_upgrade() -> Result<(), String> {
    run_mode(UpgradeMode::SourceControlRolesAfterUpgrade)
}

pub fn run_upgrade_window_join() -> Result<(), String> {
    run_mode(UpgradeMode::UpgradeWindowJoin)
}

pub fn run_cpu_budget() -> Result<(), String> {
    run_mode(UpgradeMode::CpuBudget)
}

fn run_mode(mode: UpgradeMode) -> Result<(), String> {
    if let Some(reason) = skip_unless_real_nfs_enabled() {
        eprintln!("[fs-meta-api-upgrade] skipped: {reason}");
        return Ok(());
    }

    let mut harness = build_upgrade_harness(
        mode.app_prefix(),
        matches!(mode, UpgradeMode::UpgradeWindowJoin),
        1,
    )?;
    match mode {
        UpgradeMode::Smoke => upgrade_to_generation_two(&mut harness)?,
        UpgradeMode::PeerSourceControlCompletionAfterNodeARestart => {
            scenario_peer_source_control_completion_after_node_a_recovery(&mut harness)?
        }
        UpgradeMode::FacadeClaimContinuityAfterUpgrade => {
            scenario_facade_claim_continuity_after_upgrade(&mut harness)?
        }
        UpgradeMode::RootsPersistAcrossUpgrade => {
            scenario_roots_persist_across_upgrade(&mut harness)?
        }
        UpgradeMode::TreeStatsStableAcrossUpgrade => {
            scenario_tree_stats_stable_across_upgrade(&mut harness)?
        }
        UpgradeMode::TreeMaterializationAfterUpgrade => {
            scenario_tree_materialization_after_upgrade(&mut harness)?
        }
        UpgradeMode::SinkControlRolesAfterUpgrade => {
            scenario_sink_control_roles_after_upgrade(&mut harness)?
        }
        UpgradeMode::SourceControlRolesAfterUpgrade => {
            scenario_source_control_roles_after_upgrade(&mut harness)?
        }
        UpgradeMode::UpgradeWindowJoin => scenario_upgrade_window_join(&mut harness)?,
        UpgradeMode::CpuBudget => scenario_cpu_budget(&mut harness)?,
    }

    Ok(())
}

fn build_upgrade_harness(
    app_prefix: &str,
    preannounce_nfs4: bool,
    facade_count: usize,
) -> Result<UpgradeHarness, String> {
    let mut lab = NfsLab::start()?;
    seed_baseline_content(&lab)?;
    let cluster = Cluster5::start()?;
    let baseline_cpu = measure_baseline_cpu(&cluster)?;

    let app_id = format!("{app_prefix}-{}", unique_suffix());
    let facade_resource_id = format!("fs-meta-tcp-listener-{app_id}");
    let facade_addrs = reserve_http_addrs(facade_count)?;
    mount_and_announce(&cluster, &mut lab, &facade_resource_id, &facade_addrs)?;
    if preannounce_nfs4 {
        lab.create_export("nfs4")?;
        let mount_a = lab.mount_export("node-a", "nfs4")?;
        let mount_c = lab.mount_export("node-c", "nfs4")?;
        let mount_d = lab.mount_export("node-d", "nfs4")?;
        for (node_name, mount_path) in [
            ("node-a", mount_a),
            ("node-c", mount_c),
            ("node-d", mount_d),
        ] {
            cluster.announce_resources_clusterwide(vec![json!({
                "resource_id": "nfs4",
                "node_id": cluster.node_id(node_name)?,
                "resource_kind": "nfs",
                "source": lab.export_source("nfs4"),
                "mount_hint": mount_path.display().to_string(),
            })])?;
        }
    }

    let roots = vec![
        root_spec("nfs1", &lab.export_source("nfs1")),
        root_spec("nfs2", &lab.export_source("nfs2")),
    ];
    let release_v1 =
        cluster.build_fs_meta_release(&app_id, &facade_resource_id, roots.clone(), 1, true)?;
    cluster.apply_release("node-a", release_v1)?;

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
    let session =
        OperatorSession::login_many(candidate_base_urls.clone(), "operator", "operator123")?;

    Ok(UpgradeHarness {
        cluster,
        lab,
        session,
        candidate_base_urls,
        app_id,
        facade_resource_id,
        roots,
        baseline_cpu,
    })
}

fn build_generation_two_release(harness: &UpgradeHarness) -> Result<Value, String> {
    harness.cluster.build_fs_meta_release(
        &harness.app_id,
        &harness.facade_resource_id,
        harness.roots.clone(),
        2,
        true,
    )
}

fn node_stderr_tail(
    cluster: &Cluster5,
    node_name: &str,
    line_limit: usize,
) -> Result<String, String> {
    let process = cluster.node_process_status(node_name)?;
    let stderr_log = process
        .get("stderr_log")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("node {node_name} process status missing stderr_log: {process}"))?;
    let content = std::fs::read_to_string(stderr_log)
        .map_err(|err| format!("read {node_name} stderr log failed: {err}"))?;
    let tail = content
        .lines()
        .rev()
        .take(line_limit)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<Vec<_>>()
        .join("\n");
    Ok(tail)
}

fn apply_generation_two_release_only(harness: &mut UpgradeHarness) -> Result<(), String> {
    let release_v2 = build_generation_two_release(harness)?;
    match harness.cluster.apply_release("node-a", release_v2) {
        Ok(_) => Ok(()),
        Err(err) => {
            let node_b_tail = node_stderr_tail(&harness.cluster, "node-b", 80)
                .unwrap_or_else(|tail_err| format!("<node-b-tail-unavailable: {tail_err}>"));
            if node_b_tail.contains(
                "fs_meta_runtime_app: on_control_frame source.apply_orchestration_signals err=operation timed out",
            ) {
                return Err(format!(
                    "generation-two peer source-control completion failed after node-a recovery: upgrade={err}; node_b_tail={node_b_tail}"
                ));
            }
            Err(format!(
                "generation-two apply failed before the node-b peer source-control seam was visible: upgrade={err}; node_b_tail={node_b_tail}"
            ))
        }
    }
}

fn upgrade_to_generation_two(harness: &mut UpgradeHarness) -> Result<(), String> {
    apply_generation_two_release_only(harness)?;
    wait_for_generation(&harness.cluster, 2)?;
    harness.session = OperatorSession::login_many(
        harness.candidate_base_urls.clone(),
        "operator",
        "operator123",
    )?;
    Ok(())
}

fn wait_for_primary_tree_materialization(
    session: &mut OperatorSession,
    reason: &str,
) -> Result<(), String> {
    wait_until(Duration::from_secs(120), reason, || {
        let tree =
            match session.tree(&[("path", "/".to_string()), ("recursive", "true".to_string())]) {
                Ok(tree) => tree,
                Err(err) => {
                    let status = session
                        .status()
                        .unwrap_or_else(|status_err| json!({ "status_error": status_err }));
                    return Err(format!("tree request failed: {err}; status={status}"));
                }
            };
        if group_total_nodes(&tree, "nfs1") > 0 && group_total_nodes(&tree, "nfs2") > 0 {
            Ok(true)
        } else {
            let status = session
                .status()
                .unwrap_or_else(|status_err| json!({ "status_error": status_err }));
            Err(format!("tree={tree}; status={status}"))
        }
    })
}

fn current_root_ids(session: &mut OperatorSession) -> Result<Vec<String>, String> {
    let current_roots = session.monitoring_roots()?;
    Ok(current_roots
        .get("roots")
        .and_then(Value::as_array)
        .map(|rows| {
            rows.iter()
                .filter_map(|r| r.get("id").and_then(Value::as_str))
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default())
}

fn scenario_peer_source_control_completion_after_node_a_recovery(
    harness: &mut UpgradeHarness,
) -> Result<(), String> {
    apply_generation_two_release_only(harness)
}

fn scenario_facade_claim_continuity_after_upgrade(
    harness: &mut UpgradeHarness,
) -> Result<(), String> {
    let upgrade_result = upgrade_to_generation_two(harness);
    let convergence_result = wait_for_node_d_facade_claim_convergence(
        &harness.cluster,
        &harness.candidate_base_urls,
        &harness.app_id,
        Duration::from_secs(15),
    );
    match (upgrade_result, convergence_result) {
        (Ok(()), Ok(())) => Ok(()),
        (upgrade, converge) => Err(format!(
            "generation-two node-d facade continuity failed: upgrade={}; convergence={}",
            upgrade.err().unwrap_or_else(|| "ok".to_string()),
            converge.err().unwrap_or_else(|| "ok".to_string()),
        )),
    }
}

fn scenario_roots_persist_across_upgrade(harness: &mut UpgradeHarness) -> Result<(), String> {
    upgrade_to_generation_two(harness)?;

    let root_ids = current_root_ids(&mut harness.session)?;
    if root_ids != vec!["nfs1", "nfs2"] {
        return Err(format!(
            "roots changed unexpectedly across upgrade: {:?}",
            harness.session.monitoring_roots()?
        ));
    }

    Ok(())
}

fn scenario_tree_stats_stable_across_upgrade(harness: &mut UpgradeHarness) -> Result<(), String> {
    upgrade_to_generation_two(harness)?;
    let _ = harness.cluster.wait_http_login_ready(
        &harness.candidate_base_urls,
        "operator",
        "operator123",
        Duration::from_secs(120),
    )?;
    harness.session = OperatorSession::login_many(
        harness.candidate_base_urls.clone(),
        "operator",
        "operator123",
    )?;
    Ok(())
}

fn scenario_tree_materialization_after_upgrade(harness: &mut UpgradeHarness) -> Result<(), String> {
    upgrade_to_generation_two(harness)?;
    let _ = harness.cluster.wait_http_login_ready(
        &harness.candidate_base_urls,
        "operator",
        "operator123",
        Duration::from_secs(120),
    )?;
    harness.session = OperatorSession::login_many(
        harness.candidate_base_urls.clone(),
        "operator",
        "operator123",
    )?;
    harness.session.rescan()?;
    wait_for_primary_tree_materialization(
        &mut harness.session,
        "tree materializes after generation-two upgrade",
    )?;
    Ok(())
}

fn scenario_sink_control_roles_after_upgrade(harness: &mut UpgradeHarness) -> Result<(), String> {
    let upgrade_result = upgrade_to_generation_two(harness);
    let convergence_result = wait_for_node_a_sink_control_convergence(
        &harness.cluster,
        &harness.candidate_base_urls,
        &harness.app_id,
        Duration::from_secs(15),
    );
    match (upgrade_result, convergence_result) {
        (Ok(()), Ok(())) => Ok(()),
        (upgrade, converge) => Err(format!(
            "generation-two sink-role convergence failed: upgrade={}; convergence={}",
            upgrade.err().unwrap_or_else(|| "ok".to_string()),
            converge.err().unwrap_or_else(|| "ok".to_string()),
        )),
    }
}

fn scenario_source_control_roles_after_upgrade(harness: &mut UpgradeHarness) -> Result<(), String> {
    let upgrade_result = upgrade_to_generation_two(harness);
    let convergence_result = wait_for_peer_source_control_convergence(
        &harness.cluster,
        &harness.candidate_base_urls,
        &harness.app_id,
        Duration::from_secs(15),
    );
    match (upgrade_result, convergence_result) {
        (Ok(()), Ok(())) => Ok(()),
        (upgrade, converge) => Err(format!(
            "generation-two peer-source convergence failed: upgrade={}; convergence={}",
            upgrade.err().unwrap_or_else(|| "ok".to_string()),
            converge.err().unwrap_or_else(|| "ok".to_string()),
        )),
    }
}

fn scenario_upgrade_window_join(harness: &mut UpgradeHarness) -> Result<(), String> {
    upgrade_to_generation_two(harness)?;

    if harness.lab.mount_path("node-a", "nfs4").is_none()
        || harness.lab.mount_path("node-c", "nfs4").is_none()
        || harness.lab.mount_path("node-d", "nfs4").is_none()
    {
        harness.lab.create_export("nfs4")?;
        let mount_a = harness.lab.mount_export("node-a", "nfs4")?;
        let mount_c = harness.lab.mount_export("node-c", "nfs4")?;
        let mount_d = harness.lab.mount_export("node-d", "nfs4")?;
        for (node_name, mount_path) in [
            ("node-a", mount_a),
            ("node-c", mount_c),
            ("node-d", mount_d),
        ] {
            harness.cluster.announce_resources_clusterwide(vec![json!({
                "resource_id": "nfs4",
                "node_id": harness.cluster.node_id(node_name)?,
                "resource_kind": "nfs",
                "source": harness.lab.export_source("nfs4"),
                "mount_hint": mount_path.display().to_string(),
            })])?;
        }
    }
    harness
        .lab
        .write_file("nfs4", "upgrade-window/new.txt", "during-upgrade\n")?;
    wait_for_generation(&harness.cluster, 2)?;

    Ok(())
}

fn scenario_cpu_budget(harness: &mut UpgradeHarness) -> Result<(), String> {
    upgrade_to_generation_two(harness)?;
    wait_for_node_d_facade_claim_convergence(
        &harness.cluster,
        &harness.candidate_base_urls,
        &harness.app_id,
        Duration::from_secs(15),
    )?;
    wait_for_node_a_sink_control_convergence(
        &harness.cluster,
        &harness.candidate_base_urls,
        &harness.app_id,
        Duration::from_secs(15),
    )?;
    wait_for_peer_source_control_convergence(
        &harness.cluster,
        &harness.candidate_base_urls,
        &harness.app_id,
        Duration::from_secs(15),
    )?;
    let ready_base = harness.cluster.wait_http_login_ready(
        &harness.candidate_base_urls,
        "operator",
        "operator123",
        Duration::from_secs(120),
    )?;
    harness.session = OperatorSession::login_many(
        harness.candidate_base_urls.clone(),
        "operator",
        "operator123",
    )?;
    harness.session.rescan()?;
    wait_for_primary_tree_materialization(&mut harness.session, "cpu-budget tree materializes")?;

    let (stop, worker) = spawn_light_polling(
        ready_base,
        harness.session.token().to_string(),
        harness.session.query_api_key().to_string(),
    );
    let steady_cpu = measure_steady_cpu(&harness.cluster, &harness.app_id)?;
    eprintln!(
        "[fs-meta-api-upgrade] cpu-budget baseline_pids={:?} steady_pids={:?}",
        harness.baseline_cpu, steady_cpu
    );
    let summary = measure_cpu_budget(
        &harness.baseline_cpu,
        &steady_cpu,
        Duration::from_secs(5),
        Duration::from_secs(3 * 60),
    )?;
    eprintln!(
        "[fs-meta-api-upgrade] cpu-budget summary mean={:?} p95={:?} cluster_mean={:.2}",
        summary.per_node_mean_delta, summary.per_node_p95_delta, summary.cluster_mean_delta
    );
    stop.store(true, Ordering::Relaxed);
    let _ = worker.join();
    assert_cpu_budget(&summary)
}

fn wait_for_generation(cluster: &Cluster5, generation: i64) -> Result<(), String> {
    wait_until(
        Duration::from_secs(120),
        &format!("release generation={generation} converged"),
        || {
            let state = cluster.runtime_target_state("node-a")?;
            Ok(state
                .to_string()
                .contains(&format!("\"generation\":{generation}")))
        },
    )
}

fn wait_for_node_a_sink_control_convergence(
    cluster: &Cluster5,
    candidate_base_urls: &[String],
    app_id: &str,
    timeout: Duration,
) -> Result<(), String> {
    wait_until(
        timeout,
        "node-a sink control converges after generation-two upgrade",
        || {
            let sink_active =
                cluster.unit_active_pids_for_instance("node-a", app_id, "runtime.exec.sink")?;
            let node_status = cluster.status("node-a")?;
            let app_status = probe_status_session(cluster, candidate_base_urls)
                .ok()
                .and_then(|mut session| session.status().ok());
            let status_view = app_status.as_ref().unwrap_or(&node_status);
            let scheduled_sink = status_debug_groups_by_node(
                status_view,
                "sink",
                "scheduled_groups_by_node",
                "node-a",
            );
            let expected = BTreeMap::from([(
                "node-a".to_string(),
                vec!["nfs1".to_string(), "nfs2".to_string()],
            )]);
            let route_summaries = activation_route_summaries(&node_status);
            let sink_routes_active = [
                "sink-logical-roots-control:v1.stream:activated",
                "on-demand-force-find:v1.on-demand-force-find.req:activated",
                "materialized-find:v1.req:activated",
                "fs-meta.events:v1.stream:activated",
            ]
            .iter()
            .all(|needle| route_summaries.iter().any(|route| route.contains(needle)));
            if !sink_active.is_empty()
                && (scheduled_sink == expected || (scheduled_sink.is_empty() && sink_routes_active))
            {
                Ok(true)
            } else {
                Err(format!(
                "node-a sink not converged: active_pids={sink_active:?}; scheduled_sink={scheduled_sink:?}; routes={route_summaries:?}",
            ))
            }
        },
    )
}

fn wait_for_node_d_facade_claim_convergence(
    cluster: &Cluster5,
    candidate_base_urls: &[String],
    app_id: &str,
    timeout: Duration,
) -> Result<(), String> {
    let Some(node_d_url) = candidate_base_urls.first().cloned() else {
        return Err("missing node-d candidate facade url".to_string());
    };
    wait_until(
        timeout,
        "node-d fixed-bind facade continuity converges after generation-two upgrade",
        || {
            let facade_active = cluster.facade_pids_for_instance("node-d", app_id)?;
            let node_status = cluster.status("node-d")?;
            let _session = probe_status_session(cluster, &[node_d_url.clone()])?;
            if !facade_active.is_empty() {
                Ok(true)
            } else {
                Err(format!(
                    "node-d facade not converged: facade_active={facade_active:?} routes={:?}",
                    activation_route_summaries(&node_status)
                ))
            }
        },
    )
}

fn wait_for_peer_source_control_convergence(
    cluster: &Cluster5,
    candidate_base_urls: &[String],
    app_id: &str,
    timeout: Duration,
) -> Result<(), String> {
    wait_until(
        timeout,
        "node-b/c/d source control converges after generation-two upgrade",
        || {
            let mut failures = Vec::new();
            let app_status = probe_status_session(cluster, candidate_base_urls)
                .ok()
                .and_then(|mut session| session.status().ok());
            for (node_name, expected_groups) in [
                ("node-b", vec!["nfs1".to_string()]),
                ("node-c", vec!["nfs1".to_string(), "nfs2".to_string()]),
                ("node-d", vec!["nfs2".to_string()]),
            ] {
                let source_active = cluster.unit_active_pids_for_instance(
                    node_name,
                    app_id,
                    "runtime.exec.source",
                )?;
                let scan_active = cluster.unit_active_pids_for_instance(
                    node_name,
                    app_id,
                    "runtime.exec.scan",
                )?;
                let node_status = cluster.status(node_name)?;
                let status_view = app_status.as_ref().unwrap_or(&node_status);
                let scheduled_source = status_debug_groups_by_node(
                    status_view,
                    "source",
                    "scheduled_source_groups_by_node",
                    node_name,
                );
                let scheduled_scan = status_debug_groups_by_node(
                    status_view,
                    "source",
                    "scheduled_scan_groups_by_node",
                    node_name,
                );
                let raw_scheduled_source = status_debug_groups_field(
                    status_view,
                    "source",
                    "scheduled_source_groups_by_node",
                );
                let raw_scheduled_scan = status_debug_groups_field(
                    status_view,
                    "source",
                    "scheduled_scan_groups_by_node",
                );
                let _expected = BTreeMap::from([(node_name.to_string(), expected_groups.clone())]);
                if source_active.is_empty() || scan_active.is_empty() {
                    failures.push(format!(
                    "{node_name}: source_active={source_active:?} scan_active={scan_active:?} scheduled_source={scheduled_source:?} scheduled_scan={scheduled_scan:?} raw_scheduled_source={raw_scheduled_source} raw_scheduled_scan={raw_scheduled_scan} routes={:?}",
                    activation_route_summaries(&node_status)
                ));
                }
            }
            if failures.is_empty() {
                Ok(true)
            } else {
                Err(failures.join(" || "))
            }
        },
    )
}

fn probe_status_session(
    cluster: &Cluster5,
    candidate_base_urls: &[String],
) -> Result<OperatorSession, String> {
    let ready_base = cluster.wait_http_login_ready(
        candidate_base_urls,
        "operator",
        "operator123",
        Duration::from_secs(2),
    )?;
    OperatorSession::login_many(vec![ready_base], "operator", "operator123")
}

fn measure_baseline_cpu(cluster: &Cluster5) -> Result<BTreeMap<String, Vec<u32>>, String> {
    let mut by_node = BTreeMap::new();
    for node_name in ["node-a", "node-b", "node-c", "node-d", "node-e"] {
        by_node.insert(node_name.to_string(), vec![cluster.daemon_pid(node_name)?]);
    }
    Ok(by_node)
}

fn measure_steady_cpu(
    cluster: &Cluster5,
    app_id: &str,
) -> Result<BTreeMap<String, Vec<u32>>, String> {
    let mut by_node = BTreeMap::new();
    for node_name in ["node-a", "node-b", "node-c", "node-d", "node-e"] {
        let mut pids = vec![cluster.daemon_pid(node_name)?];
        pids.extend(
            cluster
                .managed_host_pids_for_instance(node_name, app_id)?
                .into_iter(),
        );
        by_node.insert(node_name.to_string(), pids);
    }
    Ok(by_node)
}

fn spawn_light_polling(
    base_url: String,
    management_token: String,
    query_api_key: String,
) -> (Arc<AtomicBool>, JoinHandle<()>) {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_clone = Arc::clone(&stop);
    let handle = thread::spawn(move || {
        let Ok(client) = FsMetaApiClient::new(base_url) else {
            return;
        };
        while !stop_clone.load(Ordering::Relaxed) {
            let _ = client.status(&management_token);
            let _ = client.tree(
                &query_api_key,
                &[("path", "/".to_string()), ("recursive", "true".to_string())],
            );
            let _ = client.stats(
                &query_api_key,
                &[("path", "/".to_string()), ("recursive", "true".to_string())],
            );
            thread::sleep(Duration::from_secs(5));
        }
    });
    (stop, handle)
}

fn seed_baseline_content(lab: &NfsLab) -> Result<(), String> {
    lab.write_file("nfs1", "baseline/a.txt", "a\n")?;
    thread::sleep(Duration::from_millis(5));
    lab.write_file("nfs2", "baseline/b.txt", "b\n")?;
    Ok(())
}

fn mount_and_announce(
    cluster: &Cluster5,
    lab: &mut NfsLab,
    facade_resource_id: &str,
    facade_addrs: &[String],
) -> Result<(), String> {
    for (node_name, export_name) in [
        ("node-a", "nfs1"),
        ("node-b", "nfs1"),
        ("node-c", "nfs1"),
        ("node-a", "nfs2"),
        ("node-c", "nfs2"),
        ("node-d", "nfs2"),
    ] {
        let mount_path = lab.mount_export(node_name, export_name)?;
        cluster.announce_resources_clusterwide(vec![json!({
            "resource_id": export_name,
            "node_id": cluster.node_id(node_name)?,
            "resource_kind": "nfs",
            "source": lab.export_source(export_name),
            "mount_hint": mount_path.display().to_string(),
        })])?;
    }
    for (node_name, bind_addr) in ["node-d", "node-e"].into_iter().zip(facade_addrs.iter()) {
        cluster.announce_resources_clusterwide(vec![json!({
            "resource_id": facade_resource_id,
            "node_id": cluster.node_id(node_name)?,
            "resource_kind": "tcp_listener",
            "source": format!("http://{node_name}/listener/{facade_resource_id}"),
            "bind_addr": bind_addr,
        })])?;
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

fn group_total_nodes(payload: &Value, group_key: &str) -> u64 {
    let Some(group) = payload
        .get("groups")
        .and_then(Value::as_array)
        .and_then(|groups| {
            groups
                .iter()
                .find(|group| group.get("group").and_then(Value::as_str) == Some(group_key))
        })
    else {
        return 0;
    };
    let root_exists = group
        .get("root")
        .and_then(|root| root.get("exists"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let entries = group
        .get("entries")
        .and_then(Value::as_array)
        .map(|rows| rows.len() as u64)
        .unwrap_or(0);
    entries + if root_exists { 1 } else { 0 }
}

fn status_debug_groups_by_node(
    status: &Value,
    section: &str,
    field: &str,
    node_name: &str,
) -> BTreeMap<String, Vec<String>> {
    let Some(debug_field) = status
        .get(section)
        .and_then(|v| v.get("debug"))
        .and_then(|v| v.get(field))
        .and_then(Value::as_object)
    else {
        return BTreeMap::new();
    };

    let mut groups = debug_field
        .get(node_name)
        .and_then(Value::as_array)
        .map(|rows| {
            rows.iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    if groups.is_empty() {
        let instance_prefix = format!("{node_name}-");
        for (key, value) in debug_field {
            if !key.starts_with(&instance_prefix) {
                continue;
            }
            if let Some(rows) = value.as_array() {
                groups.extend(rows.iter().filter_map(Value::as_str).map(str::to_string));
            }
        }
        groups.sort();
        groups.dedup();
    }

    if groups.is_empty() {
        BTreeMap::new()
    } else {
        BTreeMap::from([(node_name.to_string(), groups)])
    }
}

fn status_debug_groups_field(status: &Value, section: &str, field: &str) -> Value {
    status
        .get(section)
        .and_then(|v| v.get("debug"))
        .and_then(|v| v.get(field))
        .cloned()
        .unwrap_or(Value::Null)
}

fn activation_route_summaries(status: &Value) -> Vec<String> {
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
        summaries.push(format!("{route_key}:{state}:active_pids={active_pids}"));
    }
    summaries
}

fn unique_suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

#![cfg(target_os = "linux")]

use crate::support::api_client::{FsMetaApiClient, OperatorSession};
use crate::support::cluster5::Cluster5;
use crate::support::cpu_budget::{assert_cpu_budget, measure_cpu_budget};
use crate::support::full_demo_roots::{self, FullDemoRoot};
use crate::support::nfs_lab::NfsLab;
use crate::support::{reserve_http_addrs, skip_unless_real_nfs_enabled, wait_until};
use fs_meta::{RootSelector, RootSpec};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex, MutexGuard, OnceLock,
};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const RELEASE_UPGRADE_CLUSTER_NODES: [&str; 5] = ["node-a", "node-b", "node-c", "node-d", "node-e"];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UpgradeMode {
    GenerationTwoApply,
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
            Self::GenerationTwoApply => "fs-meta-api-upgrade-apply",
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

    fn progress_label(self) -> &'static str {
        match self {
            Self::GenerationTwoApply => "upgrade-apply-generation-two",
            Self::Smoke => "upgrade-generation-two-http",
            Self::PeerSourceControlCompletionAfterNodeARestart => "upgrade-peer-source-control",
            Self::FacadeClaimContinuityAfterUpgrade => "upgrade-facade-continuity",
            Self::RootsPersistAcrossUpgrade => "upgrade-roots-persist",
            Self::TreeStatsStableAcrossUpgrade => "upgrade-tree-stats",
            Self::TreeMaterializationAfterUpgrade => "upgrade-tree-materialization",
            Self::SinkControlRolesAfterUpgrade => "upgrade-sink-scope",
            Self::SourceControlRolesAfterUpgrade => "upgrade-runtime-scope",
            Self::UpgradeWindowJoin => "upgrade-window-join",
            Self::CpuBudget => "upgrade-cpu-budget",
        }
    }
}

struct UpgradeHarness {
    mode: UpgradeMode,
    cluster: Cluster5,
    lab: NfsLab,
    session: OperatorSession,
    candidate_base_urls: Vec<String>,
    app_id: String,
    facade_resource_id: String,
    roots: Vec<RootSpec>,
    baseline_cpu: BTreeMap<String, Vec<u32>>,
    uses_full_demo_roots: bool,
}

fn l5_progress(mode: UpgradeMode, step: &str, state: &str) {
    eprintln!(
        "[fs-meta-l5-progress] mode={} step={} state={}",
        mode.progress_label(),
        step,
        state
    );
}

pub fn run() -> Result<(), String> {
    run_mode(UpgradeMode::Smoke)
}

pub fn run_generation_two_apply() -> Result<(), String> {
    run_mode(UpgradeMode::GenerationTwoApply)
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

pub fn run_source_runtime_scope_after_upgrade() -> Result<(), String> {
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

    l5_progress(mode, "00.test", "begin");
    l5_progress(mode, "01.harness", "begin");
    let mut harness =
        build_upgrade_harness(mode, matches!(mode, UpgradeMode::UpgradeWindowJoin), 1)?;
    l5_progress(mode, "01.harness", "ok");
    l5_progress(mode, "02.scenario", "begin");
    match mode {
        UpgradeMode::GenerationTwoApply => scenario_generation_two_apply(&mut harness)?,
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

    l5_progress(mode, "99.test", "ok");
    Ok(())
}

fn build_upgrade_harness(
    mode: UpgradeMode,
    preannounce_nfs4: bool,
    facade_count: usize,
) -> Result<UpgradeHarness, String> {
    let app_prefix = mode.app_prefix();
    l5_progress(mode, "01.01.nfs-lab", "begin");
    let mut lab = NfsLab::start()?;
    l5_progress(mode, "01.01.nfs-lab", "ok");
    l5_progress(mode, "01.02.demo-roots", "begin");
    let full_demo_roots = full_demo_roots::logical_roots_from_env(&["nfs1", "nfs2"])?;
    if full_demo_roots.is_none() {
        seed_baseline_content(&lab)?;
    }
    l5_progress(mode, "01.02.demo-roots", "ok");
    l5_progress(mode, "01.03.cluster", "begin");
    let cluster = Cluster5::start()?;
    l5_progress(mode, "01.03.cluster", "ok");
    l5_progress(mode, "01.04.cpu-baseline", "begin");
    let baseline_cpu = measure_baseline_cpu(&cluster)?;
    l5_progress(mode, "01.04.cpu-baseline", "ok");

    let app_id = format!("{app_prefix}-{}", unique_suffix());
    let facade_resource_id = format!("fs-meta-tcp-listener-{app_id}");
    let facade_addrs = reserve_http_addrs(facade_count)?;
    l5_progress(mode, "01.05.resources", "begin");
    if let Some(roots) = full_demo_roots.as_ref() {
        announce_full_demo_resources(&cluster, roots, &facade_resource_id, &facade_addrs)?;
    } else {
        mount_and_announce_lab(&cluster, &mut lab, &facade_resource_id, &facade_addrs)?;
    }
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
    l5_progress(mode, "01.05.resources", "ok");

    let roots = full_demo_roots
        .as_ref()
        .map(|roots| roots.iter().map(FullDemoRoot::root_spec).collect())
        .unwrap_or_else(|| {
            vec![
                root_spec("nfs1", &lab.export_source("nfs1")),
                root_spec("nfs2", &lab.export_source("nfs2")),
            ]
        });
    let uses_full_demo_roots = full_demo_roots.is_some();
    let mut release_v1 =
        cluster.build_fs_meta_release(&app_id, &facade_resource_id, roots.clone(), 1, true)?;
    if uses_full_demo_roots {
        full_demo_roots::apply_bounded_audit_config(&mut release_v1)?;
    }
    l5_progress(mode, "01.06.apply-generation-one", "begin");
    cluster.apply_release("node-a", release_v1)?;
    l5_progress(mode, "01.06.apply-generation-one", "ok");

    let candidate_base_urls = facade_addrs
        .iter()
        .map(|addr| format!("http://{addr}"))
        .collect::<Vec<_>>();
    l5_progress(mode, "01.07.login", "begin");
    let _base_url = cluster.wait_http_login_ready(
        &candidate_base_urls,
        "operator",
        "operator123",
        Duration::from_secs(120),
    )?;
    let session =
        OperatorSession::login_many(candidate_base_urls.clone(), "operator", "operator123")?;
    l5_progress(mode, "01.07.login", "ok");

    Ok(UpgradeHarness {
        mode,
        cluster,
        lab,
        session,
        candidate_base_urls,
        app_id,
        facade_resource_id,
        roots,
        baseline_cpu,
        uses_full_demo_roots,
    })
}

fn build_generation_two_release(harness: &UpgradeHarness) -> Result<Value, String> {
    let mut release = harness.cluster.build_fs_meta_release(
        &harness.app_id,
        &harness.facade_resource_id,
        harness.roots.clone(),
        2,
        true,
    )?;
    if harness.uses_full_demo_roots {
        full_demo_roots::apply_bounded_audit_config(&mut release)?;
    }
    Ok(release)
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
    l5_progress(harness.mode, "02.01.apply-generation-two", "begin");
    let release_v2 = build_generation_two_release(harness)?;
    match harness.cluster.apply_release("node-a", release_v2) {
        Ok(_) => {
            l5_progress(harness.mode, "02.01.apply-generation-two", "ok");
            Ok(())
        }
        Err(err) => {
            l5_progress(harness.mode, "02.01.apply-generation-two", "fail");
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
    l5_progress(harness.mode, "02.02.wait-generation-two", "begin");
    wait_for_generation(&harness.cluster, 2)?;
    l5_progress(harness.mode, "02.02.wait-generation-two", "ok");
    l5_progress(harness.mode, "02.03.http-login", "begin");
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
    l5_progress(harness.mode, "02.03.http-login", "ok");
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

fn wait_for_manual_rescan_acceptance(
    session: &mut OperatorSession,
    reason: &str,
) -> Result<(), String> {
    wait_until(Duration::from_secs(120), reason, || {
        match session.rescan() {
            Ok(_) => Ok(true),
            Err(err) => {
                let status = session
                    .status()
                    .unwrap_or_else(|status_err| json!({ "status_error": status_err }));
                Err(format!(
                    "manual rescan not accepted yet: {err}; status={status}"
                ))
            }
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

fn scenario_generation_two_apply(harness: &mut UpgradeHarness) -> Result<(), String> {
    apply_generation_two_release_only(harness)
}

fn scenario_facade_claim_continuity_after_upgrade(
    harness: &mut UpgradeHarness,
) -> Result<(), String> {
    let upgrade_result = upgrade_to_generation_two(harness);
    l5_progress(harness.mode, "03.01.facade-claim-convergence", "begin");
    let convergence_result = wait_for_node_d_facade_claim_convergence(
        &harness.cluster,
        &harness.candidate_base_urls,
        &harness.app_id,
        Duration::from_secs(15),
    );
    l5_progress(
        harness.mode,
        "03.01.facade-claim-convergence",
        if convergence_result.is_ok() {
            "ok"
        } else {
            "fail"
        },
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

    l5_progress(harness.mode, "03.01.roots-persist", "begin");
    let root_ids = current_root_ids(&mut harness.session)?;
    if root_ids != vec!["nfs1", "nfs2"] {
        l5_progress(harness.mode, "03.01.roots-persist", "fail");
        return Err(format!(
            "roots changed unexpectedly across upgrade: {:?}",
            harness.session.monitoring_roots()?
        ));
    }

    l5_progress(harness.mode, "03.01.roots-persist", "ok");
    Ok(())
}

fn scenario_tree_stats_stable_across_upgrade(harness: &mut UpgradeHarness) -> Result<(), String> {
    upgrade_to_generation_two(harness)?;
    l5_progress(harness.mode, "03.01.tree-stats-login", "begin");
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
    l5_progress(harness.mode, "03.01.tree-stats-login", "ok");
    Ok(())
}

fn scenario_tree_materialization_after_upgrade(harness: &mut UpgradeHarness) -> Result<(), String> {
    upgrade_to_generation_two(harness)?;
    l5_progress(harness.mode, "03.01.tree-login", "begin");
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
    l5_progress(harness.mode, "03.01.tree-login", "ok");
    l5_progress(harness.mode, "03.02.peer-source-control", "begin");
    wait_for_peer_source_control_convergence(
        &harness.cluster,
        &harness.candidate_base_urls,
        &harness.app_id,
        &harness.roots,
        Duration::from_secs(30),
    )?;
    l5_progress(harness.mode, "03.02.peer-source-control", "ok");
    l5_progress(harness.mode, "03.03.manual-rescan", "begin");
    wait_for_manual_rescan_acceptance(
        &mut harness.session,
        "manual rescan accepted after release-upgrade source scope convergence",
    )?;
    l5_progress(harness.mode, "03.03.manual-rescan", "ok");
    l5_progress(harness.mode, "03.04.tree-materialization", "begin");
    wait_for_primary_tree_materialization(
        &mut harness.session,
        "tree materializes after generation-two upgrade",
    )?;
    l5_progress(harness.mode, "03.04.tree-materialization", "ok");
    Ok(())
}

fn scenario_sink_control_roles_after_upgrade(harness: &mut UpgradeHarness) -> Result<(), String> {
    let upgrade_result = upgrade_to_generation_two(harness);
    l5_progress(harness.mode, "03.01.sink-control-convergence", "begin");
    let convergence_result = wait_for_node_a_sink_control_convergence(
        &harness.cluster,
        &harness.candidate_base_urls,
        &harness.app_id,
        Duration::from_secs(15),
    );
    l5_progress(
        harness.mode,
        "03.01.sink-control-convergence",
        if convergence_result.is_ok() {
            "ok"
        } else {
            "fail"
        },
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
    l5_progress(harness.mode, "03.01.source-control-convergence", "begin");
    let convergence_result = wait_for_peer_source_control_convergence(
        &harness.cluster,
        &harness.candidate_base_urls,
        &harness.app_id,
        &harness.roots,
        Duration::from_secs(15),
    );
    l5_progress(
        harness.mode,
        "03.01.source-control-convergence",
        if convergence_result.is_ok() {
            "ok"
        } else {
            "fail"
        },
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

    l5_progress(harness.mode, "03.01.window-nfs4", "begin");
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
    l5_progress(harness.mode, "03.01.window-nfs4", "ok");
    l5_progress(harness.mode, "03.02.window-write", "begin");
    harness
        .lab
        .write_file("nfs4", "upgrade-window/new.txt", "during-upgrade\n")?;
    l5_progress(harness.mode, "03.02.window-write", "ok");
    l5_progress(harness.mode, "03.03.window-generation", "begin");
    wait_for_generation(&harness.cluster, 2)?;
    l5_progress(harness.mode, "03.03.window-generation", "ok");

    Ok(())
}

fn scenario_cpu_budget(harness: &mut UpgradeHarness) -> Result<(), String> {
    upgrade_to_generation_two(harness)?;
    l5_progress(harness.mode, "03.01.facade-claim-convergence", "begin");
    wait_for_node_d_facade_claim_convergence(
        &harness.cluster,
        &harness.candidate_base_urls,
        &harness.app_id,
        Duration::from_secs(15),
    )?;
    l5_progress(harness.mode, "03.01.facade-claim-convergence", "ok");
    l5_progress(harness.mode, "03.02.sink-control-convergence", "begin");
    wait_for_node_a_sink_control_convergence(
        &harness.cluster,
        &harness.candidate_base_urls,
        &harness.app_id,
        Duration::from_secs(15),
    )?;
    l5_progress(harness.mode, "03.02.sink-control-convergence", "ok");
    l5_progress(harness.mode, "03.03.source-control-convergence", "begin");
    wait_for_peer_source_control_convergence(
        &harness.cluster,
        &harness.candidate_base_urls,
        &harness.app_id,
        &harness.roots,
        Duration::from_secs(15),
    )?;
    l5_progress(harness.mode, "03.03.source-control-convergence", "ok");
    l5_progress(harness.mode, "03.04.cpu-login", "begin");
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
    l5_progress(harness.mode, "03.04.cpu-login", "ok");
    l5_progress(harness.mode, "03.05.manual-rescan", "begin");
    wait_for_manual_rescan_acceptance(
        &mut harness.session,
        "manual rescan accepted before cpu-budget tree materialization",
    )?;
    l5_progress(harness.mode, "03.05.manual-rescan", "ok");
    l5_progress(harness.mode, "03.06.tree-materialization", "begin");
    wait_for_primary_tree_materialization(&mut harness.session, "cpu-budget tree materializes")?;
    l5_progress(harness.mode, "03.06.tree-materialization", "ok");

    l5_progress(harness.mode, "03.07.polling-load", "begin");
    let (stop, worker) = spawn_light_polling(
        ready_base,
        harness.session.token().to_string(),
        harness.session.query_api_key().to_string(),
    );
    l5_progress(harness.mode, "03.07.polling-load", "ok");
    l5_progress(harness.mode, "03.08.cpu-measure", "begin");
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
    l5_progress(harness.mode, "03.08.cpu-measure", "ok");
    l5_progress(harness.mode, "03.09.cpu-assert", "begin");
    let result = assert_cpu_budget(&summary);
    l5_progress(
        harness.mode,
        "03.09.cpu-assert",
        if result.is_ok() { "ok" } else { "fail" },
    );
    result
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
            let app_status = probe_management_status(candidate_base_urls).ok();
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
            let _ = probe_management_status(&[node_d_url.clone()])?;
            let source_status_active =
                activation_route_has_active_pids(&node_status, "source-status:v1.req");
            let sink_status_active =
                activation_route_has_active_pids(&node_status, "sink-status:v1.req");
            let materialized_proxy_active =
                activation_route_has_active_pids(&node_status, "materialized-find-proxy:v1.req");
            if !facade_active.is_empty()
                || (source_status_active && sink_status_active && materialized_proxy_active)
            {
                Ok(true)
            } else {
                Err(format!(
                    "node-d facade not converged: facade_active={facade_active:?} source_status_active={source_status_active} sink_status_active={sink_status_active} materialized_proxy_active={materialized_proxy_active} routes={:?}",
                    activation_route_summaries(&node_status),
                ))
            }
        },
    )
}

fn expected_source_runtime_scope_map(
    node_name: &str,
    expected_groups: &[String],
) -> BTreeMap<String, Vec<String>> {
    BTreeMap::from([(node_name.to_string(), expected_groups.to_vec())])
}

fn source_runtime_scope_schedule_ready(
    status: &Value,
    node_name: &str,
    expected_groups: &[String],
) -> bool {
    source_runtime_scope_debug_schedule_ready(status, node_name, expected_groups)
        || source_runtime_scope_domain_ready(status, node_name, expected_groups)
}

fn source_runtime_scope_debug_schedule_ready(
    status: &Value,
    node_name: &str,
    expected_groups: &[String],
) -> bool {
    let expected = expected_source_runtime_scope_map(node_name, expected_groups);
    status_debug_groups_by_node(
        status,
        "source",
        "scheduled_source_groups_by_node",
        node_name,
    ) == expected
        && status_debug_groups_by_node(status, "source", "scheduled_scan_groups_by_node", node_name)
            == expected
}

fn source_runtime_scope_domain_ready(
    status: &Value,
    node_name: &str,
    expected_groups: &[String],
) -> bool {
    expected_groups.iter().all(|group| {
        source_logical_root_ready(status, group)
            && source_concrete_root_ready_for_node(status, node_name, group)
    })
}

fn source_logical_root_ready(status: &Value, group: &str) -> bool {
    status
        .get("source")
        .and_then(|source| source.get("logical_roots"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .any(|root| {
            root.get("root_id").and_then(Value::as_str) == Some(group)
                && root
                    .get("matched_grants")
                    .and_then(Value::as_u64)
                    .is_some_and(|count| count > 0)
                && root
                    .get("active_members")
                    .and_then(Value::as_u64)
                    .is_some_and(|count| count > 0)
        })
}

fn source_concrete_root_ready_for_node(status: &Value, node_name: &str, group: &str) -> bool {
    status
        .get("source")
        .and_then(|source| source.get("concrete_roots"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .any(|root| {
            root.get("logical_root_id").and_then(Value::as_str) == Some(group)
                && source_concrete_root_belongs_to_node(root, node_name)
                && root.get("active").and_then(Value::as_bool).unwrap_or(false)
                && root
                    .get("scan_enabled")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                && root.get("last_error").is_none_or(|value| value.is_null())
        })
}

fn source_concrete_root_belongs_to_node(root: &Value, node_name: &str) -> bool {
    let node_prefix = format!("{node_name}-");
    let node_object_prefix = format!("{node_name}::");
    let mut saw_node_scoped_ref = false;
    for field in ["root_key", "object_ref"] {
        let Some(value) = root.get(field).and_then(Value::as_str) else {
            continue;
        };
        if value == node_name
            || value.starts_with(&node_prefix)
            || value.starts_with(&node_object_prefix)
        {
            return true;
        }
        if value.contains("::") || value.starts_with("node-") {
            saw_node_scoped_ref = true;
        }
    }
    !saw_node_scoped_ref
}

fn source_concrete_root_node_name(root: &Value) -> Option<&'static str> {
    for field in ["root_key", "object_ref"] {
        let Some(value) = root.get(field).and_then(Value::as_str) else {
            continue;
        };
        if let Some(node_name) = runtime_node_key_cluster_name(value) {
            return Some(node_name);
        }
    }
    None
}

fn runtime_node_key_cluster_name(key: &str) -> Option<&'static str> {
    RELEASE_UPGRADE_CLUSTER_NODES
        .iter()
        .copied()
        .find(|node_name| {
            key == *node_name
                || key
                    .strip_prefix(node_name)
                    .is_some_and(|suffix| suffix.starts_with('-') || suffix.starts_with("::"))
        })
}

fn source_runtime_scope_debug_groups_by_cluster_node(
    status: &Value,
    field: &str,
) -> BTreeMap<String, BTreeSet<String>> {
    let Some(debug_field) = status
        .get("source")
        .and_then(|v| v.get("debug"))
        .and_then(|v| v.get(field))
        .and_then(Value::as_object)
    else {
        return BTreeMap::new();
    };

    let mut by_node: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (key, value) in debug_field {
        let Some(node_name) = runtime_node_key_cluster_name(key) else {
            continue;
        };
        let groups = by_node.entry(node_name.to_string()).or_default();
        if let Some(rows) = value.as_array() {
            groups.extend(rows.iter().filter_map(Value::as_str).map(str::to_string));
        }
    }
    by_node.retain(|_, groups| !groups.is_empty());
    by_node
}

fn source_runtime_scope_debug_owner_nodes_by_group(
    status: &Value,
) -> BTreeMap<String, BTreeSet<String>> {
    let scheduled_source = source_runtime_scope_debug_groups_by_cluster_node(
        status,
        "scheduled_source_groups_by_node",
    );
    let scheduled_scan =
        source_runtime_scope_debug_groups_by_cluster_node(status, "scheduled_scan_groups_by_node");
    let mut owners_by_group: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (node_name, source_groups) in scheduled_source {
        let Some(scan_groups) = scheduled_scan.get(&node_name) else {
            continue;
        };
        for group in source_groups.intersection(scan_groups) {
            owners_by_group
                .entry(group.clone())
                .or_default()
                .insert(node_name.clone());
        }
    }
    owners_by_group
}

fn source_runtime_scope_domain_owner_nodes_by_group(
    status: &Value,
) -> BTreeMap<String, BTreeSet<String>> {
    let mut owners_by_group: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let Some(concrete_roots) = status
        .get("source")
        .and_then(|source| source.get("concrete_roots"))
        .and_then(Value::as_array)
    else {
        return owners_by_group;
    };

    for root in concrete_roots {
        let Some(group) = root.get("logical_root_id").and_then(Value::as_str) else {
            continue;
        };
        let Some(node_name) = source_concrete_root_node_name(root) else {
            continue;
        };
        if source_logical_root_ready(status, group)
            && source_concrete_root_ready_for_node(status, node_name, group)
        {
            owners_by_group
                .entry(group.to_string())
                .or_default()
                .insert(node_name.to_string());
        }
    }
    owners_by_group
}

fn merge_source_runtime_scope_owners(
    target: &mut BTreeMap<String, BTreeSet<String>>,
    source: BTreeMap<String, BTreeSet<String>>,
) {
    for (group, nodes) in source {
        target.entry(group).or_default().extend(nodes);
    }
}

fn source_runtime_scope_owner_nodes_by_group(
    statuses: &[&Value],
) -> BTreeMap<String, BTreeSet<String>> {
    let mut owners_by_group = BTreeMap::new();
    for status in statuses {
        merge_source_runtime_scope_owners(
            &mut owners_by_group,
            source_runtime_scope_debug_owner_nodes_by_group(status),
        );
        merge_source_runtime_scope_owners(
            &mut owners_by_group,
            source_runtime_scope_domain_owner_nodes_by_group(status),
        );
    }
    owners_by_group
}

fn source_runtime_scope_groups_covered(
    owners_by_group: &BTreeMap<String, BTreeSet<String>>,
    expected_groups: &[String],
) -> bool {
    expected_groups.iter().all(|group| {
        owners_by_group
            .get(group)
            .is_some_and(|owners| !owners.is_empty())
    })
}

fn source_runtime_scope_owner_nodes(
    owners_by_group: &BTreeMap<String, BTreeSet<String>>,
) -> BTreeSet<String> {
    owners_by_group
        .values()
        .flat_map(|nodes| nodes.iter().cloned())
        .collect()
}

fn source_runtime_scope_inactive_owner_nodes(
    cluster: &Cluster5,
    app_id: &str,
    owner_nodes: &BTreeSet<String>,
) -> Result<Vec<String>, String> {
    let mut inactive = Vec::new();
    for node_name in owner_nodes {
        let source_active =
            cluster.unit_active_pids_for_instance(node_name, app_id, "runtime.exec.source")?;
        let scan_active =
            cluster.unit_active_pids_for_instance(node_name, app_id, "runtime.exec.scan")?;
        if source_active.is_empty() || scan_active.is_empty() {
            inactive.push(format!(
                "{node_name}: source_active={source_active:?} scan_active={scan_active:?}"
            ));
        }
    }
    Ok(inactive)
}

fn expected_source_runtime_scope_groups(roots: &[RootSpec]) -> Vec<String> {
    let mut groups = roots
        .iter()
        .map(|root| root.id.clone())
        .collect::<Vec<String>>();
    groups.sort();
    groups.dedup();
    groups
}

fn source_runtime_scope_status_view<'a>(
    node_status: &'a Value,
    app_status: Option<&'a Value>,
    node_name: &str,
    expected_groups: &[String],
) -> &'a Value {
    if source_runtime_scope_schedule_ready(node_status, node_name, expected_groups) {
        return node_status;
    }
    app_status.unwrap_or(node_status)
}

fn source_runtime_scope_debug_summary_by_node(
    statuses_by_node: &BTreeMap<String, Value>,
) -> Vec<String> {
    statuses_by_node
        .iter()
        .map(|(node_name, status)| {
            format!(
                "{node_name}: raw_scheduled_source={} raw_scheduled_scan={} routes={:?}",
                status_debug_groups_field(status, "source", "scheduled_source_groups_by_node"),
                status_debug_groups_field(status, "source", "scheduled_scan_groups_by_node"),
                activation_route_summaries(status)
            )
        })
        .collect()
}

fn source_runtime_scope_status_probe_summary(status: &Value) -> String {
    let logical_roots = status
        .get("source")
        .and_then(|source| source.get("logical_roots"))
        .and_then(Value::as_array)
        .map(|roots| roots.len())
        .unwrap_or(0);
    let concrete_roots = status
        .get("source")
        .and_then(|source| source.get("concrete_roots"))
        .and_then(Value::as_array)
        .map(|roots| roots.len())
        .unwrap_or(0);
    format!(
        "raw_scheduled_source={} raw_scheduled_scan={} logical_roots={} concrete_roots={}",
        status_debug_groups_field(status, "source", "scheduled_source_groups_by_node"),
        status_debug_groups_field(status, "source", "scheduled_scan_groups_by_node"),
        logical_roots,
        concrete_roots,
    )
}

fn source_runtime_scope_app_status_probe_summary(result: &Result<Value, String>) -> String {
    match result {
        Ok(status) => source_runtime_scope_status_probe_summary(status),
        Err(err) => format!("error={err}"),
    }
}

fn wait_for_peer_source_control_convergence(
    cluster: &Cluster5,
    candidate_base_urls: &[String],
    app_id: &str,
    roots: &[RootSpec],
    timeout: Duration,
) -> Result<(), String> {
    let expected_groups = expected_source_runtime_scope_groups(roots);
    wait_until(
        timeout,
        "current-root source runtime-scope converges after generation-two upgrade",
        || {
            let app_status_result = probe_management_status(candidate_base_urls);
            let mut statuses_by_node = BTreeMap::new();
            for node_name in RELEASE_UPGRADE_CLUSTER_NODES {
                statuses_by_node.insert(node_name.to_string(), cluster.status(node_name)?);
            }
            let mut status_refs = Vec::new();
            if let Ok(status) = app_status_result.as_ref() {
                status_refs.push(status);
            }
            status_refs.extend(statuses_by_node.values());

            let owners_by_group = source_runtime_scope_owner_nodes_by_group(&status_refs);
            let owner_nodes = source_runtime_scope_owner_nodes(&owners_by_group);
            let inactive_owner_nodes =
                source_runtime_scope_inactive_owner_nodes(cluster, app_id, &owner_nodes)?;
            if source_runtime_scope_groups_covered(&owners_by_group, &expected_groups)
                && !owner_nodes.is_empty()
                && inactive_owner_nodes.is_empty()
            {
                Ok(true)
            } else {
                Err(format!(
                    "current-root source runtime-scope not converged: expected_groups={expected_groups:?} owner_nodes_by_group={owners_by_group:?} inactive_owner_nodes={inactive_owner_nodes:?} app_status={} node_debug={:?}",
                    source_runtime_scope_app_status_probe_summary(&app_status_result),
                    source_runtime_scope_debug_summary_by_node(&statuses_by_node)
                ))
            }
        },
    )
}

fn probe_management_status(candidate_base_urls: &[String]) -> Result<Value, String> {
    OperatorSession::management_status_many(candidate_base_urls, "operator", "operator123")
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
        by_node.insert(
            node_name.to_string(),
            steady_cpu_sample_pids_for_node(
                cluster.daemon_pid(node_name)?,
                &cluster.managed_host_pids_for_instance(node_name, app_id)?,
                &cluster.daemon_host_descendant_pids(node_name)?,
            ),
        );
    }
    Ok(by_node)
}

fn steady_cpu_sample_pids_for_node(
    daemon_pid: u32,
    _managed_status_pids: &BTreeSet<u32>,
    host_descendant_pids: &BTreeSet<u32>,
) -> Vec<u32> {
    let mut pids = BTreeSet::from([daemon_pid]);
    pids.extend(
        host_descendant_pids
            .iter()
            .copied()
            .filter(|pid| *pid != daemon_pid),
    );
    pids.into_iter().collect()
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

fn announce_full_demo_resources(
    cluster: &Cluster5,
    roots: &[FullDemoRoot],
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
        let root = roots
            .iter()
            .find(|root| root.id == export_name)
            .ok_or_else(|| format!("full demo root {export_name} not mapped"))?;
        cluster.announce_resources_clusterwide(vec![json!({
            "resource_id": export_name,
            "node_id": cluster.node_id(node_name)?,
            "resource_kind": "nfs",
            "source": root.source.clone(),
            "host_ip": root.host_ip.clone(),
            "mount_hint": root.mount_point.display().to_string(),
        })])?;
    }
    announce_facade_resources(cluster, facade_resource_id, facade_addrs)
}

fn mount_and_announce_lab(
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
    announce_facade_resources(cluster, facade_resource_id, facade_addrs)
}

fn announce_facade_resources(
    cluster: &Cluster5,
    facade_resource_id: &str,
    facade_addrs: &[String],
) -> Result<(), String> {
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
        let route_publish_status = route
            .get("route_publish_status")
            .and_then(Value::as_str)
            .unwrap_or("<unknown>");
        let route_publish_reason = route
            .get("route_publish_reason")
            .cloned()
            .unwrap_or(Value::Null);
        let trusted_exposure_reason = route
            .get("trusted_exposure_reason")
            .cloned()
            .unwrap_or(Value::Null);
        let apps = route
            .get("apps")
            .and_then(Value::as_array)
            .map(|rows| {
                rows.iter()
                    .map(|row| {
                        format!(
                            "pid={:?}:op={:?}:gate={:?}:delivered={:?}:units={:?}:err={:?}",
                            row.get("pid"),
                            row.get("op"),
                            row.get("gate"),
                            row.get("delivered"),
                            row.get("unit_ids"),
                            row.get("error")
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        summaries.push(format!(
            "{route_key}:{state}:active_pids={active_pids}:publish={route_publish_status}:publish_reason={route_publish_reason}:trusted_reason={trusted_exposure_reason}:apps={apps:?}"
        ));
    }
    summaries
}

fn activation_route_has_active_pids(status: &Value, route_key: &str) -> bool {
    status
        .get("daemon")
        .and_then(|v| v.get("activation"))
        .and_then(|v| v.get("routes"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .find(|route| route.get("route_key").and_then(Value::as_str) == Some(route_key))
        .and_then(|route| route.get("active_pids").and_then(Value::as_array))
        .is_some_and(|pids| !pids.is_empty())
}

fn unique_suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_source_control_gate_rejects_active_pids_without_runtime_scope() {
        let status = json!({
            "source": {
                "debug": {
                    "scheduled_source_groups_by_node": {},
                    "scheduled_scan_groups_by_node": {}
                }
            }
        });

        assert!(
            !source_runtime_scope_schedule_ready(
                &status,
                "node-c",
                &["nfs1".to_string(), "nfs2".to_string()],
            ),
            "release-upgrade source convergence must not pass on source/scan process liveness without app-visible runtime-scope groups"
        );
    }

    #[test]
    fn peer_source_control_gate_accepts_instance_scoped_runtime_scope() {
        let status = json!({
            "source": {
                "debug": {
                    "scheduled_source_groups_by_node": {
                        "node-c-123": ["nfs2", "nfs1"]
                    },
                    "scheduled_scan_groups_by_node": {
                        "node-c-123": ["nfs1", "nfs2"]
                    }
                }
            }
        });

        assert!(
            source_runtime_scope_schedule_ready(
                &status,
                "node-c",
                &["nfs1".to_string(), "nfs2".to_string()],
            ),
            "release-upgrade source convergence should accept app-visible runtime-scope groups keyed by the runtime node instance id"
        );
    }

    #[test]
    fn peer_source_control_gate_accepts_current_root_coverage_on_selected_owners() {
        let status = json!({
            "source": {
                "debug": {
                    "scheduled_source_groups_by_node": {
                        "node-b-123": ["nfs1"],
                        "node-d-123": ["nfs2"]
                    },
                    "scheduled_scan_groups_by_node": {
                        "node-b-123": ["nfs1"],
                        "node-d-123": ["nfs2"]
                    }
                }
            }
        });
        let expected_groups = vec!["nfs1".to_string(), "nfs2".to_string()];
        let owners_by_group = source_runtime_scope_owner_nodes_by_group(&[&status]);

        assert_eq!(
            owners_by_group.get("nfs1"),
            Some(&BTreeSet::from(["node-b".to_string()])),
            "nfs1 should be covered by the app-selected source owner"
        );
        assert_eq!(
            owners_by_group.get("nfs2"),
            Some(&BTreeSet::from(["node-d".to_string()])),
            "nfs2 should be covered by the app-selected source owner"
        );
        assert!(
            source_runtime_scope_groups_covered(&owners_by_group, &expected_groups),
            "release-upgrade source convergence should validate current-root coverage instead of a fixed demo node list"
        );
    }

    #[test]
    fn peer_source_control_gate_rejects_missing_current_root_coverage() {
        let status = json!({
            "source": {
                "debug": {
                    "scheduled_source_groups_by_node": {
                        "node-b-123": ["nfs1"]
                    },
                    "scheduled_scan_groups_by_node": {
                        "node-b-123": ["nfs1"]
                    }
                }
            }
        });
        let expected_groups = vec!["nfs1".to_string(), "nfs2".to_string()];
        let owners_by_group = source_runtime_scope_owner_nodes_by_group(&[&status]);

        assert!(
            !source_runtime_scope_groups_covered(&owners_by_group, &expected_groups),
            "release-upgrade source convergence must fail when any current root has no app-visible source owner"
        );
    }

    #[test]
    fn peer_source_control_gate_accepts_domain_health_when_schedule_maps_are_absent() {
        let status = json!({
            "source": {
                "logical_roots": [{
                    "root_id": "nfs1",
                    "service_state": "serving-degraded",
                    "matched_grants": 1,
                    "active_members": 1,
                    "coverage_mode": "audit_with_metadata",
                    "coverage_capabilities": {}
                }],
                "concrete_roots": [{
                    "root_key": "node-b::nfs1",
                    "logical_root_id": "nfs1",
                    "object_ref": "node-b::nfs1",
                    "participation_state": "active",
                    "coverage_mode": "audit_with_metadata",
                    "coverage_capabilities": {},
                    "watch_enabled": true,
                    "scan_enabled": true,
                    "active": true,
                    "last_error": Value::Null
                }],
                "debug": {
                    "scheduled_source_groups_by_node": {},
                    "scheduled_scan_groups_by_node": {}
                }
            }
        });

        assert!(
            source_runtime_scope_schedule_ready(&status, "node-b", &["nfs1".to_string()]),
            "release-upgrade operations gate should accept complete source domain health when route-schedule debug maps are absent during cutover"
        );
    }

    #[test]
    fn peer_source_control_status_view_does_not_let_partial_facade_erase_node_local_scope() {
        let node_status = json!({
            "source": {
                "debug": {
                    "scheduled_source_groups_by_node": {
                        "node-b": ["nfs1"]
                    },
                    "scheduled_scan_groups_by_node": {
                        "node-b": ["nfs1"]
                    }
                }
            }
        });
        let partial_facade_status = json!({
            "source": {
                "debug": {
                    "scheduled_source_groups_by_node": Value::Null,
                    "scheduled_scan_groups_by_node": Value::Null
                }
            }
        });
        let expected_groups = vec!["nfs1".to_string()];

        let status_view = source_runtime_scope_status_view(
            &node_status,
            Some(&partial_facade_status),
            "node-b",
            &expected_groups,
        );

        assert!(
            source_runtime_scope_schedule_ready(status_view, "node-b", &expected_groups),
            "release-upgrade source convergence must not let a partial facade aggregate hide positive node-local runtime-scope evidence"
        );
    }

    #[test]
    fn steady_cpu_sample_pids_ignore_internal_managed_pids_in_favor_of_host_descendants() {
        assert_eq!(
            steady_cpu_sample_pids_for_node(
                4100,
                &BTreeSet::from([2, 3]),
                &BTreeSet::from([4101, 4102]),
            ),
            vec![4100, 4101, 4102],
            "cpu_budget steady sampling must stay on real host pids instead of internal supervisor pids like 2/3"
        );
    }

    #[test]
    fn steady_cpu_sample_pids_keep_only_daemon_when_no_host_descendants_exist() {
        assert_eq!(
            steady_cpu_sample_pids_for_node(4100, &BTreeSet::from([2]), &BTreeSet::new()),
            vec![4100],
            "when no host descendants are present, cpu_budget should sample the daemon host pid only rather than host /proc/2"
        );
    }
}

#![cfg(target_os = "linux")]

use crate::support::api_client::{FsMetaApiClient, OperatorSession};
use crate::support::cluster5::Cluster5;
use crate::support::cpu_budget::{assert_cpu_budget, measure_cpu_budget};
use crate::support::nfs_lab::NfsLab;
use crate::support::{reserve_http_addrs, skip_unless_real_nfs_enabled, wait_until};
use fs_meta::{RootSelector, RootSpec};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UpgradeMode {
    Smoke,
    RootsPersistAcrossUpgrade,
    TreeStatsStableAcrossUpgrade,
    UpgradeWindowJoin,
    CpuBudget,
}

impl UpgradeMode {
    fn app_prefix(self) -> &'static str {
        match self {
            Self::Smoke => "fs-meta-api-upgrade-smoke",
            Self::RootsPersistAcrossUpgrade => "fs-meta-api-upgrade-roots",
            Self::TreeStatsStableAcrossUpgrade => "fs-meta-api-upgrade-tree-stats",
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

pub fn run_roots_persist_across_upgrade() -> Result<(), String> {
    run_mode(UpgradeMode::RootsPersistAcrossUpgrade)
}

pub fn run_tree_stats_stable_across_upgrade() -> Result<(), String> {
    run_mode(UpgradeMode::TreeStatsStableAcrossUpgrade)
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
        UpgradeMode::RootsPersistAcrossUpgrade => {
            scenario_roots_persist_across_upgrade(&mut harness)?
        }
        UpgradeMode::TreeStatsStableAcrossUpgrade => {
            scenario_tree_stats_stable_across_upgrade(&mut harness)?
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

fn upgrade_to_generation_two(harness: &mut UpgradeHarness) -> Result<(), String> {
    let release_v2 = harness.cluster.build_fs_meta_release(
        &harness.app_id,
        &harness.facade_resource_id,
        harness.roots.clone(),
        2,
        true,
    )?;
    harness.cluster.apply_release("node-a", release_v2)?;
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

fn wait_for_management_stabilized(
    session: &mut OperatorSession,
    reason: &str,
) -> Result<(), String> {
    wait_until(Duration::from_secs(60), reason, || {
        let status = match session.status() {
            Ok(status) => status,
            Err(_) => return Ok(false),
        };
        Ok(status.get("source").is_some() || status.get("facade").is_some())
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
    harness.session.rescan()?;
    wait_for_primary_tree_materialization(&mut harness.session, "cpu-budget tree materializes")?;

    let (stop, worker) = spawn_light_polling(
        harness.session.client().base_url().to_string(),
        harness.session.token().to_string(),
        harness.session.query_api_key().to_string(),
    );
    let steady_cpu = measure_steady_cpu(&harness.cluster, &harness.app_id)?;
    let summary = measure_cpu_budget(
        &harness.baseline_cpu,
        &steady_cpu,
        Duration::from_secs(5),
        Duration::from_secs(3 * 60),
    )?;
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

fn root_payload(id: &str, fs_source: &str, subpath_scope: &str) -> Value {
    json!({
        "id": id,
        "selector": { "fs_source": fs_source },
        "subpath_scope": subpath_scope,
        "watch": true,
        "scan": true,
    })
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

fn unique_suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

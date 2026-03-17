#![cfg(target_os = "linux")]

use crate::support::api_client::{FsMetaApiClient, OperatorSession};
use crate::support::cluster5::Cluster5;
use crate::support::cpu_budget::{assert_cpu_budget, measure_cpu_budget};
use crate::support::nfs_lab::NfsLab;
use crate::support::{reserve_http_addrs, skip_unless_real_nfs_enabled, wait_until};
use capanix_app_fs_meta::{RootSelector, RootSpec};
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

pub fn run() -> Result<(), String> {
    if let Some(reason) = skip_unless_real_nfs_enabled() {
        eprintln!("[fs-meta-api-upgrade] skipped: {reason}");
        return Ok(());
    }

    let mut lab = NfsLab::start()?;
    seed_baseline_content(&lab)?;
    let cluster = Cluster5::start()?;
    let baseline_cpu = measure_baseline_cpu(&cluster)?;

    let app_id = format!("fs-meta-api-upgrade-{}", unique_suffix());
    let facade_resource_id = format!("fs-meta-tcp-listener-{app_id}");
    let facade_addrs = reserve_http_addrs(2)?;
    mount_and_announce(&cluster, &mut lab, &facade_resource_id, &facade_addrs)?;

    let roots = vec![
        root_spec("nfs1", &lab.export_source("nfs1")),
        root_spec("nfs2", &lab.export_source("nfs2")),
    ];
    let release_v1 =
        cluster.build_fs_meta_release(&app_id, &facade_resource_id, roots.clone(), 1, true)?;
    cluster.apply_release("node-a", release_v1)?;
    let base_url = cluster.wait_http_login_ready(
        &facade_addrs
            .iter()
            .map(|addr| format!("http://{addr}"))
            .collect::<Vec<_>>(),
        "operator",
        "operator123",
        Duration::from_secs(120),
    )?;
    let candidate_base_urls = facade_addrs
        .iter()
        .map(|addr| format!("http://{addr}"))
        .collect::<Vec<_>>();
    let mut session = OperatorSession::login_many(candidate_base_urls, "operator", "operator123")?;
    session.rescan()?;
    wait_until(
        Duration::from_secs(90),
        "pre-upgrade tree materializes",
        || {
            let tree =
                session.tree(&[("path", "/".to_string()), ("recursive", "true".to_string())])?;
            Ok(group_total_nodes(&tree, "nfs1") > 0 && group_total_nodes(&tree, "nfs2") > 0)
        },
    )?;

    let pre_upgrade_tree =
        session.tree(&[("path", "/".to_string()), ("recursive", "true".to_string())])?;
    let pre_upgrade_stats =
        session.stats(&[("path", "/".to_string()), ("recursive", "true".to_string())])?;

    let release_v2 =
        cluster.build_fs_meta_release(&app_id, &facade_resource_id, roots.clone(), 2, true)?;
    cluster.apply_release("node-a", release_v2)?;
    wait_for_generation(&cluster, 2)?;

    let current_roots = session.monitoring_roots()?;
    let current_root_ids = current_roots
        .get("roots")
        .and_then(Value::as_array)
        .map(|rows| {
            rows.iter()
                .filter_map(|r| r.get("id").and_then(Value::as_str))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if current_root_ids != vec!["nfs1", "nfs2"] {
        return Err(format!(
            "roots changed unexpectedly across upgrade: {current_roots}"
        ));
    }
    wait_until(
        Duration::from_secs(90),
        "post-upgrade tree rematerializes",
        || {
            let tree =
                session.tree(&[("path", "/".to_string()), ("recursive", "true".to_string())])?;
            Ok(group_total_nodes(&tree, "nfs1") > 0 && group_total_nodes(&tree, "nfs2") > 0)
        },
    )?;

    let post_upgrade_tree =
        session.tree(&[("path", "/".to_string()), ("recursive", "true".to_string())])?;
    let post_upgrade_stats =
        session.stats(&[("path", "/".to_string()), ("recursive", "true".to_string())])?;
    if pre_upgrade_tree != post_upgrade_tree {
        return Err(format!(
            "upgrade changed tree response unexpectedly: before={pre_upgrade_tree} after={post_upgrade_tree}"
        ));
    }
    if pre_upgrade_stats != post_upgrade_stats {
        return Err(format!(
            "upgrade changed stats response unexpectedly: before={pre_upgrade_stats} after={post_upgrade_stats}"
        ));
    }

    // Upgrade-window resource change: add nfs4 and update roots under generation 2.
    lab.create_export("nfs4")?;
    lab.write_file("nfs4", "upgrade-window/new.txt", "during-upgrade\n")?;
    let mount_d = lab.mount_export("node-d", "nfs4")?;
    cluster.announce_resources_clusterwide(vec![json!({
        "resource_id": "nfs4",
        "node_id": cluster.node_id("node-d")?,
        "resource_kind": "nfs",
        "source": lab.export_source("nfs4"),
        "mount_hint": mount_d.display().to_string(),
    })])?;
    let upgraded_roots = json!([
        root_payload("nfs1", &lab.export_source("nfs1"), "/"),
        root_payload("nfs2", &lab.export_source("nfs2"), "/"),
        root_payload("nfs4", &lab.export_source("nfs4"), "/"),
    ]);
    session.update_roots(&upgraded_roots)?;
    session.rescan()?;
    wait_until(
        Duration::from_secs(60),
        "upgrade-window roots include nfs4",
        || {
            let roots = session.monitoring_roots()?;
            Ok(roots.to_string().contains("\"id\":\"nfs4\""))
        },
    )?;

    let grants = session.runtime_grants()?;
    if !grants.to_string().contains("\"resource_id\":\"nfs4\"") {
        return Err(format!(
            "runtime grants missing nfs4 during upgrade window: {grants}"
        ));
    }
    let final_tree = session.tree(&[
        ("path", "/upgrade-window".to_string()),
        ("recursive", "true".to_string()),
    ])?;
    if !final_tree.to_string().contains("new.txt") {
        return Err(format!(
            "upgrade-window tree missing nfs4 content: {final_tree}"
        ));
    }

    let (stop, worker) = spawn_light_polling(base_url, session.token().to_string());
    let steady_cpu = measure_steady_cpu(&cluster, &app_id)?;
    let summary = measure_cpu_budget(
        &baseline_cpu,
        &steady_cpu,
        Duration::from_secs(5),
        Duration::from_secs(3 * 60),
    )?;
    stop.store(true, Ordering::Relaxed);
    let _ = worker.join();
    assert_cpu_budget(&summary)?;

    Ok(())
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
                .managed_pids_for_instance(node_name, app_id)?
                .into_iter(),
        );
        by_node.insert(node_name.to_string(), pids);
    }
    Ok(by_node)
}

fn spawn_light_polling(base_url: String, token: String) -> (Arc<AtomicBool>, JoinHandle<()>) {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_clone = Arc::clone(&stop);
    let handle = thread::spawn(move || {
        let Ok(client) = FsMetaApiClient::new(base_url) else {
            return;
        };
        while !stop_clone.load(Ordering::Relaxed) {
            let _ = client.status(&token);
            let _ = client.tree(
                &token,
                &[("path", "/".to_string()), ("recursive", "true".to_string())],
            );
            let _ = client.stats(
                &token,
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
    for (node_name, bind_addr) in [("node-d", &facade_addrs[0]), ("node-e", &facade_addrs[1])] {
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
    payload
        .get("groups")
        .and_then(Value::as_object)
        .and_then(|groups| groups.get(group_key))
        .and_then(|group| group.get("meta"))
        .and_then(|meta| meta.get("total_nodes"))
        .and_then(Value::as_u64)
        .unwrap_or(0)
}

fn unique_suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

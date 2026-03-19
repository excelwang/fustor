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
    let _session =
        OperatorSession::login_many(candidate_base_urls, "operator", "operator123")?;

    let release_v2 =
        cluster.build_fs_meta_release(&app_id, &facade_resource_id, roots.clone(), 2, true)?;
    cluster.apply_release("node-a", release_v2)?;
    wait_for_generation(&cluster, 2)?;

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

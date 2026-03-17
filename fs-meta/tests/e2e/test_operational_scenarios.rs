#![cfg(target_os = "linux")]

use crate::support::api_client::{ApiResponse, FsMetaApiClient, OperatorSession};
use crate::support::cluster5::Cluster5;
use crate::support::nfs_lab::NfsLab;
use crate::support::{reserve_http_addrs, skip_unless_real_nfs_enabled, wait_until};
use capanix_app_fs_meta::{RootSelector, RootSpec};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn run() -> Result<(), String> {
    if let Some(reason) = skip_unless_real_nfs_enabled() {
        eprintln!("[fs-meta-api-ops] skipped: {reason}");
        return Ok(());
    }

    let mut lab = NfsLab::start()?;
    seed_baseline_content(&lab)?;
    let cluster = Cluster5::start()?;
    let app_id = format!("fs-meta-api-ops-{}", unique_suffix());
    let facade_resource_id = format!("fs-meta-tcp-listener-{app_id}");
    let facade_addrs = reserve_http_addrs(2)?;

    install_baseline_resources(&cluster, &mut lab, &facade_resource_id, &facade_addrs)?;
    let roots = baseline_roots(&lab);
    let release =
        cluster.build_fs_meta_release(&app_id, &facade_resource_id, roots.clone(), 1, true)?;
    cluster.apply_release("node-a", release)?;

    let _base_url = cluster.wait_http_login_ready(
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
    let mut session =
        OperatorSession::login_many(candidate_base_urls.clone(), "operator", "operator123")?;
    session.rescan()?;

    scenario_force_find_execution_semantics(&cluster, &mut lab, &mut session)?;
    scenario_new_nfs_join(&cluster, &mut lab, &mut session)?;
    scenario_root_path_modify(&cluster, &lab, &mut session)?;
    scenario_visibility_change_and_sink_selection(
        &cluster,
        &mut lab,
        &mut session,
        &app_id,
        &facade_resource_id,
    )?;
    scenario_sink_failover(&cluster, &mut session, &app_id)?;
    scenario_facade_failover_and_resource_switch(
        &cluster,
        &mut session,
        &facade_resource_id,
        &facade_addrs,
        &app_id,
    )?;
    scenario_nfs_retire(&cluster, &mut lab, &mut session)?;

    Ok(())
}

fn scenario_force_find_execution_semantics(
    cluster: &Cluster5,
    lab: &mut NfsLab,
    session: &mut OperatorSession,
) -> Result<(), String> {
    seed_force_find_stress_content(lab, "nfs1", "force-find-stress", 40, 100)?;
    seed_force_find_stress_content(lab, "nfs2", "force-find-stress", 40, 100)?;
    session.rescan()?;
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

    let _ = session.force_find(&[
        ("path", "/force-find-stress".to_string()),
        ("recursive", "true".to_string()),
    ])?;
    let runner_first = wait_last_force_find_runner(session, "nfs1", None)?;
    let _ = session.force_find(&[
        ("path", "/force-find-stress".to_string()),
        ("recursive", "true".to_string()),
    ])?;
    let runner_second = wait_last_force_find_runner(session, "nfs1", Some(runner_first.as_str()))?;
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

    let thread_client = session.client().clone();
    let thread_query_api_key = session.query_api_key().to_string();
    let inflight_join = thread::spawn(move || {
        thread_client.force_find_raw(
            &thread_query_api_key,
            &[
                ("path", "/force-find-stress".to_string()),
                ("recursive", "true".to_string()),
            ],
        )
    });
    wait_until(
        Duration::from_secs(30),
        "nfs1 force-find in-flight lock becomes visible",
        || Ok(force_find_inflight_groups(session)?.contains("nfs1")),
    )?;

    let same_group = session.client().force_find_raw(
        session.query_api_key(),
        &[
            ("path", "/force-find-stress".to_string()),
            ("recursive", "true".to_string()),
        ],
    )?;
    assert_api_error(&same_group, 503, "NOT_READY")?;

    let inflight_resp = inflight_join
        .join()
        .map_err(|_| "join same-group in-flight force-find thread failed".to_string())??;
    assert_status(
        inflight_resp.status,
        200,
        "primary nfs1 in-flight force-find",
    )?;

    let grants = session.runtime_grants()?;
    let failing_runner = runner_first.clone();
    let failing_node = node_name_for_object_ref(&grants, &failing_runner)?;
    let _ = lab.unmount_export(&failing_node, "nfs1");
    let fallback_resp = session.force_find(&[
        ("path", "/force-find-stress".to_string()),
        ("recursive", "true".to_string()),
    ])?;
    if group_total_nodes(&fallback_resp, "nfs1") == 0 {
        return Err(format!(
            "fallback force-find returned empty payload after unmounting runner {failing_runner}: {fallback_resp}"
        ));
    }
    let fallback_runner = wait_last_force_find_runner(session, "nfs1", None)?;
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
    lab.create_export("nfs4")?;
    lab.write_file("nfs4", "joined/a.txt", "a\n")?;
    lab.write_file("nfs4", "joined/deeper/b.txt", "b\n")?;
    let mount_a = lab.mount_export("node-a", "nfs4")?;
    let mount_d = lab.mount_export("node-d", "nfs4")?;
    announce_nfs(cluster, lab, "node-a", "nfs4", &mount_a)?;
    announce_nfs(cluster, lab, "node-d", "nfs4", &mount_d)?;

    wait_until(
        Duration::from_secs(30),
        "runtime grants include nfs4",
        || {
            let grants = session.runtime_grants()?;
            Ok(first_mount_for_fs_source(&grants, &lab.export_source("nfs4")).is_ok())
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
    wait_until(Duration::from_secs(90), "nfs4 tree materializes", || {
        let tree = session.tree(&[
            ("path", "/joined".to_string()),
            ("recursive", "true".to_string()),
        ])?;
        Ok(group_total_nodes(&tree, "nfs4") > 0)
    })?;

    let tree = session.tree(&[
        ("path", "/joined".to_string()),
        ("recursive", "true".to_string()),
    ])?;
    if group_total_nodes(&tree, "nfs4") == 0 || !tree.to_string().contains("joined.txt") {
        return Err(format!("nfs4 join tree missing joined payload: {tree}"));
    }
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
    wait_until(
        Duration::from_secs(90),
        "nfs1 hot tree materializes",
        || {
            let tree =
                session.tree(&[("path", "/".to_string()), ("recursive", "true".to_string())])?;
            Ok(group_total_nodes(&tree, "nfs1") > 0 && tree.to_string().contains("only-hot.txt"))
        },
    )?;

    let tree = session.tree(&[("path", "/".to_string()), ("recursive", "true".to_string())])?;
    if group_total_nodes(&tree, "nfs1") == 0 || !tree.to_string().contains("only-hot.txt") {
        return Err(format!("nfs1 hot-only tree missing expected path: {tree}"));
    }
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

    wait_until(
        Duration::from_secs(60),
        "source active on nfs2 visible members",
        || {
            let a =
                cluster.unit_active_pids_for_instance("node-a", app_id, "runtime.exec.source")?;
            let c =
                cluster.unit_active_pids_for_instance("node-c", app_id, "runtime.exec.source")?;
            let d =
                cluster.unit_active_pids_for_instance("node-d", app_id, "runtime.exec.source")?;
            Ok(!a.is_empty() && !c.is_empty() && !d.is_empty())
        },
    )?;

    let before_holder = current_sink_holder(cluster, app_id)?;
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
            let holder = current_sink_holder(cluster, app_id)?;
            Ok(holder.is_some() && holder != Some("node-d".to_string()))
        },
    )?;
    let after_holder = current_sink_holder(cluster, app_id)?
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
    let holder = current_sink_holder(cluster, app_id)?
        .ok_or_else(|| "no current sink holder to fail over".to_string())?;
    let pids = cluster.unit_active_pids_for_instance(&holder, app_id, "runtime.exec.sink")?;
    let pid = pids
        .iter()
        .next()
        .copied()
        .ok_or_else(|| format!("no sink pid found on holder {holder}"))?;
    cluster.kill_pid(pid)?;

    wait_until(
        Duration::from_secs(90),
        "status remains available after sink failover",
        || {
            let status = session.status()?;
            Ok(status.get("sink").is_some())
        },
    )?;

    wait_until(Duration::from_secs(90), "new sink holder elected", || {
        let next = current_sink_holder(cluster, app_id)?;
        Ok(next.is_some() && next != Some(holder.clone()))
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
    let holders = current_facade_holders(cluster, app_id)?;
    let (holder_node, holder_pid) = holders
        .into_iter()
        .next()
        .ok_or_else(|| "no active facade pid found".to_string())?;
    cluster.kill_pid(holder_pid)?;
    wait_until(
        Duration::from_secs(90),
        "facade recovers after holder kill",
        || {
            let status = session.status()?;
            Ok(status.get("facade").is_some() || status.get("source").is_some())
        },
    )?;

    let after_kill_holders = current_facade_holders(cluster, app_id)?;
    if after_kill_holders.is_empty() {
        return Err("facade did not recover after holder kill".to_string());
    }

    let replacement_node = if holder_node == "node-d" {
        "node-e"
    } else {
        "node-d"
    };
    let replacement_addr = if replacement_node == "node-d" {
        &facade_addrs[0]
    } else {
        &facade_addrs[1]
    };
    cluster.withdraw_resources_clusterwide(
        &cluster.node_id(&holder_node)?,
        vec![facade_resource_id.to_string()],
    )?;
    wait_until(
        Duration::from_secs(90),
        "facade resource switch reaches replacement node",
        || {
            let other = FsMetaApiClient::new(format!("http://{replacement_addr}"))?;
            Ok(other.login("operator", "operator123").is_ok())
        },
    )?;

    let mut replacement_session = OperatorSession::login_many(
        facade_addrs
            .iter()
            .map(|addr| format!("http://{addr}"))
            .collect::<Vec<_>>(),
        "operator",
        "operator123",
    )?;
    let status = replacement_session.status()?;
    if status.get("source").is_none() {
        return Err(format!(
            "replacement facade status missing source after switch: {status}"
        ));
    }
    let tree = replacement_session
        .tree(&[("path", "/".to_string()), ("recursive", "true".to_string())])?;
    if group_total_nodes(&tree, "nfs1") == 0
        && group_total_nodes(&tree, "nfs2") == 0
        && group_total_nodes(&tree, "nfs4") == 0
    {
        return Err(format!(
            "replacement facade tree returned empty groups after switch: {tree}"
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
    for (node_name, bind_addr) in [("node-d", &facade_addrs[0]), ("node-e", &facade_addrs[1])] {
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

fn current_sink_holder(cluster: &Cluster5, app_id: &str) -> Result<Option<String>, String> {
    let mut holders = Vec::new();
    for node_name in ["node-a", "node-b", "node-c", "node-d", "node-e"] {
        let pids = cluster.unit_active_pids_for_instance(node_name, app_id, "runtime.exec.sink")?;
        if !pids.is_empty() {
            holders.push(node_name.to_string());
        }
    }
    if holders.len() > 1 {
        return Err(format!("multiple sink holders detected: {holders:?}"));
    }
    Ok(holders.into_iter().next())
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

fn source_primary_for_group(
    session: &mut OperatorSession,
    group_id: &str,
) -> Result<Option<String>, String> {
    let status = session.status()?;
    Ok(status
        .get("source")
        .and_then(|source| source.get("debug"))
        .and_then(|debug| debug.get("source_primary_by_group"))
        .and_then(Value::as_object)
        .and_then(|groups| groups.get(group_id))
        .and_then(Value::as_str)
        .map(str::to_string))
}

fn force_find_inflight_groups(session: &mut OperatorSession) -> Result<BTreeSet<String>, String> {
    let status = session.status()?;
    Ok(status
        .get("source")
        .and_then(|source| source.get("debug"))
        .and_then(|debug| debug.get("force_find_inflight_groups"))
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default())
}

fn wait_last_force_find_runner(
    session: &mut OperatorSession,
    group_id: &str,
    previous: Option<&str>,
) -> Result<String, String> {
    let mut last_seen = None::<String>;
    wait_until(
        Duration::from_secs(30),
        &format!("last force-find runner for {group_id}"),
        || {
            let status = session.status()?;
            let runner = status
                .get("source")
                .and_then(|source| source.get("debug"))
                .and_then(|debug| debug.get("last_force_find_runner_by_group"))
                .and_then(Value::as_object)
                .and_then(|groups| groups.get(group_id))
                .and_then(Value::as_str)
                .map(str::to_string);
            last_seen = runner.clone();
            Ok(runner
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

fn assert_status(actual: u16, expected: u16, context: &str) -> Result<(), String> {
    if actual != expected {
        return Err(format!("{context} expected http {expected}, got {actual}"));
    }
    Ok(())
}

fn assert_api_error(
    response: &ApiResponse,
    expected_status: u16,
    expected_code: &str,
) -> Result<(), String> {
    assert_status(response.status, expected_status, "api error response")?;
    let code = response
        .body
        .get("code")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if code != expected_code {
        return Err(format!(
            "expected error code {expected_code}, got {code}; body={}",
            response.body
        ));
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

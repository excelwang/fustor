use super::*;

fn write_fs_meta_auth_files(tag: &str) -> Result<(String, String), String> {
    let dir = std::env::temp_dir().join(format!("dx-fsmeta-auth-{tag}-{}", unique_suffix()));
    fs::create_dir_all(&dir).map_err(|e| format!("create fs-meta auth temp dir failed: {e}"))?;
    let passwd_path = dir.join("fs-meta.passwd");
    let shadow_path = dir.join("fs-meta.shadow");
    fs::write(
        &passwd_path,
        "operator:1000:1000:fsmeta_management:/home/operator:/bin/bash:0\n",
    )
    .map_err(|e| format!("write fs-meta passwd fixture failed: {e}"))?;
    fs::write(&shadow_path, "operator:plain$operator123:0\n")
        .map_err(|e| format!("write fs-meta shadow fixture failed: {e}"))?;
    Ok((
        passwd_path.to_string_lossy().to_string(),
        shadow_path.to_string_lossy().to_string(),
    ))
}

fn fs_meta_api_status(bind_addr: &str, username: &str, password: &str) -> Result<Value, String> {
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_millis(300))
        .build()
        .map_err(|e| format!("build reqwest client failed: {e}"))?;
    let base = format!("http://{bind_addr}/api/fs-meta/v1");
    let login = client
        .post(format!("{base}/session/login"))
        .json(&json!({ "username": username, "password": password }))
        .send()
        .map_err(|e| format!("login request failed: {e}"))?;
    if !login.status().is_success() {
        return Err(format!("login returned HTTP {}", login.status()));
    }
    let login_body: Value = login
        .json()
        .map_err(|e| format!("decode login response failed: {e}"))?;
    let token = login_body
        .get("token")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("login response missing token: {login_body}"))?;
    let status = client
        .get(format!("{base}/status"))
        .bearer_auth(token)
        .send()
        .map_err(|e| format!("status request failed: {e}"))?;
    if !status.status().is_success() {
        return Err(format!("status returned HTTP {}", status.status()));
    }
    status
        .json()
        .map_err(|e| format!("decode status response failed: {e}"))
}

fn install_facade_resources(
    baseline: &mut Value,
    resource_id: &str,
    addr_a: &str,
    addr_b: &str,
    node_a: &str,
    node_b: &str,
) {
    baseline["announced_resources"] = json!([
        {
            "resource_id": resource_id,
            "node_id": node_a,
            "resource_kind": "tcp_listener",
            "source": format!("http://{node_a}/facade-resource/{resource_id}"),
            "bind_addr": addr_a,
        },
        {
            "resource_id": resource_id,
            "node_id": node_b,
            "resource_kind": "tcp_listener",
            "source": format!("http://{node_b}/facade-resource/{resource_id}"),
            "bind_addr": addr_b,
        }
    ]);
}

fn append_mount_root_resources(
    baseline: &mut Value,
    resource_id: &str,
    mount_point: &str,
    node_a: &str,
    node_b: &str,
) {
    let Some(resources) = baseline
        .get_mut("announced_resources")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    resources.push(json!({
        "resource_id": resource_id,
        "node_id": node_a,
        "resource_kind": "fs",
        "source": mount_point,
        "mount_hint": mount_point,
    }));
    resources.push(json!({
        "resource_id": resource_id,
        "node_id": node_b,
        "resource_kind": "fs",
        "source": mount_point,
        "mount_hint": mount_point,
    }));
}

pub(crate) fn scenario_runtime_orchestrated_worker_hosting_config_apply_e2e() -> Result<(), String>
{
    with_cluster(
        FULL_NODE_DELEGATION_SCOPES,
        FULL_NODE_DELEGATION_SCOPES,
        |c| {
            let Some(fs_meta_app) = try_find_fs_meta_app_cdylib() else {
                return Err(
                    "fs-meta app runtime path not found; set CAPANIX_FS_META_APP_BINARY or build fs-meta-runtime"
                        .to_string(),
                );
            };

            let instance_id = format!("fs-meta-runtime-e2e-{}", unique_suffix());
            let (passwd_path, shadow_path) = write_fs_meta_auth_files("runtime-e2e")?;
            let (api_bind_addr_a, api_bind_addr_b) = reserve_distinct_bind_addrs()?;
            let facade_resource_scope_id = format!("fs-meta-facade-resource-{instance_id}");
            let roots_dir = tempfile::tempdir()
                .map_err(|e| format!("create fs-meta runtime-e2e roots temp dir failed: {e}"))?;
            let nfs1_path = roots_dir.path().join("nfs1");
            let nfs2_path = roots_dir.path().join("nfs2");
            fs::create_dir_all(&nfs1_path)
                .map_err(|e| format!("create fs-meta runtime-e2e nfs1 dir failed: {e}"))?;
            fs::create_dir_all(&nfs2_path)
                .map_err(|e| format!("create fs-meta runtime-e2e nfs2 dir failed: {e}"))?;
            fs::write(nfs1_path.join("ready-a.txt"), "ready-a\n")
                .map_err(|e| format!("seed fs-meta runtime-e2e nfs1 dir failed: {e}"))?;
            fs::write(nfs2_path.join("ready-b.txt"), "ready-b\n")
                .map_err(|e| format!("seed fs-meta runtime-e2e nfs2 dir failed: {e}"))?;
            let nfs1_path_str = nfs1_path.to_string_lossy().to_string();
            let nfs2_path_str = nfs2_path.to_string_lossy().to_string();

            let mut baseline = c
                .ctl_ok_a_local(json!({ "command": "config_get" }))
                .map_err(|e| {
                    format!("runtime-e2e config_get(before baseline reset) failed: {e}")
                })?;
            if let Some(route_plans) = baseline
                .get_mut("route_plans")
                .and_then(Value::as_array_mut)
            {
                route_plans.clear();
            }
            if let Some(apps) = baseline.get_mut("apps").and_then(Value::as_array_mut) {
                apps.clear();
            }
            install_facade_resources(
                &mut baseline,
                &facade_resource_scope_id,
                &api_bind_addr_a,
                &api_bind_addr_b,
                &c.node_a_identity.node_id,
                &c.node_b_identity.node_id,
            );
            append_mount_root_resources(
                &mut baseline,
                "nfs1",
                &nfs1_path_str,
                &c.node_a_identity.node_id,
                &c.node_b_identity.node_id,
            );
            append_mount_root_resources(
                &mut baseline,
                "nfs2",
                &nfs2_path_str,
                &c.node_a_identity.node_id,
                &c.node_b_identity.node_id,
            );
            let baseline_apply = match c.ctl_ok_a_local(json!({
                "command": "config_set",
                "config": baseline,
            })) {
                Ok(v) => v,
                Err(e) => {
                    let status_a = c
                        .layered_status_a_local()
                        .unwrap_or_else(|_| serde_json::json!(null));
                    let status_b = c
                        .layered_status_b_local()
                        .unwrap_or_else(|_| serde_json::json!(null));
                    let audit_a = c
                        .ctl_ok_a_local(json!({ "command": "audit_tail", "count": 32 }))
                        .unwrap_or_else(|_| serde_json::json!(null));
                    return Err(format!(
                        "runtime-e2e baseline config_set failed: {e}; status_a={status_a}; status_b={status_b}; audit_a={audit_a}"
                    ));
                }
            };
            if baseline_apply.get("ok").and_then(Value::as_bool) != Some(true) {
                return Err(format!(
                    "baseline config_set did not return ok=true: {baseline_apply}"
                ));
            }
            let mut baseline_b = c
                .ctl_ok_b_local(json!({ "command": "config_get" }))
                .map_err(|e| {
                    format!("runtime-e2e config_get(node-b before baseline reset) failed: {e}")
                })?;
            if let Some(route_plans) = baseline_b
                .get_mut("route_plans")
                .and_then(Value::as_array_mut)
            {
                route_plans.clear();
            }
            if let Some(apps) = baseline_b.get_mut("apps").and_then(Value::as_array_mut) {
                apps.clear();
            }
            install_facade_resources(
                &mut baseline_b,
                &facade_resource_scope_id,
                &api_bind_addr_a,
                &api_bind_addr_b,
                &c.node_a_identity.node_id,
                &c.node_b_identity.node_id,
            );
            append_mount_root_resources(
                &mut baseline_b,
                "nfs1",
                &nfs1_path_str,
                &c.node_a_identity.node_id,
                &c.node_b_identity.node_id,
            );
            append_mount_root_resources(
                &mut baseline_b,
                "nfs2",
                &nfs2_path_str,
                &c.node_a_identity.node_id,
                &c.node_b_identity.node_id,
            );
            let baseline_apply_b = c
                .ctl_ok_b_local(json!({
                    "command": "config_set",
                    "config": baseline_b,
                }))
                .map_err(|e| format!("runtime-e2e baseline config_set(node-b) failed: {e}"))?;
            if baseline_apply_b.get("ok").and_then(Value::as_bool) != Some(true) {
                return Err(format!(
                    "baseline config_set(node-b) did not return ok=true: {baseline_apply_b}"
                ));
            }

            let app_decl = json!({
                "schema_version": "release-v2",
                "apps": [
                    {
                        "id": instance_id,
                        "app": fs_meta_app.to_string_lossy().to_string(),
                        "manifest": "fs-meta/fixtures/manifests/fs-meta.yaml",
                        "version": "0.0.2",
                        "restart_policy": "Never",
                        "config": {
                            "workers": {
                                "facade": { "mode": "embedded" },
                                "source": { "mode": "embedded" },
                                "sink": { "mode": "embedded" }
                            },
                            "roots": [
                                {
                                    "id": "nfs1",
                                    "selector": { "mount_point": nfs1_path_str.clone() },
                                    "subpath_scope": "/",
                                    "watch": true,
                                    "scan": true
                                },
                                {
                                    "id": "nfs2",
                                    "selector": { "mount_point": nfs2_path_str.clone() },
                                    "subpath_scope": "/",
                                    "watch": true,
                                    "scan": true
                                }
                            ],
                            "api": {
                                "facade_resource_id": facade_resource_scope_id.clone(),
                                "auth": {
                                    "passwd_path": passwd_path,
                                    "shadow_path": shadow_path,
                                    "management_group": "fsmeta_management"
                                }
                            }
                        },
                        "runtime": {
                            "control_subscriptions": ["runtime.host_object_grants.changed"],
                            "app_scopes": [
                                {
                                    "scope_id": "nfs1",
                                    "resource_ids": ["nfs1"],
                                    "unit_scopes": [
                                        { "unit_id": "runtime.exec.source", "eligibility": "scope_members", "cardinality": "all" },
                                        { "unit_id": "runtime.exec.scan", "eligibility": "scope_members", "cardinality": "all" },
                                        { "unit_id": "runtime.exec.sink", "eligibility": "resource_visible_nodes", "cardinality": "one" }
                                    ]
                                },
                                {
                                    "scope_id": "nfs2",
                                    "resource_ids": ["nfs2"],
                                    "unit_scopes": [
                                        { "unit_id": "runtime.exec.source", "eligibility": "scope_members", "cardinality": "all" },
                                        { "unit_id": "runtime.exec.scan", "eligibility": "scope_members", "cardinality": "all" },
                                        { "unit_id": "runtime.exec.sink", "eligibility": "resource_visible_nodes", "cardinality": "one" }
                                    ]
                                },
                                {
                                    "scope_id": facade_resource_scope_id.clone(),
                                    "resource_ids": [facade_resource_scope_id.clone()],
                                    "unit_scopes": [
                                        { "unit_id": "runtime.exec.facade", "eligibility": "resource_visible_nodes", "cardinality": "one" }
                                    ]
                                }
                            ],
                            "units": {
                                "runtime.exec.facade": { "enabled": true }
                            },
                            "state_carrier": {
                                "enabled": true,
                                "units": {
                                    "runtime.exec.scan": { "enabled": false }
                                }
                            }
                        },
                        "policy": {
                            "generation": 1,}
                    }
                ]
            });

            let intent = release_v2_doc_to_scope_unit_intent_value(&app_decl)?;
            let apply_add = c
                .ctl_ok_a_local(json!({
                    "command": "relation_target_apply",
                    "declaration": intent,
                }))
                .map_err(|e| format!("runtime-e2e relation_target_apply failed: {e}"))?;
            if apply_add.get("ok").and_then(Value::as_bool) != Some(true) {
                return Err(format!(
                    "relation_target_apply did not return ok=true: {apply_add}"
                ));
            }

            let mut seen_instance = false;
            let mut owner_on_a = false;
            let mut last_status_a = serde_json::json!(null);
            let mut last_status_b = serde_json::json!(null);
            let mut last_api_a = String::new();
            let mut last_api_b = String::new();
            for _ in 0..160 {
                let status_a = c
                    .layered_status_a_local()
                    .map_err(|e| format!("runtime-e2e status(node-a wait add) failed: {e}"))?;
                let status_b = c
                    .layered_status_b_local()
                    .map_err(|e| format!("runtime-e2e status(node-b wait add) failed: {e}"))?;
                last_status_a = status_a.clone();
                last_status_b = status_b.clone();
                let facade_a = fs_meta_api_status(&api_bind_addr_a, "operator", "operator123");
                let facade_b = fs_meta_api_status(&api_bind_addr_b, "operator", "operator123");
                last_api_a = match &facade_a {
                    Ok(status) => format!("ok:{status}"),
                    Err(err) => err.clone(),
                };
                last_api_b = match &facade_b {
                    Ok(status) => format!("ok:{status}"),
                    Err(err) => err.clone(),
                };
                if facade_a.is_ok() ^ facade_b.is_ok() {
                    seen_instance = true;
                    owner_on_a = facade_a.is_ok();
                    break;
                }
                thread::sleep(Duration::from_millis(100));
            }
            if !seen_instance {
                let cfg_a = c
                    .ctl_ok_a_local(json!({ "command": "config_get" }))
                    .unwrap_or_else(|_| serde_json::json!(null));
                let cfg_b = c
                    .ctl_ok_b_local(json!({ "command": "config_get" }))
                    .unwrap_or_else(|_| serde_json::json!(null));
                let audit_a = c
                    .ctl_ok_a_local(json!({ "command": "audit_tail", "count": 32 }))
                    .unwrap_or_else(|_| serde_json::json!(null));
                let audit_b = c
                    .ctl_ok_b_local(json!({ "command": "audit_tail", "count": 32 }))
                    .unwrap_or_else(|_| serde_json::json!(null));
                return Err(format!(
                    "app instance with exactly one active facade was not observed after relation_target_apply; node_a_status={last_status_a}; node_b_status={last_status_b}; node_a_api={last_api_a}; node_b_api={last_api_b}; node_a_config={cfg_a}; node_b_config={cfg_b}; node_a_audit={audit_a}; node_b_audit={audit_b}"
                ));
            }

            let mut seen_fs_meta_channels = false;
            for _ in 0..80 {
                let channels_a = c
                    .ctl_ok_a_local(json!({ "command": "channel_list" }))
                    .map_err(|e| {
                        format!(
                            "runtime-e2e channel_list(node-a wait fs-meta channels) failed: {e}"
                        )
                    })?;
                let channels_b = c
                    .ctl_ok_b_local(json!({ "command": "channel_list" }))
                    .map_err(|e| {
                        format!(
                            "runtime-e2e channel_list(node-b wait fs-meta channels) failed: {e}"
                        )
                    })?;
                let has_find = channels_a.as_array().is_some_and(|rows| {
                    rows.iter().any(|row| {
                        row.get("route_key")
                            .and_then(Value::as_str)
                            .is_some_and(|route| route.contains("find:v1.find"))
                            || row
                                .get("route_key")
                                .and_then(|v| v.get("0"))
                                .and_then(Value::as_str)
                                .is_some_and(|route| route.contains("find:v1.find"))
                    })
                }) || channels_b.as_array().is_some_and(|rows| {
                    rows.iter().any(|row| {
                        row.get("route_key")
                            .and_then(Value::as_str)
                            .is_some_and(|route| route.contains("find:v1.find"))
                            || row
                                .get("route_key")
                                .and_then(|v| v.get("0"))
                                .and_then(Value::as_str)
                                .is_some_and(|route| route.contains("find:v1.find"))
                    })
                });
                let has_force_find = channels_a.as_array().is_some_and(|rows| {
                    rows.iter().any(|row| {
                        row.get("route_key")
                            .and_then(Value::as_str)
                            .is_some_and(|route| route.contains("on-demand-force-find:v1"))
                            || row
                                .get("route_key")
                                .and_then(|v| v.get("0"))
                                .and_then(Value::as_str)
                                .is_some_and(|route| route.contains("on-demand-force-find:v1"))
                    })
                }) || channels_b.as_array().is_some_and(|rows| {
                    rows.iter().any(|row| {
                        row.get("route_key")
                            .and_then(Value::as_str)
                            .is_some_and(|route| route.contains("on-demand-force-find:v1"))
                            || row
                                .get("route_key")
                                .and_then(|v| v.get("0"))
                                .and_then(Value::as_str)
                                .is_some_and(|route| route.contains("on-demand-force-find:v1"))
                    })
                });
                if has_find && has_force_find {
                    seen_fs_meta_channels = true;
                    break;
                }
                thread::sleep(Duration::from_millis(100));
            }
            if !seen_fs_meta_channels {
                return Err(
                    "fs-meta runtime-e2e did not expose expected find/force-find channels"
                        .to_string(),
                );
            }

            let clear_cmd = json!({
                "command": "relation_target_clear",
                "target_id": instance_id,
            });
            let apply_remove = if owner_on_a {
                c.ctl_ok_a_local(clear_cmd)
            } else {
                c.ctl_ok_b_local(clear_cmd)
            }
            .map_err(|e| format!("runtime-e2e relation_target_clear failed: {e}"))?;
            if apply_remove.get("ok").and_then(Value::as_bool) != Some(true) {
                return Err(format!(
                    "relation_target_clear did not return ok=true: {apply_remove}"
                ));
            }

            for _ in 0..80 {
                let _status_a = c
                    .layered_status_a_local()
                    .map_err(|e| format!("runtime-e2e status(node-a wait remove) failed: {e}"))?;
                let _status_b = c
                    .layered_status_b_local()
                    .map_err(|e| format!("runtime-e2e status(node-b wait remove) failed: {e}"))?;
                let facade_present_a =
                    fs_meta_api_status(&api_bind_addr_a, "operator", "operator123").is_ok();
                let facade_present_b =
                    fs_meta_api_status(&api_bind_addr_b, "operator", "operator123").is_ok();
                if !facade_present_a && !facade_present_b {
                    return Ok(());
                }
                thread::sleep(Duration::from_millis(100));
            }

            Err(
                "app instance or active facade still present on at least one node after relation_target_clear"
                    .to_string(),
            )
        },
    )
}

pub(crate) fn scenario_single_entrypoint_distributed_apply_e2e() -> Result<(), String> {
    with_cluster(
        FULL_NODE_DELEGATION_SCOPES,
        FULL_NODE_DELEGATION_SCOPES,
        |c| {
            let Some(fs_meta_app) = try_find_fs_meta_app_cdylib() else {
                return Err(
                    "fs-meta app runtime path not found; set CAPANIX_FS_META_APP_BINARY or build fs-meta-runtime"
                        .to_string(),
                );
            };

            let instance_id = format!("fs-meta-distributed-e2e-{}", unique_suffix());
            let (passwd_path, shadow_path) = write_fs_meta_auth_files("distributed-e2e")?;
            let (api_bind_addr_a, api_bind_addr_b) = reserve_distinct_bind_addrs()?;
            let facade_resource_scope_id = format!("fs-meta-facade-resource-{instance_id}");
            let roots_dir = tempfile::tempdir().map_err(|e| {
                format!("create fs-meta distributed-e2e roots temp dir failed: {e}")
            })?;
            let nfs1_path = roots_dir.path().join("nfs1");
            let nfs2_path = roots_dir.path().join("nfs2");
            fs::create_dir_all(&nfs1_path)
                .map_err(|e| format!("create fs-meta distributed-e2e nfs1 dir failed: {e}"))?;
            fs::create_dir_all(&nfs2_path)
                .map_err(|e| format!("create fs-meta distributed-e2e nfs2 dir failed: {e}"))?;
            fs::write(nfs1_path.join("ready-a.txt"), "ready-a\n")
                .map_err(|e| format!("seed fs-meta distributed-e2e nfs1 dir failed: {e}"))?;
            fs::write(nfs2_path.join("ready-b.txt"), "ready-b\n")
                .map_err(|e| format!("seed fs-meta distributed-e2e nfs2 dir failed: {e}"))?;
            let nfs1_path_str = nfs1_path.to_string_lossy().to_string();
            let nfs2_path_str = nfs2_path.to_string_lossy().to_string();
            let mut baseline = c
                .ctl_ok_a_local(json!({ "command": "config_get" }))
                .map_err(|e| {
                    format!("distributed-e2e config_get(before baseline reset) failed: {e}")
                })?;
            if let Some(apps) = baseline.get_mut("apps").and_then(Value::as_array_mut) {
                apps.clear();
            }
            if let Some(route_plans) = baseline
                .get_mut("route_plans")
                .and_then(Value::as_array_mut)
            {
                route_plans.clear();
            }
            install_facade_resources(
                &mut baseline,
                &facade_resource_scope_id,
                &api_bind_addr_a,
                &api_bind_addr_b,
                &c.node_a_identity.node_id,
                &c.node_b_identity.node_id,
            );
            append_mount_root_resources(
                &mut baseline,
                "nfs1",
                &nfs1_path_str,
                &c.node_a_identity.node_id,
                &c.node_b_identity.node_id,
            );
            append_mount_root_resources(
                &mut baseline,
                "nfs2",
                &nfs2_path_str,
                &c.node_a_identity.node_id,
                &c.node_b_identity.node_id,
            );
            let baseline_apply = c
                .ctl_ok_a_local(json!({
                    "command": "config_set",
                    "config": baseline,
                }))
                .map_err(|e| format!("distributed-e2e baseline config_set failed: {e}"))?;
            if baseline_apply.get("ok").and_then(Value::as_bool) != Some(true) {
                return Err(format!(
                    "distributed-e2e baseline config_set did not return ok=true: {baseline_apply}"
                ));
            }
            let mut baseline_b = c
                .ctl_ok_b_local(json!({ "command": "config_get" }))
                .map_err(|e| {
                    format!("distributed-e2e config_get(node-b before baseline reset) failed: {e}")
                })?;
            if let Some(apps) = baseline_b.get_mut("apps").and_then(Value::as_array_mut) {
                apps.clear();
            }
            if let Some(route_plans) = baseline_b
                .get_mut("route_plans")
                .and_then(Value::as_array_mut)
            {
                route_plans.clear();
            }
            install_facade_resources(
                &mut baseline_b,
                &facade_resource_scope_id,
                &api_bind_addr_a,
                &api_bind_addr_b,
                &c.node_a_identity.node_id,
                &c.node_b_identity.node_id,
            );
            append_mount_root_resources(
                &mut baseline_b,
                "nfs1",
                &nfs1_path_str,
                &c.node_a_identity.node_id,
                &c.node_b_identity.node_id,
            );
            append_mount_root_resources(
                &mut baseline_b,
                "nfs2",
                &nfs2_path_str,
                &c.node_a_identity.node_id,
                &c.node_b_identity.node_id,
            );
            let baseline_apply_b = c
                .ctl_ok_b_local(json!({
                    "command": "config_set",
                    "config": baseline_b,
                }))
                .map_err(|e| format!("distributed-e2e baseline config_set(node-b) failed: {e}"))?;
            if baseline_apply_b.get("ok").and_then(Value::as_bool) != Some(true) {
                return Err(format!(
                    "distributed-e2e baseline config_set(node-b) did not return ok=true: {baseline_apply_b}"
                ));
            }

            let app_decl = json!({
                "schema_version": "release-v2",
                "apps": [
                    {
                        "id": instance_id,
                        "app": fs_meta_app.to_string_lossy().to_string(),
                        "manifest": "fs-meta/fixtures/manifests/fs-meta.yaml",
                        "version": "0.0.2",
                        "restart_policy": "Never",
                        "config": {
                            "workers": {
                                "facade": { "mode": "embedded" },
                                "source": { "mode": "embedded" },
                                "sink": { "mode": "embedded" }
                            },
                            "roots": [
                                {
                                    "id": "nfs1",
                                    "selector": { "mount_point": nfs1_path_str.clone() },
                                    "subpath_scope": "/",
                                    "watch": true,
                                    "scan": true
                                },
                                {
                                    "id": "nfs2",
                                    "selector": { "mount_point": nfs2_path_str.clone() },
                                    "subpath_scope": "/",
                                    "watch": true,
                                    "scan": true
                                }
                            ],
                            "api": {
                                "facade_resource_id": facade_resource_scope_id.clone(),
                                "auth": {
                                    "passwd_path": passwd_path,
                                    "shadow_path": shadow_path,
                                    "management_group": "fsmeta_management"
                                }
                            }
                        },
                        "runtime": {
                            "control_subscriptions": ["runtime.host_object_grants.changed"],
                            "app_scopes": [
                                {
                                    "scope_id": "nfs1",
                                    "resource_ids": ["nfs1"],
                                    "unit_scopes": [
                                        { "unit_id": "runtime.exec.source", "eligibility": "scope_members", "cardinality": "all" },
                                        { "unit_id": "runtime.exec.scan", "eligibility": "scope_members", "cardinality": "all" },
                                        { "unit_id": "runtime.exec.sink", "eligibility": "resource_visible_nodes", "cardinality": "one" }
                                    ]
                                },
                                {
                                    "scope_id": "nfs2",
                                    "resource_ids": ["nfs2"],
                                    "unit_scopes": [
                                        { "unit_id": "runtime.exec.source", "eligibility": "scope_members", "cardinality": "all" },
                                        { "unit_id": "runtime.exec.scan", "eligibility": "scope_members", "cardinality": "all" },
                                        { "unit_id": "runtime.exec.sink", "eligibility": "resource_visible_nodes", "cardinality": "one" }
                                    ]
                                },
                                {
                                    "scope_id": facade_resource_scope_id.clone(),
                                    "resource_ids": [facade_resource_scope_id.clone()],
                                    "unit_scopes": [
                                        { "unit_id": "runtime.exec.facade", "eligibility": "resource_visible_nodes", "cardinality": "one" }
                                    ]
                                }
                            ],
                            "units": {
                                "runtime.exec.facade": { "enabled": true }
                            },
                            "state_carrier": {
                                "enabled": true,
                                "units": {
                                    "runtime.exec.scan": { "enabled": false }
                                }
                            }
                        },
                        "policy": {
                            "generation": 1,}
                    }
                ]
            });

            let intent = release_v2_doc_to_scope_unit_intent_value(&app_decl)?;
            let apply = c
                .ctl_ok_a_local(json!({
                    "command": "relation_target_apply",
                    "declaration": intent,
                }))
                .map_err(|e| format!("distributed-e2e relation_target_apply failed: {e}"))?;
            if apply.get("ok").and_then(Value::as_bool) != Some(true) {
                return Err(format!(
                    "distributed-e2e relation_target_apply did not return ok=true: {apply}"
                ));
            }

            let mut last_status_a = serde_json::json!(null);
            let mut last_status_b = serde_json::json!(null);
            let mut last_api_a = String::new();
            let mut last_api_b = String::new();
            for _ in 0..120 {
                let status_a = c
                    .layered_status_a_local()
                    .map_err(|e| format!("distributed-e2e status(node-a) failed: {e}"))?;
                let status_b = c
                    .layered_status_b_local()
                    .map_err(|e| format!("distributed-e2e status(node-b) failed: {e}"))?;
                last_status_a = status_a.clone();
                last_status_b = status_b.clone();
                let facade_a = fs_meta_api_status(&api_bind_addr_a, "operator", "operator123");
                let facade_b = fs_meta_api_status(&api_bind_addr_b, "operator", "operator123");
                last_api_a = match &facade_a {
                    Ok(status) => format!("ok:{status}"),
                    Err(err) => err.clone(),
                };
                last_api_b = match &facade_b {
                    Ok(status) => format!("ok:{status}"),
                    Err(err) => err.clone(),
                };
                if facade_a.is_ok() ^ facade_b.is_ok() {
                    return Ok(());
                }
                thread::sleep(Duration::from_millis(100));
            }

            let cfg_b = c
                .ctl_ok_b_local(json!({ "command": "config_get" }))
                .unwrap_or_else(|_| serde_json::json!(null));
            let cfg_a = c
                .ctl_ok_a_local(json!({ "command": "config_get" }))
                .unwrap_or_else(|_| serde_json::json!(null));
            let audit_b = c
                .ctl_ok_b_local(json!({ "command": "audit_tail", "count": 32 }))
                .unwrap_or_else(|_| serde_json::json!(null));
            let audit_a = c
                .ctl_ok_a_local(json!({ "command": "audit_tail", "count": 32 }))
                .unwrap_or_else(|_| serde_json::json!(null));
            Err(format!(
                "one-cardinality facade realization did not converge to exactly one active facade; apply={apply}; node_a_config={cfg_a}; node_b_config={cfg_b}; node_a_status={last_status_a}; node_b_status={last_status_b}; node_a_api={last_api_a}; node_b_api={last_api_b}; node_a_audit={audit_a}; node_b_audit={audit_b}"
            ))
        },
    )
}

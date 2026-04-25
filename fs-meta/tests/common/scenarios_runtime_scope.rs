use super::*;
use fs_meta_deploy::{
    build_release_doc_value, compile_release_doc_to_relation_target_intent, write_startup_manifest,
    ApiAuthConfig, FsMetaReleaseSpec, FsMetaReleaseWorkerMode, FsMetaReleaseWorkerModes,
};

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

fn append_mount_root_resource_on_node(
    baseline: &mut Value,
    resource_id: &str,
    mount_point: &str,
    node_id: &str,
) {
    let Some(resources) = baseline
        .get_mut("announced_resources")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    resources.push(json!({
        "resource_id": resource_id,
        "node_id": node_id,
        "resource_kind": "fs",
        "source": mount_point,
        "mount_hint": mount_point,
    }));
}

fn fs_meta_api_client() -> Result<reqwest::blocking::Client, String> {
    reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|e| format!("build reqwest client failed: {e}"))
}

fn format_reqwest_error(context: &str, err: reqwest::Error) -> String {
    let mut message = format!("{context}: {err}; debug={err:?}");
    let mut source = std::error::Error::source(&err);
    let mut depth = 0usize;
    while let Some(next) = source {
        depth += 1;
        message.push_str(&format!("; source[{depth}]={next}"));
        source = std::error::Error::source(next);
    }
    message
}

fn fs_meta_login_token(bind_addr: &str, username: &str, password: &str) -> Result<String, String> {
    let client = fs_meta_api_client()?;
    let base = format!("http://{bind_addr}/api/fs-meta/v1");
    let login = client
        .post(format!("{base}/session/login"))
        .json(&json!({ "username": username, "password": password }))
        .send()
        .map_err(|e| format_reqwest_error("login request failed", e))?;
    if !login.status().is_success() {
        return Err(format!("login returned HTTP {}", login.status()));
    }
    let body: Value = login
        .json()
        .map_err(|e| format!("decode login response failed: {e}"))?;
    body.get("token")
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| format!("login response missing token: {body}"))
}

fn fs_meta_login_ready(bind_addr: &str, username: &str, password: &str) -> Result<(), String> {
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(1))
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
    Ok(())
}

fn fs_meta_post_json(
    bind_addr: &str,
    path: &str,
    bearer: &str,
    body: &Value,
) -> Result<Value, String> {
    let client = fs_meta_api_client()?;
    let base = format!("http://{bind_addr}/api/fs-meta/v1");
    let response = client
        .post(format!("{base}{path}"))
        .bearer_auth(bearer)
        .json(body)
        .send()
        .map_err(|e| format!("POST {path} failed: {e}"))?;
    if !response.status().is_success() {
        return Err(format!("POST {path} returned HTTP {}", response.status()));
    }
    response
        .json()
        .map_err(|e| format!("decode POST {path} response failed: {e}"))
}

fn fs_meta_put_json(
    bind_addr: &str,
    path: &str,
    bearer: &str,
    body: &Value,
) -> Result<Value, String> {
    let client = fs_meta_api_client()?;
    let base = format!("http://{bind_addr}/api/fs-meta/v1");
    let response = client
        .put(format!("{base}{path}"))
        .bearer_auth(bearer)
        .json(body)
        .send()
        .map_err(|e| format!("PUT {path} failed: {e}"))?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response
            .text()
            .unwrap_or_else(|err| format!("<failed to read body: {err}>"));
        return Err(format!("PUT {path} returned HTTP {} body={}", status, body));
    }
    response
        .json()
        .map_err(|e| format!("decode PUT {path} response failed: {e}"))
}

fn fs_meta_get_json(bind_addr: &str, path: &str, bearer: &str) -> Result<Value, String> {
    let client = fs_meta_api_client()?;
    let base = format!("http://{bind_addr}/api/fs-meta/v1");
    let response = client
        .get(format!("{base}{path}"))
        .bearer_auth(bearer)
        .send()
        .map_err(|e| format_reqwest_error(&format!("GET {path} failed"), e))?;
    if !response.status().is_success() {
        return Err(format!("GET {path} returned HTTP {}", response.status()));
    }
    response
        .json()
        .map_err(|e| format!("decode GET {path} response failed: {e}"))
}

fn fs_meta_create_query_api_key(
    bind_addr: &str,
    management_token: &str,
    label: &str,
) -> Result<String, String> {
    let created = fs_meta_post_json(
        bind_addr,
        "/query-api-keys",
        management_token,
        &json!({ "label": label }),
    )?;
    created
        .get("api_key")
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| format!("query api key response missing api_key: {created}"))
}

fn response_group_keys(resp: &Value) -> std::collections::BTreeSet<String> {
    resp.get("groups")
        .and_then(Value::as_array)
        .map(|groups| {
            groups
                .iter()
                .filter_map(|group| group.get("group").and_then(Value::as_str))
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn route_activation_summary(status: &Value) -> Value {
    let interesting = [
        "source-logical-roots-control",
        "sink-logical-roots-control",
        "source-status",
        "sink-status",
        "source-on-demand-force-find",
        "materialized-find",
        "materialized-find-proxy",
    ];
    Value::Array(
        status
            .get("daemon")
            .and_then(|daemon| daemon.get("activation"))
            .and_then(|activation| activation.get("routes"))
            .and_then(Value::as_array)
            .map(|routes| {
                routes
                    .iter()
                    .filter(|route| {
                        route
                            .get("route_key")
                            .and_then(Value::as_str)
                            .is_some_and(|key| interesting.iter().any(|needle| key.contains(needle)))
                    })
                    .map(|route| {
                        json!({
                            "route_key": route.get("route_key").cloned().unwrap_or(Value::Null),
                            "state": route.get("state").cloned().unwrap_or(Value::Null),
                            "route_publish_status": route
                                .get("route_publish_status")
                                .cloned()
                                .unwrap_or(Value::Null),
                            "route_publish_reason": route
                                .get("route_publish_reason")
                                .cloned()
                                .unwrap_or(Value::Null),
                            "active_pids": route.get("active_pids").cloned().unwrap_or(Value::Null),
                            "apps": route
                                .get("apps")
                                .and_then(Value::as_array)
                                .map(|apps| {
                                    Value::Array(
                                        apps.iter()
                                            .map(|app| {
                                                json!({
                                                    "node_id": app.get("node_id").cloned().unwrap_or(Value::Null),
                                                    "pid": app.get("pid").cloned().unwrap_or(Value::Null),
                                                    "unit_ids": app.get("unit_ids").cloned().unwrap_or(Value::Null),
                                                    "bound_scopes_by_unit": app
                                                        .get("bound_scopes_by_unit")
                                                        .cloned()
                                                        .unwrap_or(Value::Null),
                                                    "delivered": app.get("delivered").cloned().unwrap_or(Value::Null),
                                                    "gate": app.get("gate").cloned().unwrap_or(Value::Null),
                                                    "op": app.get("op").cloned().unwrap_or(Value::Null),
                                                    "realization": app.get("realization").cloned().unwrap_or(Value::Null),
                                                })
                                            })
                                            .collect(),
                                    )
                                })
                                .unwrap_or(Value::Null),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default(),
    )
}

fn source_root_summary(source: &Value) -> Value {
    let root_matches = |value: &Value| {
        value.get("root_id").and_then(Value::as_str) == Some("root-b")
            || value.get("logical_root_id").and_then(Value::as_str) == Some("root-b")
            || value.get("object_ref").and_then(Value::as_str) == Some("node-b::nfs2")
    };
    json!({
        "grants_count": source.get("grants_count").cloned().unwrap_or(Value::Null),
        "roots_count": source.get("roots_count").cloned().unwrap_or(Value::Null),
        "logical_roots_root_b": source
            .get("logical_roots")
            .and_then(Value::as_array)
            .map(|roots| Value::Array(roots.iter().filter(|root| root_matches(root)).cloned().collect()))
            .unwrap_or(Value::Null),
        "concrete_roots_root_b": source
            .get("concrete_roots")
            .and_then(Value::as_array)
            .map(|roots| Value::Array(roots.iter().filter(|root| root_matches(root)).cloned().collect()))
            .unwrap_or(Value::Null),
        "debug": source.get("debug").cloned().unwrap_or(Value::Null),
    })
}

fn sink_root_summary(sink: &Value) -> Value {
    json!({
        "aggregates": sink.get("aggregates").cloned().unwrap_or(Value::Null),
        "groups_count": sink.get("groups_count").cloned().unwrap_or(Value::Null),
        "groups": sink.get("groups").cloned().unwrap_or(Value::Null),
        "debug": sink.get("debug").cloned().unwrap_or(Value::Null),
    })
}

fn local_fs_meta_status_summary(status: &Value) -> Value {
    json!({
        "source": source_root_summary(&status.get("source").cloned().unwrap_or(Value::Null)),
        "sink": sink_root_summary(&status.get("sink").cloned().unwrap_or(Value::Null)),
        "facade": status.get("facade").cloned().unwrap_or(Value::Null),
        "daemon_activation_routes": route_activation_summary(status),
    })
}

fn local_route_plan_summary(config: &Value) -> Value {
    let route_plans = config
        .get("route_plans")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let target_route_plans = config
        .get("runtime_target_state")
        .and_then(|v| v.get("target_route_plans"))
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let interesting = [
        "source-status:v1.req",
        "source-status:v1.req:reply",
        "source-logical-roots-control:v1.stream",
        "sink-logical-roots-control:v1.stream",
    ];
    let route_keys = route_plans
        .iter()
        .filter_map(|plan| plan.get("route_key").and_then(Value::as_str))
        .filter(|route_key| interesting.contains(route_key))
        .map(|route_key| route_key.to_string())
        .collect::<Vec<_>>();
    let target_summaries = target_route_plans
        .iter()
        .map(|(target_id, plans)| {
            let plans = plans.as_array().cloned().unwrap_or_default();
            let keys = plans
                .iter()
                .filter_map(|plan| plan.get("route_key").and_then(Value::as_str))
                .filter(|route_key| interesting.contains(route_key))
                .map(|route_key| route_key.to_string())
                .collect::<Vec<_>>();
            json!({
                "target_id": target_id,
                "plans_count": plans.len(),
                "interesting_routes": keys,
            })
        })
        .collect::<Vec<_>>();
    json!({
        "route_plans_count": route_plans.len(),
        "interesting_route_plans": route_keys,
        "target_route_plans": target_summaries,
    })
}

fn grants_contain_all_mounts(grants: &Value, mounts: &[&str]) -> bool {
    let rows = grants
        .as_array()
        .or_else(|| grants.get("grants").and_then(Value::as_array));
    let Some(rows) = rows else {
        return false;
    };
    mounts.iter().all(|mount| {
        rows.iter().any(|row| {
            row.get("mount_point").and_then(Value::as_str) == Some(*mount)
                || row.get("source").and_then(Value::as_str) == Some(*mount)
        })
    })
}

fn wait_for_runtime_grants_mounts(
    bind_addr: &str,
    bearer: &str,
    mounts: &[&str],
) -> Result<Value, String> {
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut last = serde_json::json!(null);
    while Instant::now() < deadline {
        let grants = fs_meta_get_json(bind_addr, "/runtime/grants", bearer)?;
        if grants_contain_all_mounts(&grants, mounts) {
            return Ok(grants);
        }
        last = grants;
        thread::sleep(Duration::from_millis(150));
    }
    Err(format!(
        "runtime grants did not include all mounts {mounts:?}; last={last}"
    ))
}

fn wait_for_query_groups_and_paths(
    bind_addrs: &[&str],
    bearer: &str,
    path: &str,
    expected_groups: &std::collections::BTreeSet<String>,
    expected_paths: &[&str],
) -> Result<Value, String> {
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut last = serde_json::json!(null);
    let mut last_errs = Vec::<String>::new();
    while Instant::now() < deadline {
        last_errs.clear();
        for bind_addr in bind_addrs {
            let body = match fs_meta_get_json(
                bind_addr,
                &format!("/on-demand-force-find?path={path}"),
                bearer,
            ) {
                Ok(body) => body,
                Err(err) => {
                    last_errs.push(format!("{bind_addr}: {err}"));
                    continue;
                }
            };
            let groups = response_group_keys(&body);
            let body_text = body.to_string();
            if &groups == expected_groups
                && expected_paths
                    .iter()
                    .all(|expected| body_text.contains(expected))
            {
                return Ok(body);
            }
            last = body;
        }
        thread::sleep(Duration::from_millis(150));
    }
    Err(format!(
        "force-find did not converge to expected groups {expected_groups:?} and paths {expected_paths:?}; last={last}; last_errs={last_errs:?}"
    ))
}

fn wait_for_single_active_bind_addr(
    bind_addr_a: &str,
    bind_addr_b: &str,
    username: &str,
    password: &str,
) -> Result<String, String> {
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut last_a = String::new();
    let mut last_b = String::new();
    while Instant::now() < deadline {
        let facade_a = fs_meta_login_ready(bind_addr_a, username, password);
        let facade_b = fs_meta_login_ready(bind_addr_b, username, password);
        last_a = match &facade_a {
            Ok(()) => "ok".to_string(),
            Err(err) => err.clone(),
        };
        last_b = match &facade_b {
            Ok(()) => "ok".to_string(),
            Err(err) => err.clone(),
        };
        if facade_a.is_ok() ^ facade_b.is_ok() {
            return if facade_a.is_ok() {
                Ok(bind_addr_a.to_string())
            } else {
                Ok(bind_addr_b.to_string())
            };
        }
        thread::sleep(Duration::from_millis(100));
    }
    Err(format!(
        "did not converge to exactly one active facade; bind_a={bind_addr_a} result_a={last_a}; bind_b={bind_addr_b} result_b={last_b}"
    ))
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

pub(crate) fn scenario_empty_roots_online_roots_apply_distributed_force_find_e2e(
) -> Result<(), String> {
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

            let instance_id = format!("fs-meta-empty-roots-e2e-{}", unique_suffix());
            let (passwd_path, shadow_path) = write_fs_meta_auth_files("empty-roots-e2e")?;
            let query_keys_path = PathBuf::from(&passwd_path)
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join("query-keys.json");
            let (api_bind_addr_a, api_bind_addr_b) = reserve_distinct_bind_addrs()?;
            let facade_resource_scope_id = format!("fs-meta-facade-resource-{instance_id}");
            let roots_dir = tempfile::tempdir()
                .map_err(|e| format!("create fs-meta empty-roots temp dir failed: {e}"))?;
            let nfs1_path = roots_dir.path().join("nfs1");
            let nfs2_path = roots_dir.path().join("nfs2");
            fs::create_dir_all(&nfs1_path)
                .map_err(|e| format!("create fs-meta empty-roots nfs1 dir failed: {e}"))?;
            fs::create_dir_all(&nfs2_path)
                .map_err(|e| format!("create fs-meta empty-roots nfs2 dir failed: {e}"))?;
            fs::write(nfs1_path.join("ready-a.txt"), "ready-a\n")
                .map_err(|e| format!("seed fs-meta empty-roots nfs1 dir failed: {e}"))?;
            fs::write(nfs2_path.join("ready-b.txt"), "ready-b\n")
                .map_err(|e| format!("seed fs-meta empty-roots nfs2 dir failed: {e}"))?;
            let nfs1_path_str = nfs1_path.to_string_lossy().to_string();
            let nfs2_path_str = nfs2_path.to_string_lossy().to_string();

            let mut baseline = c
                .ctl_ok_a_local(json!({ "command": "config_get" }))
                .map_err(|e| {
                    format!("empty-roots-e2e config_get(before baseline reset) failed: {e}")
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
            append_mount_root_resource_on_node(
                &mut baseline,
                "nfs1",
                &nfs1_path_str,
                &c.node_a_identity.node_id,
            );
            append_mount_root_resource_on_node(
                &mut baseline,
                "nfs2",
                &nfs2_path_str,
                &c.node_b_identity.node_id,
            );
            let baseline_apply = c
                .ctl_ok_a_local(json!({
                    "command": "config_set",
                    "config": baseline,
                }))
                .map_err(|e| format!("empty-roots-e2e baseline config_set failed: {e}"))?;
            if baseline_apply.get("ok").and_then(Value::as_bool) != Some(true) {
                return Err(format!(
                    "empty-roots-e2e baseline config_set did not return ok=true: {baseline_apply}"
                ));
            }
            let mut baseline_b = c
                .ctl_ok_b_local(json!({ "command": "config_get" }))
                .map_err(|e| {
                    format!("empty-roots-e2e config_get(node-b before baseline reset) failed: {e}")
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
            append_mount_root_resource_on_node(
                &mut baseline_b,
                "nfs1",
                &nfs1_path_str,
                &c.node_a_identity.node_id,
            );
            append_mount_root_resource_on_node(
                &mut baseline_b,
                "nfs2",
                &nfs2_path_str,
                &c.node_b_identity.node_id,
            );
            let baseline_apply_b = c
                .ctl_ok_b_local(json!({
                    "command": "config_set",
                    "config": baseline_b,
                }))
                .map_err(|e| format!("empty-roots-e2e baseline config_set(node-b) failed: {e}"))?;
            if baseline_apply_b.get("ok").and_then(Value::as_bool) != Some(true) {
                return Err(format!(
                    "empty-roots-e2e baseline config_set(node-b) did not return ok=true: {baseline_apply_b}"
                ));
            }

            let spec = FsMetaReleaseSpec {
                app_id: instance_id.clone(),
                api_facade_resource_id: facade_resource_scope_id.clone(),
                auth: ApiAuthConfig {
                    passwd_path: PathBuf::from(&passwd_path),
                    shadow_path: PathBuf::from(&shadow_path),
                    query_keys_path,
                    session_ttl_secs: 3600,
                    management_group: "fsmeta_management".to_string(),
                    bootstrap_management: None,
                },
                roots: Vec::new(),
                route_plan_node_ids: vec![
                    c.node_a_identity.node_id.clone(),
                    c.node_b_identity.node_id.clone(),
                ],
                worker_module_path: Some(fs_meta_app.clone()),
                worker_modes: FsMetaReleaseWorkerModes {
                    facade: FsMetaReleaseWorkerMode::Embedded,
                    source: FsMetaReleaseWorkerMode::Embedded,
                    sink: FsMetaReleaseWorkerMode::Embedded,
                },
            };
            let manifest_dir = tempfile::tempdir()
                .map_err(|e| format!("create fs-meta empty-roots manifest temp dir failed: {e}"))?;
            let manifest_path = manifest_dir.path().join("fs-meta-empty-roots.yaml");
            write_startup_manifest(
                &repo_root().join("fs-meta/fixtures/manifests/fs-meta.yaml"),
                &spec,
                &manifest_path,
            )
            .map_err(|e| format!("write fs-meta empty-roots startup manifest failed: {e}"))?;
            let mut release_doc = build_release_doc_value(&spec);
            release_doc["units"][0]["startup"]["path"] =
                json!(fs_meta_app.to_string_lossy().to_string());
            release_doc["units"][0]["startup"]["manifest"] =
                json!(manifest_path.display().to_string());
            release_doc["units"][0]["version"] = json!("0.0.2");
            release_doc["units"][0]["restart_policy"] = json!("Never");
            release_doc["units"][0]["policy"]["replicas"] = json!(1_i64);
            let intent = compile_release_doc_to_relation_target_intent(&release_doc)
                .map_err(|e| format!("compile empty-roots release doc failed: {e}"))?;
            let apply = c
                .ctl_ok_a_local(json!({
                    "command": "relation_target_apply",
                    "declaration": intent,
                }))
                .map_err(|e| format!("empty-roots-e2e relation_target_apply failed: {e}"))?;
            if apply.get("ok").and_then(Value::as_bool) != Some(true) {
                return Err(format!(
                    "empty-roots-e2e relation_target_apply did not return ok=true: {apply}"
                ));
            }

            let mut last_status_a = serde_json::json!(null);
            let mut last_status_b = serde_json::json!(null);
            for _ in 0..160 {
                let status_a = c
                    .layered_status_a_local()
                    .map_err(|e| format!("empty-roots-e2e status(node-a) failed: {e}"))?;
                let status_b = c
                    .layered_status_b_local()
                    .map_err(|e| format!("empty-roots-e2e status(node-b) failed: {e}"))?;
                last_status_a = status_a.clone();
                last_status_b = status_b.clone();
                if let Ok(active_bind_addr) = wait_for_single_active_bind_addr(
                    &api_bind_addr_a,
                    &api_bind_addr_b,
                    "operator",
                    "operator123",
                ) {
                    let management_token =
                        fs_meta_login_token(&active_bind_addr, "operator", "operator123")?;
                    let _grants = wait_for_runtime_grants_mounts(
                        &active_bind_addr,
                        &management_token,
                        &[&nfs1_path_str, &nfs2_path_str],
                    )?;
                    let roots_payload = json!({
                        "roots": [
                            {
                                "id": "root-a",
                                "selector": { "mount_point": nfs1_path_str.clone() },
                                "subpath_scope": "/",
                                "watch": true,
                                "scan": true
                            },
                            {
                                "id": "root-b",
                                "selector": { "mount_point": nfs2_path_str.clone() },
                                "subpath_scope": "/",
                                "watch": true,
                                "scan": true
                            }
                        ]
                    });
                    let preview = fs_meta_post_json(
                        &active_bind_addr,
                        "/monitoring/roots/preview",
                        &management_token,
                        &roots_payload,
                    )?;
                    let matched_count = preview
                        .get("matched")
                        .or_else(|| preview.get("preview"))
                        .and_then(Value::as_array)
                        .map(|rows| rows.len())
                        .unwrap_or_default();
                    let unmatched_count = preview
                        .get("unmatched")
                        .or_else(|| preview.get("unmatched_roots"))
                        .and_then(Value::as_array)
                        .map(|rows| rows.len())
                        .unwrap_or_default();
                    if matched_count != 2 || unmatched_count != 0 {
                        return Err(format!(
                            "empty-roots-e2e preview did not fully match roots; preview={preview}"
                        ));
                    }
                    let applied = match fs_meta_put_json(
                        &active_bind_addr,
                        "/monitoring/roots",
                        &management_token,
                        &roots_payload,
                    ) {
                        Ok(applied) => applied,
                        Err(err) => {
                            let status_a = c
                                .layered_status_a_local()
                                .map(|status| local_fs_meta_status_summary(&status))
                                .unwrap_or_else(|e| serde_json::json!({ "error": e }));
                            let status_b = c
                                .layered_status_b_local()
                                .map(|status| local_fs_meta_status_summary(&status))
                                .unwrap_or_else(|e| serde_json::json!({ "error": e }));
                            let audit_a = c
                                .ctl_ok_a_local(json!({ "command": "audit_tail", "count": 32 }))
                                .unwrap_or_else(|_| serde_json::json!(null));
                            let audit_b = c
                                .ctl_ok_b_local(json!({ "command": "audit_tail", "count": 32 }))
                                .unwrap_or_else(|_| serde_json::json!(null));
                            let cfg_a = c
                                .ctl_ok_a_local(json!({ "command": "config_get" }))
                                .map(|cfg| local_route_plan_summary(&cfg))
                                .unwrap_or_else(|e| serde_json::json!({ "error": e }));
                            let cfg_b = c
                                .ctl_ok_b_local(json!({ "command": "config_get" }))
                                .map(|cfg| local_route_plan_summary(&cfg))
                                .unwrap_or_else(|e| serde_json::json!({ "error": e }));
                            return Err(format!(
                                "empty-roots-e2e roots apply failed: {err}; node_a_local={status_a}; node_b_local={status_b}; node_a_cfg={cfg_a}; node_b_cfg={cfg_b}; node_a_audit={audit_a}; node_b_audit={audit_b}"
                            ));
                        }
                    };
                    if applied.get("roots_count").and_then(Value::as_u64) != Some(2) {
                        return Err(format!(
                            "empty-roots-e2e roots apply did not confirm roots_count=2: {applied}"
                        ));
                    }
                    let active_bind_addr = wait_for_single_active_bind_addr(
                        &api_bind_addr_a,
                        &api_bind_addr_b,
                        "operator",
                        "operator123",
                    )?;
                    let management_token =
                        fs_meta_login_token(&active_bind_addr, "operator", "operator123")?;
                    let rescan = fs_meta_post_json(
                        &active_bind_addr,
                        "/index/rescan",
                        &management_token,
                        &json!({}),
                    )?;
                    if rescan.get("accepted").and_then(Value::as_bool) != Some(true) {
                        return Err(format!(
                            "empty-roots-e2e rescan did not return accepted=true: {rescan}"
                        ));
                    }
                    let active_bind_addr = wait_for_single_active_bind_addr(
                        &api_bind_addr_a,
                        &api_bind_addr_b,
                        "operator",
                        "operator123",
                    )?;
                    let management_token =
                        fs_meta_login_token(&active_bind_addr, "operator", "operator123")?;
                    let mut last_ready_status = serde_json::json!(null);
                    let readiness_deadline =
                        std::time::Instant::now() + std::time::Duration::from_secs(10);
                    let node_b_object_ref = format!("{}::nfs2", c.node_b_identity.node_id);
                    let node_b_id = c.node_b_identity.node_id.clone();
                    loop {
                        let current_active_bind_addr = wait_for_single_active_bind_addr(
                            &api_bind_addr_a,
                            &api_bind_addr_b,
                            "operator",
                            "operator123",
                        )?;
                        let current_management_token = fs_meta_login_token(
                            &current_active_bind_addr,
                            "operator",
                            "operator123",
                        )?;
                        let status = fs_meta_get_json(
                            &current_active_bind_addr,
                            "/status",
                            &current_management_token,
                        )?;
                        last_ready_status = status.clone();
                        let has_root_b_concrete = status
                            .get("source")
                            .and_then(|source| source.get("concrete_roots"))
                            .and_then(Value::as_array)
                            .map(|roots| {
                                roots.iter().any(|root| {
                                    root.get("logical_root_id").and_then(Value::as_str)
                                        == Some("root-b")
                                        && root.get("object_ref").and_then(Value::as_str)
                                            == Some(node_b_object_ref.as_str())
                                })
                            })
                            .unwrap_or(false);
                        let node_b_scheduled_root_b = status
                            .get("source")
                            .and_then(|source| source.get("debug"))
                            .and_then(|debug| debug.get("scheduled_source_groups_by_node"))
                            .and_then(|groups| groups.get(&node_b_id))
                            .and_then(Value::as_array)
                            .map(|groups| {
                                groups.iter().any(|group| group.as_str() == Some("root-b"))
                            })
                            .unwrap_or(false);
                        if has_root_b_concrete && node_b_scheduled_root_b {
                            break;
                        }
                        if std::time::Instant::now() >= readiness_deadline {
                            let status_a = c
                                .layered_status_a_local()
                                .map(|status| local_fs_meta_status_summary(&status))
                                .unwrap_or_else(|e| serde_json::json!({ "error": e }));
                            let status_b = c
                                .layered_status_b_local()
                                .map(|status| local_fs_meta_status_summary(&status))
                                .unwrap_or_else(|e| serde_json::json!({ "error": e }));
                            return Err(format!(
                                "empty-roots-e2e source second-wave readiness did not converge before force-find; active_bind_addr={current_active_bind_addr} public_status={last_ready_status}; node_a_local={status_a}; node_b_local={status_b}"
                            ));
                        }
                        std::thread::sleep(std::time::Duration::from_millis(100));
                    }
                    let query_api_key = fs_meta_create_query_api_key(
                        &active_bind_addr,
                        &management_token,
                        "empty-roots",
                    )?;
                    let expected_groups = std::collections::BTreeSet::from([
                        "root-a".to_string(),
                        "root-b".to_string(),
                    ]);
                    if let Err(err) = wait_for_query_groups_and_paths(
                        &[&api_bind_addr_a, &api_bind_addr_b],
                        &query_api_key,
                        "/",
                        &expected_groups,
                        &["ready-a.txt", "ready-b.txt"],
                    ) {
                        let login_a =
                            fs_meta_login_ready(&api_bind_addr_a, "operator", "operator123")
                                .map(|_| "ok".to_string())
                                .unwrap_or_else(|e| e);
                        let login_b =
                            fs_meta_login_ready(&api_bind_addr_b, "operator", "operator123")
                                .map(|_| "ok".to_string())
                                .unwrap_or_else(|e| e);
                        let single_active_after_force_find = wait_for_single_active_bind_addr(
                            &api_bind_addr_a,
                            &api_bind_addr_b,
                            "operator",
                            "operator123",
                        )
                        .map(|addr| format!("ok:{addr}"))
                        .unwrap_or_else(|e| e);
                        let roots_current_active = if let Some(active_bind_addr) =
                            single_active_after_force_find.strip_prefix("ok:")
                        {
                            match fs_meta_get_json(
                                active_bind_addr,
                                "/monitoring/roots",
                                &management_token,
                            ) {
                                Ok(body) => body,
                                Err(err) => serde_json::json!({"error": err}),
                            }
                        } else {
                            serde_json::json!(null)
                        };
                        let public_status_active = if let Some(active_bind_addr) =
                            single_active_after_force_find.strip_prefix("ok:")
                        {
                            match fs_meta_get_json(active_bind_addr, "/status", &management_token) {
                                Ok(body) => body,
                                Err(err) => serde_json::json!({"error": err}),
                            }
                        } else {
                            serde_json::json!(null)
                        };
                        let status_a = c
                            .layered_status_a_local()
                            .unwrap_or_else(|_| serde_json::json!(null));
                        let status_b = c
                            .layered_status_b_local()
                            .unwrap_or_else(|_| serde_json::json!(null));
                        let audit_a = c
                            .ctl_ok_a_local(json!({ "command": "audit_tail", "count": 32 }))
                            .unwrap_or_else(|_| serde_json::json!(null));
                        let audit_b = c
                            .ctl_ok_b_local(json!({ "command": "audit_tail", "count": 32 }))
                            .unwrap_or_else(|_| serde_json::json!(null));
                        return Err(format!(
                            "{err}; login_a={login_a}; login_b={login_b}; single_active_after_force_find={single_active_after_force_find}; roots_current_active={roots_current_active}; public_status_active={public_status_active}; node_a_status={status_a}; node_b_status={status_b}; node_a_audit={audit_a}; node_b_audit={audit_b}"
                        ));
                    }
                    return Ok(());
                }
                thread::sleep(Duration::from_millis(100));
            }
            Err(format!(
                "empty-roots-e2e did not complete online roots apply and distributed force-find; node_a_status={last_status_a}; node_b_status={last_status_b}"
            ))
        },
    )
}

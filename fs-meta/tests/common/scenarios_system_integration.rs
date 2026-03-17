use super::*;
use std::sync::atomic::{AtomicI64, Ordering};

fn next_release_generation() -> i64 {
    static NEXT: AtomicI64 = AtomicI64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

fn wait_until<F>(timeout: Duration, label: &str, mut check: F) -> Result<(), String>
where
    F: FnMut() -> Result<bool, String>,
{
    let deadline = Instant::now() + timeout;
    let mut last_err = String::new();
    loop {
        match check() {
            Ok(true) => return Ok(()),
            Ok(false) => {}
            Err(e) => last_err = e,
        }
        if Instant::now() > deadline {
            return Err(format!("timeout waiting for {label}: {last_err}"));
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn managed_count_with_prefix(status: &Value, prefix: &str) -> Result<usize, String> {
    let managed = status
        .get("daemon")
        .and_then(|v| v.get("managed_processes"))
        .and_then(Value::as_array)
        .ok_or_else(|| format!("status missing daemon.managed_processes: {status}"))?;
    Ok(managed
        .iter()
        .filter(|row| {
            row.get("instance_id")
                .and_then(Value::as_str)
                .is_some_and(|id| id.starts_with(prefix))
        })
        .count())
}

fn bump_entry_release_generation(entry: &mut Value) {
    let Some(policy) = entry.get_mut("policy").and_then(Value::as_object_mut) else {
        return;
    };
    let current = policy
        .get("generation")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    policy.insert("generation".to_string(), Value::from(current + 1));
}

fn relation_target_entrypoint_is_node_b(c: &ClusterContext, target_id: &str) -> bool {
    let mut members = [
        c.node_a_identity.node_id.as_str(),
        c.node_b_identity.node_id.as_str(),
    ];
    members.sort_unstable();
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in target_id.bytes() {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    members[(hash as usize) % members.len()] == c.node_b_identity.node_id
}

fn relation_target_ok(
    c: &ClusterContext,
    target_id: &str,
    command: Value,
) -> Result<Value, String> {
    let send = |cmd: Value| {
        if relation_target_entrypoint_is_node_b(c, target_id) {
            c.ctl_ok_b_local(cmd)
        } else {
            c.ctl_ok_a_local(cmd)
        }
    };
    let mut last_err = String::new();
    for _ in 0..6 {
        match send(command.clone()) {
            Ok(v) => return Ok(v),
            Err(err)
                if err.contains("relation-target replication quorum not reached")
                    || err.contains("cluster not ready for remote control propagation")
                    || err.contains("cluster not ready for forwarded control execution") =>
            {
                last_err = err;
                thread::sleep(Duration::from_millis(250));
            }
            Err(err) => return Err(err),
        }
    }
    Err(last_err)
}

fn release_apply_single_with_retry(c: &ClusterContext, mut entry: Value) -> Result<Value, String> {
    for _ in 0..4 {
        let target_id = entry
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or("target")
            .to_string();
        let target_generation = entry
            .get("policy")
            .and_then(|v| v.get("generation"))
            .and_then(Value::as_i64)
            .unwrap_or(1);
        let command = json!({
            "command": "relation_target_apply",
            "intent": {
                "schema_version": "scope-unit-intent-v1",
                "target_id": target_id,
                "target_generation": target_generation,
                "units": [{
                    "unit_id": entry.get("id").and_then(Value::as_str).unwrap_or("unit"),
                    "scope_ids": [],
                    "startup": {
                        "path": entry.get("app").and_then(Value::as_str).unwrap_or_default(),
                        "manifest": entry.get("manifest").cloned().unwrap_or(Value::Null),
                    },
                    "config": entry.get("config").cloned().unwrap_or_else(|| json!({})),
                    "runtime": entry.get("runtime").cloned().unwrap_or_else(|| json!({})),
                    "policy": entry.get("policy").cloned().unwrap_or_else(|| json!({})),
                    "restart_policy": entry
                        .get("restart_policy")
                        .cloned()
                        .unwrap_or_else(|| Value::String("Never".to_string())),
                    "version": entry
                        .get("version")
                        .cloned()
                        .unwrap_or_else(|| Value::String("0.0.1".to_string())),
                }]
            }
        });
        let reply = match relation_target_ok(c, &target_id, command) {
            Ok(v) => v,
            Err(err) if err.contains("policy conflict for app") => {
                bump_entry_release_generation(&mut entry);
                continue;
            }
            Err(err) => return Err(err),
        };
        if reply.get("ok").and_then(Value::as_bool) == Some(true) {
            return Ok(reply);
        }
        return Err(format!(
            "release_apply single-entry call did not return ok=true: {reply}"
        ));
    }
    Err("release_apply single-entry retry budget exhausted after policy conflict".to_string())
}

fn release_apply_entries(c: &ClusterContext, entries: Vec<Value>) -> Result<Value, String> {
    let mut applied = 0usize;
    for entry in entries {
        let reply = release_apply_single_with_retry(c, entry)?;
        if reply.get("ok").and_then(Value::as_bool) != Some(true) {
            return Err(format!(
                "relation_target_apply single-entry call did not return ok=true: {reply}"
            ));
        }
        applied += 1;
    }
    Ok(json!({
        "ok": true,
        "applied": applied,
        "mode": "relation-target-apply-sequential",
    }))
}

fn release_apply_entry(
    c: &ClusterContext,
    app_path: &str,
    instance_id: &str,
    replicas: i64,
    _strategy: &str,
    budget_max_replicas: i64,
) -> Result<Value, String> {
    release_apply_entries(
        c,
        vec![json!({
            "id": instance_id,
            "app": app_path,
            "manifest": "fs-meta/fixtures/manifests/capanix-app-test-runtime.yaml",
            "version": "0.0.1",
            "restart_policy": "Never",
            "config": {},
            "runtime": {},
            "policy": {
                "generation": next_release_generation(),
                "replicas": replicas,
                "budget": { "max_replicas": budget_max_replicas }
            }
        })],
    )
}

fn release_clear(c: &ClusterContext, instance_id: &str) -> Result<Value, String> {
    let command = json!({
        "command": "relation_target_clear",
        "target_id": instance_id,
    });
    relation_target_ok(c, instance_id, command)
}

fn release_policy_entry(
    app_path: &str,
    instance_id: &str,
    replicas: i64,
    _strategy: &str,
    budget_max_replicas: i64,
) -> Value {
    json!({
        "id": instance_id,
        "app": app_path,
        "manifest": "fs-meta/fixtures/manifests/capanix-app-test-runtime.yaml",
        "version": "0.0.1",
        "restart_policy": "Never",
        "config": {},
        "runtime": {},
        "policy": {
            "generation": next_release_generation(),
            "replicas": replicas,
            "budget": { "max_replicas": budget_max_replicas }
        }
    })
}

pub(crate) fn scenario_end_to_end_bootstrap() -> Result<(), String> {
    with_cluster(
        FULL_NODE_DELEGATION_SCOPES,
        FULL_NODE_DELEGATION_SCOPES,
        |c| {
            let status = c.layered_status_a_local()?;
            ensure_layered_local_status_shape(&status)?;
            let cluster = c.cluster_state_a()?;
            if status_node_count(&cluster) < 2 {
                return Err(format!("expected converged cluster, got: {cluster}"));
            }
            Ok(())
        },
    )
}

pub(crate) fn scenario_logical_single_kernel_semantics() -> Result<(), String> {
    with_cluster(
        FULL_NODE_DELEGATION_SCOPES,
        FULL_NODE_DELEGATION_SCOPES,
        |c| {
            let Some(test_app) = try_find_test_app_cdylib() else {
                return Err(
                    "test app runtime path not found; set CAPANIX_TEST_APP_BINARY or build capanix-app-test-runtime"
                        .to_string(),
                );
            };
            let app_path = test_app.to_string_lossy().to_string();
            let instance_id = format!("logical-single-kernel-{}", unique_suffix());

            let apply = release_apply_entry(c, &app_path, &instance_id, 1, "pack", 1)?;
            if apply.get("ok").and_then(Value::as_bool) != Some(true) {
                return Err(format!(
                    "logical single-kernel release_apply did not return ok=true: {apply}"
                ));
            }

            let mut last_status_a = serde_json::json!(null);
            let mut last_status_b = serde_json::json!(null);
            let mut last_cfg_a = serde_json::json!(null);
            let mut last_cfg_b = serde_json::json!(null);

            let result = wait_until(
                Duration::from_secs(40),
                "logical single-kernel release convergence",
                || {
                    let status_a = c.layered_status_a_local()?;
                    let status_b = c.layered_status_b_local()?;
                    let cfg_a = c.ctl_ok_a_local(json!({ "command": "config_get" }))?;
                    let cfg_b = c.ctl_ok_b_local(json!({ "command": "config_get" }))?;
                    last_status_a = status_a.clone();
                    last_status_b = status_b.clone();
                    last_cfg_a = cfg_a.clone();
                    last_cfg_b = cfg_b.clone();

                    let active_processes_a = status_a
                        .get("metrics")
                        .and_then(|v| v.get("active_processes"))
                        .and_then(Value::as_u64)
                        .unwrap_or(0);
                    let active_processes_b = status_b
                        .get("metrics")
                        .and_then(|v| v.get("active_processes"))
                        .and_then(Value::as_u64)
                        .unwrap_or(0);
                    if active_processes_a + active_processes_b == 0 {
                        return Ok(false);
                    }

                    let records_a = cfg_a
                        .get("runtime_target_state")
                        .and_then(|v| v.get("records"))
                        .and_then(Value::as_array)
                        .cloned()
                        .unwrap_or_default();
                    let records_b = cfg_b
                        .get("runtime_target_state")
                        .and_then(|v| v.get("records"))
                        .and_then(Value::as_array)
                        .cloned()
                        .unwrap_or_default();
                    let has_decl_a = records_a.iter().any(|row| {
                        row.get("target_id").and_then(Value::as_str) == Some(instance_id.as_str())
                    });
                    let has_decl_b = records_b.iter().any(|row| {
                        row.get("target_id").and_then(Value::as_str) == Some(instance_id.as_str())
                    });
                    if !has_decl_a || !has_decl_b {
                        return Ok(false);
                    }
                    let apps_a = cfg_a
                        .get("apps")
                        .and_then(Value::as_array)
                        .map(|apps| apps.len())
                        .unwrap_or(0);
                    let apps_b = cfg_b
                        .get("apps")
                        .and_then(Value::as_array)
                        .map(|apps| apps.len())
                        .unwrap_or(0);
                    if apps_a == 0 || apps_a != apps_b {
                        return Ok(false);
                    }
                    Ok(true)
                },
            );

            let _ = release_clear(c, &instance_id);
            result.map_err(|e| {
                format!(
                    "{e}; apply={apply}; status_a={last_status_a}; status_b={last_status_b}; cfg_a={last_cfg_a}; cfg_b={last_cfg_b}"
                )
            })
        },
    )
}

pub(crate) fn scenario_in_process_kernel_hosting() -> Result<(), String> {
    with_cluster(
        FULL_NODE_DELEGATION_SCOPES,
        FULL_NODE_DELEGATION_SCOPES,
        |c| {
            let before = c.cluster_state_a()?;
            thread::sleep(Duration::from_millis(200));
            let after = c.cluster_state_a()?;
            let before_up = before["nodes"][0]["health"]["uptime_secs"]
                .as_u64()
                .unwrap_or(0);
            let after_up = after["nodes"][0]["health"]["uptime_secs"]
                .as_u64()
                .unwrap_or(0);
            if after_up < before_up {
                return Err(format!(
                    "uptime regressed: before={before_up} after={after_up}"
                ));
            }
            Ok(())
        },
    )
}

pub(crate) fn scenario_decoupled_client_queries_no_restart() -> Result<(), String> {
    with_cluster(
        FULL_NODE_DELEGATION_SCOPES,
        FULL_NODE_DELEGATION_SCOPES,
        |c| {
            let pid_a = c.node_a_pid();
            let pid_b = c.node_b_pid();
            for _ in 0..3 {
                let status_a = c.layered_status_a_local()?;
                ensure_layered_local_status_shape(&status_a)?;
                let status_b = c.layered_status_b_local()?;
                ensure_layered_local_status_shape(&status_b)?;
            }
            if c.node_a_pid() != pid_a || c.node_b_pid() != pid_b {
                return Err("daemon pid changed during control-client status queries".to_string());
            }
            Ok(())
        },
    )
}

pub(crate) fn scenario_config_submission_path() -> Result<(), String> {
    with_cluster(
        FULL_NODE_DELEGATION_SCOPES,
        FULL_NODE_DELEGATION_SCOPES,
        |c| {
            let mut cfg = c.ctl_ok_a_local(json!({ "command": "config_get" }))?;
            cfg["route_plans"] = json!([{
                "route_key": format!("route.config-path:{}:v1", unique_suffix()),
                "mode": "local",
                "peers": [],
            }]);

            let err = c.ctl_err_code_a(
                json!({
                    "command": "config_set",
                    "config": cfg,
                }),
                signed_auth_for_command(
                    &json!({
                        "command": "config_set",
                        "config": cfg,
                    }),
                    FULL_NODE_DELEGATION_SCOPES,
                    next_auth_seq_a(),
                    &format!("config-set-route-plans-{}", unique_suffix()),
                    "local-admin-ed25519-1",
                ),
                local_target(),
            )?;
            if err != "invalid_request" {
                return Err(format!(
                    "config_set with route_plans must fail invalid_request after hard-cut, got {err}"
                ));
            }
            Ok(())
        },
    )
}

pub(crate) fn scenario_early_remote_operability() -> Result<(), String> {
    with_cluster(
        FULL_NODE_DELEGATION_SCOPES,
        FULL_NODE_DELEGATION_SCOPES,
        |c| {
            let cfg = c.ctl_ok_a_local(json!({ "command": "config_get" }))?;
            if cfg
                .get("apps")
                .and_then(Value::as_array)
                .is_some_and(|v| !v.is_empty())
            {
                return Err(format!("expected zero apps in startup config: {cfg}"));
            }
            let status_a = c.cluster_state_a()?;
            let status_b = c.cluster_state_b()?;
            let a_reaches_b = status_has_reachable_node(&status_a, &c.node_b_identity.node_id);
            let b_control_ready = status_has_reachable_node(&status_b, &c.node_b_identity.node_id);
            if !a_reaches_b || !b_control_ready {
                return Err(format!(
                    "expected remote operability with zero channels; node-a={status_a} node-b={status_b}"
                ));
            }
            Ok(())
        },
    )
}

pub(crate) fn scenario_cluster_bootstrap_governance() -> Result<(), String> {
    with_cluster(
        FULL_NODE_DELEGATION_SCOPES,
        FULL_NODE_DELEGATION_SCOPES,
        |c| c.assert_fail_closed_node_without_identity(),
    )
}

pub(crate) fn scenario_peer_membership_change_discipline() -> Result<(), String> {
    with_cluster(
        FULL_NODE_DELEGATION_SCOPES,
        FULL_NODE_DELEGATION_SCOPES,
        |c| {
            let peer_node_id = c.node_b_identity.node_id.clone();
            let mut cfg = c.ctl_ok_a_local(json!({ "command": "config_get" }))?;
            let peers_before = cfg["management_peers"]
                .as_array()
                .ok_or_else(|| format!("config_dump missing management_peers: {cfg}"))?;
            if !peers_before
                .iter()
                .any(|p| p["node_id"].as_str() == Some(peer_node_id.as_str()))
            {
                return Err(format!(
                    "expected peer {} in initial management_peers: {cfg}",
                    peer_node_id
                ));
            }

            cfg["management_peers"] = json!(peers_before
                .iter()
                .filter(|p| p["node_id"].as_str() != Some(peer_node_id.as_str()))
                .cloned()
                .collect::<Vec<_>>());

            let node_keys = cfg["trust_bundle"]["node_keys"]
                .as_array_mut()
                .ok_or_else(|| "config_dump missing trust_bundle.node_keys".to_string())?;
            let mut revoked = false;
            for k in node_keys.iter_mut() {
                if k["principal_id"].as_str() == Some(peer_node_id.as_str()) {
                    k["state"] = json!("Revoked");
                    revoked = true;
                }
            }
            if !revoked {
                return Err(format!(
                    "missing node trust key for peer {} in config: {cfg}",
                    peer_node_id
                ));
            }

            let apply = c.ctl_ok_a_local(json!({
                "command": "config_set",
                "config": cfg,
            }))?;
            if apply.get("ok").and_then(Value::as_bool) != Some(true) {
                return Err(format!(
                    "peer membership change request did not return ok=true: {apply}"
                ));
            }

            let after = c.ctl_ok_a_local(json!({ "command": "config_get" }))?;
            let peer_left = after["management_peers"].as_array().is_some_and(|arr| {
                arr.iter()
                    .any(|p| p["node_id"].as_str() == Some(peer_node_id.as_str()))
            });
            if peer_left {
                return Err(format!(
                    "relation-target peer removal did not converge for {}: {after}",
                    peer_node_id
                ));
            }

            let key_state = after["trust_bundle"]["node_keys"]
                .as_array()
                .and_then(|arr| {
                    arr.iter()
                        .find(|k| k["principal_id"].as_str() == Some(peer_node_id.as_str()))
                        .and_then(|k| k["state"].as_str())
                });
            if key_state != Some("Revoked") {
                return Err(format!(
                    "trust-relation key revocation did not converge for {}: {after}",
                    peer_node_id
                ));
            }
            Ok(())
        },
    )
}

pub(crate) fn scenario_local_remote_semantic_equivalence() -> Result<(), String> {
    with_cluster(
        FULL_NODE_DELEGATION_SCOPES,
        FULL_NODE_DELEGATION_SCOPES,
        |c| {
            let local_status = c.layered_status_a_local()?;
            ensure_layered_local_status_shape(&local_status)?;
            let remote_status = c.layered_status_a_target(json!({
                "scope": "node",
                "node_id": c.node_b_identity.node_id,
            }))?;
            ensure_layered_local_status_shape(&remote_status)?;
            let local_status_b = c.layered_status_b_local()?;
            ensure_layered_local_status_shape(&local_status_b)?;
            if remote_status.get("cluster") != local_status_b.get("cluster") {
                return Err(format!(
                    "forwarded remote layered status must preserve target cluster projection; remote={} local_b={}",
                    remote_status["cluster"], local_status_b["cluster"]
                ));
            }
            if remote_status.get("metrics") != local_status_b.get("metrics") {
                return Err(format!(
                    "forwarded remote layered status must preserve target metrics projection; remote={} local_b={}",
                    remote_status["metrics"], local_status_b["metrics"]
                ));
            }
            if remote_status.get("daemon") != local_status_b.get("daemon") {
                return Err(format!(
                    "forwarded remote layered status must preserve target daemon projection; remote={} local_b={}",
                    remote_status["daemon"], local_status_b["daemon"]
                ));
            }

            // Local vs remote config_get should preserve shape parity while node_id reflects target node.
            let config_cmd = json!({ "command": "config_get" });
            let local_cfg = c.ctl_ok_a_local(config_cmd.clone())?;
            if local_cfg.get("node_id").and_then(Value::as_str)
                != Some(c.node_a_identity.node_id.as_str())
            {
                return Err(format!(
                    "local config_get should report node-a id={}, got: {local_cfg}",
                    c.node_a_identity.node_id
                ));
            }
            if !local_cfg
                .get("runtime_target_state")
                .is_some_and(Value::is_object)
            {
                return Err(format!(
                    "local config_get missing runtime_target_state object: {local_cfg}"
                ));
            }
            let seq_remote_cfg = next_auth_seq_a();
            let nonce_remote_cfg = format!("{}-a-remote-config", unique_suffix());
            let auth_remote_cfg = signed_auth_for_command(
                &config_cmd,
                FULL_NODE_DELEGATION_SCOPES,
                seq_remote_cfg,
                &nonce_remote_cfg,
                "local-admin-ed25519-1",
            );
            let remote_cfg = c.ctl_ok_a(
                config_cmd,
                auth_remote_cfg,
                json!({
                    "scope": "node",
                    "node_id": c.node_b_identity.node_id,
                }),
            )?;
            if !remote_cfg.is_object() {
                return Err(format!(
                    "remote config_get must return object payload, got: {remote_cfg}"
                ));
            }
            if !remote_cfg
                .get("runtime_target_state")
                .is_some_and(Value::is_object)
            {
                return Err(format!(
                    "remote config_get missing runtime_target_state object: {remote_cfg}"
                ));
            }
            if remote_cfg.get("runtime_target_state") != local_cfg.get("runtime_target_state") {
                return Err(format!(
                    "local/remote config_get should project same runtime declarations; local={} remote={}",
                    local_cfg["runtime_target_state"], remote_cfg["runtime_target_state"]
                ));
            }

            // Local vs remote channel_list should both return list-shaped payloads.
            let channel_cmd = json!({ "command": "channel_list" });
            let local_channels = c.ctl_ok_a_local(channel_cmd.clone())?;
            if !local_channels.is_array() {
                return Err(format!(
                    "local channel_list must return array payload, got: {local_channels}"
                ));
            }
            let seq_remote_channel = next_auth_seq_a();
            let nonce_remote_channel = format!("{}-a-remote-channel-list", unique_suffix());
            let auth_remote_channel = signed_auth_for_command(
                &channel_cmd,
                FULL_NODE_DELEGATION_SCOPES,
                seq_remote_channel,
                &nonce_remote_channel,
                "local-admin-ed25519-1",
            );
            let remote_channels = c.ctl_ok_a(
                channel_cmd,
                auth_remote_channel,
                json!({
                    "scope": "node",
                    "node_id": c.node_b_identity.node_id,
                }),
            )?;
            if !remote_channels.is_array() {
                return Err(format!(
                    "remote channel_list must return array payload, got: {remote_channels}"
                ));
            }

            let kill_cmd = json!({
                "command": "tx_execute",
                "tx": {
                    "tx_id": format!("tx-kill-compare-{}", unique_suffix()),
                    "steps": [{ "kind": "proc_kill", "pid": 424242u32 }]
                }
            });

            let seq_local_err = next_auth_seq_a();
            let nonce_local_err = format!("{}-a-local-err", unique_suffix());
            let auth_local_err = signed_auth_for_command(
                &kill_cmd,
                FULL_NODE_DELEGATION_SCOPES,
                seq_local_err,
                &nonce_local_err,
                "local-admin-ed25519-1",
            );
            let local_result = c.ctl_ok_a(kill_cmd.clone(), auth_local_err, local_target())?;

            let seq_remote_err = next_auth_seq_a();
            let nonce_remote_err = format!("{}-a-remote-err", unique_suffix());
            let auth_remote_err = signed_auth_for_command(
                &kill_cmd,
                FULL_NODE_DELEGATION_SCOPES,
                seq_remote_err,
                &nonce_remote_err,
                "local-admin-ed25519-1",
            );
            let remote_result = c.ctl_ok_a(
                kill_cmd,
                auth_remote_err,
                json!({
                    "scope": "node",
                    "node_id": c.node_b_identity.node_id,
                }),
            )?;

            if local_result.get("status").and_then(Value::as_str) != Some("aborted")
                || remote_result.get("status").and_then(Value::as_str) != Some("aborted")
            {
                return Err(format!(
                    "expected equivalent aborted tx_precondition result for local vs remote missing proc_kill, local={local_result} remote={remote_result}"
                ));
            }
            if local_result.get("reason") != remote_result.get("reason") {
                return Err(format!(
                    "expected identical deterministic abort reason for local and remote missing proc_kill, local={local_result} remote={remote_result}"
                ));
            }
            Ok(())
        },
    )
}

pub(crate) fn scenario_multi_node_policy_variance() -> Result<(), String> {
    with_cluster(
        FULL_NODE_DELEGATION_SCOPES,
        FULL_NODE_DELEGATION_SCOPES,
        |c| {
            let Some(test_app) = try_find_test_app_cdylib() else {
                return Err(
                    "test app runtime path not found; set CAPANIX_TEST_APP_BINARY or build capanix-app-test-runtime"
                        .to_string(),
                );
            };
            let app_path = test_app.to_string_lossy().to_string();
            let fast_id = format!("policy-variance-fast-{}", unique_suffix());
            let weighted_id = format!("policy-variance-weighted-{}", unique_suffix());
            let apply = release_apply_entries(
                c,
                vec![
                    release_policy_entry(&app_path, &fast_id, 1, "pack", 1),
                    release_policy_entry(&app_path, &weighted_id, 2, "spread", 2),
                ],
            )?;
            if apply.get("ok").and_then(Value::as_bool) != Some(true) {
                return Err(format!(
                    "multi-node policy variance release_apply did not return ok=true: {apply}"
                ));
            }

            wait_until(
                Duration::from_secs(20),
                "dual-node control health after policy variance apply",
                || {
                    let status_a = c.layered_status_a_local()?;
                    ensure_layered_local_status_shape(&status_a)?;
                    let status_b = c.layered_status_b_local()?;
                    ensure_layered_local_status_shape(&status_b)?;
                    Ok(true)
                },
            )?;
            Ok(())
        },
    )
}

pub(crate) fn scenario_node_failure_recovery_realization_convergence() -> Result<(), String> {
    with_cluster(
        FULL_NODE_DELEGATION_SCOPES,
        FULL_NODE_DELEGATION_SCOPES,
        |c| {
            let Some(test_app) = try_find_test_app_cdylib() else {
                return Err(
                    "test app runtime path not found; set CAPANIX_TEST_APP_BINARY or build capanix-app-test-runtime"
                        .to_string(),
                );
            };
            let app_path = test_app.to_string_lossy().to_string();
            let instance_id = format!("recovery-{}", unique_suffix());
            let expected_replicas = 2usize;
            let apply = release_apply_entry(
                c,
                &app_path,
                &instance_id,
                expected_replicas as i64,
                "spread",
                4,
            )?;
            if apply.get("ok").and_then(Value::as_bool) != Some(true) {
                return Err(format!(
                    "initial recovery release_apply did not return ok=true: {apply}"
                ));
            }

            wait_until(
                Duration::from_secs(20),
                "initial replica convergence before node failure",
                || {
                    let status = c.layered_status_a_local()?;
                    Ok(managed_count_with_prefix(&status, &instance_id)? == expected_replicas)
                },
            )?;

            let kill_status = Command::new("kill")
                .arg("-9")
                .arg(c.node_b_pid().to_string())
                .status()
                .map_err(|e| format!("failed to execute kill for node-b: {e}"))?;
            if !kill_status.success() {
                return Err(format!(
                    "kill -9 for node-b failed with status: {kill_status}"
                ));
            }

            wait_until(
                Duration::from_secs(20),
                "node-b local control unavailability",
                || match c.layered_status_b_local() {
                    Ok(_) => Ok(false),
                    Err(_) => Ok(true),
                },
            )?;

            let all_nodes = vec![&c.node_a_identity, &c.node_b_identity];
            let seeds = vec![c.addr_a.clone()];
            let cfg_b = build_cluster_config(
                &c.node_b_identity,
                &all_nodes,
                &seeds,
                &[(c.node_a_identity.node_id.clone(), c.addr_a.clone())],
                &c.admin_pub_b64,
                FULL_NODE_DELEGATION_SCOPES,
            );
            let mut recovered_node_b = RunningNode::start(
                &c.capanixd_bin,
                &c.node_b_identity,
                &cfg_b,
                &c.addr_b,
                true,
                true,
            )
            .map_err(|e| format!("node-b restart failed: {e}"))?;
            recovered_node_b
                .wait_for_socket(control_socket_startup_timeout())
                .map_err(|e| format!("node-b restart socket failed: {e}"))?;

            wait_until(
                Duration::from_secs(60),
                "node-b membership recovery",
                || {
                    let status_a = c.cluster_state_a()?;
                    let status_b = request_cluster_state(&recovered_node_b.socket_path)?;
                    let a_reaches_b =
                        status_has_reachable_node(&status_a, &c.node_b_identity.node_id);
                    let b_reaches_a =
                        status_has_reachable_node(&status_b, &c.node_a_identity.node_id);
                    Ok(a_reaches_b && b_reaches_a)
                },
            )?;

            wait_until(
                Duration::from_secs(40),
                "realization convergence after node recovery",
                || {
                    let status = c.layered_status_a_local()?;
                    Ok(managed_count_with_prefix(&status, &instance_id)? == expected_replicas)
                },
            )?;
            let _ = release_clear(c, &instance_id);
            Ok(())
        },
    )
}

pub(crate) fn scenario_policy_update_reload_regression() -> Result<(), String> {
    with_cluster(
        FULL_NODE_DELEGATION_SCOPES,
        FULL_NODE_DELEGATION_SCOPES,
        |c| {
            let Some(test_app) = try_find_test_app_cdylib() else {
                return Err(
                    "test app runtime path not found; set CAPANIX_TEST_APP_BINARY or build capanix-app-test-runtime"
                        .to_string(),
                );
            };
            let app_path = test_app.to_string_lossy().to_string();
            let instance_id = format!("policy-reload-{}", unique_suffix());

            let apply_1 = release_apply_entry(c, &app_path, &instance_id, 1, "pack", 8)?;
            if apply_1.get("ok").and_then(Value::as_bool) != Some(true) {
                return Err(format!("policy reload step-1 failed: {apply_1}"));
            }
            wait_until(
                Duration::from_secs(20),
                "policy reload step-1 convergence",
                || {
                    let status = c.layered_status_a_local()?;
                    Ok(managed_count_with_prefix(&status, &instance_id)? == 1)
                },
            )?;

            let apply_2 = release_apply_entry(c, &app_path, &instance_id, 1, "spread", 6)?;
            if apply_2.get("ok").and_then(Value::as_bool) != Some(true) {
                return Err(format!("policy reload step-2 failed: {apply_2}"));
            }
            wait_until(
                Duration::from_secs(20),
                "policy reload step-2 convergence",
                || {
                    let status = c.layered_status_a_local()?;
                    Ok(managed_count_with_prefix(&status, &instance_id)? == 1)
                },
            )?;

            let apply_3 = release_apply_entry(c, &app_path, &instance_id, 1, "pack", 4)?;
            if apply_3.get("ok").and_then(Value::as_bool) != Some(true) {
                return Err(format!("policy reload step-3 failed: {apply_3}"));
            }
            wait_until(
                Duration::from_secs(20),
                "policy reload step-3 convergence",
                || {
                    let status = c.layered_status_a_local()?;
                    Ok(managed_count_with_prefix(&status, &instance_id)? == 1)
                },
            )?;

            let status = c.layered_status_a_local()?;
            ensure_layered_local_status_shape(&status)?;
            let _ = release_clear(c, &instance_id);
            Ok(())
        },
    )
}

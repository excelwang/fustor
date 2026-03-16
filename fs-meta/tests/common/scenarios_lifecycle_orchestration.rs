use super::*;

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

fn release_apply_one_cardinality(
    c: &ClusterContext,
    app_path: &str,
    app_id: &str,
    version: &str,
) -> Result<Value, String> {
    release_apply_one_cardinality_with_generation(c, app_path, app_id, version, 1)
}

fn release_apply_one_cardinality_with_generation(
    c: &ClusterContext,
    app_path: &str,
    app_id: &str,
    version: &str,
    generation: u64,
) -> Result<Value, String> {
    relation_target_ok(
        c,
        app_id,
        json!({
            "command": "relation_target_apply",
            "intent": {
                "schema_version": "scope-unit-intent-v1",
                "target_id": app_id,
                "target_generation": generation,
                "units": [{
                    "unit_id": app_id,
                    "scope_ids": [],
                    "startup": {
                        "path": app_path,
                        "manifest": "fs-meta/fixtures/manifests/capanix-app-test-runtime.yaml"
                    },
                    "config": {},
                    "runtime": {},
                    "policy": {
                        "generation": generation,
                    },
                    "restart_policy": "Never",
                    "version": version
                }]
            }
        }),
    )
}

fn release_clear(c: &ClusterContext, app_id: &str) -> Result<Value, String> {
    relation_target_ok(
        c,
        app_id,
        json!({
            "command": "relation_target_clear",
            "target_id": app_id,
        }),
    )
}

fn managed_pid_for_instance(status: &Value, instance_id: &str) -> Option<u64> {
    let rows = status
        .get("daemon")
        .and_then(|v| v.get("managed_processes"))
        .and_then(Value::as_array)?;
    rows.iter()
        .find(|row| row.get("instance_id").and_then(Value::as_str) == Some(instance_id))
        .and_then(|row| row.get("pid").and_then(Value::as_u64))
}

fn managed_pids_for_instance(status: &Value, instance_id: &str) -> std::collections::BTreeSet<u64> {
    status
        .get("daemon")
        .and_then(|v| v.get("managed_processes"))
        .and_then(Value::as_array)
        .map(|rows| {
            rows.iter()
                .filter(|row| row.get("instance_id").and_then(Value::as_str) == Some(instance_id))
                .filter_map(|row| row.get("pid").and_then(Value::as_u64))
                .collect()
        })
        .unwrap_or_default()
}

pub(crate) fn scenario_no_implicit_binding_after_config_load() -> Result<(), String> {
    with_cluster(
        FULL_NODE_DELEGATION_SCOPES,
        FULL_NODE_DELEGATION_SCOPES,
        |c| {
            let baseline_channels = c.ctl_ok_a_local(json!({ "command": "channel_list" }))?;
            let mut cfg = c.ctl_ok_a_local(json!({ "command": "config_get" }))?;
            cfg["route_plans"] = json!([{
                "route_key": format!("route.translation:{}:v1", unique_suffix()),
                "mode": "local",
                "peers": [],
            }]);
            let err = c.ctl_err_code_a(
                json!({
                    "command": "config_set",
                    "config": cfg
                }),
                signed_auth_for_command(
                    &json!({
                        "command": "config_set",
                        "config": cfg
                    }),
                    FULL_NODE_DELEGATION_SCOPES,
                    next_auth_seq_a(),
                    &format!("route-translation-hard-cut-{}", unique_suffix()),
                    "local-admin-ed25519-1",
                ),
                local_target(),
            )?;
            if err != "invalid_request" {
                return Err(format!(
                    "route_plans hard-cut must reject config_set with invalid_request, got {err}"
                ));
            }
            let channels = c.ctl_ok_a_local(json!({ "command": "channel_list" }))?;
            if channels != baseline_channels {
                return Err(format!(
                    "rejected route_plans config_set must not mutate channel inventory: before={baseline_channels} after={channels}"
                ));
            }
            let leaked = channels.as_array().is_some_and(|rows| {
                rows.iter().any(|row| {
                    row.get("route_key")
                        .and_then(Value::as_str)
                        .is_some_and(|rk| rk.contains("route.translation:"))
                        || row
                            .get("route_key")
                            .and_then(|v| v.get("0"))
                            .and_then(Value::as_str)
                            .is_some_and(|rk| rk.contains("route.translation:"))
                })
            });
            if leaked {
                return Err(format!(
                    "route translation must not create implicit bindings/channels for translated route: {channels}"
                ));
            }
            Ok(())
        },
    )
}

pub(crate) fn scenario_binding_authority_consistency() -> Result<(), String> {
    with_cluster(
        FULL_NODE_DELEGATION_SCOPES,
        FULL_NODE_DELEGATION_SCOPES,
        |c| {
            // Produce an explicit denied control attempt.
            let denied_cmd = json!({
                "command": "limit_set",
                "key": "max_channels",
                "value": 91u64
            });
            let denied_auth = signed_auth_for_command(
                &denied_cmd,
                &["cluster_read"],
                309,
                "binding-authority-denied",
                "local-admin-ed25519-1",
            );
            let denied = c.ctl_err_code_a(denied_cmd, denied_auth, cluster_target())?;
            if denied != "scope_denied" && denied != "access_denied" {
                return Err(format!(
                    "expected denied control attempt before authorized mutation, got {denied}"
                ));
            }

            // Authorized privileged-mutation transaction must leave an auditable mutation trail.
            let auth_cmd = json!({
                "command": "limit_set",
                "key": "max_channels",
                "value": 92u64
            });
            let auth = signed_auth_for_command(
                &auth_cmd,
                &["config_write"],
                310,
                "binding-authority-control",
                "local-admin-ed25519-1",
            );
            let _ = c.ctl_ok_a(auth_cmd, auth, cluster_target())?;

            let audit = c.ctl_ok_a_local(json!({ "command": "audit_tail", "count": 64usize }))?;
            let entries = audit
                .as_array()
                .ok_or_else(|| format!("audit payload is not an array: {audit}"))?;
            let has_denied = entries
                .iter()
                .any(|e| e.get("event_type").and_then(Value::as_str) == Some("ControlDenied"));
            let has_changed = entries
                .iter()
                .any(|e| e.get("event_type").and_then(Value::as_str) == Some("ConfigChanged"));
            if !has_denied || !has_changed {
                return Err(format!(
                    "binding-authority consistency requires denied+authorized auditable events: {audit}"
                ));
            }
            Ok(())
        },
    )
}

pub(crate) fn scenario_tx_abort_no_partial_effect() -> Result<(), String> {
    with_cluster(
        FULL_NODE_DELEGATION_SCOPES,
        FULL_NODE_DELEGATION_SCOPES,
        |c| {
            let before = c.ctl_ok_a_local(json!({ "command": "channel_list" }))?;
            let tx = c.ctl_ok_a_local(json!({
                "command": "tx_execute",
                "tx": {
                    "tx_id": format!("tx-abort-{}", unique_suffix()),
                    "steps": [{ "kind": "proc_kill", "pid": 999999u32 }]
                }
            }))?;
            if tx.get("status").and_then(Value::as_str) != Some("aborted") {
                return Err(format!(
                    "expected aborted tx_precondition for missing proc_kill, got: {tx}"
                ));
            }
            if tx.get("reason").and_then(Value::as_str).is_none() {
                return Err(format!(
                    "missing proc_kill abort must expose deterministic reason, got: {tx}"
                ));
            }
            let after = c.ctl_ok_a_local(json!({ "command": "channel_list" }))?;
            if before != after {
                return Err(format!(
                    "idempotent tx leaked partial mutation: before={before} after={after}"
                ));
            }
            Ok(())
        },
    )
}

pub(crate) fn scenario_tx_spawn_abort_deterministic() -> Result<(), String> {
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
            let app_id = format!("upgrade-redeclare-{}", unique_suffix());

            let released_v1 = release_apply_one_cardinality(c, &app_path, &app_id, "1.0.0")?;
            if released_v1.get("ok").and_then(Value::as_bool) != Some(true) {
                return Err(format!("initial release_apply failed: {released_v1}"));
            }

            let mut initial_pids = std::collections::BTreeSet::new();
            let mut stable_polls = 0usize;
            wait_until(
                Duration::from_secs(20),
                "initial runtime convergence for release_apply",
                || {
                    let status_a = c.layered_status_a_local()?;
                    let status_b = c.layered_status_b_local()?;
                    let mut current = managed_pids_for_instance(&status_a, &app_id);
                    current.extend(managed_pids_for_instance(&status_b, &app_id));
                    if current.is_empty() {
                        stable_polls = 0;
                        return Ok(false);
                    }
                    if current == initial_pids {
                        stable_polls += 1;
                    } else {
                        initial_pids = current;
                        stable_polls = 1;
                    }
                    Ok(stable_polls >= 3)
                },
            )?;
            if initial_pids.is_empty() {
                return Err(
                    "missing initial managed pid set after release_apply convergence".to_string(),
                );
            }

            let released_v2 =
                release_apply_one_cardinality_with_generation(c, &app_path, &app_id, "2.0.0", 2)?;
            if released_v2.get("ok").and_then(Value::as_bool) != Some(true) {
                return Err(format!("reapply release_apply failed: {released_v2}"));
            }

            let mut restarted = false;
            let mut last_projection = String::new();
            wait_until(
                Duration::from_secs(20),
                "controlled restart cutover",
                || {
                    let status_a = c.layered_status_a_local()?;
                    let status_b = c.layered_status_b_local()?;
                    let mut current = managed_pids_for_instance(&status_a, &app_id);
                    current.extend(managed_pids_for_instance(&status_b, &app_id));
                    last_projection = format!(
                        "current_pids={current:?} initial_pids={initial_pids:?} status_a={} status_b={}",
                        status_a,
                        status_b
                    );
                    if current.is_empty() {
                        return Ok(false);
                    }
                    restarted = current.iter().all(|pid| !initial_pids.contains(pid));
                    Ok(restarted)
                },
            )
            .map_err(|e| format!("{e}; last_projection: {last_projection}"))?;
            if !restarted {
                return Err(format!(
                    "redeclare did not converge to replacement pid set; initial_pids={initial_pids:?}"
                ));
            }
            let _ = release_clear(c, &app_id);
            Ok(())
        },
    )
}

pub(crate) fn scenario_tx_recovery_after_abort() -> Result<(), String> {
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
            let app_id = format!("rollback-last-{}", unique_suffix());

            let declared = release_apply_one_cardinality(c, &app_path, &app_id, "1.0.0")?;
            if declared.get("ok").and_then(Value::as_bool) != Some(true) {
                return Err(format!("release_apply failed: {declared}"));
            }
            let cleared = release_clear(c, &app_id)?;
            if cleared.get("ok").and_then(Value::as_bool) != Some(true) {
                return Err(format!("release_clear failed: {cleared}"));
            }

            let rollback = match c.ctl_ok_a_local(json!({ "command": "config_rollback_last" })) {
                Ok(v) => v,
                Err(e) if e.contains("rollback history has fewer than two snapshots") => {
                    c.ctl_ok_b_local(json!({ "command": "config_rollback_last" }))?
                }
                Err(e) if e.contains("redirect coordinator=") => {
                    c.ctl_ok_b_local(json!({ "command": "config_rollback_last" }))?
                }
                Err(e) => return Err(e),
            };
            if rollback.get("ok").and_then(Value::as_bool) != Some(true) {
                return Err(format!("config_rollback_last failed: {rollback}"));
            }

            wait_until(
                Duration::from_secs(20),
                "rollback restore released app",
                || {
                    let status_a = c.layered_status_a_local()?;
                    let status_b = c.layered_status_b_local()?;
                    let declared_a = status_a
                        .get("daemon")
                        .and_then(|d| d.get("declared_units_count"))
                        .and_then(Value::as_u64)
                        .unwrap_or(0);
                    let declared_b = status_b
                        .get("daemon")
                        .and_then(|d| d.get("declared_units_count"))
                        .and_then(Value::as_u64)
                        .unwrap_or(0);
                    let pid_a = managed_pid_for_instance(&status_a, &app_id);
                    let pid_b = managed_pid_for_instance(&status_b, &app_id);
                    Ok(declared_a > 0
                        && declared_a == declared_b
                        && (pid_a.is_some() || pid_b.is_some()))
                },
            )?;
            let _ = release_clear(c, &app_id);
            Ok(())
        },
    )
}

pub(crate) fn scenario_topology_route_delegation() -> Result<(), String> {
    with_cluster(
        FULL_NODE_DELEGATION_SCOPES,
        FULL_NODE_DELEGATION_SCOPES,
        |c| {
            let route = format!("route.topology:{}:v1", unique_suffix());
            let mut cfg = c.ctl_ok_a_local(json!({ "command": "config_get" }))?;
            cfg["route_plans"] = json!([{
                "route_key": route,
                "mode": "mesh",
                "peers": [{ "node_id": c.node_b_identity.node_id, "addr": c.addr_b }]
            }]);
            let err = c.ctl_err_code_a(
                json!({
                    "command": "config_set",
                    "config": cfg
                }),
                signed_auth_for_command(
                    &json!({
                        "command": "config_set",
                        "config": cfg
                    }),
                    FULL_NODE_DELEGATION_SCOPES,
                    next_auth_seq_a(),
                    &format!("topology-route-hard-cut-{}", unique_suffix()),
                    "local-admin-ed25519-1",
                ),
                local_target(),
            )?;
            if err != "invalid_request" {
                return Err(format!(
                    "topology route_plans must be rejected after hard-cut, got {err}"
                ));
            }
            let dump = c.ctl_ok_a_local(json!({ "command": "config_get" }))?;
            if dump["route_plans"]
                .as_array()
                .is_some_and(|arr| !arr.is_empty())
            {
                return Err(format!(
                    "rejected topology route_plans must not persist in config_get projection: {dump}"
                ));
            }
            Ok(())
        },
    )
}

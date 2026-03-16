use super::*;

fn tx_spawn_command(app: &str, version: &str) -> serde_json::Value {
    json!({
        "command": "tx_execute",
        "tx": {
            "tx_id": format!("tx-spawn-{}", unique_suffix()),
            "steps": [{
                "kind": "proc_spawn",
                "manifest": {
                    "app": app,
                    "config": {},
                    "ports": [],
                    "restart_policy": "Never",
                    "version": version
                }
            }]
        }
    })
}

fn tx_kill_command(pid: u32) -> serde_json::Value {
    json!({
        "command": "tx_execute",
        "tx": {
            "tx_id": format!("tx-kill-{}", unique_suffix()),
            "steps": [{ "kind": "proc_kill", "pid": pid }]
        }
    })
}

pub(crate) fn scenario_app_runtime_tiered_execution_boundary() -> Result<(), String> {
    with_cluster(
        FULL_NODE_DELEGATION_SCOPES,
        FULL_NODE_DELEGATION_SCOPES,
        |c| {
            for app in [
                "missing-app.so",
                "sidecar:/tmp/missing.sock",
                "wasm:/tmp/missing.wasm",
            ] {
                let err =
                    c.ctl_err_code_a(tx_spawn_command(app, "0.0.1"), dummy_auth(), local_target())?;
                if err != "internal"
                    && err != "invalid_request"
                    && err != "not_supported"
                    && err != "signature_invalid"
                {
                    return Err(format!("unexpected spawn error code for {app}: {err}"));
                }
            }
            let _ = c.cluster_state_a()?;
            Ok(())
        },
    )
}

pub(crate) fn scenario_app_runtime_lifecycle_abstraction() -> Result<(), String> {
    with_cluster(
        FULL_NODE_DELEGATION_SCOPES,
        FULL_NODE_DELEGATION_SCOPES,
        |c| {
            let _ = c.ctl_ok_a_local(json!({
                "command": "limit_set",
                "key": "max_processes",
                "value": 32u64
            }))?;
            let status = c.layered_status_a_local()?;
            ensure_layered_local_status_shape(&status)?;
            Ok(())
        },
    )
}

pub(crate) fn scenario_app_runtime_binary_file_spawn_only() -> Result<(), String> {
    with_cluster(
        FULL_NODE_DELEGATION_SCOPES,
        FULL_NODE_DELEGATION_SCOPES,
        |c| {
            for app in [
                "python:app.py",
                "sidecar:/tmp/app.sock",
                "wasm:/tmp/app.wasm",
            ] {
                let command = tx_spawn_command(app, "0.0.1");
                let auth = signed_auth_for_command(
                    &command,
                    &["tx_execute"],
                    next_auth_seq_a(),
                    &format!("app-runtime-binary-file-{}", unique_suffix()),
                    "local-admin-ed25519-1",
                );
                let err = c.ctl_err_code_a(command, auth, local_target())?;
                if err != "invalid_request" {
                    return Err(format!(
                        "non-file app target must fail with invalid_request ({app} -> {err})"
                    ));
                }
            }

            let file_like = tx_spawn_command("missing-app.so", "0.0.1");
            let auth = signed_auth_for_command(
                &file_like,
                &["tx_execute"],
                next_auth_seq_a(),
                &format!("app-runtime-binary-file-valid-shape-{}", unique_suffix()),
                "local-admin-ed25519-1",
            );
            let result = c.ctl_ok_a(file_like, auth, local_target())?;
            if result.get("status").and_then(Value::as_str) != Some("aborted") {
                return Err(format!(
                    "binary-file-shaped app target should pass manifest validation and fail later at loader boundary, got {result}"
                ));
            }

            let Some(test_app) = try_find_test_app_cdylib() else {
                return Err(
                    "test app runtime path not found; set CAPANIX_TEST_APP_BINARY or build capanix-app-test-runtime"
                        .to_string(),
                );
            };
            let spawned =
                c.ctl_ok_a_local(tx_spawn_command(&test_app.to_string_lossy(), "0.0.2"))?;
            let pid = spawned
                .get("spawned_pids")
                .and_then(Value::as_array)
                .and_then(|rows| rows.first())
                .and_then(Value::as_u64)
                .or_else(|| {
                    spawned
                        .get("spawned_pids")
                        .and_then(Value::as_array)
                        .and_then(|rows| rows.first())
                        .and_then(|v| v.get("0"))
                        .and_then(Value::as_u64)
                })
                .ok_or_else(|| format!("tx_execute missing spawned pid in response: {spawned}"))?
                as u32;

            let _ = c.ctl_ok_a_local(tx_kill_command(pid))?;
            Ok(())
        },
    )
}

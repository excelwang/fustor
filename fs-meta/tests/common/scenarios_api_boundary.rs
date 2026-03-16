use super::*;

pub(crate) fn scenario_api_mirror_first() -> Result<(), String> {
    with_cluster(
        FULL_NODE_DELEGATION_SCOPES,
        FULL_NODE_DELEGATION_SCOPES,
        |c| {
            let cluster_state_a = c.cluster_state_a()?;
            let cluster_state_b = c.cluster_state_b()?;
            let nodes_a = cluster_state_a["nodes"]
                .as_array()
                .map(|v| v.len() as u64)
                .unwrap_or(0);
            let nodes_b = cluster_state_b["nodes"]
                .as_array()
                .map(|v| v.len() as u64)
                .unwrap_or(0);
            let a_self = status_has_reachable_node(&cluster_state_a, &c.node_a_identity.node_id);
            let b_self = status_has_reachable_node(&cluster_state_b, &c.node_b_identity.node_id);
            if nodes_a < 2 || nodes_b < 1 || !a_self || !b_self {
                return Err(format!(
                    "cluster_state_get did not preserve local-vs-remote mirror semantics after convergence: node_a={cluster_state_a} node_b={cluster_state_b}"
                ));
            }
            Ok(())
        },
    )
}

pub(crate) fn scenario_api_policy_neutrality() -> Result<(), String> {
    with_cluster(
        FULL_NODE_DELEGATION_SCOPES,
        FULL_NODE_DELEGATION_SCOPES,
        |c| {
            let cfg = c.ctl_ok_a_local(json!({ "command": "config_get" }))?;
            if cfg.get("daemon").is_some() || cfg.get("commit_report").is_some() {
                return Err(format!(
                    "config_dump leaked policy/orchestration fields: {cfg}"
                ));
            }
            Ok(())
        },
    )
}

pub(crate) fn scenario_api_boundary_split_typed() -> Result<(), String> {
    with_cluster(
        FULL_NODE_DELEGATION_SCOPES,
        FULL_NODE_DELEGATION_SCOPES,
        |c| {
            let _ = c.ctl_ok_a_local(json!({ "command": "cluster_state_get" }))?;
            let err = c.ctl_err_code_a(
                json!({ "command": "config_get" }),
                dummy_auth(),
                cluster_target(),
            )?;
            if !is_auth_rejection(&err) {
                return Err(format!("expected auth rejection, got {err}"));
            }
            Ok(())
        },
    )
}

pub(crate) fn scenario_api_transaction_step_closure() -> Result<(), String> {
    with_cluster(
        FULL_NODE_DELEGATION_SCOPES,
        FULL_NODE_DELEGATION_SCOPES,
        |c| {
            let result = c.ctl_ok_a_local(json!({
            "command": "tx_execute",
            "tx": {
                "tx_id": format!("tx-step-closure-{}", unique_suffix()),
                "steps": [
                    { "kind": "pause_route", "route_key": "route.api.closure:v1" },
                    { "kind": "resume_route", "route_key": "route.api.closure:v1" },
                    { "kind": "proc_kill", "pid": 999999u32 },
                    { "kind": "proc_spawn", "manifest": {
                        "app": "missing-app.so",
                        "config": {},
                        "ports": [],
                        "restart_policy": "Never",
                        "version": "0.0.1"
                    }},
                    { "kind": "attach_grant_attachment", "pid": 1u32, "route_key": "route.api.closure:v1", "access": 1 },
                    { "kind": "detach_grant_attachment", "grant": 0u64 },
                    { "kind": "set_route", "route_key": "route.api.closure:v1", "route": { "kind": "local" } }
                ]
            }
        }))?;
            if result.get("status").is_none() {
                return Err(format!("tx_execute missing typed status payload: {result}"));
            }
            Ok(())
        },
    )
}

pub(crate) fn scenario_control_transport_schema_ownership() -> Result<(), String> {
    with_cluster(
        FULL_NODE_DELEGATION_SCOPES,
        FULL_NODE_DELEGATION_SCOPES,
        |c| {
            let status = c.layered_status_a_local()?;
            ensure_layered_local_status_shape(&status)?;
            let cluster_state_get = c.ctl_ok_a_local(json!({ "command": "cluster_state_get" }))?;
            if cluster_state_get.get("daemon").is_some() {
                return Err(format!(
                    "cluster_state_get must not carry app-level daemon field: {cluster_state_get}"
                ));
            }
            Ok(())
        },
    )
}

pub(crate) fn scenario_config_validation_single_source() -> Result<(), String> {
    with_cluster(
        FULL_NODE_DELEGATION_SCOPES,
        FULL_NODE_DELEGATION_SCOPES,
        |c| {
            let mut cfg = c.ctl_ok_a_local(json!({ "command": "config_get" }))?;
            cfg["node_id"] = json!("");

            let command = json!({ "command": "config_set", "config": cfg });
            let local_auth = signed_auth_for_command(
                &command,
                FULL_NODE_DELEGATION_SCOPES,
                next_auth_seq_a(),
                &format!("cfg-local-{}", unique_suffix()),
                "local-admin-ed25519-1",
            );
            let local_err = c.ctl_err_code_a(command.clone(), local_auth, local_target())?;

            let signed = signed_auth_for_command(
                &command,
                FULL_NODE_DELEGATION_SCOPES,
                100,
                &format!("cfg-validate-{}", unique_suffix()),
                "dev-admin-key",
            );
            let cluster_err = c.ctl_err_code_a(command, signed, cluster_target())?;

            if local_err != "invalid_request" {
                return Err(format!(
                    "local config validation returned unexpected code: {local_err}"
                ));
            }
            if cluster_err != "invalid_request" {
                return Err(format!(
                    "cluster config validation returned unexpected code: {cluster_err}"
                ));
            }
            Ok(())
        },
    )
}

pub(crate) fn scenario_privileged_mutation_session_arbitration_ownership() -> Result<(), String> {
    with_cluster(
        FULL_NODE_DELEGATION_SCOPES,
        FULL_NODE_DELEGATION_SCOPES,
        |c| {
            let cfg = c.ctl_ok_a_local(json!({ "command": "config_get" }))?;
            if cfg.get("session_activate").is_some()
                || cfg.get("session_deactivate").is_some()
                || cfg.get("session_arbitration").is_some()
            {
                return Err(format!(
                    "session arbitration leaked into kernel/api config boundary: {cfg}"
                ));
            }
            Ok(())
        },
    )
}

pub(crate) fn scenario_session_protocol_uniformity_with_policy_variants() -> Result<(), String> {
    with_cluster(
        FULL_NODE_DELEGATION_SCOPES,
        FULL_NODE_DELEGATION_SCOPES,
        |c| {
            let mut cfg_a = c.ctl_ok_a_local(json!({ "command": "config_get" }))?;
            cfg_a["session_protocol"] = json!({
                "verb_set": "activate_deactivate",
                "lease_ms": 10000u64
            });
            let command_a = json!({ "command": "config_set", "config": cfg_a });
            let result_a = c.ctl_ok_a_local(command_a)?;

            let mut cfg_b = c.ctl_ok_a_local(json!({ "command": "config_get" }))?;
            cfg_b["session_protocol"] = json!({
                "verb_set": "activate_deactivate",
                "lease_ms": 60000u64
            });
            let command_b = json!({ "command": "config_set", "config": cfg_b });
            let result_b = c.ctl_ok_a_local(command_b)?;

            let same_shape = result_a.get("ok").is_some()
                && result_a.get("noop").is_some()
                && result_a.get("plan_size").is_some()
                && result_a.get("removed").is_some()
                && result_a.get("spawned").is_some()
                && result_a.get("commit_report").is_some()
                && result_b.get("ok").is_some()
                && result_b.get("noop").is_some()
                && result_b.get("plan_size").is_some()
                && result_b.get("removed").is_some()
                && result_b.get("spawned").is_some()
                && result_b.get("commit_report").is_some();
            if !same_shape {
                return Err(format!(
                    "session protocol extension produced non-uniform response shape: A={result_a} B={result_b}"
                ));
            }
            Ok(())
        },
    )
}

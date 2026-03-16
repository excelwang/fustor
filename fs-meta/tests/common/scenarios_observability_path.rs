use super::*;

pub(crate) fn scenario_metrics_query_path() -> Result<(), String> {
    with_cluster(
        FULL_NODE_DELEGATION_SCOPES,
        FULL_NODE_DELEGATION_SCOPES,
        |c| {
            let metrics = c.ctl_ok_a_local(json!({ "command": "metrics_get" }))?;
            if metrics.get("uptime_secs").is_none() || metrics.get("health").is_none() {
                return Err(format!(
                    "metrics payload missing required fields: {metrics}"
                ));
            }
            Ok(())
        },
    )
}

pub(crate) fn scenario_audit_query_path() -> Result<(), String> {
    with_cluster(
        FULL_NODE_DELEGATION_SCOPES,
        FULL_NODE_DELEGATION_SCOPES,
        |c| {
            let _ = c.ctl_ok_a_local(json!({
                "command": "limit_set",
                "key": "max_channels",
                "value": 32u64
            }))?;
            let audit = c.ctl_ok_a_local(json!({ "command": "audit_tail", "count": 20usize }))?;
            if !audit.as_array().is_some_and(|a| !a.is_empty()) {
                return Err(format!("audit query returned no entries: {audit}"));
            }
            Ok(())
        },
    )
}

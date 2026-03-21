use anyhow::{Context, bail};
use capanix_runtime_api::{
    RuntimeAdminCommand, RuntimeAdminEnvelope, RuntimeAdminRequest,
    canonical_runtime_admin_command_bytes,
};
use serde_json::Value;

#[allow(dead_code)]
pub fn canonical_runtime_admin_command_value_bytes(command: &Value) -> anyhow::Result<Vec<u8>> {
    let typed: RuntimeAdminCommand = serde_json::from_value(command.clone())?;
    Ok(canonical_runtime_admin_command_bytes(&typed)?)
}

pub fn encode_runtime_admin_request_value(req: Value) -> anyhow::Result<Vec<u8>> {
    let admin_req: RuntimeAdminRequest = serde_json::from_value(req)?;
    Ok(rmp_serde::to_vec_named(
        &RuntimeAdminEnvelope::AdminCommand(admin_req),
    )?)
}

pub fn decode_runtime_admin_or_kernel_response_value(buf: &[u8]) -> anyhow::Result<Value> {
    let envelope: Value = rmp_serde::from_slice(buf)?;
    let kind = envelope
        .get("kind")
        .and_then(Value::as_str)
        .context("response envelope missing kind")?;
    match kind {
        "admin_result" | "result" => envelope
            .get("data")
            .cloned()
            .context("response envelope missing data"),
        other => bail!("unexpected response envelope kind: {other}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn runtime_admin_request_roundtrip_preserves_worker_intent_payload_shape() {
        let command = json!({
            "command": "relation_target_apply",
            "intent": {
                "schema_version": "scope-worker-intent-v1",
                "target_id": "fs-meta-test",
                "target_generation": 7,
                "route_plans": [],
                "workers": [{
                    "worker_role": "main",
                    "worker_id": "fs-meta-test",
                    "scope_ids": [],
                    "startup": {
                        "path": "/tmp/fs-meta-runtime.so",
                        "manifest": "/tmp/fs-meta.yaml"
                    },
                    "config": {
                        "workers": {
                            "source": {
                                "mode": "external",
                                "startup": { "path": "/tmp/libfs_meta_runtime.so" }
                            },
                            "sink": {
                                "mode": "external",
                                "startup": { "path": "/tmp/libfs_meta_runtime.so" }
                            }
                        }
                    },
                    "runtime": {
                        "control_subscriptions": ["runtime.host_object_grants.changed"],
                        "app_scopes": [],
                        "units": {
                            "runtime.exec.facade": { "enabled": true }
                        },
                        "state_carrier": {
                            "enabled": true
                        }
                    },
                    "policy": {
                        "generation": 7,
                        "replicas": 2
                    },
                    "restart_policy": "Never",
                    "version": "dev"
                }]
            }
        });
        let req = json!({
            "command": command.clone(),
            "auth": {
                "actor_id": "test-admin",
                "domain_id": "local",
                "key_id": "local-admin-ed25519-1",
                "algorithm": "ed25519",
                "epoch": 1,
                "seq": 1,
                "nonce": "n1",
                "payload_sha256": "00",
                "signature_b64": "sig",
                "scopes": ["config_write"]
            },
            "forwarded": false
        });

        let body =
            encode_runtime_admin_request_value(req).expect("encode typed runtime-admin request");
        let envelope: RuntimeAdminEnvelope =
            rmp_serde::from_slice(&body).expect("decode encoded request");
        let RuntimeAdminEnvelope::AdminCommand(decoded) = envelope else {
            panic!("expected admin command envelope");
        };

        let decoded_value =
            serde_json::to_value(&decoded.command).expect("serialize typed command");
        assert_eq!(
            decoded_value, command,
            "typed request encoding must not drop or reshape scope-worker intent fields before daemon verification"
        );
        let expected = canonical_runtime_admin_command_value_bytes(&command)
            .expect("canonicalize raw command");
        let actual = canonical_runtime_admin_command_bytes(&decoded.command)
            .expect("canonicalize decoded command");
        assert_eq!(
            expected, actual,
            "signed payload bytes must match the typed command that runtime verifies"
        );
    }
}

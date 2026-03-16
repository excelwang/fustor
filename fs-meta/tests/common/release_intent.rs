use std::path::{Path, PathBuf};

use serde_json::{Value, json};

pub(crate) fn release_v2_doc_to_scope_unit_intent_value(doc: &Value) -> Result<Value, String> {
    let apps = doc
        .get("apps")
        .and_then(Value::as_array)
        .ok_or_else(|| "release-v2 doc missing apps".to_string())?;
    let first = apps
        .first()
        .ok_or_else(|| "release-v2 doc requires at least one app".to_string())?;
    let target_id = first
        .get("id")
        .or_else(|| first.get("app"))
        .and_then(Value::as_str)
        .unwrap_or("target");
    let target_generation = first
        .get("policy")
        .and_then(|policy| policy.get("generation"))
        .and_then(Value::as_i64)
        .unwrap_or(1);
    let workers = apps
        .iter()
        .map(|entry| {
            let worker_role = entry
                .get("worker_role")
                .or_else(|| entry.get("runtime").and_then(|runtime| runtime.get("worker_role")))
                .and_then(Value::as_str)
                .unwrap_or("main");
            let worker_id = entry
                .get("id")
                .or_else(|| entry.get("app"))
                .and_then(Value::as_str)
                .unwrap_or("worker");
            let startup_path = entry
                .get("app")
                .or_else(|| entry.get("id"))
                .and_then(Value::as_str)
                .unwrap_or("app");
            let mut startup = serde_json::Map::new();
            startup.insert("path".into(), Value::String(startup_path.to_string()));
            if let Some(manifest) = entry.get("manifest").cloned() {
                let manifest = manifest
                    .as_str()
                    .map(PathBuf::from)
                    .map(|path| {
                        if path.is_absolute() {
                            path
                        } else {
                            repo_root().join(path)
                        }
                    })
                    .map(|path| Value::String(path.display().to_string()))
                    .unwrap_or(manifest);
                startup.insert("manifest".into(), manifest);
            }
            json!({
                "worker_role": worker_role,
                "worker_id": worker_id,
                "scope_ids": [],
                "startup": Value::Object(startup),
                "config": entry.get("config").cloned().unwrap_or_else(|| json!({})),
                "runtime": entry.get("runtime").cloned().unwrap_or_else(|| json!({})),
                "policy": entry.get("policy").cloned().unwrap_or_else(|| json!({})),
                "restart_policy": entry.get("restart_policy").cloned().unwrap_or_else(|| json!("Always")),
                "version": entry.get("version").cloned().unwrap_or_else(|| json!("dev"))
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "schema_version": "scope-worker-intent-v1",
        "target_id": target_id,
        "target_generation": target_generation,
        "workers": workers
    }))
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap_or_else(|_| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .map(Path::to_path_buf)
                .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")))
        })
}

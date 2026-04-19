use std::collections::HashMap;
use std::env;

use capanix_app_sdk::runtime::{ConfigValue, NodeId};
use capanix_runtime_entry_sdk::control::{
    RuntimeBoundScope, RuntimeExecActivate, RuntimeExecControl, encode_runtime_exec_control,
};
use fs_meta_runtime::query::request::{InternalQueryRequest, QueryOp, QueryScope};
use fs_meta_runtime::{FSMetaApp, FSMetaConfig};
use serde::Deserialize;

const FACADE_CONTROL_ROUTE_KEY: &str = "fs-meta.internal.facade-control:v1.stream";

#[derive(Debug, Deserialize)]
struct RootSelectorEnvSpec {
    #[serde(default)]
    mount_point: Option<String>,
    #[serde(default)]
    fs_source: Option<String>,
    #[serde(default)]
    fs_type: Option<String>,
    #[serde(default)]
    host_ip: Option<String>,
    #[serde(default)]
    host_ref: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RootEnvSpec {
    id: String,
    selector: RootSelectorEnvSpec,
    #[serde(default = "default_root_subpath_scope")]
    subpath_scope: String,
    #[serde(default = "default_true")]
    watch: bool,
    #[serde(default = "default_true")]
    scan: bool,
    #[serde(default)]
    audit_interval_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct HostObjectGrantEnvSpec {
    object_ref: String,
    host_ref: String,
    host_ip: String,
    mount_point: String,
    fs_source: String,
    fs_type: String,
    #[serde(default)]
    interfaces: Vec<String>,
    #[serde(default = "default_true")]
    active: bool,
}

fn default_true() -> bool {
    true
}

fn default_root_subpath_scope() -> String {
    "/".to_string()
}

fn env_required(key: &str) -> Result<String, String> {
    env::var(key).map_err(|_| format!("missing required env {key}"))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let facade_resource_id = env_required("FS_META_API_FACADE_RESOURCE_ID")?;
    let bind_addr = env_required("FS_META_API_LISTENER_BIND_ADDR")?;
    let roots_json = env_required("FS_META_ROOTS_JSON")?;
    let host_object_grants_json = env::var("FS_META_HOST_OBJECT_GRANTS_JSON").ok();
    let passwd_path = env_required("FS_META_PASSWD_PATH")?;
    let shadow_path = env_required("FS_META_SHADOW_PATH")?;
    let query_keys_path = env::var("FS_META_QUERY_KEYS_PATH").ok();
    let node_id =
        env::var("FS_META_NODE_ID").unwrap_or_else(|_| "fs-meta-fixture-node".to_string());

    let roots_env: Vec<RootEnvSpec> = serde_json::from_str(&roots_json)
        .map_err(|e| format!("invalid FS_META_ROOTS_JSON: {e}"))?;
    let roots_cfg = roots_env
        .iter()
        .map(|r| {
            let mut row = HashMap::new();
            row.insert("id".to_string(), ConfigValue::String(r.id.clone()));
            row.insert(
                "selector".to_string(),
                ConfigValue::Map(
                    [
                        r.selector.mount_point.as_ref().map(|value| {
                            (
                                "mount_point".to_string(),
                                ConfigValue::String(value.clone()),
                            )
                        }),
                        r.selector.fs_source.as_ref().map(|value| {
                            ("fs_source".to_string(), ConfigValue::String(value.clone()))
                        }),
                        r.selector.fs_type.as_ref().map(|value| {
                            ("fs_type".to_string(), ConfigValue::String(value.clone()))
                        }),
                        r.selector.host_ip.as_ref().map(|value| {
                            ("host_ip".to_string(), ConfigValue::String(value.clone()))
                        }),
                        r.selector.host_ref.as_ref().map(|value| {
                            ("host_ref".to_string(), ConfigValue::String(value.clone()))
                        }),
                    ]
                    .into_iter()
                    .flatten()
                    .collect(),
                ),
            );
            row.insert(
                "subpath_scope".to_string(),
                ConfigValue::String(r.subpath_scope.clone()),
            );
            row.insert("watch".to_string(), ConfigValue::Bool(r.watch));
            row.insert("scan".to_string(), ConfigValue::Bool(r.scan));
            if let Some(v) = r.audit_interval_ms {
                row.insert("audit_interval_ms".to_string(), ConfigValue::Int(v as i64));
            }
            ConfigValue::Map(row)
        })
        .collect::<Vec<_>>();
    let host_object_grants_cfg = if let Some(raw) = host_object_grants_json {
        let host_object_grants: Vec<HostObjectGrantEnvSpec> = serde_json::from_str(&raw)
            .map_err(|e| format!("invalid FS_META_HOST_OBJECT_GRANTS_JSON: {e}"))?;
        host_object_grants
            .into_iter()
            .map(|item| {
                ConfigValue::Map(HashMap::from([
                    (
                        "object_ref".to_string(),
                        ConfigValue::String(item.object_ref),
                    ),
                    (
                        "interfaces".to_string(),
                        ConfigValue::Array(
                            item.interfaces
                                .into_iter()
                                .map(ConfigValue::String)
                                .collect(),
                        ),
                    ),
                    (
                        "host_ref".to_string(),
                        ConfigValue::String(item.host_ref.clone()),
                    ),
                    (
                        "host_descriptors".to_string(),
                        ConfigValue::Map(HashMap::from([(
                            "host_ip".to_string(),
                            ConfigValue::String(item.host_ip),
                        )])),
                    ),
                    (
                        "object_descriptors".to_string(),
                        ConfigValue::Map(HashMap::from([
                            (
                                "mount_point".to_string(),
                                ConfigValue::String(item.mount_point),
                            ),
                            ("fs_source".to_string(), ConfigValue::String(item.fs_source)),
                            ("fs_type".to_string(), ConfigValue::String(item.fs_type)),
                        ])),
                    ),
                    (
                        "grant_state".to_string(),
                        ConfigValue::String(
                            if item.active { "active" } else { "revoked" }.to_string(),
                        ),
                    ),
                ]))
            })
            .collect::<Vec<_>>()
    } else {
        roots_env
            .iter()
            .filter_map(|r| {
                let mount_point = r.selector.mount_point.clone()?;
                Some(ConfigValue::Map(HashMap::from([
                    (
                        "object_ref".to_string(),
                        ConfigValue::String(format!("{node_id}::{}", r.id)),
                    ),
                    ("host_ref".to_string(), ConfigValue::String(node_id.clone())),
                    (
                        "host_descriptors".to_string(),
                        ConfigValue::Map(HashMap::from([(
                            "host_ip".to_string(),
                            ConfigValue::String("127.0.0.1".to_string()),
                        )])),
                    ),
                    (
                        "object_descriptors".to_string(),
                        ConfigValue::Map(HashMap::from([
                            ("mount_point".to_string(), ConfigValue::String(mount_point)),
                            (
                                "fs_source".to_string(),
                                ConfigValue::String(format!("fixture:{}", r.id)),
                            ),
                            (
                                "fs_type".to_string(),
                                ConfigValue::String("fixture".to_string()),
                            ),
                        ])),
                    ),
                    (
                        "interfaces".to_string(),
                        ConfigValue::Array(vec![
                            ConfigValue::String("posix-fs".to_string()),
                            ConfigValue::String("inotify".to_string()),
                        ]),
                    ),
                    (
                        "grant_state".to_string(),
                        ConfigValue::String("active".to_string()),
                    ),
                ])))
            })
            .collect::<Vec<_>>()
    };

    let mut cfg = HashMap::new();
    cfg.insert("roots".to_string(), ConfigValue::Array(roots_cfg));
    cfg.insert(
        "__cnx_runtime".to_string(),
        ConfigValue::Map(HashMap::from([
            (
                "local_host_ref".to_string(),
                ConfigValue::String(node_id.clone()),
            ),
            (
                "host_object_grants".to_string(),
                ConfigValue::Array(host_object_grants_cfg),
            ),
            (
                "announced_resources".to_string(),
                ConfigValue::Array(vec![ConfigValue::Map(HashMap::from([
                    (
                        "resource_id".to_string(),
                        ConfigValue::String(facade_resource_id.clone()),
                    ),
                    ("node_id".to_string(), ConfigValue::String(node_id.clone())),
                    (
                        "resource_kind".to_string(),
                        ConfigValue::String("tcp_listener".to_string()),
                    ),
                    (
                        "bind_addr".to_string(),
                        ConfigValue::String(bind_addr.clone()),
                    ),
                    (
                        "source".to_string(),
                        ConfigValue::String("fs-meta-fixture".to_string()),
                    ),
                ]))]),
            ),
        ])),
    );
    cfg.insert(
        "api".to_string(),
        ConfigValue::Map({
            let mut api = HashMap::from([
                ("enabled".to_string(), ConfigValue::Bool(true)),
                (
                    "facade_resource_id".to_string(),
                    ConfigValue::String(facade_resource_id.clone()),
                ),
            ]);
            let mut auth = HashMap::from([
                ("passwd_path".to_string(), ConfigValue::String(passwd_path)),
                ("shadow_path".to_string(), ConfigValue::String(shadow_path)),
            ]);
            if let Some(path) = query_keys_path {
                auth.insert("query_keys_path".to_string(), ConfigValue::String(path));
            }
            api.insert("auth".to_string(), ConfigValue::Map(auth));
            api
        }),
    );
    let config = FSMetaConfig::from_runtime_manifest_config(&cfg)?;
    let app = FSMetaApp::new(config, NodeId(node_id.clone()))?;
    app.start().await?;
    let worker_scopes = roots_env
        .iter()
        .map(|root| RuntimeBoundScope {
            scope_id: root.id.clone(),
            resource_ids: Vec::new(),
        })
        .collect::<Vec<_>>();
    let source_activate =
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: FACADE_CONTROL_ROUTE_KEY.to_string(),
            unit_id: "runtime.exec.source".to_string(),
            lease: None,
            generation: 1,
            expires_at_ms: 60_000,
            bound_scopes: worker_scopes.clone(),
        }))
        .map_err(|e| format!("encode source activate envelope failed: {e}"))?;
    let scan_activate =
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: FACADE_CONTROL_ROUTE_KEY.to_string(),
            unit_id: "runtime.exec.scan".to_string(),
            lease: None,
            generation: 1,
            expires_at_ms: 60_000,
            bound_scopes: worker_scopes.clone(),
        }))
        .map_err(|e| format!("encode scan activate envelope failed: {e}"))?;
    let sink_activate =
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: FACADE_CONTROL_ROUTE_KEY.to_string(),
            unit_id: "runtime.exec.sink".to_string(),
            lease: None,
            generation: 1,
            expires_at_ms: 60_000,
            bound_scopes: worker_scopes.clone(),
        }))
        .map_err(|e| format!("encode sink activate envelope failed: {e}"))?;
    let facade_activate =
        encode_runtime_exec_control(&RuntimeExecControl::Activate(RuntimeExecActivate {
            route_key: FACADE_CONTROL_ROUTE_KEY.to_string(),
            unit_id: "runtime.exec.facade".to_string(),
            lease: None,
            generation: 1,
            expires_at_ms: 60_000,
            bound_scopes: vec![RuntimeBoundScope {
                scope_id: facade_resource_id.clone(),
                resource_ids: vec![facade_resource_id.clone()],
            }],
        }))
        .map_err(|e| format!("encode facade activate envelope failed: {e}"))?;
    app.on_control_frame(&[
        source_activate,
        scan_activate,
        sink_activate,
        facade_activate,
    ])
    .await?;
    wait_for_fixture_query_ready(&app).await?;
    println!("FS_META_API_FIXTURE_READY {bind_addr}");

    wait_for_shutdown().await;
    app.close().await?;
    Ok(())
}

async fn wait_for_shutdown() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm = signal(SignalKind::terminate()).expect("install sigterm handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = sigterm.recv() => {},
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

async fn wait_for_fixture_query_ready(app: &FSMetaApp) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    let request = InternalQueryRequest::materialized(
        QueryOp::Tree,
        QueryScope {
            path: b"/".to_vec(),
            recursive: false,
            max_depth: Some(0),
            selected_group: None,
        },
        None,
    );
    let mut iteration = 0u64;
    loop {
        iteration += 1;
        let source_ready = app
            .source_status_snapshot()
            .await
            .map(|status| !status.concrete_roots.is_empty())
            .unwrap_or(false);
        let sink_ready = app
            .sink_status_snapshot()
            .await
            .map(|status| {
                !status.groups.is_empty()
                    && status.live_nodes > 0
                    && status.groups.iter().all(|group| group.is_ready())
            })
            .unwrap_or(false);
        let query_ready = match app.query_tree(&request).await {
            Ok(groups) => !groups.is_empty(),
            Err(capanix_app_sdk::CnxError::NotReady(_)) => false,
            Err(err) => {
                eprintln!("fs_meta_api_fixture: waiting for initial query readiness: {err}");
                false
            }
        };
        if iteration % 10 == 0 && source_ready && !sink_ready {
            let _ = app.trigger_rescan_when_ready().await;
        }
        if source_ready && sink_ready && query_ready {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(Box::new(std::io::Error::other("Timeout")));
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

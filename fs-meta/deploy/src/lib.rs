use std::path::Path;
use std::path::PathBuf;

use capanix_app_sdk::runtime::{RouteKey, RoutePlanMode, RoutePlanSpec};
use capanix_app_sdk::{CnxError, Result};
use capanix_deploy_sdk::compile_relation_target_intent_value;
use fs_meta::product_model::execution_units::{
    QUERY_PEER_RUNTIME_UNIT_ID, QUERY_RUNTIME_UNIT_ID, SINK_RUNTIME_UNIT_ID,
    SOURCE_RUNTIME_UNIT_ID, SOURCE_SCAN_RUNTIME_UNIT_ID,
};
use fs_meta::product_model::routes::{
    ROUTE_KEY_EVENTS, ROUTE_KEY_FACADE_CONTROL, ROUTE_KEY_FORCE_FIND, ROUTE_KEY_QUERY,
    ROUTE_KEY_SINK_QUERY_INTERNAL, ROUTE_KEY_SINK_QUERY_PROXY, ROUTE_KEY_SINK_STATUS_INTERNAL,
    ROUTE_KEY_SOURCE_FIND_INTERNAL, ROUTE_KEY_SOURCE_RESCAN_CONTROL,
    ROUTE_KEY_SOURCE_RESCAN_INTERNAL, ROUTE_KEY_SOURCE_STATUS_INTERNAL,
};
use fs_meta::RootSpec;
pub use fs_meta::api::types::RootEntry;
pub use fs_meta::api::{ApiAuthConfig, BootstrapAdminConfig, BootstrapManagementConfig};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FsMetaReleaseWorkerMode {
    Embedded,
    External,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FsMetaReleaseWorkerModes {
    pub facade: FsMetaReleaseWorkerMode,
    pub source: FsMetaReleaseWorkerMode,
    pub sink: FsMetaReleaseWorkerMode,
}

impl Default for FsMetaReleaseWorkerModes {
    fn default() -> Self {
        Self {
            facade: FsMetaReleaseWorkerMode::Embedded,
            source: FsMetaReleaseWorkerMode::External,
            sink: FsMetaReleaseWorkerMode::External,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FsMetaReleaseSpec {
    pub app_id: String,
    pub api_facade_resource_id: String,
    pub auth: ApiAuthConfig,
    pub roots: Vec<RootSpec>,
    pub worker_module_path: Option<PathBuf>,
    pub worker_modes: FsMetaReleaseWorkerModes,
}

pub fn build_release_doc_value(spec: &FsMetaReleaseSpec) -> serde_json::Value {
    let mut auth = serde_json::json!({
        "passwd_path": spec.auth.passwd_path.display().to_string(),
        "shadow_path": spec.auth.shadow_path.display().to_string(),
        "query_keys_path": spec.auth.query_keys_path.display().to_string(),
        "session_ttl_secs": spec.auth.session_ttl_secs,
        "management_group": spec.auth.management_group,
    });
    if let Some(bootstrap) = &spec.auth.bootstrap_management {
        auth["bootstrap_management"] = serde_json::json!({
            "username": bootstrap.username,
            "password": bootstrap.password,
            "uid": bootstrap.uid,
            "gid": bootstrap.gid,
            "home": bootstrap.home,
            "shell": bootstrap.shell,
        });
    }
    let mut config = serde_json::json!({
        "roots": spec.roots.iter().map(root_spec_release_json).collect::<Vec<_>>(),
        "api": {
            "enabled": true,
            "facade_resource_id": spec.api_facade_resource_id,
            "auth": auth,
        }
    });
    if let Some(worker_module_path) = spec.worker_module_path.as_deref() {
        config["workers"] = build_worker_config_json(worker_module_path, spec.worker_modes);
    }
    serde_json::json!({
        "schema_version": "scope-unit-intent-v1",
        "target_id": spec.app_id,
        "target_generation": generation_now(),
        "route_plans": build_route_plans_json(spec),
        "units": [
            {
                "unit_id": spec.app_id,
                "startup": {
                    "path": "fs-meta-runtime"
                },
                "config": config,
                "runtime": {
                    "control_subscriptions": ["runtime.host_object_grants.changed"],
                    "app_scopes": build_app_scopes_json(spec),
                    "route_units": build_route_units_json(),
                    "units": {
                        "runtime.exec.query": {
                            "enabled": true
                        },
                        "runtime.exec.query-peer": {
                            "enabled": true
                        },
                        "runtime.exec.scan": {
                            "interval_ms": 30000,
                            "enabled": true
                        },
                        "runtime.exec.facade": {
                            "enabled": true
                        }
                    },
                    "state_carrier": {
                        "enabled": true,
                        "units": {
                            "runtime.exec.query": {
                                "enabled": false
                            },
                            "runtime.exec.scan": {
                                "enabled": false
                            },
                            "runtime.exec.facade": {
                                "enabled": true
                            }
                        }
                    }
                },
                "policy": {
                    "generation": generation_now()
                }
            }
        ]
    })
}

pub fn compile_release_doc_to_relation_target_intent(
    release_doc: &serde_json::Value,
) -> Result<serde_json::Value> {
    let intent_value = scope_unit_intent_to_scope_worker_intent_value(release_doc)?;
    compile_relation_target_intent_value(intent_value)
        .map_err(|err| CnxError::InvalidInput(format!("invalid fs-meta deploy intent: {err}")))
}

fn build_worker_config_json(
    worker_module_path: &Path,
    worker_modes: FsMetaReleaseWorkerModes,
) -> serde_json::Value {
    let worker_module_path = worker_module_path.display().to_string();
    let worker_json = |mode: FsMetaReleaseWorkerMode| match mode {
        FsMetaReleaseWorkerMode::Embedded => serde_json::json!({
            "mode": "embedded"
        }),
        FsMetaReleaseWorkerMode::External => serde_json::json!({
            "mode": "external",
            "startup": {
                "path": worker_module_path
            }
        }),
    };
    serde_json::json!({
        "facade": worker_json(worker_modes.facade),
        "source": worker_json(worker_modes.source),
        "sink": worker_json(worker_modes.sink)
    })
}

fn scope_unit_intent_to_scope_worker_intent_value(
    doc: &serde_json::Value,
) -> Result<serde_json::Value> {
    let units = doc
        .get("units")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| CnxError::InvalidInput("scope-unit-intent doc missing units".into()))?;
    let target_id = doc
        .get("target_id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("target");
    let target_generation = doc
        .get("target_generation")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(1);
    let workers = units
        .iter()
        .map(|entry| {
            let worker_role = entry
                .get("worker_role")
                .or_else(|| {
                    entry
                        .get("runtime")
                        .and_then(|runtime| runtime.get("worker_role"))
                })
                .and_then(serde_json::Value::as_str)
                .unwrap_or("main");
            let worker_id = entry
                .get("unit_id")
                .or_else(|| entry.get("app"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("worker");
            let startup_path = entry
                .get("startup")
                .and_then(|startup| startup.get("path"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("app");
            let mut startup = serde_json::Map::new();
            startup.insert("path".into(), serde_json::json!(startup_path));
            if let Some(manifest) = entry
                .get("startup")
                .and_then(|startup| startup.get("manifest"))
                .and_then(serde_json::Value::as_str)
            {
                startup.insert("manifest".into(), serde_json::json!(manifest));
            }
            let mode = entry
                .get("config")
                .and_then(|config| config.get("workers"))
                .and_then(|workers| workers.get(worker_role))
                .and_then(|worker| worker.get("mode"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("external");
            serde_json::json!({
                "worker_id": worker_id,
                "worker_role": worker_role,
                "target_id": target_id,
                "target_generation": target_generation,
                "startup": serde_json::Value::Object(startup),
                "mode": mode,
                "config": entry.get("config").cloned().unwrap_or(serde_json::Value::Null),
            })
        })
        .collect::<Vec<_>>();
    Ok(serde_json::json!({
        "schema_version": "scope-worker-intent-v1",
        "target_id": target_id,
        "target_generation": target_generation,
        "workers": workers,
    }))
}

fn build_route_plans_json(_spec: &FsMetaReleaseSpec) -> Vec<serde_json::Value> {
    let mut plans = Vec::new();
    plans.push(
        serde_json::to_value(RoutePlanSpec {
            route_key: RouteKey(format!("{}.stream", ROUTE_KEY_SOURCE_RESCAN_CONTROL)),
            mode: RoutePlanMode::FanOut,
            peers: Vec::new(),
        })
        .unwrap_or_else(|_| serde_json::json!({})),
    );
    plans.push(
        serde_json::to_value(RoutePlanSpec {
            route_key: RouteKey(format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY)),
            mode: RoutePlanMode::FanOut,
            peers: Vec::new(),
        })
        .unwrap_or_else(|_| serde_json::json!({})),
    );
    plans.push(
        serde_json::to_value(RoutePlanSpec {
            route_key: RouteKey(format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL)),
            mode: RoutePlanMode::FanOut,
            peers: Vec::new(),
        })
        .unwrap_or_else(|_| serde_json::json!({})),
    );
    plans.push(
        serde_json::to_value(RoutePlanSpec {
            route_key: RouteKey(format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL)),
            mode: RoutePlanMode::FanOut,
            peers: Vec::new(),
        })
        .unwrap_or_else(|_| serde_json::json!({})),
    );
    plans.push(
        serde_json::to_value(RoutePlanSpec {
            route_key: RouteKey(format!("{}.req", ROUTE_KEY_SOURCE_FIND_INTERNAL)),
            mode: RoutePlanMode::FanOut,
            peers: Vec::new(),
        })
        .unwrap_or_else(|_| serde_json::json!({})),
    );
    plans
}

fn request_reply_activation_route_key(base: &str) -> String {
    format!("{base}.req")
}

fn stream_activation_route_key(base: &str) -> String {
    format!("{base}.stream")
}

fn build_route_units_json() -> serde_json::Value {
    let mut route_units = serde_json::Map::new();
    route_units.insert(
        stream_activation_route_key(ROUTE_KEY_FACADE_CONTROL),
        serde_json::json!(["runtime.exec.facade"]),
    );
    route_units.insert(
        request_reply_activation_route_key(ROUTE_KEY_SOURCE_FIND_INTERNAL),
        serde_json::json!([QUERY_RUNTIME_UNIT_ID, QUERY_PEER_RUNTIME_UNIT_ID]),
    );
    route_units.insert(
        request_reply_activation_route_key(ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
        serde_json::json!([SOURCE_RUNTIME_UNIT_ID, SOURCE_SCAN_RUNTIME_UNIT_ID]),
    );
    route_units.insert(
        stream_activation_route_key(ROUTE_KEY_SOURCE_RESCAN_CONTROL),
        serde_json::json!([SOURCE_RUNTIME_UNIT_ID]),
    );
    route_units.insert(
        request_reply_activation_route_key(ROUTE_KEY_QUERY),
        serde_json::json!([QUERY_RUNTIME_UNIT_ID]),
    );
    route_units.insert(
        request_reply_activation_route_key(ROUTE_KEY_SINK_QUERY_INTERNAL),
        serde_json::json!([SINK_RUNTIME_UNIT_ID]),
    );
    route_units.insert(
        request_reply_activation_route_key(ROUTE_KEY_SINK_QUERY_PROXY),
        serde_json::json!([QUERY_RUNTIME_UNIT_ID, QUERY_PEER_RUNTIME_UNIT_ID]),
    );
    route_units.insert(
        request_reply_activation_route_key(ROUTE_KEY_SINK_STATUS_INTERNAL),
        serde_json::json!([QUERY_RUNTIME_UNIT_ID, QUERY_PEER_RUNTIME_UNIT_ID]),
    );
    route_units.insert(
        request_reply_activation_route_key(ROUTE_KEY_SOURCE_STATUS_INTERNAL),
        serde_json::json!([QUERY_RUNTIME_UNIT_ID, QUERY_PEER_RUNTIME_UNIT_ID]),
    );
    route_units.insert(
        request_reply_activation_route_key(ROUTE_KEY_FORCE_FIND),
        serde_json::json!([SINK_RUNTIME_UNIT_ID]),
    );
    route_units.insert(
        stream_activation_route_key(ROUTE_KEY_EVENTS),
        serde_json::json!([SINK_RUNTIME_UNIT_ID]),
    );
    serde_json::Value::Object(route_units)
}

fn build_app_scopes_json(spec: &FsMetaReleaseSpec) -> Vec<serde_json::Value> {
    let mut scopes = spec
        .roots
        .iter()
        .map(root_spec_app_scope_json)
        .collect::<Vec<_>>();
    scopes.push(facade_app_scope_json(spec));
    scopes
}

fn facade_app_scope_json(spec: &FsMetaReleaseSpec) -> serde_json::Value {
    serde_json::json!({
        "scope_id": spec.api_facade_resource_id,
        "resource_ids": [spec.api_facade_resource_id],
        "unit_scopes": [
            {
                "unit_id": "runtime.exec.facade",
                "eligibility": "resource_visible_nodes",
                "cardinality": "one"
            },
            {
                "unit_id": "runtime.exec.query",
                "eligibility": "resource_visible_nodes",
                "cardinality": "one"
            }
        ]
    })
}

fn root_spec_release_json(root: &RootSpec) -> serde_json::Value {
    let mut selector = serde_json::Map::new();
    if let Some(mount_point) = &root.selector.mount_point {
        selector.insert(
            "mount_point".into(),
            serde_json::json!(mount_point.display().to_string()),
        );
    }
    if let Some(fs_source) = &root.selector.fs_source {
        selector.insert("fs_source".into(), serde_json::json!(fs_source));
    }
    if let Some(fs_type) = &root.selector.fs_type {
        selector.insert("fs_type".into(), serde_json::json!(fs_type));
    }
    if let Some(host_ip) = &root.selector.host_ip {
        selector.insert("host_ip".into(), serde_json::json!(host_ip));
    }
    if let Some(host_ref) = &root.selector.host_ref {
        selector.insert("host_ref".into(), serde_json::json!(host_ref));
    }
    let mut row = serde_json::Map::new();
    row.insert("id".into(), serde_json::json!(root.id));
    row.insert("selector".into(), serde_json::Value::Object(selector));
    row.insert(
        "subpath_scope".into(),
        serde_json::json!(root.subpath_scope.display().to_string()),
    );
    row.insert("watch".into(), serde_json::json!(root.watch));
    row.insert("scan".into(), serde_json::json!(root.scan));
    if let Some(v) = root.audit_interval_ms {
        row.insert("audit_interval_ms".into(), serde_json::json!(v));
    }
    serde_json::Value::Object(row)
}

fn root_spec_app_scope_json(root: &RootSpec) -> serde_json::Value {
    let mut unit_scopes = vec![serde_json::json!({
        "unit_id": "runtime.exec.sink",
        "eligibility": "resource_visible_nodes",
        "cardinality": "one"
    })];
    if root.watch || root.scan {
        unit_scopes.push(serde_json::json!({
            "unit_id": "runtime.exec.source",
            "eligibility": "scope_members",
            "cardinality": "all"
        }));
        unit_scopes.push(serde_json::json!({
            "unit_id": "runtime.exec.query-peer",
            "eligibility": "resource_visible_nodes",
            "cardinality": "one"
        }));
    }
    if root.scan {
        unit_scopes.push(serde_json::json!({
            "unit_id": "runtime.exec.scan",
            "eligibility": "scope_members",
            "cardinality": "all"
        }));
    }
    serde_json::json!({
        "scope_id": root.id,
        "resource_ids": [root.id],
        "unit_scopes": unit_scopes,
    })
}

fn generation_now() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

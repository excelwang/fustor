use std::path::Path;
use std::path::PathBuf;

use capanix_app_sdk::runtime::{RouteKey, RoutePlanMode, RoutePlanSpec};
use capanix_app_sdk::{CnxError, Result};
use capanix_deploy_sdk::compile_relation_target_intent_value;
use fs_meta::RootSpec;
pub use fs_meta::api::types::RootEntry;
pub use fs_meta::api::{ApiAuthConfig, BootstrapAdminConfig, BootstrapManagementConfig};
use fs_meta::product_model::execution_units::{
    QUERY_PEER_RUNTIME_UNIT_ID, QUERY_RUNTIME_UNIT_ID, SINK_RUNTIME_UNIT_ID,
    SOURCE_RUNTIME_UNIT_ID, SOURCE_SCAN_RUNTIME_UNIT_ID,
};
use fs_meta::product_model::routes::{
    ROUTE_KEY_EVENTS, ROUTE_KEY_FACADE_CONTROL, ROUTE_KEY_FORCE_FIND, ROUTE_KEY_QUERY,
    ROUTE_KEY_SINK_QUERY_INTERNAL, ROUTE_KEY_SINK_QUERY_PROXY, ROUTE_KEY_SINK_ROOTS_CONTROL,
    ROUTE_KEY_SINK_STATUS_INTERNAL, ROUTE_KEY_SOURCE_FIND_INTERNAL,
    ROUTE_KEY_SOURCE_RESCAN_CONTROL, ROUTE_KEY_SOURCE_RESCAN_INTERNAL,
    ROUTE_KEY_SOURCE_ROOTS_CONTROL, ROUTE_KEY_SOURCE_STATUS_INTERNAL, sink_query_request_route_for,
    sink_query_route_key_for, sink_roots_control_route_key_for, source_find_request_route_for,
    source_find_route_key_for, source_rescan_request_route_for, source_rescan_route_key_for,
    source_roots_control_route_key_for,
};

const EMPTY_ROOTS_BOOTSTRAP_CONTROL_SCOPE_ID: &str = "__fsmeta_empty_roots_bootstrap";

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
    pub route_plan_node_ids: Vec<String>,
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
    if config.get("workers").is_none() || config["workers"].is_null() {
        config["workers"] = serde_json::json!({});
    }
    // The scope-worker declaration still owns a single top-level app worker.
    // Keep that internal carrier embedded unless an explicit compiler path
    // introduces a different top-level worker role.
    config["workers"]["main"] = serde_json::json!({
        "mode": "embedded"
    });
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
                    "route_units": build_route_units_json(spec),
                    "units": {
                        "runtime.exec.source": {
                            "enabled": true
                        },
                        "runtime.exec.sink": {
                            "enabled": true
                        },
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
                            "runtime.exec.source": {
                                "enabled": true
                            },
                            "runtime.exec.sink": {
                                "enabled": true
                            },
                            "runtime.exec.query": {
                                "enabled": false
                            },
                            "runtime.exec.scan": {
                                "enabled": false
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

pub fn build_startup_manifest_value(
    base_manifest_path: &Path,
    spec: &FsMetaReleaseSpec,
) -> Result<serde_yaml::Value> {
    let yaml = std::fs::read_to_string(base_manifest_path).map_err(|err| {
        CnxError::InvalidInput(format!(
            "read fs-meta startup manifest {} failed: {err}",
            base_manifest_path.display()
        ))
    })?;
    let mut manifest = serde_yaml::from_str(&yaml).map_err(|err| {
        CnxError::InvalidInput(format!(
            "parse fs-meta startup manifest {} failed: {err}",
            base_manifest_path.display()
        ))
    })?;
    ensure_scoped_internal_request_reply_ports(&mut manifest, spec)?;
    Ok(manifest)
}

pub fn write_startup_manifest(
    base_manifest_path: &Path,
    spec: &FsMetaReleaseSpec,
    out_path: &Path,
) -> Result<()> {
    let manifest = build_startup_manifest_value(base_manifest_path, spec)?;
    let yaml = serde_yaml::to_string(&manifest).map_err(|err| {
        CnxError::InvalidInput(format!(
            "encode fs-meta startup manifest {} failed: {err}",
            out_path.display()
        ))
    })?;
    std::fs::write(out_path, yaml).map_err(|err| {
        CnxError::InvalidInput(format!(
            "write fs-meta startup manifest {} failed: {err}",
            out_path.display()
        ))
    })
}

#[cfg(test)]
fn activation_routes_from_manifest(
    manifest: &serde_yaml::Value,
) -> std::collections::BTreeSet<String> {
    let mut routes = std::collections::BTreeSet::new();
    if let Some(ports) = manifest
        .get("ports")
        .and_then(serde_yaml::Value::as_sequence)
    {
        for port in ports {
            let route_key = port.get("route_key").and_then(serde_yaml::Value::as_str);
            let pattern = port.get("pattern").and_then(serde_yaml::Value::as_str);
            match (route_key, pattern) {
                (Some(route_key), Some("request_reply")) => {
                    routes.insert(format!("{route_key}.req"));
                }
                (Some(route_key), Some("stream")) => {
                    routes.insert(format!("{route_key}.stream"));
                }
                _ => {}
            }
        }
    }
    routes
}

fn ensure_scoped_internal_request_reply_ports(
    manifest: &mut serde_yaml::Value,
    spec: &FsMetaReleaseSpec,
) -> Result<()> {
    fn scoped_internal_use_port(base_use_port: &str, node_id: &str) -> String {
        let node_suffix: String = node_id
            .chars()
            .map(|ch| {
                if ch.is_ascii_alphanumeric() {
                    ch.to_ascii_lowercase()
                } else {
                    '_'
                }
            })
            .collect();
        format!("{base_use_port}.{node_suffix}")
    }

    let ports = manifest
        .get_mut("ports")
        .and_then(serde_yaml::Value::as_sequence_mut)
        .ok_or_else(|| CnxError::InvalidInput("fs-meta startup manifest missing ports[]".into()))?;
    let sink_query_template = ports
        .iter()
        .find(|port| {
            port.get("route_key").and_then(serde_yaml::Value::as_str)
                == Some(ROUTE_KEY_SINK_QUERY_INTERNAL)
        })
        .cloned()
        .ok_or_else(|| {
            CnxError::InvalidInput(format!(
                "fs-meta startup manifest missing internal sink-query port {}",
                ROUTE_KEY_SINK_QUERY_INTERNAL
            ))
        })?;
    let source_find_template = ports
        .iter()
        .find(|port| {
            port.get("route_key").and_then(serde_yaml::Value::as_str)
                == Some(ROUTE_KEY_SOURCE_FIND_INTERNAL)
        })
        .cloned()
        .ok_or_else(|| {
            CnxError::InvalidInput(format!(
                "fs-meta startup manifest missing internal source-find port {}",
                ROUTE_KEY_SOURCE_FIND_INTERNAL
            ))
        })?;
    let source_rescan_template = ports
        .iter()
        .find(|port| {
            port.get("route_key").and_then(serde_yaml::Value::as_str)
                == Some(ROUTE_KEY_SOURCE_RESCAN_INTERNAL)
        })
        .cloned()
        .ok_or_else(|| {
            CnxError::InvalidInput(format!(
                "fs-meta startup manifest missing internal source-rescan port {}",
                ROUTE_KEY_SOURCE_RESCAN_INTERNAL
            ))
        })?;
    let source_roots_control_template = ports
        .iter()
        .find(|port| {
            port.get("route_key").and_then(serde_yaml::Value::as_str)
                == Some(ROUTE_KEY_SOURCE_ROOTS_CONTROL)
        })
        .cloned()
        .ok_or_else(|| {
            CnxError::InvalidInput(format!(
                "fs-meta startup manifest missing internal source-roots-control port {}",
                ROUTE_KEY_SOURCE_ROOTS_CONTROL
            ))
        })?;
    let sink_roots_control_template = ports
        .iter()
        .find(|port| {
            port.get("route_key").and_then(serde_yaml::Value::as_str)
                == Some(ROUTE_KEY_SINK_ROOTS_CONTROL)
        })
        .cloned()
        .ok_or_else(|| {
            CnxError::InvalidInput(format!(
                "fs-meta startup manifest missing internal sink-roots-control port {}",
                ROUTE_KEY_SINK_ROOTS_CONTROL
            ))
        })?;
    for node_id in spec
        .route_plan_node_ids
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>()
    {
        for (scoped_route_key, template) in [
            (
                sink_query_route_key_for(&node_id),
                sink_query_template.clone(),
            ),
            (
                source_find_route_key_for(&node_id),
                source_find_template.clone(),
            ),
            (
                source_rescan_route_key_for(&node_id),
                source_rescan_template.clone(),
            ),
            (
                source_roots_control_route_key_for(&node_id),
                source_roots_control_template.clone(),
            ),
            (
                sink_roots_control_route_key_for(&node_id),
                sink_roots_control_template.clone(),
            ),
        ] {
            let already_present = ports.iter().any(|port| {
                port.get("route_key").and_then(serde_yaml::Value::as_str)
                    == Some(scoped_route_key.as_str())
            });
            if already_present {
                continue;
            }
            let scoped_identity_template = matches!(
                template
                    .get("route_key")
                    .and_then(serde_yaml::Value::as_str),
                Some(
                    ROUTE_KEY_SINK_QUERY_INTERNAL
                        | ROUTE_KEY_SOURCE_FIND_INTERNAL
                        | ROUTE_KEY_SOURCE_RESCAN_INTERNAL
                        | ROUTE_KEY_SOURCE_ROOTS_CONTROL
                        | ROUTE_KEY_SINK_ROOTS_CONTROL
                )
            );
            let base_use_port = if scoped_identity_template {
                Some(
                    template
                        .get("use_port")
                        .and_then(serde_yaml::Value::as_str)
                        .ok_or_else(|| {
                            CnxError::InvalidInput(
                                "fs-meta startup manifest scoped internal port missing use_port"
                                    .into(),
                            )
                        })?
                        .to_string(),
                )
            } else {
                None
            };
            let mut scoped_port = template;
            let scoped_map = scoped_port.as_mapping_mut().ok_or_else(|| {
                CnxError::InvalidInput("fs-meta startup manifest port must be a mapping".into())
            })?;
            scoped_map.insert(
                serde_yaml::Value::String("route_key".into()),
                serde_yaml::Value::String(scoped_route_key),
            );
            if let Some(base_use_port) = base_use_port.as_deref() {
                scoped_map.insert(
                    serde_yaml::Value::String("use_port".into()),
                    serde_yaml::Value::String(scoped_internal_use_port(base_use_port, &node_id)),
                );
            }
            ports.push(scoped_port);
        }
    }
    Ok(())
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
    let route_plans = doc
        .get("route_plans")
        .cloned()
        .unwrap_or_else(|| serde_json::json!([]));
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
                "runtime": entry.get("runtime").cloned().unwrap_or_else(|| serde_json::json!({})),
            })
        })
        .collect::<Vec<_>>();
    Ok(serde_json::json!({
        "schema_version": "scope-worker-declaration-v1",
        "target_id": target_id,
        "target_generation": target_generation,
        "route_plans": route_plans,
        "workers": workers,
    }))
}

fn build_route_plans_json(_spec: &FsMetaReleaseSpec) -> Vec<serde_json::Value> {
    let mut plans = Vec::new();
    let mut push_plan = |route_key: RouteKey| {
        plans.push(
            serde_json::to_value(RoutePlanSpec {
                route_key,
                mode: RoutePlanMode::FanOut,
                peers: Vec::new(),
            })
            .unwrap_or_else(|_| serde_json::json!({})),
        );
    };
    push_plan(RouteKey(format!(
        "{}.stream",
        ROUTE_KEY_SOURCE_RESCAN_CONTROL
    )));
    push_plan(RouteKey(format!(
        "{}.req",
        ROUTE_KEY_SOURCE_RESCAN_INTERNAL
    )));
    push_plan(RouteKey(format!(
        "{}.stream",
        ROUTE_KEY_SOURCE_ROOTS_CONTROL
    )));
    push_plan(RouteKey(format!("{}.stream", ROUTE_KEY_SINK_ROOTS_CONTROL)));
    push_plan(RouteKey(format!("{}.stream", ROUTE_KEY_EVENTS)));
    push_plan(RouteKey(format!("{}.req", ROUTE_KEY_SINK_QUERY_INTERNAL)));
    for node_id in _spec
        .route_plan_node_ids
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>()
    {
        push_plan(RouteKey(format!(
            "{}.stream",
            source_roots_control_route_key_for(&node_id)
        )));
        push_plan(source_rescan_request_route_for(&node_id));
        push_plan(RouteKey(format!(
            "{}.stream",
            sink_roots_control_route_key_for(&node_id)
        )));
        push_plan(sink_query_request_route_for(&node_id));
    }
    push_plan(RouteKey(format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY)));
    push_plan(RouteKey(format!("{}.req", ROUTE_KEY_SINK_STATUS_INTERNAL)));
    push_plan(RouteKey(format!(
        "{}.req",
        ROUTE_KEY_SOURCE_STATUS_INTERNAL
    )));
    push_plan(RouteKey(format!("{}.req", ROUTE_KEY_SOURCE_FIND_INTERNAL)));
    for node_id in _spec
        .route_plan_node_ids
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>()
    {
        push_plan(source_find_request_route_for(&node_id));
    }
    plans
}

fn request_reply_activation_route_key(base: &str) -> String {
    format!("{base}.req")
}

fn stream_activation_route_key(base: &str) -> String {
    format!("{base}.stream")
}

fn build_route_units_json(spec: &FsMetaReleaseSpec) -> serde_json::Value {
    let mut route_units = serde_json::Map::new();
    route_units.insert(
        stream_activation_route_key(ROUTE_KEY_FACADE_CONTROL),
        serde_json::json!(["runtime.exec.facade"]),
    );
    route_units.insert(
        request_reply_activation_route_key(ROUTE_KEY_SOURCE_FIND_INTERNAL),
        serde_json::json!([QUERY_RUNTIME_UNIT_ID, QUERY_PEER_RUNTIME_UNIT_ID]),
    );
    for node_id in spec
        .route_plan_node_ids
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>()
    {
        route_units.insert(
            source_find_request_route_for(&node_id).0,
            serde_json::json!([QUERY_RUNTIME_UNIT_ID, QUERY_PEER_RUNTIME_UNIT_ID]),
        );
        route_units.insert(
            source_rescan_request_route_for(&node_id).0,
            serde_json::json!([SOURCE_RUNTIME_UNIT_ID]),
        );
    }
    route_units.insert(
        request_reply_activation_route_key(ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
        serde_json::json!([SOURCE_RUNTIME_UNIT_ID, SOURCE_SCAN_RUNTIME_UNIT_ID]),
    );
    route_units.insert(
        stream_activation_route_key(ROUTE_KEY_SOURCE_RESCAN_CONTROL),
        serde_json::json!([SOURCE_RUNTIME_UNIT_ID]),
    );
    route_units.insert(
        stream_activation_route_key(ROUTE_KEY_SOURCE_ROOTS_CONTROL),
        serde_json::json!([SOURCE_RUNTIME_UNIT_ID]),
    );
    route_units.insert(
        stream_activation_route_key(ROUTE_KEY_SINK_ROOTS_CONTROL),
        serde_json::json!([SINK_RUNTIME_UNIT_ID]),
    );
    route_units.insert(
        request_reply_activation_route_key(ROUTE_KEY_QUERY),
        serde_json::json!([QUERY_RUNTIME_UNIT_ID]),
    );
    route_units.insert(
        request_reply_activation_route_key(ROUTE_KEY_SINK_QUERY_INTERNAL),
        serde_json::json!([SINK_RUNTIME_UNIT_ID]),
    );
    for node_id in spec
        .route_plan_node_ids
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>()
    {
        route_units.insert(
            stream_activation_route_key(&source_roots_control_route_key_for(&node_id)),
            serde_json::json!([SOURCE_RUNTIME_UNIT_ID]),
        );
        route_units.insert(
            stream_activation_route_key(&sink_roots_control_route_key_for(&node_id)),
            serde_json::json!([SINK_RUNTIME_UNIT_ID]),
        );
        route_units.insert(
            sink_query_request_route_for(&node_id).0,
            serde_json::json!([SINK_RUNTIME_UNIT_ID]),
        );
    }
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
        serde_json::json!([SOURCE_RUNTIME_UNIT_ID]),
    );
    route_units.insert(
        request_reply_activation_route_key(ROUTE_KEY_FORCE_FIND),
        serde_json::json!([SINK_RUNTIME_UNIT_ID]),
    );
    route_units.insert(
        stream_activation_route_key(ROUTE_KEY_EVENTS),
        serde_json::json!([SOURCE_RUNTIME_UNIT_ID, SINK_RUNTIME_UNIT_ID]),
    );
    serde_json::Value::Object(route_units)
}

fn build_app_scopes_json(spec: &FsMetaReleaseSpec) -> Vec<serde_json::Value> {
    let mut scopes = spec
        .roots
        .iter()
        .map(root_spec_app_scope_json)
        .collect::<Vec<_>>();
    if spec.roots.is_empty() {
        scopes.push(empty_roots_bootstrap_app_scope_json());
    }
    scopes.push(facade_app_scope_json(spec));
    scopes
}

fn empty_roots_bootstrap_app_scope_json() -> serde_json::Value {
    // `roots=[]` is a valid deployed state. Keep source/query-peer control
    // reachability alive across scope members so online roots apply can fan
    // out later without predeclared business scopes. Also keep one sink
    // execution slot alive so sink-owned public freshness routes such as
    // `/on-demand-force-find` can become active once roots are applied online.
    serde_json::json!({
        "scope_id": EMPTY_ROOTS_BOOTSTRAP_CONTROL_SCOPE_ID,
        "resource_ids": [],
        "unit_scopes": [
            {
                "unit_id": SOURCE_RUNTIME_UNIT_ID,
                "eligibility": "scope_members",
                "cardinality": "all"
            },
            {
                "unit_id": SOURCE_SCAN_RUNTIME_UNIT_ID,
                "eligibility": "scope_members",
                "cardinality": "all"
            },
            {
                "unit_id": QUERY_PEER_RUNTIME_UNIT_ID,
                "eligibility": "scope_members",
                "cardinality": "all"
            },
            {
                "unit_id": SINK_RUNTIME_UNIT_ID,
                "eligibility": "scope_members",
                "cardinality": "one"
            }
        ]
    })
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
        "eligibility": "scope_members",
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
            "eligibility": "scope_members",
            "cardinality": "all"
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watched_roots_bind_query_peer_on_all_scope_members() {
        let scope = root_spec_app_scope_json(&RootSpec::new("nfs1", "/mnt/nfs1"));
        let unit_scopes = scope
            .get("unit_scopes")
            .and_then(serde_json::Value::as_array)
            .expect("unit_scopes array");
        let query_peer = unit_scopes
            .iter()
            .find(|entry| {
                entry.get("unit_id") == Some(&serde_json::json!("runtime.exec.query-peer"))
            })
            .expect("query-peer unit scope");
        assert_eq!(
            query_peer.get("eligibility"),
            Some(&serde_json::json!("scope_members"))
        );
        assert_eq!(
            query_peer.get("cardinality"),
            Some(&serde_json::json!("all"))
        );
    }

    #[test]
    fn watched_roots_bind_one_sink_owner_from_scope_members() {
        let scope = root_spec_app_scope_json(&RootSpec::new("nfs1", "/mnt/nfs1"));
        let unit_scopes = scope
            .get("unit_scopes")
            .and_then(serde_json::Value::as_array)
            .expect("unit_scopes array");
        let sink = unit_scopes
            .iter()
            .find(|entry| entry.get("unit_id") == Some(&serde_json::json!("runtime.exec.sink")))
            .expect("sink unit scope");
        assert_eq!(
            sink.get("eligibility"),
            Some(&serde_json::json!("scope_members")),
            "sink materialization owner should be selected from the monitoring group members"
        );
        assert_eq!(
            sink.get("cardinality"),
            Some(&serde_json::json!("one")),
            "each monitoring group should have one sink materialization owner"
        );
    }

    #[test]
    fn route_plans_include_internal_sink_query_for_chosen_node_materialized_reads() {
        let spec = FsMetaReleaseSpec {
            app_id: "test-app".to_string(),
            api_facade_resource_id: "listener".to_string(),
            auth: ApiAuthConfig::default(),
            roots: vec![RootSpec::new("nfs1", "/mnt/nfs1")],
            route_plan_node_ids: Vec::new(),
            worker_module_path: None,
            worker_modes: Default::default(),
        };

        let plans = build_route_plans_json(&spec);
        let expected_route = format!("{}.req", ROUTE_KEY_SINK_QUERY_INTERNAL);
        let has_internal_sink_query = plans.iter().any(|plan| {
            plan.get("route_key").and_then(serde_json::Value::as_str)
                == Some(expected_route.as_str())
        });

        assert!(
            has_internal_sink_query,
            "release route_plans must include the internal sink-query route used by chosen-node materialized queries"
        );
    }

    #[test]
    fn route_plans_include_owner_scoped_internal_sink_query_for_known_nodes() {
        let spec = FsMetaReleaseSpec {
            app_id: "test-app".to_string(),
            api_facade_resource_id: "listener".to_string(),
            auth: ApiAuthConfig::default(),
            roots: vec![RootSpec::new("nfs1", "/mnt/nfs1")],
            route_plan_node_ids: vec![
                "node-a-123456789".to_string(),
                "node-b-987654321".to_string(),
            ],
            worker_module_path: None,
            worker_modes: Default::default(),
        };

        let plans = build_route_plans_json(&spec);
        let expected_routes = spec
            .route_plan_node_ids
            .iter()
            .map(|node_id| sink_query_request_route_for(node_id).0)
            .collect::<Vec<_>>();

        for expected_route in expected_routes {
            let has_scoped_route = plans.iter().any(|plan| {
                plan.get("route_key").and_then(serde_json::Value::as_str)
                    == Some(expected_route.as_str())
            });
            assert!(
                has_scoped_route,
                "release route_plans must include owner-scoped internal sink-query route {expected_route} when runtime node ids are known"
            );
        }
    }

    #[test]
    fn route_plans_include_owner_scoped_internal_source_find_for_known_nodes() {
        let spec = FsMetaReleaseSpec {
            app_id: "test-app".to_string(),
            api_facade_resource_id: "listener".to_string(),
            auth: ApiAuthConfig::default(),
            roots: vec![RootSpec::new("nfs1", "/mnt/nfs1")],
            route_plan_node_ids: vec![
                "node-a-123456789".to_string(),
                "node-b-987654321".to_string(),
            ],
            worker_module_path: None,
            worker_modes: Default::default(),
        };

        let plans = build_route_plans_json(&spec);
        let expected_routes = spec
            .route_plan_node_ids
            .iter()
            .map(|node_id| source_find_request_route_for(node_id).0)
            .collect::<Vec<_>>();

        for expected_route in expected_routes {
            let has_scoped_route = plans.iter().any(|plan| {
                plan.get("route_key").and_then(serde_json::Value::as_str)
                    == Some(expected_route.as_str())
            });
            assert!(
                has_scoped_route,
                "release route_plans must include owner-scoped internal source-find route {expected_route} when runtime node ids are known"
            );
        }
    }

    #[test]
    fn route_plans_include_internal_logical_roots_control_streams() {
        let spec = FsMetaReleaseSpec {
            app_id: "test-app".to_string(),
            api_facade_resource_id: "listener".to_string(),
            auth: ApiAuthConfig::default(),
            roots: vec![RootSpec::new("nfs1", "/mnt/nfs1")],
            route_plan_node_ids: Vec::new(),
            worker_module_path: None,
            worker_modes: Default::default(),
        };

        let plans = build_route_plans_json(&spec);
        for expected_route in [
            format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
            format!("{}.stream", ROUTE_KEY_SINK_ROOTS_CONTROL),
        ] {
            let has_route = plans.iter().any(|plan| {
                plan.get("route_key").and_then(serde_json::Value::as_str)
                    == Some(expected_route.as_str())
            });
            assert!(
                has_route,
                "release route_plans must include internal logical-roots control stream {expected_route}"
            );
        }
    }

    #[test]
    fn route_plans_include_internal_source_rescan_request_route() {
        let spec = FsMetaReleaseSpec {
            app_id: "test-app".to_string(),
            api_facade_resource_id: "listener".to_string(),
            auth: ApiAuthConfig::default(),
            roots: vec![RootSpec::new("nfs1", "/mnt/nfs1")],
            route_plan_node_ids: vec!["node-b-987654321".to_string()],
            worker_module_path: None,
            worker_modes: Default::default(),
        };

        let plans = build_route_plans_json(&spec);
        for expected_route in [
            request_reply_activation_route_key(ROUTE_KEY_SOURCE_RESCAN_INTERNAL),
            source_rescan_request_route_for("node-b-987654321").0,
        ] {
            assert!(
                plans.iter().any(|plan| {
                    plan.get("route_key").and_then(serde_json::Value::as_str)
                        == Some(expected_route.as_str())
                }),
                "release route_plans must include source rescan request route {expected_route} so management rescan reaches active source runners"
            );
        }
    }

    #[test]
    fn route_plans_include_events_stream_for_sink_materialization() {
        let spec = FsMetaReleaseSpec {
            app_id: "test-app".to_string(),
            api_facade_resource_id: "listener".to_string(),
            auth: ApiAuthConfig::default(),
            roots: vec![RootSpec::new("nfs1", "/mnt/nfs1")],
            route_plan_node_ids: Vec::new(),
            worker_module_path: None,
            worker_modes: Default::default(),
        };

        let plans = build_route_plans_json(&spec);
        let expected_route = format!("{}.stream", ROUTE_KEY_EVENTS);
        assert!(
            plans.iter().any(|plan| {
                plan.get("route_key").and_then(serde_json::Value::as_str)
                    == Some(expected_route.as_str())
            }),
            "release route_plans must include the fs-meta events stream so sink placement can materialize assigned groups"
        );
    }

    #[test]
    fn route_units_assign_events_stream_to_source_and_sink() {
        let spec = FsMetaReleaseSpec {
            app_id: "test-app".to_string(),
            api_facade_resource_id: "listener".to_string(),
            auth: ApiAuthConfig::default(),
            roots: vec![RootSpec::new("nfs1", "/mnt/nfs1")],
            route_plan_node_ids: Vec::new(),
            worker_module_path: None,
            worker_modes: Default::default(),
        };

        let release_doc = build_release_doc_value(&spec);
        let route_units = release_doc["units"][0]["runtime"]["route_units"]
            .as_object()
            .expect("route_units object");
        let events_route = stream_activation_route_key(ROUTE_KEY_EVENTS);
        let units = route_units
            .get(&events_route)
            .and_then(serde_json::Value::as_array)
            .expect("events route units");
        let unit_ids = units
            .iter()
            .filter_map(serde_json::Value::as_str)
            .collect::<std::collections::BTreeSet<_>>();

        assert_eq!(
            unit_ids,
            std::collections::BTreeSet::from([SOURCE_RUNTIME_UNIT_ID, SINK_RUNTIME_UNIT_ID]),
            "events stream must bind source SEND and sink RECV units so remote sink owners can receive materialization input"
        );
    }

    #[test]
    fn route_units_assign_sink_status_to_query_facades() {
        let spec = FsMetaReleaseSpec {
            app_id: "test-app".to_string(),
            api_facade_resource_id: "listener".to_string(),
            auth: ApiAuthConfig::default(),
            roots: vec![RootSpec::new("nfs1", "/mnt/nfs1")],
            route_plan_node_ids: Vec::new(),
            worker_module_path: None,
            worker_modes: Default::default(),
        };

        let release_doc = build_release_doc_value(&spec);
        let route_units = release_doc["units"][0]["runtime"]["route_units"]
            .as_object()
            .expect("route_units object");
        let sink_status_route = request_reply_activation_route_key(ROUTE_KEY_SINK_STATUS_INTERNAL);
        let units = route_units
            .get(&sink_status_route)
            .and_then(serde_json::Value::as_array)
            .expect("sink-status route units");
        let unit_ids = units
            .iter()
            .filter_map(serde_json::Value::as_str)
            .collect::<std::collections::BTreeSet<_>>();

        assert_eq!(
            unit_ids,
            std::collections::BTreeSet::from([QUERY_RUNTIME_UNIT_ID, QUERY_PEER_RUNTIME_UNIT_ID]),
            "sink-status route ownership must stay with query/query-peer facade units, not the sink runtime unit"
        );
    }

    #[test]
    fn route_plans_include_owner_scoped_internal_logical_roots_control_streams_for_known_nodes() {
        let spec = FsMetaReleaseSpec {
            app_id: "test-app".to_string(),
            api_facade_resource_id: "listener".to_string(),
            auth: ApiAuthConfig::default(),
            roots: vec![RootSpec::new("nfs1", "/mnt/nfs1")],
            route_plan_node_ids: vec![
                "node-a-123456789".to_string(),
                "node-b-987654321".to_string(),
            ],
            worker_module_path: None,
            worker_modes: Default::default(),
        };

        let plans = build_route_plans_json(&spec);
        for expected_route in spec.route_plan_node_ids.iter().flat_map(|node_id| {
            [
                format!("{}.stream", source_roots_control_route_key_for(node_id)),
                format!("{}.stream", sink_roots_control_route_key_for(node_id)),
            ]
        }) {
            let has_route = plans.iter().any(|plan| {
                plan.get("route_key").and_then(serde_json::Value::as_str)
                    == Some(expected_route.as_str())
            });
            assert!(
                has_route,
                "release route_plans must include owner-scoped logical-roots control stream {expected_route} when runtime node ids are known"
            );
        }
    }

    #[test]
    fn route_units_include_owner_scoped_internal_sink_query_for_known_nodes() {
        let spec = FsMetaReleaseSpec {
            app_id: "test-app".to_string(),
            api_facade_resource_id: "listener".to_string(),
            auth: ApiAuthConfig::default(),
            roots: vec![RootSpec::new("nfs1", "/mnt/nfs1")],
            route_plan_node_ids: vec![
                "node-a-123456789".to_string(),
                "node-b-987654321".to_string(),
            ],
            worker_module_path: None,
            worker_modes: Default::default(),
        };

        let release_doc = build_release_doc_value(&spec);
        let route_units = release_doc["units"][0]["runtime"]["route_units"].clone();
        let route_units_map = route_units.as_object().expect("route_units object");

        for expected_route in spec
            .route_plan_node_ids
            .iter()
            .map(|node_id| sink_query_request_route_for(node_id).0)
        {
            let scoped_units = route_units_map
                .get(&expected_route)
                .and_then(serde_json::Value::as_array);
            assert!(
                scoped_units.is_some(),
                "release route_units must include owner-scoped internal sink-query route {expected_route} when runtime node ids are known"
            );
        }
    }

    #[test]
    fn route_units_include_owner_scoped_internal_source_find_for_known_nodes() {
        let spec = FsMetaReleaseSpec {
            app_id: "test-app".to_string(),
            api_facade_resource_id: "listener".to_string(),
            auth: ApiAuthConfig::default(),
            roots: vec![RootSpec::new("nfs1", "/mnt/nfs1")],
            route_plan_node_ids: vec![
                "node-a-123456789".to_string(),
                "node-b-987654321".to_string(),
            ],
            worker_module_path: None,
            worker_modes: Default::default(),
        };

        let release_doc = build_release_doc_value(&spec);
        let route_units = release_doc["units"][0]["runtime"]["route_units"].clone();
        let route_units_map = route_units.as_object().expect("route_units object");

        for expected_route in spec
            .route_plan_node_ids
            .iter()
            .map(|node_id| source_find_request_route_for(node_id).0)
        {
            let scoped_units = route_units_map
                .get(&expected_route)
                .and_then(serde_json::Value::as_array);
            assert!(
                scoped_units.is_some(),
                "release route_units must include owner-scoped internal source-find route {expected_route} when runtime node ids are known"
            );
        }
    }

    #[test]
    fn route_units_include_owner_scoped_internal_logical_roots_control_streams_for_known_nodes() {
        let spec = FsMetaReleaseSpec {
            app_id: "test-app".to_string(),
            api_facade_resource_id: "listener".to_string(),
            auth: ApiAuthConfig::default(),
            roots: vec![RootSpec::new("nfs1", "/mnt/nfs1")],
            route_plan_node_ids: vec![
                "node-a-123456789".to_string(),
                "node-b-987654321".to_string(),
            ],
            worker_module_path: None,
            worker_modes: Default::default(),
        };

        let release_doc = build_release_doc_value(&spec);
        let route_units = release_doc["units"][0]["runtime"]["route_units"]
            .as_object()
            .expect("route_units object");

        for expected_route in spec.route_plan_node_ids.iter().flat_map(|node_id| {
            [
                format!("{}.stream", source_roots_control_route_key_for(node_id)),
                format!("{}.stream", sink_roots_control_route_key_for(node_id)),
            ]
        }) {
            assert!(
                route_units.contains_key(&expected_route),
                "release route_units must include owner-scoped logical-roots control stream {expected_route} when runtime node ids are known"
            );
        }
    }

    #[test]
    fn route_units_include_internal_logical_roots_control_streams() {
        let spec = FsMetaReleaseSpec {
            app_id: "test-app".to_string(),
            api_facade_resource_id: "listener".to_string(),
            auth: ApiAuthConfig::default(),
            roots: vec![RootSpec::new("nfs1", "/mnt/nfs1")],
            route_plan_node_ids: Vec::new(),
            worker_module_path: None,
            worker_modes: Default::default(),
        };

        let release_doc = build_release_doc_value(&spec);
        let route_units = release_doc["units"][0]["runtime"]["route_units"]
            .as_object()
            .expect("route_units object");

        for expected_route in [
            format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
            format!("{}.stream", ROUTE_KEY_SINK_ROOTS_CONTROL),
        ] {
            assert!(
                route_units.contains_key(&expected_route),
                "release route_units must include internal logical-roots control stream {expected_route}"
            );
        }
    }

    #[test]
    fn route_units_include_internal_source_rescan_request_route() {
        let spec = FsMetaReleaseSpec {
            app_id: "test-app".to_string(),
            api_facade_resource_id: "listener".to_string(),
            auth: ApiAuthConfig::default(),
            roots: vec![RootSpec::new("nfs1", "/mnt/nfs1")],
            route_plan_node_ids: vec!["node-b-987654321".to_string()],
            worker_module_path: None,
            worker_modes: Default::default(),
        };

        let release_doc = build_release_doc_value(&spec);
        let route_units = release_doc["units"][0]["runtime"]["route_units"]
            .as_object()
            .expect("route_units object");
        let rescan_route = request_reply_activation_route_key(ROUTE_KEY_SOURCE_RESCAN_INTERNAL);
        let units = route_units
            .get(&rescan_route)
            .and_then(serde_json::Value::as_array)
            .expect("source-rescan route units");
        let unit_ids = units
            .iter()
            .filter_map(serde_json::Value::as_str)
            .collect::<std::collections::BTreeSet<_>>();

        assert_eq!(
            unit_ids,
            std::collections::BTreeSet::from([SOURCE_RUNTIME_UNIT_ID, SOURCE_SCAN_RUNTIME_UNIT_ID]),
            "source-rescan request route must bind the source lanes that own explicit rescan delivery"
        );
        let scoped_rescan_route = source_rescan_request_route_for("node-b-987654321").0;
        let scoped_units = route_units
            .get(&scoped_rescan_route)
            .and_then(serde_json::Value::as_array)
            .expect("scoped source-rescan route units");
        let scoped_unit_ids = scoped_units
            .iter()
            .filter_map(serde_json::Value::as_str)
            .collect::<std::collections::BTreeSet<_>>();

        assert_eq!(
            scoped_unit_ids,
            std::collections::BTreeSet::from([SOURCE_RUNTIME_UNIT_ID]),
            "node-scoped source-rescan request routes must bind the source unit that returns explicit delivery evidence"
        );
    }

    #[test]
    fn startup_manifest_activation_routes_cover_internal_logical_roots_control_streams() {
        let spec = FsMetaReleaseSpec {
            app_id: "test-app".to_string(),
            api_facade_resource_id: "listener".to_string(),
            auth: ApiAuthConfig::default(),
            roots: vec![RootSpec::new("nfs1", "/mnt/nfs1")],
            route_plan_node_ids: Vec::new(),
            worker_module_path: None,
            worker_modes: Default::default(),
        };

        let manifest_path =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../fixtures/manifests/fs-meta.yaml");
        let manifest =
            build_startup_manifest_value(&manifest_path, &spec).expect("load startup manifest");
        let activation_routes = activation_routes_from_manifest(&manifest);

        for expected_route in [
            format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL),
            format!("{}.stream", ROUTE_KEY_SINK_ROOTS_CONTROL),
        ] {
            assert!(
                activation_routes.contains(&expected_route),
                "startup manifest activation routes must cover internal logical-roots control stream {expected_route}"
            );
        }
    }

    #[test]
    fn startup_manifest_activation_routes_cover_owner_scoped_internal_logical_roots_control_streams_for_known_nodes()
     {
        let spec = FsMetaReleaseSpec {
            app_id: "test-app".to_string(),
            api_facade_resource_id: "listener".to_string(),
            auth: ApiAuthConfig::default(),
            roots: vec![RootSpec::new("nfs1", "/mnt/nfs1")],
            route_plan_node_ids: vec![
                "node-a-123456789".to_string(),
                "node-b-987654321".to_string(),
            ],
            worker_module_path: None,
            worker_modes: Default::default(),
        };

        let manifest_path =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../fixtures/manifests/fs-meta.yaml");
        let manifest =
            build_startup_manifest_value(&manifest_path, &spec).expect("load startup manifest");
        let activation_routes = activation_routes_from_manifest(&manifest);

        for expected_route in spec.route_plan_node_ids.iter().flat_map(|node_id| {
            [
                format!("{}.stream", source_roots_control_route_key_for(node_id)),
                format!("{}.stream", sink_roots_control_route_key_for(node_id)),
            ]
        }) {
            assert!(
                activation_routes.contains(&expected_route),
                "startup manifest activation routes must cover owner-scoped logical-roots control stream {expected_route}"
            );
        }
    }

    #[test]
    fn startup_manifest_scoped_source_roots_control_ports_use_distinct_use_port_identity() {
        let spec = FsMetaReleaseSpec {
            app_id: "test-app".to_string(),
            api_facade_resource_id: "listener".to_string(),
            auth: ApiAuthConfig::default(),
            roots: vec![RootSpec::new("nfs1", "/mnt/nfs1")],
            route_plan_node_ids: vec![
                "node-a-123456789".to_string(),
                "node-b-987654321".to_string(),
            ],
            worker_module_path: None,
            worker_modes: Default::default(),
        };

        let manifest_path =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../fixtures/manifests/fs-meta.yaml");
        let manifest =
            build_startup_manifest_value(&manifest_path, &spec).expect("load startup manifest");
        let ports = manifest
            .get("ports")
            .and_then(serde_yaml::Value::as_sequence)
            .expect("ports sequence");

        let generic_use_port = ports
            .iter()
            .find(|port| {
                port.get("route_key").and_then(serde_yaml::Value::as_str)
                    == Some(ROUTE_KEY_SOURCE_ROOTS_CONTROL)
            })
            .and_then(|port| port.get("use_port"))
            .and_then(serde_yaml::Value::as_str)
            .expect("generic source roots-control use_port");

        for node_id in &spec.route_plan_node_ids {
            let scoped_use_port = ports
                .iter()
                .find(|port| {
                    port.get("route_key").and_then(serde_yaml::Value::as_str)
                        == Some(source_roots_control_route_key_for(node_id).as_str())
                })
                .and_then(|port| port.get("use_port"))
                .and_then(serde_yaml::Value::as_str)
                .expect("scoped source roots-control use_port");
            assert_ne!(
                scoped_use_port, generic_use_port,
                "scoped source roots-control ports must not reuse the generic use_port identity"
            );
            assert!(
                scoped_use_port.contains("node_"),
                "scoped source roots-control use_port should remain node-distinct for runtime attach identity: {scoped_use_port}"
            );
        }
    }

    #[test]
    fn startup_manifest_scoped_sink_roots_control_ports_use_distinct_use_port_identity() {
        let spec = FsMetaReleaseSpec {
            app_id: "test-app".to_string(),
            api_facade_resource_id: "listener".to_string(),
            auth: ApiAuthConfig::default(),
            roots: vec![RootSpec::new("nfs1", "/mnt/nfs1")],
            route_plan_node_ids: vec![
                "node-a-123456789".to_string(),
                "node-b-987654321".to_string(),
            ],
            worker_module_path: None,
            worker_modes: Default::default(),
        };

        let manifest_path =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../fixtures/manifests/fs-meta.yaml");
        let manifest =
            build_startup_manifest_value(&manifest_path, &spec).expect("load startup manifest");
        let ports = manifest
            .get("ports")
            .and_then(serde_yaml::Value::as_sequence)
            .expect("ports sequence");

        let generic_use_port = ports
            .iter()
            .find(|port| {
                port.get("route_key").and_then(serde_yaml::Value::as_str)
                    == Some(ROUTE_KEY_SINK_ROOTS_CONTROL)
            })
            .and_then(|port| port.get("use_port"))
            .and_then(serde_yaml::Value::as_str)
            .expect("generic sink roots-control use_port");

        for node_id in &spec.route_plan_node_ids {
            let scoped_use_port = ports
                .iter()
                .find(|port| {
                    port.get("route_key").and_then(serde_yaml::Value::as_str)
                        == Some(sink_roots_control_route_key_for(node_id).as_str())
                })
                .and_then(|port| port.get("use_port"))
                .and_then(serde_yaml::Value::as_str)
                .expect("scoped sink roots-control use_port");
            assert_ne!(
                scoped_use_port, generic_use_port,
                "scoped sink roots-control ports must not reuse the generic use_port identity"
            );
            assert!(
                scoped_use_port.contains("node_"),
                "scoped sink roots-control use_port should remain node-distinct for runtime attach identity: {scoped_use_port}"
            );
        }
    }

    #[test]
    fn startup_manifest_scoped_source_find_ports_use_distinct_use_port_identity() {
        let spec = FsMetaReleaseSpec {
            app_id: "test-app".to_string(),
            api_facade_resource_id: "listener".to_string(),
            auth: ApiAuthConfig::default(),
            roots: vec![RootSpec::new("nfs1", "/mnt/nfs1")],
            route_plan_node_ids: vec![
                "node-a-123456789".to_string(),
                "node-b-987654321".to_string(),
            ],
            worker_module_path: None,
            worker_modes: Default::default(),
        };

        let manifest_path =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../fixtures/manifests/fs-meta.yaml");
        let manifest =
            build_startup_manifest_value(&manifest_path, &spec).expect("load startup manifest");
        let ports = manifest
            .get("ports")
            .and_then(serde_yaml::Value::as_sequence)
            .expect("ports sequence");

        let generic_use_port = ports
            .iter()
            .find(|port| {
                port.get("route_key").and_then(serde_yaml::Value::as_str)
                    == Some(ROUTE_KEY_SOURCE_FIND_INTERNAL)
            })
            .and_then(|port| port.get("use_port"))
            .and_then(serde_yaml::Value::as_str)
            .expect("generic source find use_port");

        for node_id in &spec.route_plan_node_ids {
            let scoped_use_port = ports
                .iter()
                .find(|port| {
                    port.get("route_key").and_then(serde_yaml::Value::as_str)
                        == Some(source_find_route_key_for(node_id).as_str())
                })
                .and_then(|port| port.get("use_port"))
                .and_then(serde_yaml::Value::as_str)
                .expect("scoped source find use_port");
            assert_ne!(
                scoped_use_port, generic_use_port,
                "scoped source find ports must not reuse the generic use_port identity"
            );
            assert!(
                scoped_use_port.contains("node_"),
                "scoped source find use_port should remain node-distinct for runtime attach identity: {scoped_use_port}"
            );
        }
    }

    #[test]
    fn startup_manifest_activation_routes_cover_owner_scoped_internal_source_rescan_for_known_nodes()
     {
        let spec = FsMetaReleaseSpec {
            app_id: "test-app".to_string(),
            api_facade_resource_id: "listener".to_string(),
            auth: ApiAuthConfig::default(),
            roots: vec![RootSpec::new("nfs1", "/mnt/nfs1")],
            route_plan_node_ids: vec![
                "node-a-123456789".to_string(),
                "node-b-987654321".to_string(),
            ],
            worker_module_path: None,
            worker_modes: Default::default(),
        };

        let manifest_path =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../fixtures/manifests/fs-meta.yaml");
        let manifest =
            build_startup_manifest_value(&manifest_path, &spec).expect("load startup manifest");
        let activation_routes = activation_routes_from_manifest(&manifest);

        for expected_route in spec
            .route_plan_node_ids
            .iter()
            .map(|node_id| source_rescan_request_route_for(node_id).0)
        {
            assert!(
                activation_routes.contains(&expected_route),
                "startup manifest activation routes must cover owner-scoped source-rescan route {expected_route}"
            );
        }
    }

    #[test]
    fn startup_manifest_scoped_source_rescan_ports_use_distinct_use_port_identity() {
        let spec = FsMetaReleaseSpec {
            app_id: "test-app".to_string(),
            api_facade_resource_id: "listener".to_string(),
            auth: ApiAuthConfig::default(),
            roots: vec![RootSpec::new("nfs1", "/mnt/nfs1")],
            route_plan_node_ids: vec![
                "node-a-123456789".to_string(),
                "node-b-987654321".to_string(),
            ],
            worker_module_path: None,
            worker_modes: Default::default(),
        };

        let manifest_path =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../fixtures/manifests/fs-meta.yaml");
        let manifest =
            build_startup_manifest_value(&manifest_path, &spec).expect("load startup manifest");
        let ports = manifest
            .get("ports")
            .and_then(serde_yaml::Value::as_sequence)
            .expect("ports sequence");

        let generic_use_port = ports
            .iter()
            .find(|port| {
                port.get("route_key").and_then(serde_yaml::Value::as_str)
                    == Some(ROUTE_KEY_SOURCE_RESCAN_INTERNAL)
            })
            .and_then(|port| port.get("use_port"))
            .and_then(serde_yaml::Value::as_str)
            .expect("generic source rescan use_port");

        for node_id in &spec.route_plan_node_ids {
            let scoped_use_port = ports
                .iter()
                .find(|port| {
                    port.get("route_key").and_then(serde_yaml::Value::as_str)
                        == Some(source_rescan_route_key_for(node_id).as_str())
                })
                .and_then(|port| port.get("use_port"))
                .and_then(serde_yaml::Value::as_str)
                .expect("scoped source rescan use_port");
            assert_ne!(
                scoped_use_port, generic_use_port,
                "scoped source-rescan ports must not reuse the generic use_port identity"
            );
            assert!(
                scoped_use_port.contains("node_"),
                "scoped source-rescan use_port should remain node-distinct for runtime attach identity: {scoped_use_port}"
            );
        }
    }

    #[test]
    fn startup_manifest_scoped_sink_query_ports_use_distinct_use_port_identity() {
        let spec = FsMetaReleaseSpec {
            app_id: "test-app".to_string(),
            api_facade_resource_id: "listener".to_string(),
            auth: ApiAuthConfig::default(),
            roots: vec![RootSpec::new("nfs1", "/mnt/nfs1")],
            route_plan_node_ids: vec![
                "node-a-123456789".to_string(),
                "node-b-987654321".to_string(),
            ],
            worker_module_path: None,
            worker_modes: Default::default(),
        };

        let manifest_path =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../fixtures/manifests/fs-meta.yaml");
        let manifest =
            build_startup_manifest_value(&manifest_path, &spec).expect("load startup manifest");
        let ports = manifest
            .get("ports")
            .and_then(serde_yaml::Value::as_sequence)
            .expect("ports sequence");

        let generic_use_port = ports
            .iter()
            .find(|port| {
                port.get("route_key").and_then(serde_yaml::Value::as_str)
                    == Some(ROUTE_KEY_SINK_QUERY_INTERNAL)
            })
            .and_then(|port| port.get("use_port"))
            .and_then(serde_yaml::Value::as_str)
            .expect("generic sink query use_port");

        for node_id in &spec.route_plan_node_ids {
            let scoped_use_port = ports
                .iter()
                .find(|port| {
                    port.get("route_key").and_then(serde_yaml::Value::as_str)
                        == Some(sink_query_route_key_for(node_id).as_str())
                })
                .and_then(|port| port.get("use_port"))
                .and_then(serde_yaml::Value::as_str)
                .expect("scoped sink query use_port");
            assert_ne!(
                scoped_use_port, generic_use_port,
                "scoped sink query ports must not reuse the generic use_port identity"
            );
            assert!(
                scoped_use_port.contains("node_"),
                "scoped sink query use_port should remain node-distinct for runtime attach identity: {scoped_use_port}"
            );
        }
    }

    #[test]
    fn startup_manifest_activation_routes_cover_owner_scoped_internal_sink_query_for_known_nodes() {
        let spec = FsMetaReleaseSpec {
            app_id: "test-app".to_string(),
            api_facade_resource_id: "listener".to_string(),
            auth: ApiAuthConfig::default(),
            roots: vec![RootSpec::new("nfs1", "/mnt/nfs1")],
            route_plan_node_ids: vec![
                "node-a-123456789".to_string(),
                "node-b-987654321".to_string(),
            ],
            worker_module_path: None,
            worker_modes: Default::default(),
        };

        let release_doc = build_release_doc_value(&spec);
        let route_units = release_doc["units"][0]["runtime"]["route_units"]
            .as_object()
            .expect("route_units object");
        let manifest_path =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../fixtures/manifests/fs-meta.yaml");
        let manifest =
            build_startup_manifest_value(&manifest_path, &spec).expect("load startup manifest");
        let activation_routes = activation_routes_from_manifest(&manifest);

        for route_key in route_units.keys() {
            assert!(
                activation_routes.contains(route_key),
                "startup manifest activation routes must cover release runtime.route_units key {route_key}"
            );
        }
    }

    #[test]
    fn compile_relation_target_intent_preserves_runtime_scopes_and_route_units() {
        let spec = FsMetaReleaseSpec {
            app_id: "test-app".to_string(),
            api_facade_resource_id: "listener".to_string(),
            auth: ApiAuthConfig::default(),
            roots: vec![RootSpec::new("nfs1", "/mnt/nfs1")],
            route_plan_node_ids: vec!["node-a".to_string(), "node-b".to_string()],
            worker_module_path: None,
            worker_modes: Default::default(),
        };

        let release_doc = build_release_doc_value(&spec);
        let declaration = scope_unit_intent_to_scope_worker_intent_value(&release_doc)
            .expect("convert scope-unit intent to scope-worker intent");
        let worker_runtime = declaration["workers"][0]["runtime"]
            .as_object()
            .expect("worker runtime object");

        assert!(
            worker_runtime.contains_key("app_scopes"),
            "compiled relation target intent must preserve runtime.app_scopes"
        );
        assert!(
            worker_runtime.contains_key("route_units"),
            "compiled relation target intent must preserve runtime.route_units"
        );
        assert!(
            worker_runtime.contains_key("units"),
            "compiled relation target intent must preserve runtime.units"
        );
    }

    #[test]
    fn compile_relation_target_intent_preserves_scoped_logical_roots_control_route_units() {
        let spec = FsMetaReleaseSpec {
            app_id: "test-app".to_string(),
            api_facade_resource_id: "listener".to_string(),
            auth: ApiAuthConfig::default(),
            roots: vec![RootSpec::new("nfs1", "/mnt/nfs1")],
            route_plan_node_ids: vec![
                "node-a-123456789".to_string(),
                "node-b-987654321".to_string(),
            ],
            worker_module_path: None,
            worker_modes: Default::default(),
        };

        let release_doc = build_release_doc_value(&spec);
        let declaration = scope_unit_intent_to_scope_worker_intent_value(&release_doc)
            .expect("convert scope-unit intent to scope-worker intent");
        let worker_runtime = declaration["workers"][0]["runtime"]
            .as_object()
            .expect("worker runtime object");
        let route_units = worker_runtime
            .get("route_units")
            .and_then(serde_json::Value::as_object)
            .expect("compiled route_units object");

        for expected_route in spec.route_plan_node_ids.iter().flat_map(|node_id| {
            [
                format!("{}.stream", source_roots_control_route_key_for(node_id)),
                format!("{}.stream", sink_roots_control_route_key_for(node_id)),
            ]
        }) {
            assert!(
                route_units.contains_key(&expected_route),
                "compiled relation target intent must preserve scoped logical-roots control route {expected_route}"
            );
        }
    }

    #[test]
    fn compile_relation_target_intent_preserves_route_plans() {
        let spec = FsMetaReleaseSpec {
            app_id: "test-app".to_string(),
            api_facade_resource_id: "listener".to_string(),
            auth: ApiAuthConfig::default(),
            roots: vec![RootSpec::new("nfs1", "/mnt/nfs1")],
            route_plan_node_ids: vec!["node-a".to_string(), "node-b".to_string()],
            worker_module_path: None,
            worker_modes: Default::default(),
        };

        let release_doc = build_release_doc_value(&spec);
        let expected = release_doc["route_plans"]
            .as_array()
            .cloned()
            .expect("release route_plans array");
        let declaration = scope_unit_intent_to_scope_worker_intent_value(&release_doc)
            .expect("convert scope-unit intent to scope-worker intent");
        let actual = declaration["route_plans"]
            .as_array()
            .cloned()
            .expect("compiled declaration route_plans array");

        assert_eq!(
            actual, expected,
            "compiled relation target intent must preserve top-level route_plans"
        );
        assert!(
            actual.iter().any(|plan| {
                plan.get("route_key").and_then(serde_json::Value::as_str)
                    == Some(&format!("{}.req", ROUTE_KEY_SOURCE_STATUS_INTERNAL))
            }),
            "compiled relation target intent must keep generic internal source-status route plan",
        );
        assert!(
            actual.iter().any(|plan| {
                plan.get("route_key").and_then(serde_json::Value::as_str)
                    == Some(&format!("{}.stream", ROUTE_KEY_SOURCE_ROOTS_CONTROL))
            }),
            "compiled relation target intent must keep generic source logical-roots control route plan",
        );
    }

    #[test]
    fn facade_state_carrier_is_disabled_for_resource_scoped_failover() {
        let spec = FsMetaReleaseSpec {
            app_id: "test-app".to_string(),
            api_facade_resource_id: "listener".to_string(),
            auth: ApiAuthConfig::default(),
            roots: vec![RootSpec::new("nfs1", "/mnt/nfs1")],
            route_plan_node_ids: Vec::new(),
            worker_module_path: None,
            worker_modes: Default::default(),
        };

        let release_doc = build_release_doc_value(&spec);
        let facade_state_carrier = release_doc["units"][0]["runtime"]["state_carrier"]["units"]
            .get("runtime.exec.facade")
            .and_then(serde_json::Value::as_object)
            .and_then(|row| row.get("enabled"))
            .and_then(serde_json::Value::as_bool);

        assert!(
            facade_state_carrier != Some(true),
            "resource-scoped one-cardinality facade must not stay pinned through runtime state carrier"
        );
    }

    #[test]
    fn release_runtime_units_include_source_and_sink_workers() {
        let spec = FsMetaReleaseSpec {
            app_id: "test-app".to_string(),
            api_facade_resource_id: "listener".to_string(),
            auth: ApiAuthConfig::default(),
            roots: vec![RootSpec::new("nfs1", "/mnt/nfs1")],
            route_plan_node_ids: Vec::new(),
            worker_module_path: None,
            worker_modes: Default::default(),
        };

        let release_doc = build_release_doc_value(&spec);
        let runtime_units = release_doc["units"][0]["runtime"]["units"]
            .as_object()
            .expect("runtime units object");

        for unit_id in ["runtime.exec.source", "runtime.exec.sink"] {
            assert_eq!(
                runtime_units
                    .get(unit_id)
                    .and_then(serde_json::Value::as_object)
                    .and_then(|row| row.get("enabled"))
                    .and_then(serde_json::Value::as_bool),
                Some(true),
                "release runtime.units must explicitly enable {unit_id}",
            );
        }
    }

    #[test]
    fn release_state_carrier_keeps_source_and_sink_enabled() {
        let spec = FsMetaReleaseSpec {
            app_id: "test-app".to_string(),
            api_facade_resource_id: "listener".to_string(),
            auth: ApiAuthConfig::default(),
            roots: vec![RootSpec::new("nfs1", "/mnt/nfs1")],
            route_plan_node_ids: Vec::new(),
            worker_module_path: None,
            worker_modes: Default::default(),
        };

        let release_doc = build_release_doc_value(&spec);
        let state_carrier_units = release_doc["units"][0]["runtime"]["state_carrier"]["units"]
            .as_object()
            .expect("state carrier units object");

        for unit_id in ["runtime.exec.source", "runtime.exec.sink"] {
            assert_eq!(
                state_carrier_units
                    .get(unit_id)
                    .and_then(serde_json::Value::as_object)
                    .and_then(|row| row.get("enabled"))
                    .and_then(serde_json::Value::as_bool),
                Some(true),
                "release state_carrier.units must keep {unit_id} enabled",
            );
        }
    }

    #[test]
    fn empty_roots_release_app_scopes_keep_runtime_bootstrap_control_scope() {
        let spec = FsMetaReleaseSpec {
            app_id: "test-app".to_string(),
            api_facade_resource_id: "listener".to_string(),
            auth: ApiAuthConfig::default(),
            roots: Vec::new(),
            route_plan_node_ids: Vec::new(),
            worker_module_path: None,
            worker_modes: Default::default(),
        };

        let release_doc = build_release_doc_value(&spec);
        let app_scopes = release_doc["units"][0]["runtime"]["app_scopes"]
            .as_array()
            .expect("app_scopes array");
        let bootstrap = app_scopes
            .iter()
            .find(|scope| {
                scope.get("scope_id").and_then(serde_json::Value::as_str)
                    == Some(EMPTY_ROOTS_BOOTSTRAP_CONTROL_SCOPE_ID)
            })
            .expect("empty-roots bootstrap app scope");
        assert_eq!(
            bootstrap.get("resource_ids"),
            Some(&serde_json::json!([])),
            "empty-roots bootstrap scope must not bind business resource ids",
        );
        let unit_scopes = bootstrap["unit_scopes"]
            .as_array()
            .expect("bootstrap unit_scopes array");
        for unit_id in [
            SOURCE_RUNTIME_UNIT_ID,
            SOURCE_SCAN_RUNTIME_UNIT_ID,
            QUERY_PEER_RUNTIME_UNIT_ID,
        ] {
            let scope = unit_scopes
                .iter()
                .find(|row| row.get("unit_id").and_then(serde_json::Value::as_str) == Some(unit_id))
                .unwrap_or_else(|| panic!("missing bootstrap unit scope for {unit_id}"));
            assert_eq!(
                scope.get("eligibility"),
                Some(&serde_json::json!("scope_members")),
                "bootstrap unit {unit_id} must stay cluster-reachable through scope_members",
            );
            assert_eq!(
                scope.get("cardinality"),
                Some(&serde_json::json!("all")),
                "bootstrap unit {unit_id} must stay active on all scope members for empty-roots online reconfiguration",
            );
        }
        let sink_scope = unit_scopes
            .iter()
            .find(|row| {
                row.get("unit_id").and_then(serde_json::Value::as_str) == Some(SINK_RUNTIME_UNIT_ID)
            })
            .expect("missing bootstrap unit scope for runtime.exec.sink");
        assert_eq!(
            sink_scope.get("eligibility"),
            Some(&serde_json::json!("scope_members")),
            "bootstrap sink unit must stay schedulable across scope members before business scopes exist",
        );
        assert_eq!(
            sink_scope.get("cardinality"),
            Some(&serde_json::json!("one")),
            "bootstrap sink unit must keep a single active owner before roots are applied online",
        );
    }

    #[test]
    fn nonempty_roots_release_app_scopes_do_not_add_empty_roots_bootstrap_scope() {
        let spec = FsMetaReleaseSpec {
            app_id: "test-app".to_string(),
            api_facade_resource_id: "listener".to_string(),
            auth: ApiAuthConfig::default(),
            roots: vec![RootSpec::new("nfs1", "/mnt/nfs1")],
            route_plan_node_ids: Vec::new(),
            worker_module_path: None,
            worker_modes: Default::default(),
        };

        let release_doc = build_release_doc_value(&spec);
        let app_scopes = release_doc["units"][0]["runtime"]["app_scopes"]
            .as_array()
            .expect("app_scopes array");
        assert!(
            app_scopes.iter().all(|scope| {
                scope.get("scope_id").and_then(serde_json::Value::as_str)
                    != Some(EMPTY_ROOTS_BOOTSTRAP_CONTROL_SCOPE_ID)
            }),
            "non-empty roots release must not add the empty-roots bootstrap scope",
        );
    }

    #[test]
    fn release_config_keeps_internal_main_worker_embedded() {
        let spec = FsMetaReleaseSpec {
            app_id: "test-app".to_string(),
            api_facade_resource_id: "listener".to_string(),
            auth: ApiAuthConfig::default(),
            roots: vec![RootSpec::new("nfs1", "/mnt/nfs1")],
            route_plan_node_ids: Vec::new(),
            worker_module_path: None,
            worker_modes: Default::default(),
        };

        let release_doc = build_release_doc_value(&spec);
        assert_eq!(
            release_doc["units"][0]["config"]["workers"]["main"]["mode"].as_str(),
            Some("embedded"),
            "release config must keep the top-level main worker embedded",
        );
    }

    #[test]
    fn compiled_worker_declaration_keeps_main_worker_embedded() {
        let spec = FsMetaReleaseSpec {
            app_id: "test-app".to_string(),
            api_facade_resource_id: "listener".to_string(),
            auth: ApiAuthConfig::default(),
            roots: vec![RootSpec::new("nfs1", "/mnt/nfs1")],
            route_plan_node_ids: Vec::new(),
            worker_module_path: None,
            worker_modes: Default::default(),
        };

        let release_doc = build_release_doc_value(&spec);
        let declaration = scope_unit_intent_to_scope_worker_intent_value(&release_doc)
            .expect("convert scope-unit intent to scope-worker intent");

        assert_eq!(
            declaration["workers"][0]["worker_role"].as_str(),
            Some("main"),
            "release intent still compiles a single top-level main worker",
        );
        assert_eq!(
            declaration["workers"][0]["mode"].as_str(),
            Some("embedded"),
            "compiled main worker must stay embedded",
        );
    }
}

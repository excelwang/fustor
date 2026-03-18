use crate::api::config::ApiAuthConfig;
use crate::runtime::execution_units::{
    QUERY_PEER_RUNTIME_UNIT_ID, QUERY_RUNTIME_UNIT_ID, SINK_RUNTIME_UNIT_ID, SOURCE_RUNTIME_UNIT_ID, SOURCE_SCAN_RUNTIME_UNIT_ID,
};
use crate::runtime::routes::{
    ROUTE_KEY_EVENTS, ROUTE_KEY_FACADE_CONTROL, ROUTE_KEY_FORCE_FIND, ROUTE_KEY_QUERY,
    ROUTE_KEY_SINK_QUERY_INTERNAL, ROUTE_KEY_SINK_QUERY_PROXY, ROUTE_KEY_SINK_STATUS_INTERNAL,
    ROUTE_KEY_SOURCE_FIND_INTERNAL, ROUTE_KEY_SOURCE_RESCAN_CONTROL,
    ROUTE_KEY_SOURCE_RESCAN_INTERNAL,
};
use crate::source::config::RootSpec;
use capanix_app_sdk::runtime::{RouteKey, RoutePlanMode, RoutePlanSpec};

#[derive(Debug, Clone)]
pub struct FsMetaReleaseSpec {
    pub app_id: String,
    pub api_facade_resource_id: String,
    pub auth: ApiAuthConfig,
    pub roots: Vec<RootSpec>,
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
    serde_json::json!({
        "schema_version": "scope-unit-intent-v1",
        "target_id": spec.app_id,
        "target_generation": generation_now(),
        "route_plans": build_route_plans_json(spec),
        "units": [
            {
                "unit_id": spec.app_id,
                "startup": {
                    "path": "capanix-app-fs-meta"
                },
                "config": {
                    "roots": spec.roots.iter().map(root_spec_release_json).collect::<Vec<_>>(),
                    "api": {
                        "enabled": true,
                        "facade_resource_id": spec.api_facade_resource_id,
                        "auth": auth,
                    }
                },
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

fn build_route_plans_json(_spec: &FsMetaReleaseSpec) -> Vec<serde_json::Value> {
    let mut plans = Vec::new();
    plans.push(serde_json::to_value(RoutePlanSpec {
        route_key: RouteKey(format!("{}.req", ROUTE_KEY_SINK_QUERY_PROXY)),
        mode: RoutePlanMode::FanOut,
        peers: Vec::new(),
    }).unwrap_or_else(|_| serde_json::json!({})));
    plans.push(serde_json::to_value(RoutePlanSpec {
        route_key: RouteKey(format!("{}.req", ROUTE_KEY_SOURCE_FIND_INTERNAL)),
        mode: RoutePlanMode::FanOut,
        peers: Vec::new(),
    }).unwrap_or_else(|_| serde_json::json!({})));
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
        serde_json::json!([QUERY_RUNTIME_UNIT_ID]),
    );
    route_units.insert(
        request_reply_activation_route_key(ROUTE_KEY_FORCE_FIND),
        serde_json::json!([SINK_RUNTIME_UNIT_ID]),
    );
    route_units.insert(
        stream_activation_route_key(ROUTE_KEY_EVENTS),
        serde_json::json!([SOURCE_RUNTIME_UNIT_ID, SOURCE_SCAN_RUNTIME_UNIT_ID]),
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

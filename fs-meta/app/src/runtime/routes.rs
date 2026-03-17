use std::sync::Arc;

use capanix_app_sdk::runtime::RouteKey;
use capanix_host_adapter_fs_meta::{
    ROUTE_KEY_HOST_OBJECT_PASSTHROUGH, ROUTE_TOKEN_HOST_OBJECT, RouteLookup, RouteLookupTable,
    USE_PORT_HOST_PASSTHROUGH,
};

pub const ROUTE_TOKEN_FS_META: &str = "fs-meta";
pub const ROUTE_TOKEN_FS_META_EVENTS: &str = "fs-meta.events";
pub const ROUTE_TOKEN_FS_META_INTERNAL: &str = "fs-meta.internal";

pub const METHOD_QUERY: &str = "query";
pub const METHOD_FIND: &str = "find";
pub const METHOD_STREAM: &str = "stream";
pub const METHOD_SINK_QUERY: &str = "sink.query";
pub const METHOD_SOURCE_FIND: &str = "source.find";
pub const METHOD_SOURCE_RESCAN: &str = "source.rescan";

pub const ROUTE_KEY_EVENTS: &str = "fs-meta.events:v1";
pub const ROUTE_KEY_QUERY: &str = "find:v1.find";
pub const ROUTE_KEY_FORCE_FIND: &str = "on-demand-force-find:v1.on-demand-force-find";
pub const ROUTE_KEY_FACADE_CONTROL: &str = "fs-meta.internal.facade-control:v1";
pub const ROUTE_KEY_SINK_QUERY_INTERNAL: &str = "materialized-find:v1";
pub const ROUTE_KEY_SOURCE_FIND_INTERNAL: &str = "source-on-demand-force-find:v1";
pub const ROUTE_KEY_SOURCE_RESCAN_INTERNAL: &str = "source-manual-rescan:v1";
pub const ROUTE_KEY_FACADE_SOURCE_RESCAN_CONTROL: &str = "facade-source-manual-rescan:v1";

fn scoped_internal_route_key(base: &str, node_id: &str) -> String {
    let suffix: String = node_id
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                "_".chars().next().unwrap()
            }
        })
        .collect();
    match base.rsplit_once(':') {
        Some((stem, version)) => format!("{stem}.{suffix}:{version}"),
        None => format!("{base}.{suffix}"),
    }
}

pub fn source_rescan_route_key_for(node_id: &str) -> String {
    scoped_internal_route_key(ROUTE_KEY_SOURCE_RESCAN_INTERNAL, node_id)
}

pub fn facade_source_rescan_route_key_for(node_id: &str) -> String {
    scoped_internal_route_key(ROUTE_KEY_FACADE_SOURCE_RESCAN_CONTROL, node_id)
}

pub fn default_route_bindings() -> Arc<RouteLookupTable> {
    Arc::new(RouteLookupTable::new([
        RouteLookup {
            route_token: ROUTE_TOKEN_FS_META_EVENTS.into(),
            use_port: METHOD_STREAM.into(),
            route: RouteKey(ROUTE_KEY_EVENTS.into()),
        },
        RouteLookup {
            route_token: ROUTE_TOKEN_FS_META.into(),
            use_port: METHOD_QUERY.into(),
            route: RouteKey(ROUTE_KEY_QUERY.into()),
        },
        RouteLookup {
            route_token: ROUTE_TOKEN_FS_META.into(),
            use_port: METHOD_FIND.into(),
            route: RouteKey(ROUTE_KEY_FORCE_FIND.into()),
        },
        RouteLookup {
            route_token: ROUTE_TOKEN_FS_META_INTERNAL.into(),
            use_port: METHOD_SINK_QUERY.into(),
            route: RouteKey(ROUTE_KEY_SINK_QUERY_INTERNAL.into()),
        },
        RouteLookup {
            route_token: ROUTE_TOKEN_FS_META_INTERNAL.into(),
            use_port: METHOD_SOURCE_FIND.into(),
            route: RouteKey(ROUTE_KEY_SOURCE_FIND_INTERNAL.into()),
        },
        RouteLookup {
            route_token: ROUTE_TOKEN_FS_META_INTERNAL.into(),
            use_port: METHOD_SOURCE_RESCAN.into(),
            route: RouteKey(ROUTE_KEY_SOURCE_RESCAN_INTERNAL.into()),
        },
        RouteLookup {
            route_token: ROUTE_TOKEN_HOST_OBJECT.into(),
            use_port: USE_PORT_HOST_PASSTHROUGH.into(),
            route: RouteKey(ROUTE_KEY_HOST_OBJECT_PASSTHROUGH.into()),
        },
    ]))
}

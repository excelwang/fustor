use std::sync::Arc;

use capanix_app_sdk::runtime::RouteKey;
use capanix_host_adapter_fs::{
    PostBindDispatch, PostBindDispatchTable, ROUTE_KEY_HOST_OBJECT_PASSTHROUGH,
    ROUTE_TOKEN_HOST_OBJECT, USE_PORT_HOST_PASSTHROUGH,
};

pub const ROUTE_TOKEN_FS_META: &str = "fs-meta";
pub const ROUTE_TOKEN_FS_META_EVENTS: &str = "fs-meta.events";
pub const ROUTE_TOKEN_FS_META_INTERNAL: &str = "fs-meta.internal";

pub const METHOD_QUERY: &str = "query";
pub const METHOD_FIND: &str = "find";
pub const METHOD_STREAM: &str = "stream";
pub const METHOD_SINK_QUERY: &str = "sink.query";
pub const METHOD_SINK_QUERY_PROXY: &str = "sink.query.proxy";
pub const METHOD_SINK_STATUS: &str = "sink.status";
pub const METHOD_SOURCE_STATUS: &str = "source.status";
pub const METHOD_SOURCE_FIND: &str = "source.find";
pub const METHOD_SOURCE_RESCAN: &str = "source.rescan";
pub const METHOD_SOURCE_RESCAN_CONTROL: &str = "source.rescan.control";
pub const METHOD_SOURCE_ROOTS_CONTROL: &str = "source.roots.control";
pub const METHOD_SINK_ROOTS_CONTROL: &str = "sink.roots.control";

pub const ROUTE_KEY_EVENTS: &str = "fs-meta.events:v1";
pub const ROUTE_KEY_QUERY: &str = "find:v1.find";
pub const ROUTE_KEY_FORCE_FIND: &str = "on-demand-force-find:v1.on-demand-force-find";
pub const ROUTE_KEY_FACADE_CONTROL: &str = "fs-meta.internal.facade-control:v1";
pub const ROUTE_KEY_SINK_QUERY_INTERNAL: &str = "materialized-find:v1";
pub const ROUTE_KEY_SINK_QUERY_PROXY: &str = "materialized-find-proxy:v1";
pub const ROUTE_KEY_SINK_STATUS_INTERNAL: &str = "sink-status:v1";
pub const ROUTE_KEY_SOURCE_STATUS_INTERNAL: &str = "source-status:v1";
pub const ROUTE_KEY_SOURCE_FIND_INTERNAL: &str = "source-on-demand-force-find:v1";
pub const ROUTE_KEY_SOURCE_RESCAN_INTERNAL: &str = "source-manual-rescan:v1";
pub const ROUTE_KEY_SOURCE_RESCAN_CONTROL: &str = "source-manual-rescan-control:v1";
pub const ROUTE_KEY_SOURCE_ROOTS_CONTROL: &str = "source-logical-roots-control:v1";
pub const ROUTE_KEY_SINK_ROOTS_CONTROL: &str = "sink-logical-roots-control:v1";

fn request_reply_route_key(base: &str) -> RouteKey {
    RouteKey(format!("{base}.req"))
}

fn stream_route_key(base: &str) -> RouteKey {
    RouteKey(format!("{base}.stream"))
}

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

pub fn source_rescan_request_route_for(node_id: &str) -> RouteKey {
    request_reply_route_key(&source_rescan_route_key_for(node_id))
}

pub fn source_roots_control_route_key_for(node_id: &str) -> String {
    scoped_internal_route_key(ROUTE_KEY_SOURCE_ROOTS_CONTROL, node_id)
}

pub fn source_find_route_key_for(node_id: &str) -> String {
    scoped_internal_route_key(ROUTE_KEY_SOURCE_FIND_INTERNAL, node_id)
}

pub fn source_find_request_route_for(node_id: &str) -> RouteKey {
    request_reply_route_key(&source_find_route_key_for(node_id))
}

pub fn source_roots_control_stream_route_for(node_id: &str) -> RouteKey {
    stream_route_key(&source_roots_control_route_key_for(node_id))
}

pub fn sink_query_route_key_for(node_id: &str) -> String {
    scoped_internal_route_key(ROUTE_KEY_SINK_QUERY_INTERNAL, node_id)
}

pub fn sink_query_request_route_for(node_id: &str) -> RouteKey {
    request_reply_route_key(&sink_query_route_key_for(node_id))
}

pub fn sink_roots_control_route_key_for(node_id: &str) -> String {
    scoped_internal_route_key(ROUTE_KEY_SINK_ROOTS_CONTROL, node_id)
}

pub fn sink_roots_control_stream_route_for(node_id: &str) -> RouteKey {
    stream_route_key(&sink_roots_control_route_key_for(node_id))
}

fn build_route_bindings(
    sink_query_route_key: &str,
    source_find_route_key: &str,
    source_rescan_route_key: &str,
    source_roots_control_route_key: &str,
    sink_roots_control_route_key: &str,
) -> Arc<PostBindDispatchTable> {
    Arc::new(PostBindDispatchTable::new([
        PostBindDispatch {
            route_token: ROUTE_TOKEN_FS_META_EVENTS.into(),
            use_port: METHOD_STREAM.into(),
            route: stream_route_key(ROUTE_KEY_EVENTS),
        },
        PostBindDispatch {
            route_token: ROUTE_TOKEN_FS_META.into(),
            use_port: METHOD_QUERY.into(),
            route: request_reply_route_key(ROUTE_KEY_QUERY),
        },
        PostBindDispatch {
            route_token: ROUTE_TOKEN_FS_META.into(),
            use_port: METHOD_FIND.into(),
            route: request_reply_route_key(ROUTE_KEY_FORCE_FIND),
        },
        PostBindDispatch {
            route_token: ROUTE_TOKEN_FS_META_INTERNAL.into(),
            use_port: METHOD_SINK_QUERY.into(),
            route: request_reply_route_key(sink_query_route_key),
        },
        PostBindDispatch {
            route_token: ROUTE_TOKEN_FS_META_INTERNAL.into(),
            use_port: METHOD_SINK_QUERY_PROXY.into(),
            route: request_reply_route_key(ROUTE_KEY_SINK_QUERY_PROXY),
        },
        PostBindDispatch {
            route_token: ROUTE_TOKEN_FS_META_INTERNAL.into(),
            use_port: METHOD_SINK_STATUS.into(),
            route: request_reply_route_key(ROUTE_KEY_SINK_STATUS_INTERNAL),
        },
        PostBindDispatch {
            route_token: ROUTE_TOKEN_FS_META_INTERNAL.into(),
            use_port: METHOD_SOURCE_STATUS.into(),
            route: request_reply_route_key(ROUTE_KEY_SOURCE_STATUS_INTERNAL),
        },
        PostBindDispatch {
            route_token: ROUTE_TOKEN_FS_META_INTERNAL.into(),
            use_port: METHOD_SOURCE_FIND.into(),
            route: request_reply_route_key(source_find_route_key),
        },
        PostBindDispatch {
            route_token: ROUTE_TOKEN_FS_META_INTERNAL.into(),
            use_port: METHOD_SOURCE_RESCAN.into(),
            route: request_reply_route_key(source_rescan_route_key),
        },
        PostBindDispatch {
            route_token: ROUTE_TOKEN_FS_META_INTERNAL.into(),
            use_port: METHOD_SOURCE_RESCAN_CONTROL.into(),
            route: stream_route_key(ROUTE_KEY_SOURCE_RESCAN_CONTROL),
        },
        PostBindDispatch {
            route_token: ROUTE_TOKEN_FS_META_INTERNAL.into(),
            use_port: METHOD_SOURCE_ROOTS_CONTROL.into(),
            route: stream_route_key(source_roots_control_route_key),
        },
        PostBindDispatch {
            route_token: ROUTE_TOKEN_FS_META_INTERNAL.into(),
            use_port: METHOD_SINK_ROOTS_CONTROL.into(),
            route: stream_route_key(sink_roots_control_route_key),
        },
        PostBindDispatch {
            route_token: ROUTE_TOKEN_HOST_OBJECT.into(),
            use_port: USE_PORT_HOST_PASSTHROUGH.into(),
            route: request_reply_route_key(ROUTE_KEY_HOST_OBJECT_PASSTHROUGH),
        },
    ]))
}

pub fn default_route_bindings() -> Arc<PostBindDispatchTable> {
    build_route_bindings(
        ROUTE_KEY_SINK_QUERY_INTERNAL,
        ROUTE_KEY_SOURCE_FIND_INTERNAL,
        ROUTE_KEY_SOURCE_RESCAN_INTERNAL,
        ROUTE_KEY_SOURCE_ROOTS_CONTROL,
        ROUTE_KEY_SINK_ROOTS_CONTROL,
    )
}

pub fn sink_query_route_bindings_for(node_id: &str) -> Arc<PostBindDispatchTable> {
    let route_key = sink_query_route_key_for(node_id);
    let sink_roots_control_route_key = sink_roots_control_route_key_for(node_id);
    build_route_bindings(
        &route_key,
        ROUTE_KEY_SOURCE_FIND_INTERNAL,
        ROUTE_KEY_SOURCE_RESCAN_INTERNAL,
        ROUTE_KEY_SOURCE_ROOTS_CONTROL,
        &sink_roots_control_route_key,
    )
}

pub fn source_rescan_route_bindings_for(node_id: &str) -> Arc<PostBindDispatchTable> {
    let source_rescan_route_key = source_rescan_route_key_for(node_id);
    build_route_bindings(
        ROUTE_KEY_SINK_QUERY_INTERNAL,
        ROUTE_KEY_SOURCE_FIND_INTERNAL,
        &source_rescan_route_key,
        ROUTE_KEY_SOURCE_ROOTS_CONTROL,
        ROUTE_KEY_SINK_ROOTS_CONTROL,
    )
}

pub fn source_find_route_bindings_for(node_id: &str) -> Arc<PostBindDispatchTable> {
    let route_key = source_find_route_key_for(node_id);
    let source_roots_control_route_key = source_roots_control_route_key_for(node_id);
    build_route_bindings(
        ROUTE_KEY_SINK_QUERY_INTERNAL,
        &route_key,
        ROUTE_KEY_SOURCE_RESCAN_INTERNAL,
        &source_roots_control_route_key,
        ROUTE_KEY_SINK_ROOTS_CONTROL,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_find_route_bindings_bind_scoped_source_roots_control_for_known_node() {
        let node_id = "node-b-987654321";
        let routes = source_find_route_bindings_for(node_id);
        let route = routes
            .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_ROOTS_CONTROL)
            .expect("resolve source roots control route");
        assert_eq!(
            route.0,
            format!("{}.stream", source_roots_control_route_key_for(node_id)),
            "node-scoped source route bindings must listen on the owner-scoped roots-control stream",
        );
    }

    #[test]
    fn source_rescan_route_bindings_bind_scoped_source_rescan_for_known_node() {
        let node_id = "node-b-987654321";
        let routes = source_rescan_route_bindings_for(node_id);
        let route = routes
            .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SOURCE_RESCAN)
            .expect("resolve source rescan route");
        assert_eq!(
            route,
            source_rescan_request_route_for(node_id),
            "node-scoped source rescan route bindings must collect explicit delivery from the active source node",
        );
    }

    #[test]
    fn sink_query_route_bindings_bind_scoped_sink_roots_control_for_known_node() {
        let node_id = "node-b-987654321";
        let routes = sink_query_route_bindings_for(node_id);
        let route = routes
            .resolve(ROUTE_TOKEN_FS_META_INTERNAL, METHOD_SINK_ROOTS_CONTROL)
            .expect("resolve sink roots control route");
        assert_eq!(
            route.0,
            format!("{}.stream", sink_roots_control_route_key_for(node_id)),
            "node-scoped sink route bindings must listen on the owner-scoped roots-control stream",
        );
    }
}

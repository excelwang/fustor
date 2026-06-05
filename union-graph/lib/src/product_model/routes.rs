use capanix_app_sdk::runtime::RouteKey;

pub const ROUTE_TOKEN_UNION_GRAPH_INTERNAL: &str = "union-graph.internal";
pub const METHOD_QUERY: &str = "query";
pub const METHOD_INGEST: &str = "ingest";

pub const ROUTE_KEY_QUERY_INTERNAL: &str = "union-graph.query:v1";
pub const ROUTE_KEY_INGEST_INTERNAL: &str = "union-graph.ingest:v1";

fn request_reply_route_key(base: &str) -> RouteKey {
    RouteKey(format!("{base}.req"))
}

fn scoped_internal_route_key(base: &str, node_id: &str) -> String {
    let suffix: String = node_id
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect();
    match base.rsplit_once(':') {
        Some((stem, version)) => format!("{stem}.{suffix}:{version}"),
        None => format!("{base}.{suffix}"),
    }
}

pub fn query_route_key_for(node_id: &str) -> String {
    scoped_internal_route_key(ROUTE_KEY_QUERY_INTERNAL, node_id)
}

pub fn query_request_route_for(node_id: &str) -> RouteKey {
    request_reply_route_key(&query_route_key_for(node_id))
}

pub fn ingest_route_key_for(node_id: &str) -> String {
    scoped_internal_route_key(ROUTE_KEY_INGEST_INTERNAL, node_id)
}

pub fn ingest_request_route_for(node_id: &str) -> RouteKey {
    request_reply_route_key(&ingest_route_key_for(node_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scoped_route_key_sanitizes_node_id() {
        assert_eq!(
            query_request_route_for("Node-A/1").0,
            "union-graph.query.node_a_1:v1.req"
        );
    }
}

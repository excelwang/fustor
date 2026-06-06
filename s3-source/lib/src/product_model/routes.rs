use capanix_app_sdk::runtime::RouteKey;

pub const ROUTE_TOKEN_S3_SOURCE_INTERNAL: &str = "s3-source.internal";
pub const METHOD_FETCH: &str = "fetch";
pub const METHOD_STATUS: &str = "status";

pub const ROUTE_KEY_FETCH_INTERNAL: &str = "s3-source.fetch:v1";
pub const ROUTE_KEY_STATUS_INTERNAL: &str = "s3-source.status:v1";

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

pub fn fetch_route_key_for(node_id: &str) -> String {
    scoped_internal_route_key(ROUTE_KEY_FETCH_INTERNAL, node_id)
}

pub fn fetch_request_route_for(node_id: &str) -> RouteKey {
    request_reply_route_key(&fetch_route_key_for(node_id))
}

pub fn status_route_key_for(node_id: &str) -> String {
    scoped_internal_route_key(ROUTE_KEY_STATUS_INTERNAL, node_id)
}

pub fn status_request_route_for(node_id: &str) -> RouteKey {
    request_reply_route_key(&status_route_key_for(node_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_key_is_scoped_by_node_id() {
        assert_eq!(
            fetch_request_route_for("Node-A/1").0,
            "s3-source.fetch.node_a_1:v1.req"
        );
    }
}

use capanix_host_fs_types::query::StabilityMode;
use serde::{Deserialize, Serialize};

use crate::query::models::SubtreeStats;
use crate::query::tree::TreeGroupPayload;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum QueryTransport {
    Materialized,
    ForceFind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum QueryOp {
    Tree,
    Stats,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryScope {
    pub path: Vec<u8>,
    pub recursive: bool,
    pub max_depth: Option<usize>,
    pub selected_group: Option<String>,
}

impl Default for QueryScope {
    fn default() -> Self {
        Self {
            path: b"/".to_vec(),
            recursive: true,
            max_depth: None,
            selected_group: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TreeQueryOptions {
    pub stability_mode: StabilityMode,
    pub quiet_window_ms: Option<u64>,
}

impl Default for TreeQueryOptions {
    fn default() -> Self {
        Self {
            stability_mode: StabilityMode::None,
            quiet_window_ms: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InternalQueryRequest {
    pub transport: QueryTransport,
    pub op: QueryOp,
    pub scope: QueryScope,
    pub tree_options: Option<TreeQueryOptions>,
}

impl InternalQueryRequest {
    pub fn materialized(
        op: QueryOp,
        scope: QueryScope,
        tree_options: Option<TreeQueryOptions>,
    ) -> Self {
        Self {
            transport: QueryTransport::Materialized,
            op,
            scope,
            tree_options,
        }
    }

    pub fn force_find(op: QueryOp, scope: QueryScope) -> Self {
        Self {
            transport: QueryTransport::ForceFind,
            op,
            scope,
            tree_options: None,
        }
    }
}

impl Default for InternalQueryRequest {
    fn default() -> Self {
        Self::materialized(
            QueryOp::Tree,
            QueryScope::default(),
            Some(TreeQueryOptions::default()),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveScanRequest {
    pub path: Vec<u8>,
    pub recursive: bool,
    pub max_depth: Option<usize>,
}

impl Default for LiveScanRequest {
    fn default() -> Self {
        Self {
            path: b"/".to_vec(),
            recursive: true,
            max_depth: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MaterializedQueryPayload {
    Tree(TreeGroupPayload),
    Stats(SubtreeStats),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ForceFindQueryPayload {
    Tree(TreeGroupPayload),
    Stats(SubtreeStats),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn internal_query_request_msgpack_roundtrip_preserves_utf8_selected_group() {
        let selected_group = "组-é-e\u{301}-北京-👩🏽‍💻-العربية-עברית";
        let request = InternalQueryRequest::materialized(
            QueryOp::Tree,
            QueryScope {
                path: b"/mnt/data".to_vec(),
                recursive: true,
                max_depth: Some(3),
                selected_group: Some(selected_group.to_string()),
            },
            Some(TreeQueryOptions {
                stability_mode: StabilityMode::QuietWindow,
                quiet_window_ms: Some(5_000),
            }),
        );

        let encoded = rmp_serde::to_vec_named(&request).expect("encode request");
        let restored: InternalQueryRequest =
            rmp_serde::from_slice(&encoded).expect("decode request");

        assert_eq!(
            restored.scope.selected_group.as_deref(),
            Some(selected_group)
        );
        assert_eq!(
            restored
                .scope
                .selected_group
                .as_deref()
                .expect("selected group")
                .as_bytes(),
            selected_group.as_bytes()
        );
    }
}

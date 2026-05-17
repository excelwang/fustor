use serde::{Deserialize, Serialize};

use crate::query::models::SubtreeStats;
use crate::query::tree::{ReadClass, TreeGroupPayload};

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
    pub read_class: ReadClass,
    #[serde(default)]
    pub entry_offset: usize,
    #[serde(default)]
    pub entry_limit: Option<usize>,
    #[serde(default)]
    pub payload_limit_bytes: Option<usize>,
}

impl Default for TreeQueryOptions {
    fn default() -> Self {
        Self {
            read_class: ReadClass::TrustedMaterialized,
            entry_offset: 0,
            entry_limit: None,
            payload_limit_bytes: None,
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
    use serde::Deserialize;

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
                read_class: ReadClass::TrustedMaterialized,
                ..TreeQueryOptions::default()
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
            restored.tree_options.expect("tree options").read_class,
            ReadClass::TrustedMaterialized
        );
    }

    #[derive(Debug, Deserialize)]
    struct LegacyTreeQueryOptions {
        read_class: ReadClass,
        entry_offset: usize,
        entry_limit: Option<usize>,
    }

    #[derive(Debug, Deserialize)]
    struct LegacyInternalQueryRequest {
        transport: QueryTransport,
        op: QueryOp,
        scope: QueryScope,
        tree_options: Option<LegacyTreeQueryOptions>,
    }

    #[test]
    fn internal_query_request_named_msgpack_ignores_new_tree_option_for_legacy_decoders() {
        let request = InternalQueryRequest::materialized(
            QueryOp::Tree,
            QueryScope {
                path: b"/".to_vec(),
                recursive: true,
                max_depth: None,
                selected_group: Some("nfs1".to_string()),
            },
            Some(TreeQueryOptions {
                read_class: ReadClass::Materialized,
                entry_offset: 7,
                entry_limit: Some(11),
                payload_limit_bytes: Some(32 * 1024),
            }),
        );

        let encoded = rmp_serde::to_vec_named(&request).expect("encode named request");
        let restored: LegacyInternalQueryRequest =
            rmp_serde::from_slice(&encoded).expect("legacy decoder must ignore unknown fields");
        let options = restored.tree_options.expect("legacy tree options");

        assert_eq!(restored.transport, QueryTransport::Materialized);
        assert_eq!(restored.op, QueryOp::Tree);
        assert_eq!(restored.scope.selected_group.as_deref(), Some("nfs1"));
        assert_eq!(options.read_class, ReadClass::Materialized);
        assert_eq!(options.entry_offset, 7);
        assert_eq!(options.entry_limit, Some(11));
    }
}

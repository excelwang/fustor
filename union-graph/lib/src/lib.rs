use std::collections::{BTreeMap, HashMap};

use capanix_app_sdk::runtime::ConfigValue;
use capanix_app_sdk::{CnxError, Result};
use serde::{Deserialize, Serialize};

pub mod product_model;

pub use source_kit::{SourceErrorPayload, decode_msgpack, encode_msgpack};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SourceKind {
    Fs,
    S3,
    Es,
    Mysql,
    Pg,
    Other(String),
}

impl SourceKind {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Fs => "fs",
            Self::S3 => "s3",
            Self::Es => "es",
            Self::Mysql => "mysql",
            Self::Pg => "pg",
            Self::Other(value) => value,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NodeKind {
    Root,
    Namespace,
    Collection,
    Asset,
    Field,
    Entity,
    Other(String),
}

impl NodeKind {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Root => "root",
            Self::Namespace => "namespace",
            Self::Collection => "collection",
            Self::Asset => "asset",
            Self::Field => "field",
            Self::Entity => "entity",
            Self::Other(value) => value,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EdgeKind {
    Contains,
    HasField,
    Indexes,
    DerivedFrom,
    SameAsCandidate,
    Other(String),
}

impl EdgeKind {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Contains => "contains",
            Self::HasField => "has-field",
            Self::Indexes => "indexes",
            Self::DerivedFrom => "derived-from",
            Self::SameAsCandidate => "same-as-candidate",
            Self::Other(value) => value,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Confidence {
    Observed,
    Declared,
    Inferred,
}

impl Confidence {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Observed => "observed",
            Self::Declared => "declared",
            Self::Inferred => "inferred",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ObservationKind {
    Snapshot,
    Poll,
    Query,
    Manual,
    Inferred,
}

impl ObservationKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Snapshot => "snapshot",
            Self::Poll => "poll",
            Self::Query => "query",
            Self::Manual => "manual",
            Self::Inferred => "inferred",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceGraphNode {
    pub source_kind: SourceKind,
    pub source_instance: String,
    pub local_id: String,
    pub node_kind: NodeKind,
    pub native_ref: String,
    pub display_name: String,
    pub native_pointer: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fingerprint: Option<String>,
    pub observed_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grant_epoch: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceGraphEdge {
    pub source_kind: SourceKind,
    pub source_instance: String,
    pub from_local_id: String,
    pub to_local_id: String,
    pub edge_kind: EdgeKind,
    pub evidence_ref: String,
    pub confidence: Confidence,
    pub observed_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grant_epoch: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeAnchor {
    pub source_kind: SourceKind,
    pub source_instance: String,
    pub native_ref: String,
    pub native_pointer: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_pointer: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grant_epoch: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceRecord {
    pub evidence_id: String,
    pub anchor: NativeAnchor,
    pub observed_by: String,
    pub observation_kind: ObservationKind,
    pub observed_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watermark: Option<String>,
    pub adapter_version: String,
    pub confidence: Confidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphNode {
    pub gid: String,
    pub source_kind: SourceKind,
    pub source_instance: String,
    pub local_id: String,
    pub node_kind: NodeKind,
    pub native_ref: String,
    pub display_name: String,
    pub native_pointer: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fingerprint: Option<String>,
    pub observed_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grant_epoch: Option<u64>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphEdge {
    pub edge_id: String,
    pub source_gid: String,
    pub target_gid: String,
    pub edge_kind: EdgeKind,
    pub confidence: Confidence,
    pub observed_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grant_epoch: Option<u64>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceGraphSkeleton {
    #[serde(default)]
    pub nodes: Vec<SourceGraphNode>,
    #[serde(default)]
    pub edges: Vec<SourceGraphEdge>,
    #[serde(default)]
    pub evidence: Vec<EvidenceRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "kebab-case")]
pub enum UnionGraphRequest {
    IngestSkeleton {
        skeleton: SourceGraphSkeleton,
    },
    ListNodes {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        source_kind: Option<SourceKind>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        node_kind: Option<NodeKind>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<usize>,
    },
    GetNode {
        gid: String,
    },
    ListEdges {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        source_gid: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target_gid: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        edge_kind: Option<EdgeKind>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<usize>,
    },
    Neighbors {
        gid: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        direction: Option<NeighborDirection>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        edge_kind: Option<EdgeKind>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<usize>,
    },
    SourceCoverage,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NeighborDirection {
    Outgoing,
    Incoming,
    Both,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload", rename_all = "kebab-case")]
pub enum UnionGraphPayload {
    IngestAck(IngestAck),
    Nodes(NodeBatch),
    Edges(EdgeBatch),
    Coverage(SourceCoverage),
    Error(SourceErrorPayload),
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestAck {
    pub nodes_upserted: usize,
    pub edges_upserted: usize,
    pub evidence_upserted: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeBatch {
    #[serde(default)]
    pub nodes: Vec<GraphNode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgeBatch {
    #[serde(default)]
    pub edges: Vec<GraphEdge>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceCoverage {
    #[serde(default)]
    pub source_counts: BTreeMap<SourceKind, usize>,
    #[serde(default)]
    pub node_kind_counts: BTreeMap<NodeKind, usize>,
    pub evidence_count: usize,
}

#[derive(Debug, Clone, Default)]
pub struct UnionGraphStore {
    nodes: BTreeMap<String, GraphNode>,
    edges: BTreeMap<String, GraphEdge>,
    evidence: BTreeMap<String, EvidenceRecord>,
}

impl UnionGraphStore {
    pub fn ingest(&mut self, skeleton: SourceGraphSkeleton) -> Result<IngestAck> {
        let mut evidence_upserted = 0;
        for evidence in skeleton.evidence {
            if self
                .evidence
                .insert(evidence.evidence_id.clone(), evidence)
                .is_none()
            {
                evidence_upserted += 1;
            }
        }

        let mut nodes_upserted = 0;
        for source_node in skeleton.nodes {
            let gid = graph_gid(
                &source_node.source_kind,
                &source_node.source_instance,
                &source_node.local_id,
            );
            let evidence_id = default_evidence_for_node(&source_node);
            if !self.evidence.contains_key(&evidence_id) {
                self.evidence.insert(
                    evidence_id.clone(),
                    evidence_from_source_node(&source_node, &evidence_id),
                );
                evidence_upserted += 1;
            }
            let node = GraphNode {
                gid: gid.clone(),
                source_kind: source_node.source_kind,
                source_instance: source_node.source_instance,
                local_id: source_node.local_id,
                node_kind: source_node.node_kind,
                native_ref: source_node.native_ref,
                display_name: source_node.display_name,
                native_pointer: source_node.native_pointer,
                fingerprint: source_node.fingerprint,
                observed_at: source_node.observed_at,
                grant_epoch: source_node.grant_epoch,
                evidence_refs: vec![evidence_id],
            };
            if self.nodes.insert(gid, node).is_none() {
                nodes_upserted += 1;
            }
        }

        let mut edges_upserted = 0;
        for source_edge in skeleton.edges {
            let source_gid = graph_gid(
                &source_edge.source_kind,
                &source_edge.source_instance,
                &source_edge.from_local_id,
            );
            let target_gid = graph_gid(
                &source_edge.source_kind,
                &source_edge.source_instance,
                &source_edge.to_local_id,
            );
            if !self.nodes.contains_key(&source_gid) || !self.nodes.contains_key(&target_gid) {
                return Err(CnxError::InvalidInput(format!(
                    "edge references missing node: {} -> {}",
                    source_edge.from_local_id, source_edge.to_local_id
                )));
            }
            let edge_id = graph_edge_id(&source_gid, &source_edge.edge_kind, &target_gid);
            let evidence_ref = if source_edge.evidence_ref.trim().is_empty() {
                default_evidence_for_edge(&source_edge)
            } else {
                source_edge.evidence_ref.clone()
            };
            if !self.evidence.contains_key(&evidence_ref) {
                self.evidence.insert(
                    evidence_ref.clone(),
                    evidence_from_source_edge(&source_edge, &evidence_ref),
                );
                evidence_upserted += 1;
            }
            let edge = GraphEdge {
                edge_id: edge_id.clone(),
                source_gid,
                target_gid,
                edge_kind: source_edge.edge_kind,
                confidence: source_edge.confidence,
                observed_at: source_edge.observed_at,
                grant_epoch: source_edge.grant_epoch,
                evidence_refs: vec![evidence_ref],
            };
            if self.edges.insert(edge_id, edge).is_none() {
                edges_upserted += 1;
            }
        }

        Ok(IngestAck {
            nodes_upserted,
            edges_upserted,
            evidence_upserted,
        })
    }

    pub fn get_node(&self, gid: &str) -> Option<GraphNode> {
        self.nodes.get(gid).cloned()
    }

    pub fn list_nodes(
        &self,
        source_kind: Option<&SourceKind>,
        node_kind: Option<&NodeKind>,
        limit: Option<usize>,
    ) -> Vec<GraphNode> {
        let limit = limit.unwrap_or(usize::MAX);
        self.nodes
            .values()
            .filter(|node| source_kind.is_none_or(|kind| &node.source_kind == kind))
            .filter(|node| node_kind.is_none_or(|kind| &node.node_kind == kind))
            .take(limit)
            .cloned()
            .collect()
    }

    pub fn list_edges(
        &self,
        source_gid: Option<&str>,
        target_gid: Option<&str>,
        edge_kind: Option<&EdgeKind>,
        limit: Option<usize>,
    ) -> Vec<GraphEdge> {
        let limit = limit.unwrap_or(usize::MAX);
        self.edges
            .values()
            .filter(|edge| source_gid.is_none_or(|gid| edge.source_gid == gid))
            .filter(|edge| target_gid.is_none_or(|gid| edge.target_gid == gid))
            .filter(|edge| edge_kind.is_none_or(|kind| &edge.edge_kind == kind))
            .take(limit)
            .cloned()
            .collect()
    }

    pub fn neighbors(
        &self,
        gid: &str,
        direction: NeighborDirection,
        edge_kind: Option<&EdgeKind>,
        limit: Option<usize>,
    ) -> Vec<GraphEdge> {
        let limit = limit.unwrap_or(usize::MAX);
        self.edges
            .values()
            .filter(|edge| edge_kind.is_none_or(|kind| &edge.edge_kind == kind))
            .filter(|edge| match direction {
                NeighborDirection::Outgoing => edge.source_gid == gid,
                NeighborDirection::Incoming => edge.target_gid == gid,
                NeighborDirection::Both => edge.source_gid == gid || edge.target_gid == gid,
            })
            .take(limit)
            .cloned()
            .collect()
    }

    pub fn coverage(&self) -> SourceCoverage {
        let mut coverage = SourceCoverage {
            evidence_count: self.evidence.len(),
            ..SourceCoverage::default()
        };
        for node in self.nodes.values() {
            *coverage
                .source_counts
                .entry(node.source_kind.clone())
                .or_default() += 1;
            *coverage
                .node_kind_counts
                .entry(node.node_kind.clone())
                .or_default() += 1;
        }
        coverage
    }
}

pub fn graph_gid(source_kind: &SourceKind, source_instance: &str, local_id: &str) -> String {
    format!(
        "{}:{}:{}",
        source_kind.as_str(),
        stable_escape(source_instance),
        stable_escape(local_id)
    )
}

pub fn graph_edge_id(source_gid: &str, edge_kind: &EdgeKind, target_gid: &str) -> String {
    format!(
        "edge:{}:{}:{}",
        stable_escape(source_gid),
        edge_kind.as_str(),
        stable_escape(target_gid)
    )
}

fn stable_escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        let ch = byte as char;
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
            out.push(ch);
        } else {
            out.push('~');
            out.push_str(&format!("{byte:02x}"));
        }
    }
    out
}

fn default_evidence_for_node(node: &SourceGraphNode) -> String {
    format!(
        "evidence:node:{}",
        stable_escape(&graph_gid(
            &node.source_kind,
            &node.source_instance,
            &node.local_id
        ))
    )
}

fn default_evidence_for_edge(edge: &SourceGraphEdge) -> String {
    format!(
        "evidence:edge:{}:{}:{}:{}",
        edge.source_kind.as_str(),
        stable_escape(&edge.source_instance),
        stable_escape(&edge.from_local_id),
        stable_escape(&edge.to_local_id)
    )
}

fn evidence_from_source_node(node: &SourceGraphNode, evidence_id: &str) -> EvidenceRecord {
    EvidenceRecord {
        evidence_id: evidence_id.to_string(),
        anchor: NativeAnchor {
            source_kind: node.source_kind.clone(),
            source_instance: node.source_instance.clone(),
            native_ref: node.native_ref.clone(),
            native_pointer: node.native_pointer.clone(),
            native_version: node.fingerprint.clone(),
            cache_pointer: None,
            grant_epoch: node.grant_epoch,
        },
        observed_by: observed_by(&node.source_kind).to_string(),
        observation_kind: ObservationKind::Snapshot,
        observed_at: node.observed_at,
        watermark: None,
        adapter_version: "union-graph-v1".into(),
        confidence: Confidence::Observed,
    }
}

fn evidence_from_source_edge(edge: &SourceGraphEdge, evidence_id: &str) -> EvidenceRecord {
    EvidenceRecord {
        evidence_id: evidence_id.to_string(),
        anchor: NativeAnchor {
            source_kind: edge.source_kind.clone(),
            source_instance: edge.source_instance.clone(),
            native_ref: format!(
                "{}:{}->{}",
                edge.edge_kind.as_str(),
                edge.from_local_id,
                edge.to_local_id
            ),
            native_pointer: format!("{}->{}", edge.from_local_id, edge.to_local_id),
            native_version: None,
            cache_pointer: None,
            grant_epoch: edge.grant_epoch,
        },
        observed_by: observed_by(&edge.source_kind).to_string(),
        observation_kind: ObservationKind::Snapshot,
        observed_at: edge.observed_at,
        watermark: None,
        adapter_version: "union-graph-v1".into(),
        confidence: edge.confidence.clone(),
    }
}

fn observed_by(source_kind: &SourceKind) -> &str {
    match source_kind {
        SourceKind::Fs => "fs-meta",
        SourceKind::S3 => "s3-source",
        SourceKind::Es => "es-source",
        SourceKind::Mysql => "mysql-source",
        SourceKind::Pg => "pg-source",
        SourceKind::Other(_) => "source-adapter",
    }
}

pub fn handle_store_request(
    store: &mut UnionGraphStore,
    request: UnionGraphRequest,
) -> Result<UnionGraphPayload> {
    match request {
        UnionGraphRequest::IngestSkeleton { skeleton } => {
            Ok(UnionGraphPayload::IngestAck(store.ingest(skeleton)?))
        }
        UnionGraphRequest::ListNodes {
            source_kind,
            node_kind,
            limit,
        } => Ok(UnionGraphPayload::Nodes(NodeBatch {
            nodes: store.list_nodes(source_kind.as_ref(), node_kind.as_ref(), limit),
            next_cursor: None,
        })),
        UnionGraphRequest::GetNode { gid } => Ok(UnionGraphPayload::Nodes(NodeBatch {
            nodes: store.get_node(&gid).into_iter().collect(),
            next_cursor: None,
        })),
        UnionGraphRequest::ListEdges {
            source_gid,
            target_gid,
            edge_kind,
            limit,
        } => Ok(UnionGraphPayload::Edges(EdgeBatch {
            edges: store.list_edges(
                source_gid.as_deref(),
                target_gid.as_deref(),
                edge_kind.as_ref(),
                limit,
            ),
            next_cursor: None,
        })),
        UnionGraphRequest::Neighbors {
            gid,
            direction,
            edge_kind,
            limit,
        } => Ok(UnionGraphPayload::Edges(EdgeBatch {
            edges: store.neighbors(
                &gid,
                direction.unwrap_or(NeighborDirection::Both),
                edge_kind.as_ref(),
                limit,
            ),
            next_cursor: None,
        })),
        UnionGraphRequest::SourceCoverage => Ok(UnionGraphPayload::Coverage(store.coverage())),
    }
}

pub fn skeleton_from_manifest_config(
    cfg: &HashMap<String, ConfigValue>,
) -> Result<SourceGraphSkeleton> {
    let mut skeleton = SourceGraphSkeleton::default();
    let Some(ConfigValue::Array(seeds)) = cfg.get("graph_seeds") else {
        return Ok(skeleton);
    };
    for item in seeds {
        let ConfigValue::Map(row) = item else {
            return Err(CnxError::InvalidInput(
                "graph_seeds[] item must be map".into(),
            ));
        };
        let source_kind = parse_source_kind(required_str(row, "source_kind")?);
        let source_instance = required_str(row, "source_instance")?;
        let local_id = required_str(row, "local_id")?;
        let node_kind =
            parse_node_kind(optional_str(row, "node_kind").unwrap_or_else(|| "asset".to_string()));
        let native_ref = optional_str(row, "native_ref")
            .unwrap_or_else(|| format!("{}://{}", source_kind.as_str(), local_id));
        let display_name = optional_str(row, "display_name").unwrap_or_else(|| local_id.clone());
        let native_pointer =
            optional_str(row, "native_pointer").unwrap_or_else(|| native_ref.clone());
        let observed_at = optional_int(row, "observed_at")
            .and_then(|value| u64::try_from(value).ok())
            .unwrap_or_default();
        let grant_epoch =
            optional_int(row, "grant_epoch").and_then(|value| u64::try_from(value).ok());
        skeleton.nodes.push(SourceGraphNode {
            source_kind,
            source_instance,
            local_id,
            node_kind,
            native_ref,
            display_name,
            native_pointer,
            fingerprint: optional_str(row, "fingerprint"),
            observed_at,
            grant_epoch,
        });
    }
    Ok(skeleton)
}

pub fn fs_node_from_query_node(
    source_instance: impl Into<String>,
    group: impl Into<String>,
    node: &fs_meta::query::QueryNode,
) -> SourceGraphNode {
    let source_instance = source_instance.into();
    let group = group.into();
    let path = String::from_utf8_lossy(&node.path).to_string();
    let display_name = String::from_utf8_lossy(&node.file_name).to_string();
    SourceGraphNode {
        source_kind: SourceKind::Fs,
        source_instance: source_instance.clone(),
        local_id: path.clone(),
        node_kind: if node.is_dir {
            NodeKind::Namespace
        } else {
            NodeKind::Asset
        },
        native_ref: format!("fs://{source_instance}{path}"),
        display_name,
        native_pointer: format!("fs-meta:{group}:{path}"),
        fingerprint: Some(format!(
            "size={};mtime={}",
            node.size, node.modified_time_us
        )),
        observed_at: node.modified_time_us,
        grant_epoch: None,
    }
}

pub fn skeleton_from_s3_batch(batch: &s3_source::S3ObjectBatch) -> SourceGraphSkeleton {
    let mut skeleton = SourceGraphSkeleton::default();
    let bucket_id = batch.bucket.clone();
    skeleton.nodes.push(SourceGraphNode {
        source_kind: SourceKind::S3,
        source_instance: batch.object_ref.clone(),
        local_id: bucket_id.clone(),
        node_kind: NodeKind::Root,
        native_ref: format!("s3://{}", batch.bucket),
        display_name: batch.bucket.clone(),
        native_pointer: format!("s3://{}", batch.bucket),
        fingerprint: None,
        observed_at: 0,
        grant_epoch: batch.grant_epoch,
    });
    for object in &batch.objects {
        let local_id = format!("{}/{}", batch.bucket, object.key);
        skeleton.nodes.push(SourceGraphNode {
            source_kind: SourceKind::S3,
            source_instance: batch.object_ref.clone(),
            local_id: local_id.clone(),
            node_kind: NodeKind::Asset,
            native_ref: format!("s3://{}/{}", batch.bucket, object.key),
            display_name: object.key.clone(),
            native_pointer: format!("s3://{}/{}", batch.bucket, object.key),
            fingerprint: object.etag.clone().or_else(|| {
                Some(format!(
                    "size={};mtime={}",
                    object.size, object.last_modified_unix_ms
                ))
            }),
            observed_at: object.last_modified_unix_ms,
            grant_epoch: batch.grant_epoch,
        });
        skeleton.edges.push(SourceGraphEdge {
            source_kind: SourceKind::S3,
            source_instance: batch.object_ref.clone(),
            from_local_id: bucket_id.clone(),
            to_local_id: local_id,
            edge_kind: EdgeKind::Contains,
            evidence_ref: String::new(),
            confidence: Confidence::Observed,
            observed_at: object.last_modified_unix_ms,
            grant_epoch: batch.grant_epoch,
        });
    }
    skeleton
}

pub fn skeleton_from_es_fields(catalog: &es_source::EsFieldCatalog) -> SourceGraphSkeleton {
    let mut skeleton = SourceGraphSkeleton::default();
    let index_id = catalog.index.clone();
    skeleton.nodes.push(SourceGraphNode {
        source_kind: SourceKind::Es,
        source_instance: catalog.object_ref.clone(),
        local_id: index_id.clone(),
        node_kind: NodeKind::Collection,
        native_ref: format!("es://{}/{}", catalog.object_ref, catalog.index),
        display_name: catalog.index.clone(),
        native_pointer: format!("es://{}/{}", catalog.object_ref, catalog.index),
        fingerprint: Some(format!("fields={}", catalog.fields.len())),
        observed_at: 0,
        grant_epoch: None,
    });
    for field in &catalog.fields {
        let local_id = format!("{}/_field/{}", catalog.index, field.name);
        skeleton.nodes.push(SourceGraphNode {
            source_kind: SourceKind::Es,
            source_instance: catalog.object_ref.clone(),
            local_id: local_id.clone(),
            node_kind: NodeKind::Field,
            native_ref: format!(
                "es://{}/{}/_field/{}",
                catalog.object_ref, catalog.index, field.name
            ),
            display_name: field.name.clone(),
            native_pointer: format!(
                "es://{}/{}/_mapping/{}",
                catalog.object_ref, catalog.index, field.name
            ),
            fingerprint: field.field_type.clone(),
            observed_at: 0,
            grant_epoch: None,
        });
        skeleton.edges.push(SourceGraphEdge {
            source_kind: SourceKind::Es,
            source_instance: catalog.object_ref.clone(),
            from_local_id: index_id.clone(),
            to_local_id: local_id,
            edge_kind: EdgeKind::HasField,
            evidence_ref: String::new(),
            confidence: Confidence::Observed,
            observed_at: 0,
            grant_epoch: None,
        });
    }
    skeleton
}

pub fn skeleton_from_es_batch(batch: &es_source::EsSourceBatch) -> SourceGraphSkeleton {
    let mut skeleton = SourceGraphSkeleton::default();
    let index_id = batch.index.clone();
    skeleton.nodes.push(SourceGraphNode {
        source_kind: SourceKind::Es,
        source_instance: batch.object_ref.clone(),
        local_id: index_id.clone(),
        node_kind: NodeKind::Collection,
        native_ref: format!("es://{}/{}", batch.object_ref, batch.index),
        display_name: batch.index.clone(),
        native_pointer: format!("es://{}/{}", batch.object_ref, batch.index),
        fingerprint: Some(format!("docs={}", batch.documents.len())),
        observed_at: 0,
        grant_epoch: batch.grant_epoch,
    });
    for document in &batch.documents {
        let local_id = format!("{}/_doc/{}", batch.index, document.id);
        skeleton.nodes.push(SourceGraphNode {
            source_kind: SourceKind::Es,
            source_instance: batch.object_ref.clone(),
            local_id: local_id.clone(),
            node_kind: NodeKind::Asset,
            native_ref: format!(
                "es://{}/{}/_doc/{}",
                batch.object_ref, batch.index, document.id
            ),
            display_name: document.id.clone(),
            native_pointer: format!(
                "es://{}/{}/_doc/{}",
                batch.object_ref, batch.index, document.id
            ),
            fingerprint: Some(format!(
                "seq_no={:?};primary_term={:?}",
                document.seq_no, document.primary_term
            )),
            observed_at: 0,
            grant_epoch: batch.grant_epoch,
        });
        skeleton.edges.push(SourceGraphEdge {
            source_kind: SourceKind::Es,
            source_instance: batch.object_ref.clone(),
            from_local_id: index_id.clone(),
            to_local_id: local_id,
            edge_kind: EdgeKind::Contains,
            evidence_ref: String::new(),
            confidence: Confidence::Observed,
            observed_at: 0,
            grant_epoch: batch.grant_epoch,
        });
    }
    skeleton
}

pub fn skeleton_from_mysql_fields(
    catalog: &mysql_source::MysqlFieldCatalog,
) -> SourceGraphSkeleton {
    let mut skeleton = SourceGraphSkeleton::default();
    let mut tables = BTreeMap::<(String, String), Vec<&mysql_source::MysqlFieldDescriptor>>::new();
    for field in &catalog.fields {
        tables
            .entry((field.schema.clone(), field.table.clone()))
            .or_default()
            .push(field);
    }
    for ((schema, table), fields) in tables {
        let schema_id = schema.clone();
        let table_id = format!("{schema}/{table}");
        skeleton.nodes.push(SourceGraphNode {
            source_kind: SourceKind::Mysql,
            source_instance: catalog.object_ref.clone(),
            local_id: schema_id.clone(),
            node_kind: NodeKind::Namespace,
            native_ref: format!("mysql://{}/{}", catalog.object_ref, schema),
            display_name: schema.clone(),
            native_pointer: format!("mysql://{}/{}", catalog.object_ref, schema),
            fingerprint: None,
            observed_at: 0,
            grant_epoch: None,
        });
        skeleton.nodes.push(SourceGraphNode {
            source_kind: SourceKind::Mysql,
            source_instance: catalog.object_ref.clone(),
            local_id: table_id.clone(),
            node_kind: NodeKind::Collection,
            native_ref: format!("mysql://{}/{}/{}", catalog.object_ref, schema, table),
            display_name: table.clone(),
            native_pointer: format!("mysql://{}/{}/{}", catalog.object_ref, schema, table),
            fingerprint: Some(format!("fields={}", fields.len())),
            observed_at: 0,
            grant_epoch: None,
        });
        skeleton.edges.push(SourceGraphEdge {
            source_kind: SourceKind::Mysql,
            source_instance: catalog.object_ref.clone(),
            from_local_id: schema_id,
            to_local_id: table_id.clone(),
            edge_kind: EdgeKind::Contains,
            evidence_ref: String::new(),
            confidence: Confidence::Observed,
            observed_at: 0,
            grant_epoch: None,
        });
        for field in fields {
            let field_id = format!("{schema}/{table}/{}", field.column);
            skeleton.nodes.push(SourceGraphNode {
                source_kind: SourceKind::Mysql,
                source_instance: catalog.object_ref.clone(),
                local_id: field_id.clone(),
                node_kind: NodeKind::Field,
                native_ref: format!(
                    "mysql://{}/{}/{}/{}",
                    catalog.object_ref, schema, table, field.column
                ),
                display_name: field.column.clone(),
                native_pointer: format!(
                    "mysql://{}/{}/{}/{}",
                    catalog.object_ref, schema, table, field.column
                ),
                fingerprint: field.column_type.clone(),
                observed_at: 0,
                grant_epoch: None,
            });
            skeleton.edges.push(SourceGraphEdge {
                source_kind: SourceKind::Mysql,
                source_instance: catalog.object_ref.clone(),
                from_local_id: table_id.clone(),
                to_local_id: field_id,
                edge_kind: EdgeKind::HasField,
                evidence_ref: String::new(),
                confidence: Confidence::Observed,
                observed_at: 0,
                grant_epoch: None,
            });
        }
    }
    skeleton
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PgCatalogSeed {
    pub object_ref: String,
    pub database: String,
    pub schema: String,
    pub table: String,
    #[serde(default)]
    pub columns: Vec<String>,
}

pub fn skeleton_from_pg_seed(seed: &PgCatalogSeed) -> SourceGraphSkeleton {
    let mut skeleton = SourceGraphSkeleton::default();
    let schema_id = format!("{}/{}", seed.database, seed.schema);
    let table_id = format!("{}/{}/{}", seed.database, seed.schema, seed.table);
    skeleton.nodes.push(SourceGraphNode {
        source_kind: SourceKind::Pg,
        source_instance: seed.object_ref.clone(),
        local_id: schema_id.clone(),
        node_kind: NodeKind::Namespace,
        native_ref: format!("pg://{}/{}/{}", seed.object_ref, seed.database, seed.schema),
        display_name: seed.schema.clone(),
        native_pointer: format!("pg://{}/{}/{}", seed.object_ref, seed.database, seed.schema),
        fingerprint: None,
        observed_at: 0,
        grant_epoch: None,
    });
    skeleton.nodes.push(SourceGraphNode {
        source_kind: SourceKind::Pg,
        source_instance: seed.object_ref.clone(),
        local_id: table_id.clone(),
        node_kind: NodeKind::Collection,
        native_ref: format!(
            "pg://{}/{}/{}/{}",
            seed.object_ref, seed.database, seed.schema, seed.table
        ),
        display_name: seed.table.clone(),
        native_pointer: format!(
            "pg://{}/{}/{}/{}",
            seed.object_ref, seed.database, seed.schema, seed.table
        ),
        fingerprint: Some(format!("fields={}", seed.columns.len())),
        observed_at: 0,
        grant_epoch: None,
    });
    skeleton.edges.push(SourceGraphEdge {
        source_kind: SourceKind::Pg,
        source_instance: seed.object_ref.clone(),
        from_local_id: schema_id,
        to_local_id: table_id.clone(),
        edge_kind: EdgeKind::Contains,
        evidence_ref: String::new(),
        confidence: Confidence::Declared,
        observed_at: 0,
        grant_epoch: None,
    });
    for column in &seed.columns {
        let column_id = format!(
            "{}/{}/{}/{}",
            seed.database, seed.schema, seed.table, column
        );
        skeleton.nodes.push(SourceGraphNode {
            source_kind: SourceKind::Pg,
            source_instance: seed.object_ref.clone(),
            local_id: column_id.clone(),
            node_kind: NodeKind::Field,
            native_ref: format!(
                "pg://{}/{}/{}/{}/{}",
                seed.object_ref, seed.database, seed.schema, seed.table, column
            ),
            display_name: column.clone(),
            native_pointer: format!(
                "pg://{}/{}/{}/{}/{}",
                seed.object_ref, seed.database, seed.schema, seed.table, column
            ),
            fingerprint: None,
            observed_at: 0,
            grant_epoch: None,
        });
        skeleton.edges.push(SourceGraphEdge {
            source_kind: SourceKind::Pg,
            source_instance: seed.object_ref.clone(),
            from_local_id: table_id.clone(),
            to_local_id: column_id,
            edge_kind: EdgeKind::HasField,
            evidence_ref: String::new(),
            confidence: Confidence::Declared,
            observed_at: 0,
            grant_epoch: None,
        });
    }
    skeleton
}

fn parse_source_kind(value: String) -> SourceKind {
    match value.as_str() {
        "fs" => SourceKind::Fs,
        "s3" => SourceKind::S3,
        "es" => SourceKind::Es,
        "mysql" => SourceKind::Mysql,
        "pg" => SourceKind::Pg,
        _ => SourceKind::Other(value),
    }
}

fn parse_node_kind(value: String) -> NodeKind {
    match value.as_str() {
        "root" => NodeKind::Root,
        "namespace" => NodeKind::Namespace,
        "collection" => NodeKind::Collection,
        "asset" => NodeKind::Asset,
        "field" => NodeKind::Field,
        "entity" => NodeKind::Entity,
        _ => NodeKind::Other(value),
    }
}

fn required_str(row: &HashMap<String, ConfigValue>, key: &str) -> Result<String> {
    optional_str(row, key).ok_or_else(|| CnxError::InvalidInput(format!("{key} is required")))
}

fn optional_str(row: &HashMap<String, ConfigValue>, key: &str) -> Option<String> {
    match row.get(key) {
        Some(ConfigValue::String(value)) if !value.trim().is_empty() => {
            Some(value.trim().to_string())
        }
        _ => None,
    }
}

fn optional_int(row: &HashMap<String, ConfigValue>, key: &str) -> Option<i64> {
    match row.get(key) {
        Some(ConfigValue::Int(value)) => Some(*value),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn graph_gid_is_stable_and_source_scoped() {
        assert_eq!(
            graph_gid(&SourceKind::Fs, "nfs-a", "/data/S1.fastq"),
            graph_gid(&SourceKind::Fs, "nfs-a", "/data/S1.fastq")
        );
        assert_ne!(
            graph_gid(&SourceKind::Fs, "nfs-a", "/data/S1.fastq"),
            graph_gid(&SourceKind::S3, "nfs-a", "/data/S1.fastq")
        );
    }

    #[test]
    fn store_covers_all_declared_source_types() {
        let mut store = UnionGraphStore::default();
        for kind in [
            SourceKind::Fs,
            SourceKind::S3,
            SourceKind::Es,
            SourceKind::Mysql,
            SourceKind::Pg,
        ] {
            let skeleton = SourceGraphSkeleton {
                nodes: vec![SourceGraphNode {
                    source_kind: kind,
                    source_instance: "source-a".into(),
                    local_id: "asset-a".into(),
                    node_kind: NodeKind::Asset,
                    native_ref: "native://asset-a".into(),
                    display_name: "asset-a".into(),
                    native_pointer: "native://asset-a".into(),
                    fingerprint: None,
                    observed_at: 1,
                    grant_epoch: None,
                }],
                edges: Vec::new(),
                evidence: Vec::new(),
            };
            store.ingest(skeleton).expect("ingest");
        }
        let coverage = store.coverage();
        assert_eq!(coverage.source_counts.len(), 5);
        assert_eq!(coverage.source_counts[&SourceKind::Fs], 1);
        assert_eq!(coverage.source_counts[&SourceKind::Pg], 1);
    }

    #[test]
    fn neighbors_returns_contains_edges() {
        let mut store = UnionGraphStore::default();
        store
            .ingest(SourceGraphSkeleton {
                nodes: vec![
                    SourceGraphNode {
                        source_kind: SourceKind::S3,
                        source_instance: "s3-prod".into(),
                        local_id: "bucket".into(),
                        node_kind: NodeKind::Root,
                        native_ref: "s3://bucket".into(),
                        display_name: "bucket".into(),
                        native_pointer: "s3://bucket".into(),
                        fingerprint: None,
                        observed_at: 1,
                        grant_epoch: None,
                    },
                    SourceGraphNode {
                        source_kind: SourceKind::S3,
                        source_instance: "s3-prod".into(),
                        local_id: "bucket/a.txt".into(),
                        node_kind: NodeKind::Asset,
                        native_ref: "s3://bucket/a.txt".into(),
                        display_name: "a.txt".into(),
                        native_pointer: "s3://bucket/a.txt".into(),
                        fingerprint: None,
                        observed_at: 1,
                        grant_epoch: None,
                    },
                ],
                edges: vec![SourceGraphEdge {
                    source_kind: SourceKind::S3,
                    source_instance: "s3-prod".into(),
                    from_local_id: "bucket".into(),
                    to_local_id: "bucket/a.txt".into(),
                    edge_kind: EdgeKind::Contains,
                    evidence_ref: String::new(),
                    confidence: Confidence::Observed,
                    observed_at: 1,
                    grant_epoch: None,
                }],
                evidence: Vec::new(),
            })
            .expect("ingest");
        let bucket_gid = graph_gid(&SourceKind::S3, "s3-prod", "bucket");
        let edges = store.neighbors(&bucket_gid, NeighborDirection::Outgoing, None, None);
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].edge_kind, EdgeKind::Contains);
    }

    #[test]
    fn es_adapter_does_not_copy_document_source_payload() {
        let batch = es_source::EsSourceBatch {
            object_ref: "es-prod".into(),
            index: "reads".into(),
            documents: vec![es_source::EsDocumentEvent {
                event_kind: es_source::EsDocumentEventKind::SnapshotInsert,
                index: "reads".into(),
                id: "doc-1".into(),
                timestamp_value: None,
                sort: Vec::new(),
                seq_no: Some(1),
                primary_term: Some(2),
                source: json!({"large": "payload"}),
                metadata: BTreeMap::new(),
            }],
            next_cursor: None,
            has_more: false,
            grant_epoch: Some(3),
            diagnostics: None,
        };
        let skeleton = skeleton_from_es_batch(&batch);
        assert!(skeleton.nodes.iter().all(|node| {
            !node.native_pointer.contains("payload")
                && !node
                    .fingerprint
                    .as_deref()
                    .unwrap_or("")
                    .contains("payload")
        }));
    }

    #[test]
    fn pg_seed_promotes_catalog_skeleton() {
        let skeleton = skeleton_from_pg_seed(&PgCatalogSeed {
            object_ref: "pg-prod".into(),
            database: "lab".into(),
            schema: "public".into(),
            table: "sample".into(),
            columns: vec!["id".into(), "name".into()],
        });
        assert_eq!(skeleton.nodes.len(), 4);
        assert_eq!(skeleton.edges.len(), 3);
        assert!(
            skeleton
                .nodes
                .iter()
                .any(|node| node.source_kind == SourceKind::Pg)
        );
    }
}

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

use capanix_app_sdk::runtime::ConfigValue;
use capanix_app_sdk::{CnxError, Result};
use serde_json::{Value, json};

use crate::{
    Confidence, CrossSourceGraphEdge, EdgeKind, EvidenceRecord, NativeAnchor, NodeKind,
    ObservationKind, SourceGraphEndpoint, SourceGraphNode, SourceGraphSkeleton, SourceKind,
};

const DEFAULT_BIO_REPO_ROOT: &str = "../bio_elinks_ppg";
const DEFAULT_OBSERVED_AT: u64 = 1_779_984_000_000_000;
const BIO_PIPELINE_INSTANCE: &str = "bio-pipeline";
const FS_INSTANCE: &str = "bio-nfs";
const LOCAL_FS_INSTANCE: &str = "bio-local-files";
const PG_INSTANCE: &str = "bio-pg-10.3.200.29";
const ES_INSTANCE: &str = "bio-json-docs";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BioPipelineMockConfig {
    pub bio_repo_root: PathBuf,
    pub schema_path: PathBuf,
    pub union_schema_path: Option<PathBuf>,
    pub ppg_status_url: Option<String>,
    pub ppg_status_json_path: Option<PathBuf>,
    pub ppg_status_user: Option<String>,
    pub ppg_status_password: Option<String>,
    pub observed_at: u64,
}

impl Default for BioPipelineMockConfig {
    fn default() -> Self {
        let bio_repo_root = PathBuf::from(DEFAULT_BIO_REPO_ROOT);
        Self {
            schema_path: bio_repo_root.join("PPG_Schema.json"),
            union_schema_path: Some(bio_repo_root.join("union/PPG_Union_Schema.json")),
            bio_repo_root,
            ppg_status_url: None,
            ppg_status_json_path: None,
            ppg_status_user: Some("root".into()),
            ppg_status_password: None,
            observed_at: DEFAULT_OBSERVED_AT,
        }
    }
}

impl BioPipelineMockConfig {
    pub fn from_manifest_config(cfg: &HashMap<String, ConfigValue>) -> Result<Option<Self>> {
        let Some(ConfigValue::Map(row)) = cfg.get("bio_pipeline_mock") else {
            return Ok(None);
        };
        let enabled = match row.get("enabled") {
            Some(ConfigValue::Bool(value)) => *value,
            Some(ConfigValue::String(value)) => value.trim() != "false",
            _ => true,
        };
        if !enabled {
            return Ok(None);
        }

        let mut config = Self::default();
        if let Some(root) = optional_str(row, "bio_repo_root") {
            config.bio_repo_root = PathBuf::from(root);
            config.schema_path = config.bio_repo_root.join("PPG_Schema.json");
            config.union_schema_path =
                Some(config.bio_repo_root.join("union/PPG_Union_Schema.json"));
        }
        if let Some(schema_path) = optional_str(row, "schema_path") {
            config.schema_path = PathBuf::from(schema_path);
        }
        if let Some(union_schema_path) = optional_str(row, "union_schema_path") {
            config.union_schema_path = Some(PathBuf::from(union_schema_path));
        }
        if matches!(row.get("union_schema_path"), Some(ConfigValue::String(value)) if value.trim().is_empty())
        {
            config.union_schema_path = None;
        }
        config.ppg_status_url = optional_str(row, "ppg_status_url");
        config.ppg_status_json_path = optional_str(row, "ppg_status_json_path").map(PathBuf::from);
        config.ppg_status_user = optional_str(row, "ppg_status_user").or(config.ppg_status_user);
        config.ppg_status_password = optional_str(row, "ppg_status_password");
        if let Some(value) =
            optional_int(row, "observed_at").and_then(|value| u64::try_from(value).ok())
        {
            config.observed_at = value;
        }
        Ok(Some(config))
    }
}

pub fn skeleton_from_bio_pipeline_mock_config(
    config: &BioPipelineMockConfig,
) -> Result<SourceGraphSkeleton> {
    let mut builder = BioMockBuilder::new(config.observed_at);
    builder.add_pipeline_root();
    builder.add_nfs_raw_sources();
    builder.add_cncb_local_sources();
    builder.add_ppg_schema(&config.schema_path, "ppg-schema/base")?;
    if let Some(path) = &config.union_schema_path {
        if path.exists() {
            builder.add_ppg_schema(path, "ppg-schema/union")?;
        }
    }
    if let Some(status) = read_status_json(config) {
        builder.add_ppg_status(status);
    }
    Ok(builder.skeleton)
}

struct BioMockBuilder {
    observed_at: u64,
    skeleton: SourceGraphSkeleton,
    pg_tables: BTreeMap<(String, String, String), String>,
}

impl BioMockBuilder {
    fn new(observed_at: u64) -> Self {
        Self {
            observed_at,
            skeleton: SourceGraphSkeleton::default(),
            pg_tables: BTreeMap::new(),
        }
    }

    fn add_pipeline_root(&mut self) {
        self.node(
            SourceKind::Other("pipeline".into()),
            BIO_PIPELINE_INSTANCE,
            "bio",
            NodeKind::Root,
            "pipeline://bio",
            "bio pipeline",
            "bio-pipeline://root",
            None,
        );
    }

    fn add_nfs_raw_sources(&mut self) {
        let root = "/mnt/ncbi_backup_2022";
        self.node(
            SourceKind::Fs,
            FS_INSTANCE,
            root,
            NodeKind::Root,
            format!("fs://10.3.200.29{root}"),
            "NCBI NFS backup",
            format!("fs-meta:{FS_INSTANCE}:{root}"),
            None,
        );
        for source in raw_nfs_sources() {
            let group_id = format!("{root}/{}", source.group);
            self.node(
                SourceKind::Fs,
                FS_INSTANCE,
                &group_id,
                NodeKind::Namespace,
                format!("fs://10.3.200.29{group_id}"),
                source.group,
                format!("fs-meta:{FS_INSTANCE}:{group_id}"),
                None,
            );
            self.edge(
                SourceKind::Fs,
                FS_INSTANCE,
                root,
                &group_id,
                EdgeKind::Contains,
                Confidence::Observed,
            );
            self.node(
                SourceKind::Fs,
                FS_INSTANCE,
                source.path,
                NodeKind::Asset,
                format!("fs://10.3.200.29{}", source.path),
                source.display_name,
                format!("fs-meta:{FS_INSTANCE}:{}", source.path),
                Some(source.table_hint),
            );
            self.edge(
                SourceKind::Fs,
                FS_INSTANCE,
                &group_id,
                source.path,
                EdgeKind::Contains,
                Confidence::Observed,
            );
            self.cross_edge(
                endpoint(SourceKind::Fs, FS_INSTANCE, source.path),
                endpoint(
                    SourceKind::Other("pipeline".into()),
                    BIO_PIPELINE_INSTANCE,
                    "bio",
                ),
                EdgeKind::ParsedTo,
                Confidence::Declared,
            );
        }
    }

    fn add_cncb_local_sources(&mut self) {
        let root = "/root/repo/omix-links-data/raw/cncb/omix/current";
        self.node(
            SourceKind::LocalFs,
            LOCAL_FS_INSTANCE,
            root,
            NodeKind::Root,
            format!("file://{root}"),
            "CNCB OMIX raw snapshot",
            format!("local-fs:{root}"),
            None,
        );
        for source in cncb_local_sources() {
            let path = format!("{root}/{}", source.relpath);
            self.node(
                SourceKind::LocalFs,
                LOCAL_FS_INSTANCE,
                &path,
                NodeKind::Asset,
                format!("file://{path}"),
                source.relpath,
                format!("local-fs:{path}"),
                Some(source.logical_name),
            );
            self.edge(
                SourceKind::LocalFs,
                LOCAL_FS_INSTANCE,
                root,
                &path,
                EdgeKind::Contains,
                Confidence::Declared,
            );
            self.cross_edge(
                endpoint(SourceKind::LocalFs, LOCAL_FS_INSTANCE, &path),
                endpoint(
                    SourceKind::Other("pipeline".into()),
                    BIO_PIPELINE_INSTANCE,
                    "bio",
                ),
                EdgeKind::ParsedTo,
                Confidence::Declared,
            );
        }
    }

    fn add_ppg_schema(&mut self, path: &Path, local_id: &str) -> Result<()> {
        let value = read_json_file(path)?;
        self.add_json_document_node(local_id, path, &value);
        self.add_catalogs_and_labels(&value, local_id);
        Ok(())
    }

    fn add_json_document_node(&mut self, local_id: &str, path: &Path, value: &Value) {
        let fingerprint = schema_fingerprint(value);
        self.node(
            SourceKind::Es,
            ES_INSTANCE,
            local_id,
            NodeKind::Asset,
            format!("es://{ES_INSTANCE}/bio-json/{local_id}"),
            path.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or(local_id),
            format!("json-doc:{}", path.display()),
            Some(&fingerprint),
        );
        self.cross_edge(
            endpoint(SourceKind::Es, ES_INSTANCE, local_id),
            endpoint(
                SourceKind::Other("pipeline".into()),
                BIO_PIPELINE_INSTANCE,
                "bio",
            ),
            EdgeKind::DeclaresSchema,
            Confidence::Declared,
        );
    }

    fn add_catalogs_and_labels(&mut self, schema: &Value, schema_doc_local_id: &str) {
        let catalogs = schema
            .get("catalogs")
            .and_then(Value::as_array)
            .into_iter()
            .flatten();
        for catalog in catalogs {
            let Some(name) = catalog.get("name").and_then(Value::as_str) else {
                continue;
            };
            let db = catalog_database(catalog).unwrap_or_else(|| name.to_ascii_lowercase());
            let catalog_id = format!("{db}/public");
            self.node(
                SourceKind::Pg,
                PG_INSTANCE,
                &catalog_id,
                NodeKind::Namespace,
                format!("pg://{PG_INSTANCE}/{db}/public"),
                name,
                format!("pg-catalog:{name}:{db}:public"),
                Some("catalog"),
            );
            self.cross_edge(
                endpoint(SourceKind::Es, ES_INSTANCE, schema_doc_local_id),
                endpoint(SourceKind::Pg, PG_INSTANCE, &catalog_id),
                EdgeKind::DeclaresSchema,
                Confidence::Declared,
            );
        }

        self.add_ppg_labels(schema_doc_local_id, schema, "vertices", true);
        self.add_ppg_labels(schema_doc_local_id, schema, "edges", false);
    }

    fn add_ppg_labels(
        &mut self,
        schema_doc_local_id: &str,
        schema: &Value,
        key: &str,
        vertex: bool,
    ) {
        let items = schema
            .get("graph")
            .and_then(|graph| graph.get(key))
            .and_then(Value::as_array)
            .into_iter()
            .flatten();
        for item in items {
            let Some(label) = item.get("label").and_then(Value::as_str) else {
                continue;
            };
            let label_id = if vertex {
                format!("ppg/vertex/{label}")
            } else {
                format!("ppg/edge/{label}")
            };
            self.node(
                SourceKind::Other("ppg".into()),
                "bio-ppg-10.3.200.29",
                &label_id,
                NodeKind::Entity,
                format!("ppg://10.3.200.29/{label_id}"),
                label,
                format!("ppg-schema:{schema_doc_local_id}:{label_id}"),
                Some(if vertex { "vertex" } else { "edge" }),
            );
            self.cross_edge(
                endpoint(SourceKind::Es, ES_INSTANCE, schema_doc_local_id),
                endpoint(
                    SourceKind::Other("ppg".into()),
                    "bio-ppg-10.3.200.29",
                    &label_id,
                ),
                EdgeKind::DeclaresSchema,
                Confidence::Declared,
            );

            let Some(table_source) = table_source(item, vertex) else {
                continue;
            };
            let table_id = self.ensure_pg_table(table_source);
            self.cross_edge(
                endpoint(SourceKind::Pg, PG_INSTANCE, &table_id),
                endpoint(
                    SourceKind::Other("ppg".into()),
                    "bio-ppg-10.3.200.29",
                    &label_id,
                ),
                EdgeKind::ProjectedAs,
                Confidence::Declared,
            );
            if let Some(source_path) = raw_source_for_table(table_source.table) {
                self.cross_edge(
                    endpoint(SourceKind::Fs, FS_INSTANCE, source_path),
                    endpoint(SourceKind::Pg, PG_INSTANCE, &table_id),
                    EdgeKind::LoadedInto,
                    Confidence::Inferred,
                );
            }
            if let Some(source_path) = cncb_source_for_table(table_source.table) {
                self.cross_edge(
                    endpoint(SourceKind::LocalFs, LOCAL_FS_INSTANCE, &source_path),
                    endpoint(SourceKind::Pg, PG_INSTANCE, &table_id),
                    EdgeKind::LoadedInto,
                    Confidence::Inferred,
                );
            }
        }
    }

    fn ensure_pg_table(&mut self, table_source: TableSource<'_>) -> String {
        let db = catalog_to_database(table_source.catalog);
        let key = (
            db.to_string(),
            table_source.schema.to_string(),
            table_source.table.to_string(),
        );
        if let Some(existing) = self.pg_tables.get(&key) {
            return existing.clone();
        }
        let schema_id = format!("{}/{}", key.0, key.1);
        if !self
            .skeleton
            .nodes
            .iter()
            .any(|node| node.source_kind == SourceKind::Pg && node.local_id == schema_id)
        {
            self.node(
                SourceKind::Pg,
                PG_INSTANCE,
                &schema_id,
                NodeKind::Namespace,
                format!("pg://{PG_INSTANCE}/{}/{}", key.0, key.1),
                &key.1,
                format!("pg-catalog:{}:{}", key.0, key.1),
                None,
            );
        }
        let table_id = format!("{}/{}/{}", key.0, key.1, key.2);
        self.node(
            SourceKind::Pg,
            PG_INSTANCE,
            &table_id,
            NodeKind::Collection,
            format!("pg://{PG_INSTANCE}/{}/{}/{}", key.0, key.1, key.2),
            &key.2,
            format!("pg-table:{}:{}.{}", key.0, key.1, key.2),
            Some(table_source.catalog),
        );
        self.edge(
            SourceKind::Pg,
            PG_INSTANCE,
            &schema_id,
            &table_id,
            EdgeKind::Contains,
            Confidence::Declared,
        );
        self.pg_tables.insert(key, table_id.clone());
        table_id
    }

    fn add_ppg_status(&mut self, status: Value) {
        let status_id = "ppg-status/current";
        let cache_status = status
            .pointer("/GremlinServer/LocalCacheStatus")
            .and_then(Value::as_str)
            .unwrap_or("UNKNOWN");
        let schema_exists = status
            .pointer("/Schema/Exists")
            .and_then(Value::as_bool)
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unknown".into());
        let fingerprint = format!(
            "cache={cache_status};schema_exists={schema_exists};details={}",
            status
                .pointer("/GremlinServer/LocalCacheStatusDetail")
                .and_then(Value::as_array)
                .map(Vec::len)
                .unwrap_or_default()
        );
        self.node(
            SourceKind::Es,
            ES_INSTANCE,
            status_id,
            NodeKind::Asset,
            format!("es://{ES_INSTANCE}/bio-json/{status_id}"),
            "PPG runtime status",
            "ppg-status:http://10.3.200.29:28081/status",
            Some(&fingerprint),
        );
        self.cross_edge(
            endpoint(SourceKind::Es, ES_INSTANCE, status_id),
            endpoint(
                SourceKind::Other("ppg".into()),
                "bio-ppg-10.3.200.29",
                "ppg/vertex/gene",
            ),
            EdgeKind::HasRuntimeStatus,
            Confidence::Observed,
        );
    }

    fn node(
        &mut self,
        source_kind: SourceKind,
        source_instance: &str,
        local_id: &str,
        node_kind: NodeKind,
        native_ref: impl Into<String>,
        display_name: impl Into<String>,
        native_pointer: impl Into<String>,
        fingerprint: Option<&str>,
    ) {
        if self.skeleton.nodes.iter().any(|node| {
            node.source_kind == source_kind
                && node.source_instance == source_instance
                && node.local_id == local_id
        }) {
            return;
        }
        self.skeleton.nodes.push(SourceGraphNode {
            source_kind,
            source_instance: source_instance.into(),
            local_id: local_id.into(),
            node_kind,
            native_ref: native_ref.into(),
            display_name: display_name.into(),
            native_pointer: native_pointer.into(),
            fingerprint: fingerprint.map(str::to_string),
            observed_at: self.observed_at,
            grant_epoch: None,
        });
    }

    fn edge(
        &mut self,
        source_kind: SourceKind,
        source_instance: &str,
        from_local_id: &str,
        to_local_id: &str,
        edge_kind: EdgeKind,
        confidence: Confidence,
    ) {
        self.skeleton.edges.push(crate::SourceGraphEdge {
            source_kind,
            source_instance: source_instance.into(),
            from_local_id: from_local_id.into(),
            to_local_id: to_local_id.into(),
            edge_kind,
            evidence_ref: String::new(),
            confidence,
            observed_at: self.observed_at,
            grant_epoch: None,
        });
    }

    fn cross_edge(
        &mut self,
        from: SourceGraphEndpoint,
        to: SourceGraphEndpoint,
        edge_kind: EdgeKind,
        confidence: Confidence,
    ) {
        self.skeleton.cross_edges.push(CrossSourceGraphEdge {
            from,
            to,
            edge_kind,
            evidence_ref: String::new(),
            confidence,
            observed_at: self.observed_at,
            grant_epoch: None,
        });
    }
}

#[derive(Debug, Clone, Copy)]
struct RawNfsSource {
    group: &'static str,
    path: &'static str,
    display_name: &'static str,
    table_hint: &'static str,
}

#[derive(Debug, Clone, Copy)]
struct CncbLocalSource {
    logical_name: &'static str,
    relpath: &'static str,
}

#[derive(Debug, Clone, Copy)]
struct TableSource<'a> {
    catalog: &'a str,
    schema: &'a str,
    table: &'a str,
}

fn raw_nfs_sources() -> &'static [RawNfsSource] {
    &[
        RawNfsSource {
            group: "GEN",
            path: "/mnt/ncbi_backup_2022/GEN/gene/DATA/gene_info.gz",
            display_name: "gene_info.gz",
            table_hint: "gene_info",
        },
        RawNfsSource {
            group: "GEN",
            path: "/mnt/ncbi_backup_2022/GEN/gene/DATA/gene2pubmed.gz",
            display_name: "gene2pubmed.gz",
            table_hint: "gene2pubmed",
        },
        RawNfsSource {
            group: "GEN",
            path: "/mnt/ncbi_backup_2022/GEN/gene/DATA/gene2go.gz",
            display_name: "gene2go.gz",
            table_hint: "gene2go",
        },
        RawNfsSource {
            group: "GEN",
            path: "/mnt/ncbi_backup_2022/GEN/gene/DATA/gene2accession.gz",
            display_name: "gene2accession.gz",
            table_hint: "gene2accession",
        },
        RawNfsSource {
            group: "MED",
            path: "/mnt/ncbi_backup_2022/MED/pub/medgen/MGCONSO.RRF.gz",
            display_name: "MGCONSO.RRF.gz",
            table_hint: "medgen_names",
        },
        RawNfsSource {
            group: "TAX",
            path: "/mnt/ncbi_backup_2022/TAX/pub/taxonomy/taxdump_archive",
            display_name: "taxdump_archive",
            table_hint: "ncbi_taxon",
        },
        RawNfsSource {
            group: "BIE",
            path: "/mnt/ncbi_backup_2022/BIE/bioproject/bioproject.xml",
            display_name: "bioproject.xml",
            table_hint: "ncbi_bioproject",
        },
        RawNfsSource {
            group: "BIF",
            path: "/mnt/ncbi_backup_2022/BIF/biosample/biosample_set.xml.gz",
            display_name: "biosample_set.xml.gz",
            table_hint: "ncbi_biosample",
        },
    ]
}

fn cncb_local_sources() -> &'static [CncbLocalSource] {
    &[
        CncbLocalSource {
            logical_name: "dataset_node",
            relpath: "entities/dataset.csv",
        },
        CncbLocalSource {
            logical_name: "article_node",
            relpath: "entities/article.csv",
        },
        CncbLocalSource {
            logical_name: "bioproject_node",
            relpath: "entities/bioproject.csv",
        },
        CncbLocalSource {
            logical_name: "dataset_article_fact",
            relpath: "facts/dataset_article.csv",
        },
        CncbLocalSource {
            logical_name: "dataset_bioproject_fact",
            relpath: "facts/dataset_bioproject.csv",
        },
        CncbLocalSource {
            logical_name: "dataset_taxon_bridge",
            relpath: "bridges/taxonomy/dataset_tax_id.csv",
        },
        CncbLocalSource {
            logical_name: "doi_pmid_bridge",
            relpath: "bridges/publication/doi_pmid.xlsx",
        },
    ]
}

fn table_source(item: &Value, vertex: bool) -> Option<TableSource<'_>> {
    let source = if vertex {
        item.get("oneToOne")
            .and_then(|one_to_one| one_to_one.get("tableSource"))
            .or_else(|| item.get("tableSource"))
    } else {
        item.get("tableSource")
    }?;
    Some(TableSource {
        catalog: source.get("catalog")?.as_str()?,
        schema: source.get("schema")?.as_str()?,
        table: source.get("table")?.as_str()?,
    })
}

fn catalog_database(catalog: &Value) -> Option<String> {
    let jdbc_uri = catalog.get("jdbc")?.get("jdbcUri")?.as_str()?;
    let path = jdbc_uri.split('?').next().unwrap_or(jdbc_uri);
    path.rsplit('/').next().map(str::to_string)
}

fn catalog_to_database(catalog: &str) -> &str {
    match catalog {
        "NCBI" => "postgres",
        "OMIX" => "omix_graph",
        "BRIDGE" => "bridge_graph",
        other => other,
    }
}

fn raw_source_for_table(table: &str) -> Option<&'static str> {
    match table {
        "gene_info" => Some("/mnt/ncbi_backup_2022/GEN/gene/DATA/gene_info.gz"),
        "gene2pubmed" => Some("/mnt/ncbi_backup_2022/GEN/gene/DATA/gene2pubmed.gz"),
        "gene2go" => Some("/mnt/ncbi_backup_2022/GEN/gene/DATA/gene2go.gz"),
        "gene2accession" => Some("/mnt/ncbi_backup_2022/GEN/gene/DATA/gene2accession.gz"),
        "medgen_names" => Some("/mnt/ncbi_backup_2022/MED/pub/medgen/MGCONSO.RRF.gz"),
        "ncbi_taxon" | "ncbi_taxon_parent" => {
            Some("/mnt/ncbi_backup_2022/TAX/pub/taxonomy/taxdump_archive")
        }
        "ncbi_bioproject" | "ncbi_bioproject_pubmed" | "ncbi_bioproject_member" => {
            Some("/mnt/ncbi_backup_2022/BIE/bioproject/bioproject.xml")
        }
        "ncbi_biosample" | "ncbi_biosample_bioproject" | "ncbi_biosample_sra_sample" => {
            Some("/mnt/ncbi_backup_2022/BIF/biosample/biosample_set.xml.gz")
        }
        _ => None,
    }
}

fn cncb_source_for_table(table: &str) -> Option<String> {
    let relpath = match table {
        "cncb_dataset" | "lineage_cncb_dataset_record" => "entities/dataset.csv",
        "cncb_article" | "lineage_cncb_article_record" => "entities/article.csv",
        "cncb_bioproject" | "lineage_cncb_bioproject_record" => "entities/bioproject.csv",
        "cncb_has_article" | "lineage_cncb_dataset_article_record" => "facts/dataset_article.csv",
        "cncb_has_bioproject" | "lineage_cncb_dataset_bioproject_record" => {
            "facts/dataset_bioproject.csv"
        }
        "br_has_taxon" | "lineage_cncb_dataset_taxon_record" => {
            "bridges/taxonomy/dataset_tax_id.csv"
        }
        "br_same_publication_as" | "lineage_cncb_article_pubmed_bridge_record" => {
            "bridges/publication/doi_pmid.xlsx"
        }
        _ => return None,
    };
    Some(format!(
        "/root/repo/omix-links-data/raw/cncb/omix/current/{relpath}"
    ))
}

fn endpoint(
    source_kind: SourceKind,
    source_instance: &str,
    local_id: impl Into<String>,
) -> SourceGraphEndpoint {
    SourceGraphEndpoint::new(source_kind, source_instance, local_id)
}

fn read_status_json(config: &BioPipelineMockConfig) -> Option<Value> {
    if let Some(path) = &config.ppg_status_json_path {
        return Some(read_json_file(path).unwrap_or_else(|err| status_error("file", err)));
    }
    let Some(url) = &config.ppg_status_url else {
        return None;
    };
    let client = match reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(8))
        .build()
    {
        Ok(client) => client,
        Err(err) => return Some(status_error("client", err)),
    };
    let mut request = client.get(url);
    if let Some(user) = &config.ppg_status_user {
        request = request.basic_auth(user, config.ppg_status_password.clone());
    }
    let response = match request.send() {
        Ok(response) => response,
        Err(err) => return Some(status_error("fetch", err)),
    };
    let response = match response.error_for_status() {
        Ok(response) => response,
        Err(err) => return Some(status_error("http", err)),
    };
    match response.json::<Value>() {
        Ok(value) => Some(value),
        Err(err) => Some(status_error("decode", err)),
    }
}

fn status_error(stage: &str, err: impl std::fmt::Display) -> Value {
    json!({
        "NodeStatus": {
            "Healthy": false,
            "ErrorMsg": format!("{stage}: {err}")
        },
        "Schema": {
            "Exists": false
        },
        "GremlinServer": {
            "LocalCacheStatus": "UNAVAILABLE",
            "LocalCacheStatusDetail": []
        }
    })
}

fn read_json_file(path: &Path) -> Result<Value> {
    let raw = fs::read_to_string(path)
        .map_err(|err| CnxError::InvalidInput(format!("read {} failed: {err}", path.display())))?;
    serde_json::from_str(&raw)
        .map_err(|err| CnxError::InvalidInput(format!("parse {} failed: {err}", path.display())))
}

fn schema_fingerprint(value: &Value) -> String {
    let vertices = value
        .pointer("/graph/vertices")
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or_default();
    let edges = value
        .pointer("/graph/edges")
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or_default();
    let catalogs = value
        .get("catalogs")
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or_default();
    format!("catalogs={catalogs};vertices={vertices};edges={edges}")
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

#[allow(dead_code)]
fn _keep_evidence_types_linked(_: EvidenceRecord, _: NativeAnchor, _: ObservationKind) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bio_mock_skeleton_covers_four_source_families_and_lineage() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../fixtures/bio-mock");
        let config = BioPipelineMockConfig {
            bio_repo_root: root.clone(),
            schema_path: root.join("PPG_Schema.json"),
            union_schema_path: Some(root.join("PPG_Union_Schema.json")),
            ppg_status_url: None,
            ppg_status_json_path: Some(root.join("ppg-status.json")),
            ppg_status_user: None,
            ppg_status_password: None,
            observed_at: 42,
        };
        let skeleton = skeleton_from_bio_pipeline_mock_config(&config).expect("skeleton");
        assert!(
            skeleton
                .nodes
                .iter()
                .any(|node| node.source_kind == SourceKind::Fs)
        );
        assert!(
            skeleton
                .nodes
                .iter()
                .any(|node| node.source_kind == SourceKind::LocalFs)
        );
        assert!(
            skeleton
                .nodes
                .iter()
                .any(|node| node.source_kind == SourceKind::Es)
        );
        assert!(
            skeleton
                .nodes
                .iter()
                .any(|node| node.source_kind == SourceKind::Pg)
        );
        assert!(
            skeleton
                .cross_edges
                .iter()
                .any(|edge| edge.edge_kind == EdgeKind::LoadedInto)
        );
        assert!(
            skeleton
                .cross_edges
                .iter()
                .any(|edge| edge.edge_kind == EdgeKind::ProjectedAs)
        );
        assert!(
            skeleton
                .cross_edges
                .iter()
                .any(|edge| edge.edge_kind == EdgeKind::HasRuntimeStatus)
        );
    }
}

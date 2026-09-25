use super::*;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Proposal {
    configuration: HashMap<String, ConfigValue>,
    selection: Selection,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Selection {
    object_ref: String,
    grant_epoch: u64,
    #[serde(default)]
    catalog_metadata: bool,
    path: Option<String>,
    schema: Option<String>,
    table: Option<String>,
    index: Option<String>,
    bucket: Option<String>,
    prefix: Option<String>,
    configured_query_id: Option<String>,
}

pub fn validate(kind: &str, raw: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    let proposal: Proposal = serde_json::from_slice(raw)?;
    let cfg = &proposal.configuration;
    let s = proposal.selection;
    let fail = || capanix_app_sdk::CnxError::AccessDenied("source scope selection denied".into());
    if s.object_ref.trim().is_empty() || s.grant_epoch == 0 {
        return Err(fail().into());
    }
    if s.catalog_metadata && kind != "pg" {
        return Err(fail().into());
    }
    let fields = [
        ("path", &s.path),
        ("schema", &s.schema),
        ("table", &s.table),
        ("index", &s.index),
        ("bucket", &s.bucket),
        ("prefix", &s.prefix),
        ("configured_query_id", &s.configured_query_id),
    ];
    let expected: &[&str] = match kind {
        "pg" if s.catalog_metadata => &[],
        "jsonl" | "csv" | "local-fs" => &["path"],
        "pg" | "mysql" => &["schema", "table"],
        "es" => &["index"],
        "s3" => &["bucket", "prefix"],
        "graph" => &["configured_query_id"],
        _ => return Err("unsupported source kind".into()),
    };
    if fields.iter().any(|(name, value)| {
        if expected.contains(name) {
            value.as_ref().is_none_or(|v| {
                v.len() > 1024 || v.contains('\0') || (v.is_empty() && *name != "prefix")
            })
        } else {
            value.is_some()
        }
    }) {
        return Err(fail().into());
    }
    // Reuse source-owned endpoint resolution. Product scopes and runtime grants
    // retain their original meanings; this CLI neither intersects nor rewrites them.
    let epoch = match kind {
        "jsonl" => {
            JsonlSourceRuntimeConfig::from_manifest_config(cfg)?
                .endpoint_for(&s.object_ref, s.path.as_deref())?
                .grant_epoch
        }
        "csv" => {
            CsvSourceRuntimeConfig::from_manifest_config(cfg)?
                .endpoint_for(&s.object_ref, s.path.as_deref())?
                .grant_epoch
        }
        "local-fs" => {
            LocalFsSourceRuntimeConfig::from_manifest_config(cfg)?
                .endpoint_for(&s.object_ref, s.path.as_deref())?
                .grant_epoch
        }
        "pg" => {
            PgSourceRuntimeConfig::from_manifest_config(cfg)?
                .endpoint_for(&s.object_ref, s.schema.as_deref(), s.table.as_deref())?
                .grant_epoch
        }
        "mysql" => {
            MysqlSourceRuntimeConfig::from_manifest_config(cfg)?
                .endpoint_for(&s.object_ref, s.schema.as_deref(), s.table.as_deref())?
                .grant_epoch
        }
        "es" => {
            EsSourceRuntimeConfig::from_manifest_config(cfg)?
                .endpoint_for(&s.object_ref, s.index.as_deref().ok_or_else(fail)?)?
                .grant_epoch
        }
        "s3" => {
            S3SourceRuntimeConfig::from_manifest_config(cfg)?
                .endpoint_for(&s.object_ref, s.bucket.as_deref(), s.prefix.as_deref())?
                .grant_epoch
        }
        "graph" => {
            let config = GraphSourceRuntimeConfig::from_manifest_config(cfg)?;
            let (endpoint, grant) = config.endpoint(&s.object_ref, s.grant_epoch)?;
            let query = s.configured_query_id.as_deref().ok_or_else(fail)?;
            if !grant.query_scopes.iter().any(|id| id == query)
                || !endpoint.queries.iter().any(|item| item.query_id == query)
            {
                return Err(fail().into());
            }
            Some(grant.grant_epoch)
        }
        _ => unreachable!(),
    };
    if epoch != Some(s.grant_epoch) {
        return Err(fail().into());
    }
    println!(
        "{}",
        serde_json::json!({"valid":true,"scopeAuthorized":true,"readiness":"not-observed"})
    );
    Ok(())
}

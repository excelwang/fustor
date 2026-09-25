use capanix_app_sdk::{CnxError, Result, runtime::ConfigValue};
use serde::{Deserialize, Serialize};
use source_kit::{SourceCredentialConfig, SourceExecutionLimits};
use std::collections::{BTreeMap, HashMap};

pub(crate) fn invalid(message: &str) -> CnxError {
    CnxError::InvalidInput(message.into())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphQuery {
    pub query_id: String,
    pub statement: String,
    pub result_schema_ref: String,
    pub limits: SourceExecutionLimits,
    #[serde(default)]
    pub allowed_parameters: Vec<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphEndpoint {
    pub object_ref: String,
    pub endpoint_uri: String,
    pub credential_ref: String,
    /// Optional credential for the status/revision endpoint. Providers may use
    /// an administrative HTTP identity distinct from the read-only Bolt identity.
    #[serde(default)]
    pub status_credential_ref: Option<String>,
    /// Legacy deployment assertion only: never proof of backend privileges.
    #[serde(default)]
    pub read_only_identity: bool,
    #[serde(default)]
    pub status_url: Option<String>,
    #[serde(default)]
    pub database: Option<String>,
    pub active: bool,
    pub queries: Vec<GraphQuery>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphGrant {
    pub resource_kind: String,
    pub object_ref: String,
    pub grant_state: String,
    pub grant_epoch: u64,
    pub interfaces: Vec<String>,
    pub query_scopes: Vec<String>,
}
#[derive(Debug, Clone)]
pub struct GraphSourceRuntimeConfig {
    pub endpoints: Vec<GraphEndpoint>,
    pub credentials: Vec<SourceCredentialConfig>,
    pub grants: Vec<GraphGrant>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Document {
    endpoints: Vec<GraphEndpoint>,
    credentials: Vec<HashMap<String, ConfigValue>>,
    __cnx_runtime: Runtime,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Runtime {
    resource_grants: Vec<GraphGrant>,
}

// Mandatory Fustor gate in addition to native object/query grants and budgets.
// Provider read-only ACLs are optional defense in depth, not an assumed fact.
// No procedures, comments, escapes, multi-statements or dynamically selected text.
pub fn validate_read_only(statement: &str) -> Result<()> {
    if statement.len() > 16384
        || !statement.is_ascii()
        || statement.contains([';', '`', '\\'])
        || statement.contains("//")
        || statement.contains("/*")
        || statement.contains("--")
    {
        return Err(invalid("graph statement is outside the read-only subset"));
    }
    let words: Vec<_> = statement
        .split(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_')
        .filter(|word| !word.is_empty())
        .map(str::to_ascii_uppercase)
        .collect();
    if !words.first().is_some_and(|word| {
        ["MATCH", "OPTIONAL", "RETURN", "WITH", "UNWIND"].contains(&word.as_str())
    }) || !words.iter().any(|word| word == "RETURN")
        || words.iter().any(|word| {
            [
                "CALL", "CREATE", "MERGE", "DELETE", "DETACH", "SET", "REMOVE", "DROP", "ALTER",
                "LOAD", "FOREACH", "GRANT", "DENY", "REVOKE", "USE",
            ]
            .contains(&word.as_str())
        })
    {
        return Err(invalid("graph statement is outside the read-only subset"));
    }
    Ok(())
}
fn endpoint_url(value: &str, schemes: &[&str]) -> Result<url::Url> {
    let url = url::Url::parse(value).map_err(|_| invalid("invalid graph endpoint"))?;
    if !schemes.contains(&url.scheme())
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(invalid(
            "graph endpoint must be a credential-free approved URI",
        ));
    }
    Ok(url)
}
impl GraphSourceRuntimeConfig {
    pub fn from_manifest_config(config: &HashMap<String, ConfigValue>) -> Result<Self> {
        if source_kit::contains_forbidden_secret_key(config) {
            return Err(invalid("graph config accepts credential references only"));
        }
        let doc: Document = serde_json::from_value(
            serde_json::to_value(config).map_err(|_| invalid("graph configuration"))?,
        )
        .map_err(|_| invalid("invalid native graph configuration"))?;
        if doc.endpoints.is_empty()
            || doc.endpoints.len() > 32
            || doc.__cnx_runtime.resource_grants.len() > 32
        {
            return Err(invalid("graph configuration bounds"));
        }
        let credentials = doc
            .credentials
            .iter()
            .map(source_kit::parse_credential_config)
            .collect::<Result<Vec<_>>>()?;
        source_kit::reject_duplicate_refs(
            doc.endpoints
                .iter()
                .map(|endpoint| endpoint.object_ref.as_str()),
            "graph",
        )?;
        source_kit::reject_duplicate_refs(
            credentials
                .iter()
                .map(|credential| credential.credential_ref.as_str()),
            "graph credential",
        )?;
        source_kit::reject_duplicate_refs(
            doc.__cnx_runtime
                .resource_grants
                .iter()
                .map(|grant| grant.object_ref.as_str()),
            "graph grant",
        )?;
        for endpoint in &doc.endpoints {
            endpoint_url(&endpoint.endpoint_uri, &["bolt", "bolt+s"])?;
            if let Some(status) = &endpoint.status_url {
                endpoint_url(status, &["http", "https"])?;
            }
            if endpoint.object_ref.trim().is_empty()
                || endpoint.queries.is_empty()
                || endpoint.queries.len() > 100
                || !credentials
                    .iter()
                    .any(|credential| credential.credential_ref == endpoint.credential_ref)
                || endpoint
                    .status_credential_ref
                    .as_ref()
                    .is_some_and(|reference| {
                        !credentials
                            .iter()
                            .any(|credential| &credential.credential_ref == reference)
                    })
            {
                return Err(invalid(
                    "graph endpoint requires a credential binding and configured read queries",
                ));
            }
            source_kit::reject_duplicate_refs(
                endpoint.queries.iter().map(|query| query.query_id.as_str()),
                "graph query",
            )?;
            for query in &endpoint.queries {
                validate_read_only(&query.statement)?;
                query.limits.validate()?;
                if query.query_id.is_empty()
                    || query.result_schema_ref.is_empty()
                    || query.allowed_parameters.len() > 32
                    || query.limits.max_rows > 10000
                    || query.limits.max_bytes > 4194304
                    || query.limits.max_latency_ms > 30000
                    || query.limits.max_fanout != 1
                    || query.limits.max_calls > 3
                    || query.limits.max_cost_units > 3
                {
                    return Err(invalid("graph query hard limits"));
                }
            }
        }
        for grant in &doc.__cnx_runtime.resource_grants {
            let endpoint = doc
                .endpoints
                .iter()
                .find(|endpoint| endpoint.object_ref == grant.object_ref)
                .ok_or_else(|| invalid("graph grant target"))?;
            if grant.resource_kind != "graph"
                || grant.grant_epoch == 0
                || !["active", "revoked", "inactive"].contains(&grant.grant_state.as_str())
                || grant.interfaces.iter().any(|name| name != "read")
                || grant
                    .query_scopes
                    .iter()
                    .any(|id| !endpoint.queries.iter().any(|query| &query.query_id == id))
            {
                return Err(invalid("invalid graph runtime grant"));
            }
        }
        Ok(Self {
            endpoints: doc.endpoints,
            credentials,
            grants: doc.__cnx_runtime.resource_grants,
        })
    }
    pub fn endpoint(&self, object_ref: &str, epoch: u64) -> Result<(&GraphEndpoint, &GraphGrant)> {
        let denied = || CnxError::AccessDenied("graph object/grant unavailable".into());
        let endpoint = self
            .endpoints
            .iter()
            .find(|endpoint| endpoint.object_ref == object_ref && endpoint.active)
            .ok_or_else(denied)?;
        let grant = self
            .grants
            .iter()
            .find(|grant| {
                grant.object_ref == object_ref
                    && grant.grant_state == "active"
                    && grant.interfaces.iter().any(|name| name == "read")
            })
            .ok_or_else(denied)?;
        if grant.grant_epoch != epoch {
            return Err(denied());
        }
        Ok((endpoint, grant))
    }
}
pub(crate) fn validate_parameters(
    parameters: &BTreeMap<String, serde_json::Value>,
    query: &GraphQuery,
) -> Result<()> {
    if parameters
        .keys()
        .any(|name| !query.allowed_parameters.contains(name))
        || serde_json::to_vec(parameters)
            .map_err(|_| invalid("graph parameters"))?
            .len()
            > 65536
    {
        return Err(invalid("unapproved graph parameter"));
    }
    Ok(())
}

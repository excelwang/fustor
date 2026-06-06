use std::collections::HashMap;

use capanix_app_sdk::runtime::ConfigValue;
use capanix_app_sdk::{CnxError, Result};
use source_kit::{
    SourceCredentialConfig, bool_value, effective_limit, get_int, get_map, get_str,
    normalize_page_size, optional_nonempty_str, parse_credential_config, pattern_allowed,
    reject_secret_material, required_str, resolve_credential, string_array,
};

pub const DEFAULT_PAGE_SIZE: usize = 500;
pub const DEFAULT_MAX_PAGE_SIZE: usize = 5_000;
pub const DEFAULT_SNAPSHOT_TTL_MS: u64 = 60_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MysqlSourceRuntimeConfig {
    pub product: MysqlSourceConfig,
    pub runtime: MysqlSourceRuntimeInputs,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MysqlSourceConfig {
    pub endpoints: Vec<MysqlEndpointConfig>,
    pub credentials: Vec<SourceCredentialConfig>,
    pub default_page_size: usize,
    pub max_page_size: usize,
    pub snapshot_ttl_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MysqlEndpointConfig {
    pub object_ref: String,
    pub endpoint_uri: String,
    pub credential_ref: Option<String>,
    pub schema_scopes: Vec<String>,
    pub table_scopes: Vec<String>,
    pub primary_key: Option<String>,
    pub active: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MysqlSourceRuntimeInputs {
    pub endpoint_grants: Vec<GrantedMysqlEndpoint>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GrantedMysqlEndpoint {
    pub object_ref: String,
    pub endpoint_ref: Option<String>,
    pub endpoint_uri: Option<String>,
    pub credential_ref: Option<String>,
    pub schema_scopes: Vec<String>,
    pub table_scopes: Vec<String>,
    pub interfaces: Vec<String>,
    pub active: bool,
    pub grant_epoch: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedMysqlEndpoint {
    pub object_ref: String,
    pub endpoint_uri: String,
    pub credential_ref: Option<String>,
    pub schema_scopes: Vec<String>,
    pub table_scopes: Vec<String>,
    pub primary_key: Option<String>,
    pub grant_epoch: Option<u64>,
}

impl Default for MysqlSourceConfig {
    fn default() -> Self {
        Self {
            endpoints: Vec::new(),
            credentials: Vec::new(),
            default_page_size: DEFAULT_PAGE_SIZE,
            max_page_size: DEFAULT_MAX_PAGE_SIZE,
            snapshot_ttl_ms: DEFAULT_SNAPSHOT_TTL_MS,
        }
    }
}

impl MysqlSourceRuntimeConfig {
    pub fn from_manifest_config(cfg: &HashMap<String, ConfigValue>) -> Result<Self> {
        Ok(Self {
            product: MysqlSourceConfig::from_product_manifest_config(cfg)?,
            runtime: MysqlSourceRuntimeInputs::from_runtime_manifest_config(cfg)?,
        })
    }

    pub fn endpoint_for(
        &self,
        object_ref: &str,
        schema: Option<&str>,
        table: Option<&str>,
    ) -> Result<ResolvedMysqlEndpoint> {
        let product = self
            .product
            .endpoints
            .iter()
            .find(|endpoint| endpoint.object_ref == object_ref)
            .ok_or_else(|| {
                CnxError::AccessDenied(format!("unknown mysql endpoint object_ref '{object_ref}'"))
            })?;
        if !product.active {
            return Err(CnxError::AccessDenied(format!(
                "mysql endpoint '{object_ref}' is inactive in app config"
            )));
        }
        let grant = self
            .runtime
            .endpoint_grants
            .iter()
            .find(|grant| grant.object_ref == object_ref)
            .ok_or_else(|| {
                CnxError::AccessDenied(format!(
                    "missing active runtime grant for mysql endpoint '{object_ref}'"
                ))
            })?;
        if !grant.active {
            return Err(CnxError::AccessDenied(format!(
                "runtime grant for mysql endpoint '{object_ref}' is inactive"
            )));
        }
        let schema_scopes = if grant.schema_scopes.is_empty() {
            product.schema_scopes.clone()
        } else {
            grant.schema_scopes.clone()
        };
        let table_scopes = if grant.table_scopes.is_empty() {
            product.table_scopes.clone()
        } else {
            grant.table_scopes.clone()
        };
        if let Some(schema) = schema
            && !pattern_allowed(schema, &schema_scopes)
        {
            return Err(CnxError::ScopeDenied(format!(
                "schema '{schema}' is outside runtime scope for mysql endpoint '{object_ref}'"
            )));
        }
        if let Some(table) = table
            && !pattern_allowed(table, &table_scopes)
        {
            return Err(CnxError::ScopeDenied(format!(
                "table '{table}' is outside runtime scope for mysql endpoint '{object_ref}'"
            )));
        }
        Ok(ResolvedMysqlEndpoint {
            object_ref: object_ref.to_string(),
            endpoint_uri: grant
                .endpoint_uri
                .clone()
                .unwrap_or_else(|| product.endpoint_uri.clone()),
            credential_ref: grant
                .credential_ref
                .clone()
                .or_else(|| product.credential_ref.clone()),
            schema_scopes,
            table_scopes,
            primary_key: product.primary_key.clone(),
            grant_epoch: grant.grant_epoch,
        })
    }

    pub fn resolve_credential(
        &self,
        credential_ref: Option<&str>,
    ) -> Result<source_kit::ResolvedCredential> {
        resolve_credential(&self.product.credentials, credential_ref)
    }

    pub fn effective_limit(&self, requested: Option<usize>) -> usize {
        effective_limit(
            requested,
            self.product.default_page_size,
            self.product.max_page_size,
        )
    }
}

impl MysqlSourceConfig {
    pub fn from_product_manifest_config(cfg: &HashMap<String, ConfigValue>) -> Result<Self> {
        let mut out = Self::default();
        if let Some(value) = get_int(cfg, "default_page_size") {
            out.default_page_size = normalize_page_size(value, "default_page_size")?;
        }
        if let Some(value) = get_int(cfg, "max_page_size") {
            out.max_page_size = normalize_page_size(value, "max_page_size")?;
        }
        if let Some(value) = get_int(cfg, "snapshot_ttl_ms") {
            out.snapshot_ttl_ms = u64::try_from(value)
                .map_err(|_| CnxError::InvalidInput("snapshot_ttl_ms must be positive".into()))?;
        }
        if out.default_page_size > out.max_page_size {
            return Err(CnxError::InvalidInput(
                "default_page_size must be <= max_page_size".into(),
            ));
        }
        if let Some(ConfigValue::Array(endpoints)) = cfg.get("endpoints") {
            for item in endpoints {
                let ConfigValue::Map(row) = item else {
                    return Err(CnxError::InvalidInput(
                        "endpoints[] item must be map".into(),
                    ));
                };
                out.endpoints.push(parse_endpoint_config(row)?);
            }
        }
        if let Some(ConfigValue::Array(credentials)) = cfg.get("credentials") {
            for item in credentials {
                let ConfigValue::Map(row) = item else {
                    return Err(CnxError::InvalidInput(
                        "credentials[] item must be map".into(),
                    ));
                };
                out.credentials.push(parse_credential_config(row)?);
            }
        }
        Ok(out)
    }
}

impl MysqlSourceRuntimeInputs {
    pub fn from_runtime_manifest_config(cfg: &HashMap<String, ConfigValue>) -> Result<Self> {
        let Some(runtime) = get_map(cfg, "__cnx_runtime") else {
            return Ok(Self::default());
        };
        let mut endpoint_grants = Vec::new();
        if let Some(ConfigValue::Array(grants)) = runtime.get("resource_grants") {
            for item in grants {
                let ConfigValue::Map(row) = item else {
                    return Err(CnxError::InvalidInput(
                        "__cnx_runtime.resource_grants[] item must be map".into(),
                    ));
                };
                let Some(kind) = get_str(row, "resource_kind") else {
                    continue;
                };
                if kind != "mysql" && kind != "mysql_endpoint" {
                    continue;
                }
                reject_secret_material(row, "__cnx_runtime.resource_grants[]")?;
                endpoint_grants.push(parse_runtime_grant(row)?);
            }
        }
        Ok(Self { endpoint_grants })
    }
}

fn parse_endpoint_config(row: &HashMap<String, ConfigValue>) -> Result<MysqlEndpointConfig> {
    reject_secret_material(row, "endpoints[]")?;
    Ok(MysqlEndpointConfig {
        object_ref: required_str(row, "object_ref")?,
        endpoint_uri: required_str(row, "endpoint_uri")?,
        credential_ref: optional_nonempty_str(row, "credential_ref"),
        schema_scopes: string_array(row, "schema_scopes").unwrap_or_else(|| vec!["*".into()]),
        table_scopes: string_array(row, "table_scopes").unwrap_or_else(|| vec!["*".into()]),
        primary_key: optional_nonempty_str(row, "primary_key"),
        active: bool_value(row, "active", true),
    })
}

fn parse_runtime_grant(row: &HashMap<String, ConfigValue>) -> Result<GrantedMysqlEndpoint> {
    Ok(GrantedMysqlEndpoint {
        object_ref: required_str(row, "object_ref")?,
        endpoint_ref: optional_nonempty_str(row, "endpoint_ref"),
        endpoint_uri: optional_nonempty_str(row, "endpoint_uri"),
        credential_ref: optional_nonempty_str(row, "credential_ref"),
        schema_scopes: string_array(row, "schema_scopes").unwrap_or_default(),
        table_scopes: string_array(row, "table_scopes").unwrap_or_default(),
        interfaces: string_array(row, "interfaces").unwrap_or_default(),
        active: match optional_nonempty_str(row, "grant_state")
            .or_else(|| optional_nonempty_str(row, "state"))
        {
            Some(state) => state == "active",
            None => true,
        },
        grant_epoch: get_int(row, "grant_epoch").and_then(|v| u64::try_from(v).ok()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn string(value: &str) -> ConfigValue {
        ConfigValue::String(value.to_string())
    }

    #[test]
    fn runtime_grant_rejects_secret_material() {
        let cfg = HashMap::from([(
            "__cnx_runtime".to_string(),
            ConfigValue::Map(HashMap::from([(
                "resource_grants".to_string(),
                ConfigValue::Array(vec![ConfigValue::Map(HashMap::from([
                    ("resource_kind".to_string(), string("mysql")),
                    ("object_ref".to_string(), string("mysql-prod")),
                    ("password".to_string(), string("nope")),
                ]))]),
            )])),
        )]);

        assert!(MysqlSourceRuntimeInputs::from_runtime_manifest_config(&cfg).is_err());
    }

    #[test]
    fn endpoint_resolution_is_fail_closed_and_scope_checked() {
        let cfg = HashMap::from([
            (
                "endpoints".to_string(),
                ConfigValue::Array(vec![ConfigValue::Map(HashMap::from([
                    ("object_ref".to_string(), string("mysql-prod")),
                    ("endpoint_uri".to_string(), string("mysql://db:3306")),
                    (
                        "schema_scopes".to_string(),
                        ConfigValue::Array(vec![string("app_*")]),
                    ),
                    (
                        "table_scopes".to_string(),
                        ConfigValue::Array(vec![string("users")]),
                    ),
                ]))]),
            ),
            (
                "__cnx_runtime".to_string(),
                ConfigValue::Map(HashMap::from([(
                    "resource_grants".to_string(),
                    ConfigValue::Array(vec![ConfigValue::Map(HashMap::from([
                        ("resource_kind".to_string(), string("mysql")),
                        ("object_ref".to_string(), string("mysql-prod")),
                        ("grant_epoch".to_string(), ConfigValue::Int(4)),
                    ]))]),
                )])),
            ),
        ]);
        let config = MysqlSourceRuntimeConfig::from_manifest_config(&cfg).expect("config");

        assert!(
            config
                .endpoint_for("mysql-prod", Some("app_main"), Some("users"))
                .is_ok()
        );
        assert!(
            config
                .endpoint_for("mysql-prod", Some("other"), Some("users"))
                .is_err()
        );
        assert!(
            config
                .endpoint_for("mysql-prod", Some("app_main"), Some("orders"))
                .is_err()
        );
    }
}

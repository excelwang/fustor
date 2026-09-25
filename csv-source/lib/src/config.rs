use std::collections::HashMap;

use capanix_app_sdk::runtime::ConfigValue;
use capanix_app_sdk::{CnxError, Result};
use source_kit::{
    bool_value, effective_limit, get_int, get_map, get_str, normalize_page_size,
    optional_nonempty_str, pattern_allowed, pattern_allowed_by_product_and_grant,
    reject_duplicate_refs, reject_secret_material, require_grant_interface, required_str,
    string_array, validate_grants_match_product_endpoints,
};

pub const DEFAULT_PAGE_SIZE: usize = 1_000;
pub const DEFAULT_MAX_PAGE_SIZE: usize = 10_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CsvSourceRuntimeConfig {
    pub product: CsvSourceConfig,
    pub runtime: CsvSourceRuntimeInputs,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CsvSourceConfig {
    pub endpoints: Vec<CsvEndpointConfig>,
    pub default_page_size: usize,
    pub max_page_size: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CsvEndpointConfig {
    pub object_ref: String,
    pub root_path: String,
    pub path_scopes: Vec<String>,
    pub has_headers: bool,
    pub delimiter: u8,
    pub active: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CsvSourceRuntimeInputs {
    pub endpoint_grants: Vec<GrantedCsvEndpoint>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GrantedCsvEndpoint {
    pub object_ref: String,
    pub path_scopes: Vec<String>,
    pub interfaces: Vec<String>,
    pub active: bool,
    pub grant_epoch: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedCsvEndpoint {
    pub object_ref: String,
    pub root_path: String,
    pub path_scopes: Vec<String>,
    pub has_headers: bool,
    pub delimiter: u8,
    pub grant_epoch: Option<u64>,
}

impl Default for CsvSourceConfig {
    fn default() -> Self {
        Self {
            endpoints: Vec::new(),
            default_page_size: DEFAULT_PAGE_SIZE,
            max_page_size: DEFAULT_MAX_PAGE_SIZE,
        }
    }
}

impl CsvSourceRuntimeConfig {
    pub fn from_manifest_config(cfg: &HashMap<String, ConfigValue>) -> Result<Self> {
        let product = CsvSourceConfig::from_product_manifest_config(cfg)?;
        let runtime = CsvSourceRuntimeInputs::from_runtime_manifest_config(cfg)?;
        validate_runtime_grants_match_product_endpoints(&product, &runtime)?;
        Ok(Self { product, runtime })
    }

    pub fn endpoint_for(
        &self,
        object_ref: &str,
        path: Option<&str>,
    ) -> Result<ResolvedCsvEndpoint> {
        let product = self
            .product
            .endpoints
            .iter()
            .find(|endpoint| endpoint.object_ref == object_ref)
            .ok_or_else(|| {
                CnxError::AccessDenied(format!("unknown csv endpoint object_ref '{object_ref}'"))
            })?;
        if !product.active {
            return Err(CnxError::AccessDenied(format!(
                "csv endpoint '{object_ref}' is inactive in app config"
            )));
        }
        let grant = self
            .runtime
            .endpoint_grants
            .iter()
            .find(|grant| grant.object_ref == object_ref)
            .ok_or_else(|| {
                CnxError::AccessDenied(format!(
                    "missing active runtime grant for csv endpoint '{object_ref}'"
                ))
            })?;
        if !grant.active {
            return Err(CnxError::AccessDenied(format!(
                "runtime grant for csv endpoint '{object_ref}' is inactive"
            )));
        }
        require_grant_interface(&grant.interfaces, "read", "csv endpoint")?;
        if let Some(path) = path
            && !pattern_allowed_by_product_and_grant(path, &product.path_scopes, &grant.path_scopes)
        {
            return Err(CnxError::ScopeDenied(format!(
                "path '{path}' is outside runtime scope for csv endpoint '{object_ref}'"
            )));
        }
        Ok(ResolvedCsvEndpoint {
            object_ref: object_ref.to_string(),
            root_path: product.root_path.clone(),
            path_scopes: effective_scopes(&product.path_scopes, &grant.path_scopes),
            has_headers: product.has_headers,
            delimiter: product.delimiter,
            grant_epoch: grant.grant_epoch,
        })
    }

    pub fn effective_limit(&self, requested: Option<usize>) -> usize {
        effective_limit(
            requested,
            self.product.default_page_size,
            self.product.max_page_size,
        )
    }
}

impl CsvSourceConfig {
    pub fn from_product_manifest_config(cfg: &HashMap<String, ConfigValue>) -> Result<Self> {
        let mut out = Self::default();
        if let Some(value) = get_int(cfg, "default_page_size") {
            out.default_page_size = normalize_page_size(value, "default_page_size")?;
        }
        if let Some(value) = get_int(cfg, "max_page_size") {
            out.max_page_size = normalize_page_size(value, "max_page_size")?;
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
        reject_duplicate_refs(
            out.endpoints
                .iter()
                .map(|endpoint| endpoint.object_ref.as_str()),
            "csv endpoints[].object_ref",
        )?;
        Ok(out)
    }
}

impl CsvSourceRuntimeInputs {
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
                if kind != "csv" && kind != "csv_endpoint" {
                    continue;
                }
                reject_secret_material(row, "__cnx_runtime.resource_grants[]")?;
                endpoint_grants.push(parse_runtime_grant(row)?);
            }
        }
        Ok(Self { endpoint_grants })
    }
}

fn parse_endpoint_config(row: &HashMap<String, ConfigValue>) -> Result<CsvEndpointConfig> {
    reject_secret_material(row, "endpoints[]")?;
    Ok(CsvEndpointConfig {
        object_ref: required_str(row, "object_ref")?,
        root_path: required_str(row, "root_path")?,
        path_scopes: string_array(row, "path_scopes").unwrap_or_else(|| vec!["*".into()]),
        has_headers: bool_value(row, "has_headers", true),
        delimiter: parse_delimiter(optional_nonempty_str(row, "delimiter"))?,
        active: bool_value(row, "active", true),
    })
}

fn parse_runtime_grant(row: &HashMap<String, ConfigValue>) -> Result<GrantedCsvEndpoint> {
    Ok(GrantedCsvEndpoint {
        object_ref: required_str(row, "object_ref")?,
        path_scopes: string_array(row, "path_scopes").unwrap_or_default(),
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

fn validate_runtime_grants_match_product_endpoints(
    product: &CsvSourceConfig,
    runtime: &CsvSourceRuntimeInputs,
) -> Result<()> {
    reject_duplicate_refs(
        runtime
            .endpoint_grants
            .iter()
            .map(|grant| grant.object_ref.as_str()),
        "csv __cnx_runtime.resource_grants[].object_ref",
    )?;
    validate_grants_match_product_endpoints(
        product
            .endpoints
            .iter()
            .map(|endpoint| endpoint.object_ref.as_str()),
        runtime
            .endpoint_grants
            .iter()
            .map(|grant| grant.object_ref.as_str()),
        "csv",
    )?;
    for grant in &runtime.endpoint_grants {
        let endpoint = product
            .endpoints
            .iter()
            .find(|endpoint| endpoint.object_ref == grant.object_ref)
            .ok_or_else(|| {
                CnxError::InvalidInput(format!(
                    "csv grants unknown endpoint_ref '{}'",
                    grant.object_ref
                ))
            })?;
        for scope in &grant.path_scopes {
            if !pattern_allowed(scope, &endpoint.path_scopes) {
                return Err(CnxError::InvalidInput(format!(
                    "csv runtime grant scope '{scope}' widens product path scope for endpoint '{}'",
                    grant.object_ref
                )));
            }
        }
    }
    Ok(())
}

fn parse_delimiter(value: Option<String>) -> Result<u8> {
    let Some(value) = value else {
        return Ok(b',');
    };
    let bytes = value.as_bytes();
    if bytes.len() != 1 {
        return Err(CnxError::InvalidInput(
            "csv delimiter must be a single-byte character".into(),
        ));
    }
    Ok(bytes[0])
}

fn effective_scopes(product_scopes: &[String], grant_scopes: &[String]) -> Vec<String> {
    if grant_scopes.is_empty() {
        product_scopes.to_vec()
    } else {
        grant_scopes.to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn string(value: &str) -> ConfigValue {
        ConfigValue::String(value.to_string())
    }

    #[test]
    fn endpoint_resolution_applies_path_scope_and_delimiter() {
        let cfg = HashMap::from([
            (
                "endpoints".to_string(),
                ConfigValue::Array(vec![ConfigValue::Map(HashMap::from([
                    ("object_ref".to_string(), string("csv")),
                    ("root_path".to_string(), string("/data")),
                    ("delimiter".to_string(), string("|")),
                    (
                        "path_scopes".to_string(),
                        ConfigValue::Array(vec![string("datasets/*")]),
                    ),
                ]))]),
            ),
            (
                "__cnx_runtime".to_string(),
                ConfigValue::Map(HashMap::from([(
                    "resource_grants".to_string(),
                    ConfigValue::Array(vec![ConfigValue::Map(HashMap::from([
                        ("resource_kind".to_string(), string("csv")),
                        ("object_ref".to_string(), string("csv")),
                        (
                            "interfaces".to_string(),
                            ConfigValue::Array(vec![string("read")]),
                        ),
                    ]))]),
                )])),
            ),
        ]);
        let config = CsvSourceRuntimeConfig::from_manifest_config(&cfg).expect("config");
        let endpoint = config
            .endpoint_for("csv", Some("datasets/a.csv"))
            .expect("endpoint");

        assert_eq!(endpoint.delimiter, b'|');
        assert!(config.endpoint_for("csv", Some("secrets/a.csv")).is_err());
        let mut missing_read = config.clone();
        missing_read.runtime.endpoint_grants[0].interfaces.clear();
        assert!(matches!(
            missing_read.endpoint_for("csv", Some("datasets/a.csv")),
            Err(CnxError::AccessDenied(_))
        ));
    }

    #[test]
    fn runtime_grant_cannot_widen_product_scope() {
        let cfg = HashMap::from([
            (
                "endpoints".to_string(),
                ConfigValue::Array(vec![ConfigValue::Map(HashMap::from([
                    ("object_ref".to_string(), string("csv")),
                    ("root_path".to_string(), string("/data")),
                    (
                        "path_scopes".to_string(),
                        ConfigValue::Array(vec![string("datasets/*")]),
                    ),
                ]))]),
            ),
            (
                "__cnx_runtime".to_string(),
                ConfigValue::Map(HashMap::from([(
                    "resource_grants".to_string(),
                    ConfigValue::Array(vec![ConfigValue::Map(HashMap::from([
                        ("resource_kind".to_string(), string("csv")),
                        ("object_ref".to_string(), string("csv")),
                        (
                            "path_scopes".to_string(),
                            ConfigValue::Array(vec![string("*")]),
                        ),
                    ]))]),
                )])),
            ),
        ]);

        let err = CsvSourceRuntimeConfig::from_manifest_config(&cfg).unwrap_err();

        assert!(err.to_string().contains("widens product path scope"));
    }
}

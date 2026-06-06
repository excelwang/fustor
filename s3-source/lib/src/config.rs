use std::collections::HashMap;

use capanix_app_sdk::runtime::ConfigValue;
use capanix_app_sdk::{CnxError, Result};
use source_kit::{
    SourceCredentialConfig, bool_value, effective_limit, get_int, get_map, get_str,
    normalize_page_size, optional_nonempty_str, parse_credential_config, pattern_allowed,
    reject_secret_material, required_str, resolve_credential, string_array,
};

pub const DEFAULT_PAGE_SIZE: usize = 1_000;
pub const DEFAULT_MAX_PAGE_SIZE: usize = 10_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct S3SourceRuntimeConfig {
    pub product: S3SourceConfig,
    pub runtime: S3SourceRuntimeInputs,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct S3SourceConfig {
    pub endpoints: Vec<S3EndpointConfig>,
    pub credentials: Vec<SourceCredentialConfig>,
    pub default_page_size: usize,
    pub max_page_size: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct S3EndpointConfig {
    pub object_ref: String,
    pub endpoint_uri: String,
    pub bucket: String,
    pub region: Option<String>,
    pub prefix: String,
    pub credential_ref: Option<String>,
    pub bucket_scopes: Vec<String>,
    pub prefix_scopes: Vec<String>,
    pub active: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct S3SourceRuntimeInputs {
    pub endpoint_grants: Vec<GrantedS3Endpoint>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GrantedS3Endpoint {
    pub object_ref: String,
    pub endpoint_ref: Option<String>,
    pub endpoint_uri: Option<String>,
    pub bucket: Option<String>,
    pub credential_ref: Option<String>,
    pub bucket_scopes: Vec<String>,
    pub prefix_scopes: Vec<String>,
    pub interfaces: Vec<String>,
    pub active: bool,
    pub grant_epoch: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedS3Endpoint {
    pub object_ref: String,
    pub endpoint_uri: String,
    pub bucket: String,
    pub region: Option<String>,
    pub prefix: String,
    pub credential_ref: Option<String>,
    pub bucket_scopes: Vec<String>,
    pub prefix_scopes: Vec<String>,
    pub grant_epoch: Option<u64>,
}

impl Default for S3SourceConfig {
    fn default() -> Self {
        Self {
            endpoints: Vec::new(),
            credentials: Vec::new(),
            default_page_size: DEFAULT_PAGE_SIZE,
            max_page_size: DEFAULT_MAX_PAGE_SIZE,
        }
    }
}

impl S3SourceRuntimeConfig {
    pub fn from_manifest_config(cfg: &HashMap<String, ConfigValue>) -> Result<Self> {
        Ok(Self {
            product: S3SourceConfig::from_product_manifest_config(cfg)?,
            runtime: S3SourceRuntimeInputs::from_runtime_manifest_config(cfg)?,
        })
    }

    pub fn endpoint_for(
        &self,
        object_ref: &str,
        bucket: Option<&str>,
        prefix: Option<&str>,
    ) -> Result<ResolvedS3Endpoint> {
        let product = self
            .product
            .endpoints
            .iter()
            .find(|endpoint| endpoint.object_ref == object_ref)
            .ok_or_else(|| {
                CnxError::AccessDenied(format!("unknown s3 endpoint object_ref '{object_ref}'"))
            })?;
        if !product.active {
            return Err(CnxError::AccessDenied(format!(
                "s3 endpoint '{object_ref}' is inactive in app config"
            )));
        }
        let grant = self
            .runtime
            .endpoint_grants
            .iter()
            .find(|grant| grant.object_ref == object_ref)
            .ok_or_else(|| {
                CnxError::AccessDenied(format!(
                    "missing active runtime grant for s3 endpoint '{object_ref}'"
                ))
            })?;
        if !grant.active {
            return Err(CnxError::AccessDenied(format!(
                "runtime grant for s3 endpoint '{object_ref}' is inactive"
            )));
        }
        let resolved_bucket = bucket
            .map(str::to_string)
            .or_else(|| grant.bucket.clone())
            .unwrap_or_else(|| product.bucket.clone());
        let resolved_prefix = prefix
            .map(str::to_string)
            .unwrap_or_else(|| product.prefix.clone());
        let bucket_scopes = if grant.bucket_scopes.is_empty() {
            product.bucket_scopes.clone()
        } else {
            grant.bucket_scopes.clone()
        };
        let prefix_scopes = if grant.prefix_scopes.is_empty() {
            product.prefix_scopes.clone()
        } else {
            grant.prefix_scopes.clone()
        };
        if !pattern_allowed(&resolved_bucket, &bucket_scopes) {
            return Err(CnxError::ScopeDenied(format!(
                "bucket '{resolved_bucket}' is outside runtime scope for s3 endpoint '{object_ref}'"
            )));
        }
        if !pattern_allowed(&resolved_prefix, &prefix_scopes) {
            return Err(CnxError::ScopeDenied(format!(
                "prefix '{resolved_prefix}' is outside runtime scope for s3 endpoint '{object_ref}'"
            )));
        }
        Ok(ResolvedS3Endpoint {
            object_ref: object_ref.to_string(),
            endpoint_uri: grant
                .endpoint_uri
                .clone()
                .unwrap_or_else(|| product.endpoint_uri.clone()),
            bucket: resolved_bucket,
            region: product.region.clone(),
            prefix: resolved_prefix,
            credential_ref: grant
                .credential_ref
                .clone()
                .or_else(|| product.credential_ref.clone()),
            bucket_scopes,
            prefix_scopes,
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

impl S3SourceConfig {
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

impl S3SourceRuntimeInputs {
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
                if kind != "s3" && kind != "s3_endpoint" {
                    continue;
                }
                reject_secret_material(row, "__cnx_runtime.resource_grants[]")?;
                endpoint_grants.push(parse_runtime_grant(row)?);
            }
        }
        Ok(Self { endpoint_grants })
    }
}

fn parse_endpoint_config(row: &HashMap<String, ConfigValue>) -> Result<S3EndpointConfig> {
    reject_secret_material(row, "endpoints[]")?;
    Ok(S3EndpointConfig {
        object_ref: required_str(row, "object_ref")?,
        endpoint_uri: required_str(row, "endpoint_uri")?,
        bucket: required_str(row, "bucket")?,
        region: optional_nonempty_str(row, "region"),
        prefix: optional_nonempty_str(row, "prefix").unwrap_or_default(),
        credential_ref: optional_nonempty_str(row, "credential_ref"),
        bucket_scopes: string_array(row, "bucket_scopes").unwrap_or_else(|| vec!["*".into()]),
        prefix_scopes: string_array(row, "prefix_scopes").unwrap_or_else(|| vec!["*".into()]),
        active: bool_value(row, "active", true),
    })
}

fn parse_runtime_grant(row: &HashMap<String, ConfigValue>) -> Result<GrantedS3Endpoint> {
    Ok(GrantedS3Endpoint {
        object_ref: required_str(row, "object_ref")?,
        endpoint_ref: optional_nonempty_str(row, "endpoint_ref"),
        endpoint_uri: optional_nonempty_str(row, "endpoint_uri"),
        bucket: optional_nonempty_str(row, "bucket"),
        credential_ref: optional_nonempty_str(row, "credential_ref"),
        bucket_scopes: string_array(row, "bucket_scopes").unwrap_or_default(),
        prefix_scopes: string_array(row, "prefix_scopes").unwrap_or_default(),
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
                    ("resource_kind".to_string(), string("s3")),
                    ("object_ref".to_string(), string("s3-prod")),
                    ("secret".to_string(), string("nope")),
                ]))]),
            )])),
        )]);

        assert!(S3SourceRuntimeInputs::from_runtime_manifest_config(&cfg).is_err());
    }

    #[test]
    fn endpoint_resolution_applies_bucket_and_prefix_scope() {
        let cfg = HashMap::from([
            (
                "endpoints".to_string(),
                ConfigValue::Array(vec![ConfigValue::Map(HashMap::from([
                    ("object_ref".to_string(), string("s3-prod")),
                    ("endpoint_uri".to_string(), string("https://s3.local")),
                    ("bucket".to_string(), string("logs")),
                    ("prefix".to_string(), string("2026/")),
                ]))]),
            ),
            (
                "__cnx_runtime".to_string(),
                ConfigValue::Map(HashMap::from([(
                    "resource_grants".to_string(),
                    ConfigValue::Array(vec![ConfigValue::Map(HashMap::from([
                        ("resource_kind".to_string(), string("s3")),
                        ("object_ref".to_string(), string("s3-prod")),
                        (
                            "bucket_scopes".to_string(),
                            ConfigValue::Array(vec![string("logs")]),
                        ),
                        (
                            "prefix_scopes".to_string(),
                            ConfigValue::Array(vec![string("2026/*")]),
                        ),
                    ]))]),
                )])),
            ),
        ]);
        let config = S3SourceRuntimeConfig::from_manifest_config(&cfg).expect("config");

        assert!(
            config
                .endpoint_for("s3-prod", Some("logs"), Some("2026/06/"))
                .is_ok()
        );
        assert!(
            config
                .endpoint_for("s3-prod", Some("other"), Some("2026/06/"))
                .is_err()
        );
        assert!(
            config
                .endpoint_for("s3-prod", Some("logs"), Some("2025/"))
                .is_err()
        );
    }
}

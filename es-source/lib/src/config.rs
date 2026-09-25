use std::collections::{HashMap, HashSet};
use std::env;

use capanix_app_sdk::runtime::ConfigValue;
use capanix_app_sdk::{CnxError, Result};
use source_kit::{require_grant_interface, validate_secret_free_endpoint_uri};

const DEFAULT_TIMESTAMP_FIELD: &str = "@timestamp";
const DEFAULT_TIE_BREAKER_FIELD: &str = "_id";
const DEFAULT_PAGE_SIZE: usize = 500;
const DEFAULT_MAX_PAGE_SIZE: usize = 5_000;

const FORBIDDEN_RUNTIME_SECRET_KEYS: &[&str] = &[
    "password",
    "passwd",
    "api_key",
    "apikey",
    "authorization",
    "bearer_token",
    "token",
    "secret",
    "secret_value",
    "access_key",
    "access_key_id",
    "secret_access_key",
    "client_secret",
    "private_key",
];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EsSourceRuntimeConfig {
    pub product: EsSourceConfig,
    pub runtime: EsSourceRuntimeInputs,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EsSourceConfig {
    pub endpoints: Vec<EsEndpointConfig>,
    pub credentials: Vec<EsCredentialConfig>,
    pub default_page_size: usize,
    pub max_page_size: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EsEndpointConfig {
    pub object_ref: String,
    pub endpoint_uri: String,
    pub credential_ref: Option<String>,
    pub index_scopes: Vec<String>,
    pub timestamp_field: String,
    pub tie_breaker_field: String,
    pub active: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EsCredentialConfig {
    pub credential_ref: String,
    pub source: CredentialSource,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CredentialSource {
    None,
    BasicEnv {
        username_env: String,
        password_env: String,
    },
    ApiKeyEnv {
        api_key_env: String,
    },
    BearerEnv {
        token_env: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResolvedCredential {
    None,
    Basic { username: String, password: String },
    ApiKey { api_key: String },
    Bearer { token: String },
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EsSourceRuntimeInputs {
    pub endpoint_grants: Vec<GrantedEsEndpoint>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GrantedEsEndpoint {
    pub object_ref: String,
    pub endpoint_ref: Option<String>,
    pub endpoint_uri: Option<String>,
    pub credential_ref: Option<String>,
    pub index_scopes: Vec<String>,
    pub interfaces: Vec<String>,
    pub active: bool,
    pub grant_epoch: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedEsEndpoint {
    pub object_ref: String,
    pub endpoint_uri: String,
    pub credential_ref: Option<String>,
    pub index_scopes: Vec<String>,
    pub timestamp_field: String,
    pub tie_breaker_field: String,
    pub grant_epoch: Option<u64>,
}

impl Default for EsSourceConfig {
    fn default() -> Self {
        Self {
            endpoints: Vec::new(),
            credentials: Vec::new(),
            default_page_size: DEFAULT_PAGE_SIZE,
            max_page_size: DEFAULT_MAX_PAGE_SIZE,
        }
    }
}

impl EsSourceRuntimeConfig {
    pub fn from_manifest_config(cfg: &HashMap<String, ConfigValue>) -> Result<Self> {
        let product = EsSourceConfig::from_product_manifest_config(cfg)?;
        let runtime = EsSourceRuntimeInputs::from_runtime_manifest_config(cfg)?;
        validate_runtime_grants_match_product_endpoints(&product, &runtime)?;
        Ok(Self { product, runtime })
    }

    pub fn endpoint_for(&self, object_ref: &str, index: &str) -> Result<ResolvedEsEndpoint> {
        let product = self
            .product
            .endpoints
            .iter()
            .find(|endpoint| endpoint.object_ref == object_ref)
            .ok_or_else(|| {
                CnxError::AccessDenied(format!("unknown es endpoint object_ref '{object_ref}'"))
            })?;
        if !product.active {
            return Err(CnxError::AccessDenied(format!(
                "es endpoint '{object_ref}' is inactive in app config"
            )));
        }

        let grant = self
            .runtime
            .endpoint_grants
            .iter()
            .find(|grant| grant.object_ref == object_ref)
            .ok_or_else(|| {
                CnxError::AccessDenied(format!(
                    "missing active runtime grant for es endpoint '{object_ref}'"
                ))
            })?;
        if !grant.active {
            return Err(CnxError::AccessDenied(format!(
                "runtime grant for es endpoint '{object_ref}' is inactive"
            )));
        }
        require_grant_interface(&grant.interfaces, "read", "es endpoint")?;

        if !index_allowed_by_product_and_grant(index, &product.index_scopes, &grant.index_scopes) {
            return Err(CnxError::ScopeDenied(format!(
                "index '{index}' is outside runtime scope for es endpoint '{object_ref}'"
            )));
        }

        Ok(ResolvedEsEndpoint {
            object_ref: object_ref.to_string(),
            endpoint_uri: grant
                .endpoint_uri
                .clone()
                .unwrap_or_else(|| product.endpoint_uri.clone()),
            credential_ref: grant
                .credential_ref
                .clone()
                .or_else(|| product.credential_ref.clone()),
            index_scopes: effective_index_scopes(&product.index_scopes, &grant.index_scopes),
            timestamp_field: product.timestamp_field.clone(),
            tie_breaker_field: product.tie_breaker_field.clone(),
            grant_epoch: grant.grant_epoch,
        })
    }

    pub fn resolve_credential(&self, credential_ref: Option<&str>) -> Result<ResolvedCredential> {
        let Some(credential_ref) = credential_ref else {
            return Ok(ResolvedCredential::None);
        };
        let credential = self
            .product
            .credentials
            .iter()
            .find(|credential| credential.credential_ref == credential_ref)
            .ok_or_else(|| {
                CnxError::AccessDenied(format!(
                    "missing local credential binding '{credential_ref}'"
                ))
            })?;
        credential.resolve_from_env()
    }

    pub fn effective_limit(&self, requested: Option<usize>) -> usize {
        let limit = requested.unwrap_or(self.product.default_page_size).max(1);
        limit.min(self.product.max_page_size.max(1))
    }
}

impl EsSourceConfig {
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
        reject_duplicate_refs(
            out.endpoints
                .iter()
                .map(|endpoint| endpoint.object_ref.as_str()),
            "endpoints[].object_ref",
        )?;
        reject_duplicate_refs(
            out.credentials
                .iter()
                .map(|credential| credential.credential_ref.as_str()),
            "credentials[].credential_ref",
        )?;
        Ok(out)
    }
}

impl EsCredentialConfig {
    pub fn resolve_from_env(&self) -> Result<ResolvedCredential> {
        match &self.source {
            CredentialSource::None => Ok(ResolvedCredential::None),
            CredentialSource::BasicEnv {
                username_env,
                password_env,
            } => Ok(ResolvedCredential::Basic {
                username: read_env(username_env, &self.credential_ref)?,
                password: read_env(password_env, &self.credential_ref)?,
            }),
            CredentialSource::ApiKeyEnv { api_key_env } => Ok(ResolvedCredential::ApiKey {
                api_key: read_env(api_key_env, &self.credential_ref)?,
            }),
            CredentialSource::BearerEnv { token_env } => Ok(ResolvedCredential::Bearer {
                token: read_env(token_env, &self.credential_ref)?,
            }),
        }
    }
}

impl EsSourceRuntimeInputs {
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
                if kind != "elasticsearch" && kind != "es_endpoint" {
                    continue;
                }
                reject_secret_material(row, "__cnx_runtime.resource_grants[]")?;
                endpoint_grants.push(parse_runtime_grant(row)?);
            }
        }
        Ok(Self { endpoint_grants })
    }
}

fn parse_endpoint_config(row: &HashMap<String, ConfigValue>) -> Result<EsEndpointConfig> {
    if contains_forbidden_secret_key(row) {
        return Err(CnxError::InvalidInput(
            "endpoints[] must reference credentials, not contain secret material".into(),
        ));
    }
    let object_ref = required_str(row, "object_ref")?;
    let endpoint_uri = required_str(row, "endpoint_uri")?;
    validate_secret_free_endpoint_uri(&endpoint_uri, "es endpoints[].endpoint_uri")?;
    Ok(EsEndpointConfig {
        object_ref,
        endpoint_uri,
        credential_ref: optional_nonempty_str(row, "credential_ref"),
        index_scopes: string_array(row, "index_scopes").unwrap_or_else(|| vec!["*".into()]),
        timestamp_field: optional_nonempty_str(row, "timestamp_field")
            .unwrap_or_else(|| DEFAULT_TIMESTAMP_FIELD.into()),
        tie_breaker_field: optional_nonempty_str(row, "tie_breaker_field")
            .unwrap_or_else(|| DEFAULT_TIE_BREAKER_FIELD.into()),
        active: bool_value(row, "active", true),
    })
}

fn parse_credential_config(row: &HashMap<String, ConfigValue>) -> Result<EsCredentialConfig> {
    if contains_forbidden_secret_key(row) {
        return Err(CnxError::InvalidInput(
            "credentials[] must contain env var references, not secret material".into(),
        ));
    }
    let credential_ref = required_str(row, "credential_ref")?;
    let auth_type = optional_nonempty_str(row, "auth_type").unwrap_or_else(|| "none".into());
    let source = match auth_type.as_str() {
        "none" => CredentialSource::None,
        "basic_env" => CredentialSource::BasicEnv {
            username_env: required_str(row, "username_env")?,
            password_env: required_str(row, "password_env")?,
        },
        "api_key_env" => CredentialSource::ApiKeyEnv {
            api_key_env: required_str(row, "api_key_env")?,
        },
        "bearer_env" => CredentialSource::BearerEnv {
            token_env: required_str(row, "token_env")?,
        },
        other => {
            return Err(CnxError::InvalidInput(format!(
                "unsupported credentials[].auth_type '{other}'"
            )));
        }
    };
    Ok(EsCredentialConfig {
        credential_ref,
        source,
    })
}

fn parse_runtime_grant(row: &HashMap<String, ConfigValue>) -> Result<GrantedEsEndpoint> {
    let object_ref = required_str(row, "object_ref")?;
    let endpoint_uri = optional_nonempty_str(row, "endpoint_uri");
    if let Some(endpoint_uri) = endpoint_uri.as_deref() {
        validate_secret_free_endpoint_uri(
            endpoint_uri,
            "es __cnx_runtime.resource_grants[].endpoint_uri",
        )?;
    }
    Ok(GrantedEsEndpoint {
        object_ref,
        endpoint_ref: optional_nonempty_str(row, "endpoint_ref"),
        endpoint_uri,
        credential_ref: optional_nonempty_str(row, "credential_ref"),
        index_scopes: string_array(row, "index_scopes").unwrap_or_default(),
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
    product: &EsSourceConfig,
    runtime: &EsSourceRuntimeInputs,
) -> Result<()> {
    reject_duplicate_refs(
        runtime
            .endpoint_grants
            .iter()
            .map(|grant| grant.object_ref.as_str()),
        "__cnx_runtime.resource_grants[].object_ref",
    )?;
    let endpoint_refs = product
        .endpoints
        .iter()
        .map(|endpoint| endpoint.object_ref.as_str())
        .collect::<HashSet<_>>();
    for grant in &runtime.endpoint_grants {
        if !endpoint_refs.contains(grant.object_ref.as_str()) {
            return Err(CnxError::AccessDenied(format!(
                "runtime grant references unknown es endpoint object_ref '{}'",
                grant.object_ref
            )));
        }
        let endpoint = product
            .endpoints
            .iter()
            .find(|endpoint| endpoint.object_ref == grant.object_ref)
            .ok_or_else(|| {
                CnxError::InvalidInput(format!(
                    "runtime grant references unknown es endpoint object_ref '{}'",
                    grant.object_ref
                ))
            })?;
        validate_grant_scopes_within_product(
            &grant.index_scopes,
            &endpoint.index_scopes,
            &grant.object_ref,
        )?;
    }
    Ok(())
}

fn validate_grant_scopes_within_product(
    grant_scopes: &[String],
    product_scopes: &[String],
    object_ref: &str,
) -> Result<()> {
    for scope in grant_scopes {
        if !index_allowed(scope, product_scopes) {
            return Err(CnxError::InvalidInput(format!(
                "es runtime grant scope '{scope}' widens product index scope for endpoint '{object_ref}'"
            )));
        }
    }
    Ok(())
}

fn reject_duplicate_refs<'a>(refs: impl IntoIterator<Item = &'a str>, field: &str) -> Result<()> {
    let mut seen = HashSet::new();
    for item in refs {
        if !seen.insert(item) {
            return Err(CnxError::InvalidInput(format!(
                "duplicate {field} '{item}'"
            )));
        }
    }
    Ok(())
}

fn reject_secret_material(row: &HashMap<String, ConfigValue>, context: &str) -> Result<()> {
    if contains_forbidden_secret_key(row) {
        Err(CnxError::InvalidInput(format!(
            "{context} must not contain ES credential material"
        )))
    } else {
        Ok(())
    }
}

fn contains_forbidden_secret_key(row: &HashMap<String, ConfigValue>) -> bool {
    fn scan_map(row: &HashMap<String, ConfigValue>) -> bool {
        row.iter().any(|(key, value)| {
            let normalized = key.trim().to_ascii_lowercase();
            FORBIDDEN_RUNTIME_SECRET_KEYS
                .iter()
                .any(|forbidden| normalized == *forbidden)
                || scan_value(value)
        })
    }
    fn scan_value(value: &ConfigValue) -> bool {
        match value {
            ConfigValue::Array(items) => items.iter().any(scan_value),
            ConfigValue::Map(row) => scan_map(row),
            _ => false,
        }
    }
    scan_map(row)
}

fn index_allowed(index: &str, scopes: &[String]) -> bool {
    scopes.iter().any(|scope| pattern_matches(scope, index))
}

fn index_allowed_by_product_and_grant(
    index: &str,
    product_scopes: &[String],
    grant_scopes: &[String],
) -> bool {
    index_allowed(index, product_scopes)
        && (grant_scopes.is_empty() || index_allowed(index, grant_scopes))
}

fn effective_index_scopes(product_scopes: &[String], grant_scopes: &[String]) -> Vec<String> {
    if grant_scopes.is_empty() {
        product_scopes.to_vec()
    } else {
        grant_scopes.to_vec()
    }
}

fn pattern_matches(scope: &str, index: &str) -> bool {
    if scope == "*" || scope == index {
        return true;
    }
    match scope.split_once('*') {
        Some((prefix, suffix)) => index.starts_with(prefix) && index.ends_with(suffix),
        None => false,
    }
}

fn read_env(var: &str, credential_ref: &str) -> Result<String> {
    env::var(var).map_err(|_| {
        CnxError::AccessDenied(format!(
            "credential '{credential_ref}' references missing env var '{var}'"
        ))
    })
}

fn normalize_page_size(value: i64, key: &str) -> Result<usize> {
    if value <= 0 {
        return Err(CnxError::InvalidInput(format!("{key} must be positive")));
    }
    usize::try_from(value).map_err(|_| CnxError::InvalidInput(format!("{key} is too large")))
}

fn required_str(row: &HashMap<String, ConfigValue>, key: &str) -> Result<String> {
    optional_nonempty_str(row, key)
        .ok_or_else(|| CnxError::InvalidInput(format!("{key} is required")))
}

fn optional_nonempty_str(row: &HashMap<String, ConfigValue>, key: &str) -> Option<String> {
    match row.get(key) {
        Some(ConfigValue::String(value)) if !value.trim().is_empty() => {
            Some(value.trim().to_string())
        }
        _ => None,
    }
}

fn string_array(row: &HashMap<String, ConfigValue>, key: &str) -> Option<Vec<String>> {
    let ConfigValue::Array(items) = row.get(key)? else {
        return None;
    };
    let values = items
        .iter()
        .filter_map(|item| match item {
            ConfigValue::String(value) if !value.trim().is_empty() => {
                Some(value.trim().to_string())
            }
            _ => None,
        })
        .collect();
    Some(values)
}

fn bool_value(row: &HashMap<String, ConfigValue>, key: &str, default: bool) -> bool {
    match row.get(key) {
        Some(ConfigValue::Bool(value)) => *value,
        _ => default,
    }
}

fn get_int(row: &HashMap<String, ConfigValue>, key: &str) -> Option<i64> {
    match row.get(key) {
        Some(ConfigValue::Int(value)) => Some(*value),
        _ => None,
    }
}

fn get_str<'a>(row: &'a HashMap<String, ConfigValue>, key: &str) -> Option<&'a str> {
    match row.get(key) {
        Some(ConfigValue::String(value)) if !value.trim().is_empty() => Some(value.trim()),
        _ => None,
    }
}

fn get_map<'a>(
    row: &'a HashMap<String, ConfigValue>,
    key: &str,
) -> Option<&'a HashMap<String, ConfigValue>> {
    match row.get(key) {
        Some(ConfigValue::Map(map)) => Some(map),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn string(value: &str) -> ConfigValue {
        ConfigValue::String(value.to_string())
    }

    #[test]
    fn runtime_grants_reject_secret_material() {
        let cfg = HashMap::from([(
            "__cnx_runtime".to_string(),
            ConfigValue::Map(HashMap::from([(
                "resource_grants".to_string(),
                ConfigValue::Array(vec![ConfigValue::Map(HashMap::from([
                    ("resource_kind".to_string(), string("elasticsearch")),
                    ("object_ref".to_string(), string("es-prod")),
                    ("api_key".to_string(), string("not-allowed")),
                ]))]),
            )])),
        )]);

        let err = EsSourceRuntimeInputs::from_runtime_manifest_config(&cfg).unwrap_err();

        assert!(err.to_string().contains("must not contain ES credential"));
    }

    #[test]
    fn endpoint_uri_rejects_embedded_credentials() {
        let cfg = HashMap::from([(
            "endpoints".to_string(),
            ConfigValue::Array(vec![ConfigValue::Map(HashMap::from([
                ("object_ref".to_string(), string("es-prod")),
                (
                    "endpoint_uri".to_string(),
                    string("https://user:secret@es.internal"),
                ),
            ]))]),
        )]);

        assert!(EsSourceConfig::from_product_manifest_config(&cfg).is_err());
    }

    #[test]
    fn resolves_endpoint_from_runtime_grant_without_secret_bytes() {
        let cfg = HashMap::from([
            (
                "endpoints".to_string(),
                ConfigValue::Array(vec![ConfigValue::Map(HashMap::from([
                    ("object_ref".to_string(), string("es-prod")),
                    ("endpoint_uri".to_string(), string("https://es.internal")),
                    ("credential_ref".to_string(), string("prod-es")),
                    (
                        "index_scopes".to_string(),
                        ConfigValue::Array(vec![string("logs-*")]),
                    ),
                ]))]),
            ),
            (
                "__cnx_runtime".to_string(),
                ConfigValue::Map(HashMap::from([(
                    "resource_grants".to_string(),
                    ConfigValue::Array(vec![ConfigValue::Map(HashMap::from([
                        ("resource_kind".to_string(), string("elasticsearch")),
                        ("object_ref".to_string(), string("es-prod")),
                        (
                            "interfaces".to_string(),
                            ConfigValue::Array(vec![string("read")]),
                        ),
                        ("grant_state".to_string(), string("active")),
                        ("grant_epoch".to_string(), ConfigValue::Int(11)),
                        (
                            "index_scopes".to_string(),
                            ConfigValue::Array(vec![string("logs-2026-*")]),
                        ),
                    ]))]),
                )])),
            ),
        ]);
        let config = EsSourceRuntimeConfig::from_manifest_config(&cfg).expect("config");

        let endpoint = config
            .endpoint_for("es-prod", "logs-2026-05")
            .expect("endpoint");

        assert_eq!(endpoint.endpoint_uri, "https://es.internal");
        assert_eq!(endpoint.credential_ref.as_deref(), Some("prod-es"));
        assert_eq!(endpoint.grant_epoch, Some(11));
        assert!(config.endpoint_for("es-prod", "logs-2025-01").is_err());
        let mut missing_read = config.clone();
        missing_read.runtime.endpoint_grants[0].interfaces.clear();
        assert!(matches!(
            missing_read.endpoint_for("es-prod", "logs-2026-05"),
            Err(CnxError::AccessDenied(_))
        ));
    }

    #[test]
    fn duplicate_endpoint_object_refs_fail_closed() {
        let cfg = HashMap::from([(
            "endpoints".to_string(),
            ConfigValue::Array(vec![
                ConfigValue::Map(HashMap::from([
                    ("object_ref".to_string(), string("es-prod")),
                    ("endpoint_uri".to_string(), string("https://es-a.internal")),
                ])),
                ConfigValue::Map(HashMap::from([
                    ("object_ref".to_string(), string("es-prod")),
                    ("endpoint_uri".to_string(), string("https://es-b.internal")),
                ])),
            ]),
        )]);

        let err = EsSourceConfig::from_product_manifest_config(&cfg).unwrap_err();

        assert!(err.to_string().contains("duplicate endpoints[].object_ref"));
    }

    #[test]
    fn duplicate_credential_refs_fail_closed() {
        let cfg = HashMap::from([(
            "credentials".to_string(),
            ConfigValue::Array(vec![
                ConfigValue::Map(HashMap::from([
                    ("credential_ref".to_string(), string("prod-es")),
                    ("auth_type".to_string(), string("none")),
                ])),
                ConfigValue::Map(HashMap::from([
                    ("credential_ref".to_string(), string("prod-es")),
                    ("auth_type".to_string(), string("none")),
                ])),
            ]),
        )]);

        let err = EsSourceConfig::from_product_manifest_config(&cfg).unwrap_err();

        assert!(
            err.to_string()
                .contains("duplicate credentials[].credential_ref")
        );
    }

    #[test]
    fn runtime_grant_to_unknown_endpoint_fails_closed() {
        let cfg = HashMap::from([
            (
                "endpoints".to_string(),
                ConfigValue::Array(vec![ConfigValue::Map(HashMap::from([
                    ("object_ref".to_string(), string("es-prod")),
                    ("endpoint_uri".to_string(), string("https://es.internal")),
                ]))]),
            ),
            (
                "__cnx_runtime".to_string(),
                ConfigValue::Map(HashMap::from([(
                    "resource_grants".to_string(),
                    ConfigValue::Array(vec![ConfigValue::Map(HashMap::from([
                        ("resource_kind".to_string(), string("elasticsearch")),
                        ("object_ref".to_string(), string("es-shadow")),
                    ]))]),
                )])),
            ),
        ]);

        let err = EsSourceRuntimeConfig::from_manifest_config(&cfg).unwrap_err();

        assert!(err.to_string().contains("unknown es endpoint object_ref"));
    }

    #[test]
    fn duplicate_runtime_endpoint_grants_fail_closed() {
        let cfg = HashMap::from([
            (
                "endpoints".to_string(),
                ConfigValue::Array(vec![ConfigValue::Map(HashMap::from([
                    ("object_ref".to_string(), string("es-prod")),
                    ("endpoint_uri".to_string(), string("https://es.internal")),
                ]))]),
            ),
            (
                "__cnx_runtime".to_string(),
                ConfigValue::Map(HashMap::from([(
                    "resource_grants".to_string(),
                    ConfigValue::Array(vec![
                        ConfigValue::Map(HashMap::from([
                            ("resource_kind".to_string(), string("elasticsearch")),
                            ("object_ref".to_string(), string("es-prod")),
                        ])),
                        ConfigValue::Map(HashMap::from([
                            ("resource_kind".to_string(), string("elasticsearch")),
                            ("object_ref".to_string(), string("es-prod")),
                            (
                                "index_scopes".to_string(),
                                ConfigValue::Array(vec![string("logs-*")]),
                            ),
                        ])),
                    ]),
                )])),
            ),
        ]);

        let err = EsSourceRuntimeConfig::from_manifest_config(&cfg).unwrap_err();

        assert!(
            err.to_string()
                .contains("duplicate __cnx_runtime.resource_grants[].object_ref")
        );
    }

    #[test]
    fn missing_runtime_grant_fails_closed() {
        let config = EsSourceRuntimeConfig {
            product: EsSourceConfig {
                endpoints: vec![EsEndpointConfig {
                    object_ref: "es-prod".into(),
                    endpoint_uri: "https://es.internal".into(),
                    credential_ref: None,
                    index_scopes: vec!["*".into()],
                    timestamp_field: DEFAULT_TIMESTAMP_FIELD.into(),
                    tie_breaker_field: DEFAULT_TIE_BREAKER_FIELD.into(),
                    active: true,
                }],
                ..EsSourceConfig::default()
            },
            runtime: EsSourceRuntimeInputs::default(),
        };

        assert!(config.endpoint_for("es-prod", "logs").is_err());
    }

    #[test]
    fn runtime_grant_cannot_widen_product_scope() {
        let cfg = HashMap::from([
            (
                "endpoints".to_string(),
                ConfigValue::Array(vec![ConfigValue::Map(HashMap::from([
                    ("object_ref".to_string(), string("es-prod")),
                    ("endpoint_uri".to_string(), string("https://es.internal")),
                    (
                        "index_scopes".to_string(),
                        ConfigValue::Array(vec![string("logs-*")]),
                    ),
                ]))]),
            ),
            (
                "__cnx_runtime".to_string(),
                ConfigValue::Map(HashMap::from([(
                    "resource_grants".to_string(),
                    ConfigValue::Array(vec![ConfigValue::Map(HashMap::from([
                        ("resource_kind".to_string(), string("elasticsearch")),
                        ("object_ref".to_string(), string("es-prod")),
                        (
                            "index_scopes".to_string(),
                            ConfigValue::Array(vec![string("*")]),
                        ),
                    ]))]),
                )])),
            ),
        ]);
        let err = EsSourceRuntimeConfig::from_manifest_config(&cfg).unwrap_err();

        assert!(err.to_string().contains("widens product index scope"));
    }

    #[test]
    fn credential_config_uses_env_refs_not_secret_values() {
        let err = parse_credential_config(&HashMap::from([
            ("credential_ref".to_string(), string("prod-es")),
            ("auth_type".to_string(), string("api_key_env")),
            ("api_key".to_string(), string("not-allowed")),
        ]))
        .unwrap_err();

        assert!(err.to_string().contains("env var references"));
    }

    #[test]
    fn simple_scope_patterns_match_prefix_and_suffix() {
        assert!(index_allowed("logs-2026-05", &["logs-*".to_string()]));
        assert!(index_allowed(
            "metrics-prod-v1",
            &["metrics-*-v1".to_string()]
        ));
        assert!(!index_allowed(
            "metrics-prod-v2",
            &["metrics-*-v1".to_string()]
        ));
    }
}

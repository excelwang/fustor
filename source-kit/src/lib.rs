use std::collections::HashMap;
use std::env;

use bytes::Bytes;
use capanix_app_sdk::runtime::ConfigValue;
use capanix_app_sdk::{CnxError, Result};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

pub const FORBIDDEN_SECRET_KEYS: &[&str] = &[
    "password",
    "passwd",
    "api_key",
    "apikey",
    "authorization",
    "bearer_token",
    "token",
    "secret",
    "secret_value",
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceErrorPayload {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}

impl SourceErrorPayload {
    pub fn new(code: impl Into<String>, message: impl Into<String>, retryable: bool) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            retryable,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceCredentialConfig {
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

impl SourceCredentialConfig {
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

pub fn encode_msgpack<T: Serialize>(value: &T) -> Result<Bytes> {
    rmp_serde::to_vec_named(value)
        .map(Bytes::from)
        .map_err(|err| CnxError::Internal(format!("encode source msgpack payload: {err}")))
}

pub fn decode_msgpack<T: DeserializeOwned>(payload: &[u8]) -> Result<T> {
    rmp_serde::from_slice(payload)
        .map_err(|err| CnxError::InvalidInput(format!("decode source msgpack payload: {err}")))
}

pub fn parse_credential_config(
    row: &HashMap<String, ConfigValue>,
) -> Result<SourceCredentialConfig> {
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
    Ok(SourceCredentialConfig {
        credential_ref,
        source,
    })
}

pub fn resolve_credential(
    credentials: &[SourceCredentialConfig],
    credential_ref: Option<&str>,
) -> Result<ResolvedCredential> {
    let Some(credential_ref) = credential_ref else {
        return Ok(ResolvedCredential::None);
    };
    let credential = credentials
        .iter()
        .find(|credential| credential.credential_ref == credential_ref)
        .ok_or_else(|| {
            CnxError::AccessDenied(format!(
                "missing local credential binding '{credential_ref}'"
            ))
        })?;
    credential.resolve_from_env()
}

pub fn contains_forbidden_secret_key(row: &HashMap<String, ConfigValue>) -> bool {
    fn scan_map(row: &HashMap<String, ConfigValue>) -> bool {
        row.iter().any(|(key, value)| {
            let normalized = key.trim().to_ascii_lowercase();
            FORBIDDEN_SECRET_KEYS
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

pub fn reject_secret_material(row: &HashMap<String, ConfigValue>, context: &str) -> Result<()> {
    if contains_forbidden_secret_key(row) {
        Err(CnxError::InvalidInput(format!(
            "{context} must not contain credential material"
        )))
    } else {
        Ok(())
    }
}

pub fn pattern_allowed(value: &str, scopes: &[String]) -> bool {
    scopes.iter().any(|scope| pattern_matches(scope, value))
}

pub fn pattern_matches(scope: &str, value: &str) -> bool {
    if scope == "*" || scope == value {
        return true;
    }
    match scope.split_once('*') {
        Some((prefix, suffix)) => value.starts_with(prefix) && value.ends_with(suffix),
        None => false,
    }
}

pub fn effective_limit(
    requested: Option<usize>,
    default_page_size: usize,
    max_page_size: usize,
) -> usize {
    let limit = requested.unwrap_or(default_page_size).max(1);
    limit.min(max_page_size.max(1))
}

pub fn normalize_page_size(value: i64, key: &str) -> Result<usize> {
    if value <= 0 {
        return Err(CnxError::InvalidInput(format!("{key} must be positive")));
    }
    usize::try_from(value).map_err(|_| CnxError::InvalidInput(format!("{key} is too large")))
}

pub fn required_str(row: &HashMap<String, ConfigValue>, key: &str) -> Result<String> {
    optional_nonempty_str(row, key)
        .ok_or_else(|| CnxError::InvalidInput(format!("{key} is required")))
}

pub fn optional_nonempty_str(row: &HashMap<String, ConfigValue>, key: &str) -> Option<String> {
    match row.get(key) {
        Some(ConfigValue::String(value)) if !value.trim().is_empty() => {
            Some(value.trim().to_string())
        }
        _ => None,
    }
}

pub fn string_array(row: &HashMap<String, ConfigValue>, key: &str) -> Option<Vec<String>> {
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

pub fn bool_value(row: &HashMap<String, ConfigValue>, key: &str, default: bool) -> bool {
    match row.get(key) {
        Some(ConfigValue::Bool(value)) => *value,
        _ => default,
    }
}

pub fn get_int(row: &HashMap<String, ConfigValue>, key: &str) -> Option<i64> {
    match row.get(key) {
        Some(ConfigValue::Int(value)) => Some(*value),
        _ => None,
    }
}

pub fn get_str<'a>(row: &'a HashMap<String, ConfigValue>, key: &str) -> Option<&'a str> {
    match row.get(key) {
        Some(ConfigValue::String(value)) if !value.trim().is_empty() => Some(value.trim()),
        _ => None,
    }
}

pub fn get_map<'a>(
    row: &'a HashMap<String, ConfigValue>,
    key: &str,
) -> Option<&'a HashMap<String, ConfigValue>> {
    match row.get(key) {
        Some(ConfigValue::Map(map)) => Some(map),
        _ => None,
    }
}

pub fn error_payload_from_cnx(err: CnxError, transport_code: &'static str) -> SourceErrorPayload {
    let (code, retryable) = match &err {
        CnxError::AccessDenied(_) => ("access-denied", false),
        CnxError::ScopeDenied(_) => ("scope-denied", false),
        CnxError::InvalidInput(_) | CnxError::ProtocolViolation(_) => ("invalid-request", false),
        CnxError::Timeout
        | CnxError::Backpressure
        | CnxError::ChannelClosed
        | CnxError::NotReady(_) => ("transient-runtime", true),
        CnxError::LinkError(_) | CnxError::TransportClosed(_) => (transport_code, true),
        CnxError::PeerError(_) => ("source-peer", false),
        _ => ("internal", true),
    };
    SourceErrorPayload::new(code, err.to_string(), retryable)
}

fn read_env(var: &str, credential_ref: &str) -> Result<String> {
    env::var(var).map_err(|_| {
        CnxError::AccessDenied(format!(
            "credential '{credential_ref}' references missing env var '{var}'"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn string(value: &str) -> ConfigValue {
        ConfigValue::String(value.to_string())
    }

    #[test]
    fn secret_material_detection_scans_nested_maps() {
        let row = HashMap::from([(
            "nested".to_string(),
            ConfigValue::Map(HashMap::from([("api_key".to_string(), string("secret"))])),
        )]);

        assert!(contains_forbidden_secret_key(&row));
    }

    #[test]
    fn pattern_scope_supports_single_wildcard() {
        assert!(pattern_allowed("logs-2026-05", &["logs-*".to_string()]));
        assert!(pattern_allowed("a-middle-z", &["a-*-z".to_string()]));
        assert!(!pattern_allowed("a-middle-y", &["a-*-z".to_string()]));
    }

    #[test]
    fn msgpack_roundtrip_works_for_error_payload() {
        let payload = SourceErrorPayload::new("invalid-request", "bad", false);
        let encoded = encode_msgpack(&payload).expect("encode");
        let restored: SourceErrorPayload = decode_msgpack(&encoded).expect("decode");

        assert_eq!(restored, payload);
    }
}

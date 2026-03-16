use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::config::ApiAuthConfig;
use super::errors::ApiError;
use super::types::{QueryApiKeySummary, SessionUser};

#[derive(Clone, Debug)]
struct UserRecord {
    username: String,
    uid: u32,
    gid: u32,
    groups: Vec<String>,
    home: String,
    shell: String,
    locked: bool,
    disabled: bool,
    password_hash: String,
}

#[derive(Clone, Debug)]
pub struct SessionPrincipal {
    pub user: SessionUser,
}

#[derive(Clone, Debug)]
pub struct QueryApiKeyPrincipal {
    pub key: QueryApiKeySummary,
}

#[derive(Clone, Debug)]
struct SessionRecord {
    principal: SessionPrincipal,
    expires_at: Instant,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct QueryApiKeyRecord {
    key_id: String,
    label: String,
    created_at_us: u64,
    api_key_hash: String,
}

#[derive(Default, Serialize, Deserialize)]
struct StoredQueryApiKeys {
    #[serde(default)]
    keys: Vec<QueryApiKeyRecord>,
}

#[derive(Clone)]
pub struct AuthService {
    cfg: ApiAuthConfig,
    users: Arc<Mutex<HashMap<String, UserRecord>>>,
    sessions: Arc<Mutex<HashMap<String, SessionRecord>>>,
    query_keys: Arc<Mutex<HashMap<String, QueryApiKeyRecord>>>,
}

impl AuthService {
    pub fn new(cfg: ApiAuthConfig) -> Result<Self, ApiError> {
        let service = Self {
            cfg,
            users: Arc::new(Mutex::new(HashMap::new())),
            sessions: Arc::new(Mutex::new(HashMap::new())),
            query_keys: Arc::new(Mutex::new(HashMap::new())),
        };
        service.reload_users()?;
        service.reload_query_api_keys()?;
        Ok(service)
    }

    pub fn reload_users(&self) -> Result<(), ApiError> {
        let passwd = std::fs::read_to_string(&self.cfg.passwd_path).map_err(|e| {
            ApiError::internal(format!(
                "failed to read passwd file {}: {e}",
                self.cfg.passwd_path.display()
            ))
        })?;
        let shadow = std::fs::read_to_string(&self.cfg.shadow_path).map_err(|e| {
            ApiError::internal(format!(
                "failed to read shadow file {}: {e}",
                self.cfg.shadow_path.display()
            ))
        })?;

        let mut users = parse_passwd(&passwd)?;
        let shadow_map = parse_shadow(&shadow)?;
        for (username, (password_hash, disabled)) in shadow_map {
            if let Some(user) = users.get_mut(&username) {
                user.password_hash = password_hash;
                user.disabled = disabled;
            }
        }

        users.retain(|_, user| !user.password_hash.is_empty());

        let mut guard = lock_or_recover(&self.users, "api.auth.users");
        *guard = users;
        Ok(())
    }

    pub fn reload_query_api_keys(&self) -> Result<(), ApiError> {
        let content = std::fs::read_to_string(&self.cfg.query_keys_path).map_err(|e| {
            ApiError::internal(format!(
                "failed to read query key file {}: {e}",
                self.cfg.query_keys_path.display()
            ))
        })?;
        let stored: StoredQueryApiKeys = serde_json::from_str(&content).map_err(|e| {
            ApiError::internal(format!(
                "failed to parse query key file {}: {e}",
                self.cfg.query_keys_path.display()
            ))
        })?;
        let mut keys = HashMap::new();
        for row in stored.keys {
            keys.insert(row.key_id.clone(), row);
        }
        let mut guard = lock_or_recover(&self.query_keys, "api.auth.query_keys");
        *guard = keys;
        Ok(())
    }

    pub fn login(
        &self,
        username: &str,
        password: &str,
    ) -> Result<(String, u64, SessionUser), ApiError> {
        self.reload_users()?;
        let users = lock_or_recover(&self.users, "api.auth.users.read");
        let user = users
            .get(username)
            .ok_or_else(|| ApiError::unauthorized("invalid username or password"))?;
        if user.locked || user.disabled {
            return Err(ApiError::forbidden("user is locked or disabled"));
        }

        verify_password(&user.password_hash, password)
            .map_err(|_| ApiError::unauthorized("invalid username or password"))?;

        let principal = SessionPrincipal {
            user: SessionUser {
                username: user.username.clone(),
                uid: user.uid,
                gid: user.gid,
                groups: user.groups.clone(),
                home: user.home.clone(),
                shell: user.shell.clone(),
            },
        };

        drop(users);

        let token = generate_token("fsmeta-session");
        let expires_in = self.cfg.session_ttl_secs;
        let session = SessionRecord {
            principal: principal.clone(),
            expires_at: Instant::now() + Duration::from_secs(expires_in),
        };

        let mut sessions = lock_or_recover(&self.sessions, "api.auth.sessions");
        sessions.insert(token.clone(), session);

        Ok((token, expires_in, principal.user))
    }

    pub fn logout(&self, token: &str) {
        let mut sessions = lock_or_recover(&self.sessions, "api.auth.sessions.logout");
        sessions.remove(token);
    }

    pub fn authorize_management_session(
        &self,
        auth_header: Option<&str>,
    ) -> Result<(String, SessionPrincipal), ApiError> {
        let token = extract_bearer(auth_header)
            .ok_or_else(|| ApiError::unauthorized("missing bearer token"))?;

        let mut sessions = lock_or_recover(&self.sessions, "api.auth.sessions.authorize");
        let Some(session) = sessions.get(&token).cloned() else {
            return Err(ApiError::unauthorized("invalid session token"));
        };
        if session.expires_at <= Instant::now() {
            sessions.remove(&token);
            return Err(ApiError::unauthorized("session expired"));
        }

        if !is_management_user(&session.principal.user.groups, &self.cfg.management_group) {
            return Err(ApiError::forbidden("management access denied"));
        }

        Ok((token, session.principal))
    }

    pub fn authorize_query_api_key(
        &self,
        auth_header: Option<&str>,
    ) -> Result<QueryApiKeyPrincipal, ApiError> {
        let token = extract_bearer(auth_header)
            .ok_or_else(|| ApiError::unauthorized("missing bearer token"))?;
        self.reload_query_api_keys()?;
        let token_hash = hash_token(&token);
        let keys = lock_or_recover(&self.query_keys, "api.auth.query_keys.authorize");
        let Some(record) = keys.values().find(|row| row.api_key_hash == token_hash) else {
            return Err(ApiError::unauthorized("invalid query api key"));
        };
        Ok(QueryApiKeyPrincipal {
            key: QueryApiKeySummary {
                key_id: record.key_id.clone(),
                label: record.label.clone(),
                created_at_us: record.created_at_us,
            },
        })
    }

    pub fn list_query_api_keys(&self) -> Result<Vec<QueryApiKeySummary>, ApiError> {
        self.reload_query_api_keys()?;
        let keys = lock_or_recover(&self.query_keys, "api.auth.query_keys.list");
        let mut rows = keys
            .values()
            .map(|row| QueryApiKeySummary {
                key_id: row.key_id.clone(),
                label: row.label.clone(),
                created_at_us: row.created_at_us,
            })
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| {
            right
                .created_at_us
                .cmp(&left.created_at_us)
                .then_with(|| left.key_id.cmp(&right.key_id))
        });
        Ok(rows)
    }

    pub fn create_query_api_key(
        &self,
        label: &str,
    ) -> Result<(String, QueryApiKeySummary), ApiError> {
        let label = label.trim();
        if label.is_empty() {
            return Err(ApiError::bad_request(
                "query api key label must not be empty",
            ));
        }
        self.reload_query_api_keys()?;
        let api_key = generate_token("fsmeta-query");
        let summary = QueryApiKeySummary {
            key_id: generate_token("fsmeta-query-key"),
            label: label.to_string(),
            created_at_us: now_us(),
        };
        let record = QueryApiKeyRecord {
            key_id: summary.key_id.clone(),
            label: summary.label.clone(),
            created_at_us: summary.created_at_us,
            api_key_hash: hash_token(&api_key),
        };
        let mut keys = lock_or_recover(&self.query_keys, "api.auth.query_keys.create");
        keys.insert(summary.key_id.clone(), record);
        persist_query_keys(&self.cfg, &keys)?;
        Ok((api_key, summary))
    }

    pub fn revoke_query_api_key(&self, key_id: &str) -> Result<bool, ApiError> {
        self.reload_query_api_keys()?;
        let mut keys = lock_or_recover(&self.query_keys, "api.auth.query_keys.revoke");
        let removed = keys.remove(key_id).is_some();
        if removed {
            persist_query_keys(&self.cfg, &keys)?;
        }
        Ok(removed)
    }
}

fn persist_query_keys(
    cfg: &ApiAuthConfig,
    keys: &HashMap<String, QueryApiKeyRecord>,
) -> Result<(), ApiError> {
    let mut rows = keys.values().cloned().collect::<Vec<_>>();
    rows.sort_by(|left, right| left.key_id.cmp(&right.key_id));
    let content =
        serde_json::to_string_pretty(&StoredQueryApiKeys { keys: rows }).map_err(|e| {
            ApiError::internal(format!(
                "failed to encode query key file {}: {e}",
                cfg.query_keys_path.display()
            ))
        })?;
    std::fs::write(&cfg.query_keys_path, format!("{content}\n")).map_err(|e| {
        ApiError::internal(format!(
            "failed to write query key file {}: {e}",
            cfg.query_keys_path.display()
        ))
    })
}

fn extract_bearer(auth_header: Option<&str>) -> Option<String> {
    let value = auth_header?.trim();
    let prefix = "Bearer ";
    if !value.starts_with(prefix) {
        return None;
    }
    let token = value[prefix.len()..].trim();
    if token.is_empty() {
        return None;
    }
    Some(token.to_string())
}

fn verify_password(hash: &str, password: &str) -> Result<(), String> {
    if let Some(expected_plain) = hash.strip_prefix("plain$") {
        if password == expected_plain {
            return Ok(());
        }
        return Err("password verification failed".to_string());
    }
    if password == hash {
        return Ok(());
    }
    Err("password verification failed".to_string())
}

fn parse_bool_flag(raw: &str) -> bool {
    matches!(
        raw.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on" | "locked" | "disabled"
    )
}

fn parse_groups(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn parse_passwd(content: &str) -> Result<HashMap<String, UserRecord>, ApiError> {
    let mut users = HashMap::new();
    for (idx, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let parts: Vec<&str> = line.split(':').collect();
        if parts.len() != 7 {
            return Err(ApiError::bad_request(format!(
                "passwd line {} must contain 7 colon-separated fields",
                idx + 1
            )));
        }
        let username = parts[0].trim();
        if username.is_empty() {
            return Err(ApiError::bad_request(format!(
                "passwd line {} has empty username",
                idx + 1
            )));
        }
        let uid = parts[1].trim().parse::<u32>().map_err(|e| {
            ApiError::bad_request(format!("passwd line {} invalid uid: {e}", idx + 1))
        })?;
        let gid = parts[2].trim().parse::<u32>().map_err(|e| {
            ApiError::bad_request(format!("passwd line {} invalid gid: {e}", idx + 1))
        })?;
        users.insert(
            username.to_string(),
            UserRecord {
                username: username.to_string(),
                uid,
                gid,
                groups: parse_groups(parts[3]),
                home: parts[4].trim().to_string(),
                shell: parts[5].trim().to_string(),
                locked: parse_bool_flag(parts[6]),
                disabled: false,
                password_hash: String::new(),
            },
        );
    }
    Ok(users)
}

fn parse_shadow(content: &str) -> Result<HashMap<String, (String, bool)>, ApiError> {
    let mut map = HashMap::new();
    for (idx, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let parts: Vec<&str> = line.split(':').collect();
        if parts.len() != 3 {
            return Err(ApiError::bad_request(format!(
                "shadow line {} must contain 3 colon-separated fields",
                idx + 1
            )));
        }
        let username = parts[0].trim();
        if username.is_empty() {
            return Err(ApiError::bad_request(format!(
                "shadow line {} has empty username",
                idx + 1
            )));
        }
        let hash = parts[1].trim().to_string();
        if hash.is_empty() {
            return Err(ApiError::bad_request(format!(
                "shadow line {} has empty hash",
                idx + 1
            )));
        }
        map.insert(username.to_string(), (hash, parse_bool_flag(parts[2])));
    }
    Ok(map)
}

fn is_management_user(groups: &[String], management_group: &str) -> bool {
    groups.iter().any(|group| group == management_group)
}

fn hash_token(token: &str) -> String {
    let digest = Sha256::digest(token.as_bytes());
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;

        let _ = write!(out, "{byte:02x}");
    }
    out
}

fn now_us() -> u64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => duration.as_micros() as u64,
        Err(_) => 0,
    }
}

fn lock_or_recover<'a, T>(m: &'a Mutex<T>, context: &str) -> std::sync::MutexGuard<'a, T> {
    match m.lock() {
        Ok(g) => g,
        Err(poisoned) => {
            log::warn!("{context}: mutex poisoned; recovering");
            poisoned.into_inner()
        }
    }
}

fn generate_token(prefix: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let now_us = now_us();
    format!("{prefix}-{pid:x}-{now_us:x}-{seq:x}")
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn parse_passwd_accepts_management_group_lines() {
        let content = "alice:1000:1000:fsmeta_management:/home/alice:/bin/bash:0";
        let users = parse_passwd(content).expect("passwd parse");
        let alice = users.get("alice").expect("alice");
        assert_eq!(alice.uid, 1000);
        assert!(alice.groups.iter().any(|g| g == "fsmeta_management"));
    }

    #[test]
    fn parse_shadow_requires_hash() {
        let err = parse_shadow("alice::0").expect_err("must fail");
        assert_eq!(err.status, axum::http::StatusCode::BAD_REQUEST);
    }

    #[test]
    fn verify_password_works_for_plain_format() {
        let hash = "plain$secret".to_string();
        verify_password(&hash, "secret").expect("verify");
    }

    #[test]
    fn query_key_round_trip_works() {
        let temp = TempDir::new().expect("tempdir");
        let cfg = ApiAuthConfig {
            passwd_path: temp.path().join("passwd"),
            shadow_path: temp.path().join("shadow"),
            query_keys_path: temp.path().join("query-keys.json"),
            bootstrap_management: Some(super::super::config::BootstrapManagementConfig {
                username: "operator".to_string(),
                password: "secret".to_string(),
                uid: 1000,
                gid: 1000,
                home: "/home/operator".to_string(),
                shell: "/bin/bash".to_string(),
            }),
            ..ApiAuthConfig::default()
        };
        cfg.ensure_materialized().expect("materialize");
        let auth = AuthService::new(cfg).expect("auth");

        let (api_key, summary) = auth.create_query_api_key("ui panel").expect("create");
        let principal = auth
            .authorize_query_api_key(Some(&format!("Bearer {api_key}")))
            .expect("authorize");
        assert_eq!(principal.key.key_id, summary.key_id);
        assert!(auth.revoke_query_api_key(&summary.key_id).expect("revoke"));
        let err = auth
            .authorize_query_api_key(Some(&format!("Bearer {api_key}")))
            .expect_err("revoked key must fail");
        assert_eq!(err.status, axum::http::StatusCode::UNAUTHORIZED);
    }
}

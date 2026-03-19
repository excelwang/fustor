#![cfg(target_os = "linux")]
#![allow(dead_code)]

use reqwest::blocking::{Client, Response};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde_json::{json, Value};
use std::time::Duration;

const RECONNECT_RETRY_WINDOW: Duration = Duration::from_secs(30);
const RECONNECT_RETRY_INTERVAL: Duration = Duration::from_millis(250);
const HTTP_TIMEOUT_DEFAULT: Duration = Duration::from_secs(45);
const HTTP_TIMEOUT_FORCE_FIND: Duration = Duration::from_secs(75);

#[derive(Debug, Clone)]
pub struct ApiResponse {
    pub status: u16,
    pub body: Value,
}

#[derive(Clone)]
pub struct FsMetaApiClient {
    base_url: String,
    client: Client,
}

pub struct OperatorSession {
    client: FsMetaApiClient,
    candidate_base_urls: Vec<String>,
    username: String,
    password: String,
    management_token: String,
    query_api_key: String,
}

impl FsMetaApiClient {
    pub fn new(base_url: impl Into<String>) -> Result<Self, String> {
        let client = Client::builder()
            .pool_max_idle_per_host(0)
            .build()
            .map_err(|e| format!("build reqwest client failed: {e}"))?;
        Ok(Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            client,
        })
    }

    pub fn login(&self, username: &str, password: &str) -> Result<Value, String> {
        self.ensure_success(self.login_raw(username, password)?)
    }

    pub fn with_base_url(&self, base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            client: self.client.clone(),
        }
    }

    pub fn login_raw(&self, username: &str, password: &str) -> Result<ApiResponse, String> {
        self.post_json_noauth_raw(
            "/session/login",
            &json!({
                "username": username,
                "password": password,
            }),
        )
    }

    pub fn status(&self, token: &str) -> Result<Value, String> {
        self.ensure_success(self.get_json_raw("/status", token)?)
    }

    pub fn runtime_grants(&self, token: &str) -> Result<Value, String> {
        self.ensure_success(self.get_json_raw("/runtime/grants", token)?)
    }

    pub fn monitoring_roots(&self, token: &str) -> Result<Value, String> {
        self.ensure_success(self.get_json_raw("/monitoring/roots", token)?)
    }

    pub fn preview_roots(&self, token: &str, roots: &Value) -> Result<Value, String> {
        self.ensure_success(self.preview_roots_raw(token, roots)?)
    }

    pub fn preview_roots_raw(&self, token: &str, roots: &Value) -> Result<ApiResponse, String> {
        self.post_json_raw(
            "/monitoring/roots/preview",
            token,
            &json!({ "roots": roots }),
        )
    }

    pub fn update_roots(&self, token: &str, roots: &Value) -> Result<Value, String> {
        self.ensure_success(self.update_roots_raw(token, roots)?)
    }

    pub fn update_roots_raw(&self, token: &str, roots: &Value) -> Result<ApiResponse, String> {
        self.put_json_raw("/monitoring/roots", token, &json!({ "roots": roots }))
    }

    pub fn rescan(&self, token: &str) -> Result<Value, String> {
        self.ensure_success(self.rescan_raw(token)?)
    }

    pub fn rescan_raw(&self, token: &str) -> Result<ApiResponse, String> {
        self.post_json_raw("/index/rescan", token, &json!({}))
    }

    pub fn create_query_api_key(&self, token: &str, label: &str) -> Result<Value, String> {
        self.ensure_success(self.create_query_api_key_raw(token, label)?)
    }

    pub fn create_query_api_key_raw(
        &self,
        token: &str,
        label: &str,
    ) -> Result<ApiResponse, String> {
        self.post_json_raw("/query-api-keys", token, &json!({ "label": label }))
    }

    pub fn stats(&self, token: &str, query: &[(&str, String)]) -> Result<Value, String> {
        self.ensure_success(self.stats_raw(token, query)?)
    }

    pub fn stats_raw(&self, token: &str, query: &[(&str, String)]) -> Result<ApiResponse, String> {
        self.get_json_query_raw("/stats", token, query)
    }

    pub fn bound_route_metrics(&self, token: &str) -> Result<Value, String> {
        self.ensure_success(self.get_json_raw("/bound-route-metrics", token)?)
    }

    pub fn tree(&self, token: &str, query: &[(&str, String)]) -> Result<Value, String> {
        self.ensure_success(self.tree_raw(token, query)?)
    }

    pub fn tree_raw(&self, token: &str, query: &[(&str, String)]) -> Result<ApiResponse, String> {
        self.get_json_query_raw("/tree", token, query)
    }

    pub fn force_find(&self, token: &str, query: &[(&str, String)]) -> Result<Value, String> {
        self.ensure_success(self.force_find_raw(token, query)?)
    }

    pub fn force_find_raw(
        &self,
        token: &str,
        query: &[(&str, String)],
    ) -> Result<ApiResponse, String> {
        self.get_json_query_raw("/on-demand-force-find", token, query)
    }

    pub fn get_json(&self, path: &str, token: &str) -> Result<Value, String> {
        self.ensure_success(self.get_json_raw(path, token)?)
    }

    pub fn get_json_raw(&self, path: &str, token: &str) -> Result<ApiResponse, String> {
        self.get_json_query_raw(path, token, &[])
    }

    pub fn get_json_query(
        &self,
        path: &str,
        token: &str,
        query: &[(&str, String)],
    ) -> Result<Value, String> {
        self.ensure_success(self.get_json_query_raw(path, token, query)?)
    }

    pub fn get_json_query_raw(
        &self,
        path: &str,
        token: &str,
        query: &[(&str, String)],
    ) -> Result<ApiResponse, String> {
        let mut req = self
            .client
            .get(self.url(path))
            .timeout(self.request_timeout(path))
            .headers(self.auth_headers(token)?);
        for (key, value) in query {
            req = req.query(&[(key, value)]);
        }
        self.decode(
            req.send()
                .map_err(|e| format!("GET {} failed: {e}", path))?,
        )
    }

    pub fn post_json(&self, path: &str, token: &str, body: &Value) -> Result<Value, String> {
        self.ensure_success(self.post_json_raw(path, token, body)?)
    }

    pub fn post_json_raw(
        &self,
        path: &str,
        token: &str,
        body: &Value,
    ) -> Result<ApiResponse, String> {
        self.decode(
            self.client
                .post(self.url(path))
                .timeout(self.request_timeout(path))
                .headers(self.auth_headers(token)?)
                .json(body)
                .send()
                .map_err(|e| format!("POST {} failed: {e}", path))?,
        )
    }

    pub fn post_json_noauth(&self, path: &str, body: &Value) -> Result<Value, String> {
        self.ensure_success(self.post_json_noauth_raw(path, body)?)
    }

    pub fn post_json_noauth_raw(&self, path: &str, body: &Value) -> Result<ApiResponse, String> {
        self.decode(
            self.client
                .post(self.url(path))
                .timeout(self.request_timeout(path))
                .header(CONTENT_TYPE, "application/json")
                .json(body)
                .send()
                .map_err(|e| format!("POST {} failed: {e}", path))?,
        )
    }

    pub fn put_json(&self, path: &str, token: &str, body: &Value) -> Result<Value, String> {
        self.ensure_success(self.put_json_raw(path, token, body)?)
    }

    pub fn put_json_raw(
        &self,
        path: &str,
        token: &str,
        body: &Value,
    ) -> Result<ApiResponse, String> {
        self.decode(
            self.client
                .put(self.url(path))
                .timeout(self.request_timeout(path))
                .headers(self.auth_headers(token)?)
                .json(body)
                .send()
                .map_err(|e| format!("PUT {} failed: {e}", path))?,
        )
    }

    fn url(&self, path: &str) -> String {
        format!(
            "{}/api/fs-meta/v1{}",
            self.base_url,
            if path.starts_with('/') {
                path.to_string()
            } else {
                format!("/{path}")
            }
        )
    }

    fn request_timeout(&self, path: &str) -> Duration {
        match path {
            "/on-demand-force-find" => HTTP_TIMEOUT_FORCE_FIND,
            _ => HTTP_TIMEOUT_DEFAULT,
        }
    }

    fn auth_headers(&self, token: &str) -> Result<HeaderMap, String> {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let value = HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|e| format!("build auth header failed: {e}"))?;
        headers.insert(AUTHORIZATION, value);
        Ok(headers)
    }

    fn decode(&self, response: Response) -> Result<ApiResponse, String> {
        let status = response.status().as_u16();
        let text = response
            .text()
            .map_err(|e| format!("read response failed: {e}"))?;
        let body = if text.trim().is_empty() {
            json!({})
        } else {
            serde_json::from_str::<Value>(&text)
                .map_err(|e| format!("parse response body failed: {e}; body={text}"))?
        };
        Ok(ApiResponse { status, body })
    }

    fn ensure_success(&self, response: ApiResponse) -> Result<Value, String> {
        if !(200..300).contains(&response.status) {
            return Err(format!(
                "http {} failed: {}",
                response.status, response.body
            ));
        }
        Ok(response.body)
    }
}

impl OperatorSession {
    pub fn login_many(
        base_urls: Vec<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Result<Self, String> {
        let username = username.into();
        let password = password.into();
        let candidate_base_urls = dedupe_base_urls(base_urls);
        if candidate_base_urls.is_empty() {
            return Err("no facade base URLs provided".into());
        }
        let (client, token) = login_first_available(&candidate_base_urls, &username, &password)?;
        let query_api_key = provision_query_api_key(&client, &token, &username)?;
        Ok(Self {
            client,
            candidate_base_urls,
            username,
            password,
            management_token: token,
            query_api_key,
        })
    }

    pub fn login(
        client: FsMetaApiClient,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Result<Self, String> {
        let username = username.into();
        let password = password.into();
        let primary_base_url = client.base_url.clone();
        let token = extract_token(client.login(&username, &password)?)?;
        let query_api_key = provision_query_api_key(&client, &token, &username)?;
        Ok(Self {
            client,
            candidate_base_urls: vec![primary_base_url],
            username,
            password,
            management_token: token,
            query_api_key,
        })
    }

    pub fn client(&self) -> &FsMetaApiClient {
        &self.client
    }

    pub fn token(&self) -> &str {
        &self.management_token
    }

    pub fn query_api_key(&self) -> &str {
        &self.query_api_key
    }

    pub fn relogin(&mut self) -> Result<(), String> {
        self.management_token = extract_token(self.client.login(&self.username, &self.password)?)?;
        self.query_api_key =
            provision_query_api_key(&self.client, &self.management_token, &self.username)?;
        Ok(())
    }

    pub fn status(&mut self) -> Result<Value, String> {
        self.with_management_reauth(|client, token| client.status(token))
    }

    pub fn status_all(&mut self) -> Result<Vec<Value>, String> {
        for attempt in 0..2 {
            let mut statuses = Vec::new();
            let mut last_err = None::<String>;
            let mut should_reauth = false;
            for base_url in &self.candidate_base_urls {
                let client = FsMetaApiClient::new(base_url.clone())?;
                match client.status(&self.management_token) {
                    Ok(status) => statuses.push(status),
                    Err(err) if is_invalid_session_error(&err) => {
                        should_reauth = true;
                        last_err = Some(err);
                    }
                    Err(err) if is_transport_error(&err) => {
                        last_err = Some(err);
                    }
                    Err(err) => {
                        last_err = Some(err);
                    }
                }
            }
            if !statuses.is_empty() {
                return Ok(statuses);
            }
            if attempt == 0 && should_reauth {
                self.reconnect_and_login()?;
                continue;
            }
            return Err(last_err.unwrap_or_else(|| "no status responses available".to_string()));
        }
        Err("status_all retry loop exhausted".to_string())
    }

    pub fn runtime_grants(&mut self) -> Result<Value, String> {
        self.with_management_reauth(|client, token| client.runtime_grants(token))
    }

    pub fn monitoring_roots(&mut self) -> Result<Value, String> {
        self.with_management_reauth(|client, token| client.monitoring_roots(token))
    }

    pub fn preview_roots(&mut self, roots: &Value) -> Result<Value, String> {
        self.with_management_reauth(|client, token| client.preview_roots(token, roots))
    }

    pub fn update_roots(&mut self, roots: &Value) -> Result<Value, String> {
        self.with_management_reauth(|client, token| client.update_roots(token, roots))
    }

    pub fn rescan(&mut self) -> Result<Value, String> {
        self.with_management_reauth(|client, token| client.rescan(token))
    }

    pub fn stats(&mut self, query: &[(&str, String)]) -> Result<Value, String> {
        self.with_query_reauth(|client, query_api_key| client.stats(query_api_key, query))
    }

    pub fn bound_route_metrics(&mut self) -> Result<Value, String> {
        self.with_query_reauth(|client, query_api_key| client.bound_route_metrics(query_api_key))
    }

    pub fn tree(&mut self, query: &[(&str, String)]) -> Result<Value, String> {
        self.with_query_reauth(|client, query_api_key| client.tree(query_api_key, query))
    }

    pub fn force_find(&mut self, query: &[(&str, String)]) -> Result<Value, String> {
        self.with_query_reauth(|client, query_api_key| client.force_find(query_api_key, query))
    }

    fn with_management_reauth<T>(
        &mut self,
        op: impl Fn(&FsMetaApiClient, &str) -> Result<T, String>,
    ) -> Result<T, String> {
        let deadline = std::time::Instant::now() + RECONNECT_RETRY_WINDOW;
        let mut last_err = None::<String>;
        loop {
            let mut saw_reauthable_error = false;
            for base_url in self.prioritized_base_urls() {
                let client = self.client.with_base_url(base_url.clone());
                match op(&client, &self.management_token) {
                    Ok(value) => {
                        self.client = client;
                        return Ok(value);
                    }
                    Err(err) if is_invalid_session_error(&err) => {
                        saw_reauthable_error = true;
                        last_err = Some(format!("{base_url}: {err}"));
                    }
                    Err(err) if is_transport_error(&err) => {
                        saw_reauthable_error = true;
                        last_err = Some(format!("{base_url}: {err}"));
                    }
                    Err(err) => return Err(err),
                }
            }
            if !saw_reauthable_error || std::time::Instant::now() >= deadline {
                return Err(last_err.unwrap_or_else(|| {
                    "management operation failed without a recoverable candidate".to_string()
                }));
            }
            self.reconnect_and_login()?;
            std::thread::sleep(RECONNECT_RETRY_INTERVAL);
        }
    }

    fn with_query_reauth<T>(
        &mut self,
        op: impl Fn(&FsMetaApiClient, &str) -> Result<T, String>,
    ) -> Result<T, String> {
        let deadline = std::time::Instant::now() + RECONNECT_RETRY_WINDOW;
        let mut last_err = None::<String>;
        loop {
            let mut saw_reauthable_error = false;
            for base_url in self.prioritized_base_urls() {
                let client = self.client.with_base_url(base_url.clone());
                match op(&client, &self.query_api_key) {
                    Ok(value) => {
                        self.client = client;
                        return Ok(value);
                    }
                    Err(err) if is_invalid_query_api_key_error(&err) => {
                        saw_reauthable_error = true;
                        last_err = Some(format!("{base_url}: {err}"));
                    }
                    Err(err) if is_transport_error(&err) => {
                        saw_reauthable_error = true;
                        last_err = Some(format!("{base_url}: {err}"));
                    }
                    Err(err) => return Err(err),
                }
            }
            if !saw_reauthable_error || std::time::Instant::now() >= deadline {
                return Err(last_err.unwrap_or_else(|| {
                    "query operation failed without a recoverable candidate".to_string()
                }));
            }
            self.reconnect_and_login()?;
            std::thread::sleep(RECONNECT_RETRY_INTERVAL);
        }
    }

    fn reconnect_and_login(&mut self) -> Result<(), String> {
        let deadline = std::time::Instant::now() + RECONNECT_RETRY_WINDOW;
        loop {
            let attempt = if self.candidate_base_urls.len() <= 1 {
                self.relogin()
                    .map(|_| (self.client.clone(), self.management_token.clone()))
            } else {
                login_first_available(&self.candidate_base_urls, &self.username, &self.password)
            };
            match attempt {
                Ok((client, token)) => {
                    let query_api_key = provision_query_api_key(&client, &token, &self.username)?;
                    self.client = client;
                    self.management_token = token;
                    self.query_api_key = query_api_key;
                    return Ok(());
                }
                Err(err) => {
                    if std::time::Instant::now() >= deadline {
                        return Err(format!(
                            "reconnect/login did not recover within {:?}: {}",
                            RECONNECT_RETRY_WINDOW, err
                        ));
                    }
                    std::thread::sleep(RECONNECT_RETRY_INTERVAL);
                }
            }
        }
    }

    fn prioritized_base_urls(&self) -> Vec<String> {
        let current = self.client.base_url.trim_end_matches('/').to_string();
        let mut ordered = Vec::new();
        if !current.is_empty() {
            ordered.push(current);
        }
        for base_url in &self.candidate_base_urls {
            let normalized = base_url.trim_end_matches('/').to_string();
            if !normalized.is_empty() && !ordered.contains(&normalized) {
                ordered.push(normalized);
            }
        }
        ordered
    }
}

pub fn is_invalid_session_error(err: &str) -> bool {
    err.contains("\"error\":\"invalid session token\"") || err.contains("invalid session token")
}

pub fn is_invalid_query_api_key_error(err: &str) -> bool {
    err.contains("\"error\":\"invalid query api key\"") || err.contains("invalid query api key")
}

pub fn is_transport_error(err: &str) -> bool {
    err.contains("error sending request")
        || err.contains("connection error")
        || err.contains("Connection refused")
        || err.contains("connection closed")
        || err.contains("failed to connect")
}

pub fn extract_token(login: Value) -> Result<String, String> {
    login
        .get("token")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| format!("login response missing token: {login}"))
}

fn dedupe_base_urls(base_urls: Vec<String>) -> Vec<String> {
    let mut seen = std::collections::BTreeSet::new();
    let mut deduped = Vec::new();
    for base in base_urls {
        let normalized = base.trim_end_matches('/').to_string();
        if seen.insert(normalized.clone()) {
            deduped.push(normalized);
        }
    }
    deduped
}

fn login_first_available(
    base_urls: &[String],
    username: &str,
    password: &str,
) -> Result<(FsMetaApiClient, String), String> {
    let mut last_err = String::new();
    for base in base_urls {
        let client = FsMetaApiClient::new(base.clone())?;
        match client.login(username, password) {
            Ok(login) => return Ok((client, extract_token(login)?)),
            Err(err) => last_err = format!("{base}: {err}"),
        }
    }
    Err(format!("no reachable facade base URL: {last_err}"))
}

fn provision_query_api_key(
    client: &FsMetaApiClient,
    management_token: &str,
    username: &str,
) -> Result<String, String> {
    let created = client.create_query_api_key(
        management_token,
        &format!("e2e-{}-{}", username, unique_suffix()),
    )?;
    created
        .get("api_key")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| format!("query api key response missing api_key: {created}"))
}

fn unique_suffix() -> String {
    let micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_micros())
        .unwrap_or_default();
    format!("{}-{micros}", std::process::id())
}

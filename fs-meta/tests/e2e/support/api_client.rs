#![cfg(target_os = "linux")]
#![allow(dead_code)]

use reqwest::blocking::{Client, Response};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde_json::{json, Value};
use std::time::Duration;

const RECONNECT_RETRY_WINDOW: Duration = Duration::from_secs(90);
const RECONNECT_RETRY_INTERVAL: Duration = Duration::from_millis(250);
const HTTP_TIMEOUT_DEFAULT: Duration = Duration::from_secs(45);
const HTTP_TIMEOUT_FORCE_FIND: Duration = Duration::from_secs(75);
const HTTP_TIMEOUT_PROJECTION: Duration = Duration::from_secs(75);

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

    pub fn base_url(&self) -> &str {
        &self.base_url
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

    pub fn list_query_api_keys(&self, token: &str) -> Result<Value, String> {
        self.ensure_success(self.get_json_raw("/query-api-keys", token)?)
    }

    pub fn revoke_query_api_key(&self, token: &str, key_id: &str) -> Result<Value, String> {
        self.ensure_success(self.revoke_query_api_key_raw(token, key_id)?)
    }

    pub fn revoke_query_api_key_raw(
        &self,
        token: &str,
        key_id: &str,
    ) -> Result<ApiResponse, String> {
        self.delete_json_raw(&format!("/query-api-keys/{key_id}"), token)
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

    pub fn delete_json_raw(&self, path: &str, token: &str) -> Result<ApiResponse, String> {
        self.decode(
            self.client
                .delete(self.url(path))
                .timeout(self.request_timeout(path))
                .headers(self.auth_headers(token)?)
                .send()
                .map_err(|e| format!("DELETE {} failed: {e}", path))?,
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
            "/tree" | "/stats" => HTTP_TIMEOUT_PROJECTION,
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
        let (client, query_api_key) = provision_query_api_key_first_available(
            &prioritize_base_urls(client.base_url(), &candidate_base_urls),
            &token,
            &username,
        )?;
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
        let (client, query_api_key) = provision_query_api_key_first_available(
            std::slice::from_ref(&primary_base_url),
            &token,
            &username,
        )?;
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
        let (client, query_api_key) = provision_query_api_key_first_available(
            std::slice::from_ref(&self.client.base_url),
            &self.management_token,
            &self.username,
        )?;
        self.client = client;
        self.query_api_key = query_api_key;
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
        let response =
            self.with_management_reauth(|client, token| client.update_roots(token, roots))?;
        self.best_effort_management_fanout(|client, token| client.update_roots(token, roots));
        Ok(response)
    }

    pub fn rescan(&mut self) -> Result<Value, String> {
        let response = self.with_management_reauth(|client, token| client.rescan(token))?;
        self.best_effort_management_fanout(|client, token| client.rescan(token));
        Ok(response)
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
                    Err(err)
                        if is_transport_error(&err)
                            || is_retryable_management_unavailable_error(&err) =>
                    {
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
                    Err(err)
                        if is_transport_error(&err)
                            || is_retryable_query_unavailable_error(&err) =>
                    {
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
                    let (client, query_api_key) = provision_query_api_key_first_available(
                        &prioritize_base_urls(client.base_url(), &self.candidate_base_urls),
                        &token,
                        &self.username,
                    )?;
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

    fn best_effort_management_fanout<T>(
        &mut self,
        op: impl Fn(&FsMetaApiClient, &str) -> Result<T, String>,
    ) {
        let current = self.client.base_url().trim_end_matches('/').to_string();
        for base_url in self.prioritized_base_urls() {
            if base_url == current {
                continue;
            }
            let deadline = std::time::Instant::now() + RECONNECT_RETRY_WINDOW;
            loop {
                let client = self.client.with_base_url(base_url.clone());
                match op(&client, &self.management_token) {
                    Ok(_) => break,
                    Err(err) if is_invalid_session_error(&err) => {
                        match login_first_available(
                            std::slice::from_ref(&base_url),
                            &self.username,
                            &self.password,
                        ) {
                            Ok((target_client, token)) => match op(&target_client, &token) {
                                Ok(_) => break,
                                Err(retry_err) => {
                                    if std::time::Instant::now() >= deadline {
                                        eprintln!(
                                            "fs-meta-api-client: best-effort management fanout failed base_url={} err={}",
                                            base_url, retry_err
                                        );
                                        break;
                                    }
                                }
                            },
                            Err(login_err) => {
                                if std::time::Instant::now() >= deadline {
                                    eprintln!(
                                        "fs-meta-api-client: best-effort management fanout failed base_url={} err={}",
                                        base_url, login_err
                                    );
                                    break;
                                }
                            }
                        }
                    }
                    Err(err) if is_transport_error(&err) => {
                        if std::time::Instant::now() >= deadline {
                            eprintln!(
                                "fs-meta-api-client: best-effort management fanout failed base_url={} err={}",
                                base_url, err
                            );
                            break;
                        }
                    }
                    Err(err) => {
                        eprintln!(
                            "fs-meta-api-client: best-effort management fanout failed base_url={} err={}",
                            base_url, err
                        );
                        break;
                    }
                }
                std::thread::sleep(RECONNECT_RETRY_INTERVAL);
            }
        }
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

pub fn is_retryable_query_unavailable_error(err: &str) -> bool {
    (err.contains("http 503 failed")
        && (err.contains("\"code\":\"NOT_READY\"")
            || err.contains("trusted-materialized reads remain unavailable")
            || err.contains("runtime control initializes the app")))
        || (err.contains("http 500 failed")
            && err.contains("\"code\":\"INTERNAL_ERROR\"")
            && err.contains("drained/fenced")
            && err.contains("grant attachments"))
}

pub fn is_retryable_management_unavailable_error(err: &str) -> bool {
    err.contains("http 503 failed")
        && (err.contains("temporarily unavailable while runtime workers reconfigure")
            || err.contains("runtime control initializes the app"))
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
    let deadline = std::time::Instant::now() + RECONNECT_RETRY_WINDOW;
    let mut last_err = String::new();
    loop {
        let mut saw_transport_error = false;
        for base in base_urls {
            let client = FsMetaApiClient::new(base.clone())?;
            match client.login(username, password) {
                Ok(login) => return Ok((client, extract_token(login)?)),
                Err(err) if is_transport_error(&err) => {
                    saw_transport_error = true;
                    last_err = format!("{base}: {err}");
                }
                Err(err) if is_retryable_management_unavailable_error(&err) => {
                    saw_transport_error = true;
                    last_err = format!("{base}: {err}");
                }
                Err(err) => return Err(format!("{base}: {err}")),
            }
        }
        if !saw_transport_error || std::time::Instant::now() >= deadline {
            return Err(format!("no reachable facade base URL: {last_err}"));
        }
        std::thread::sleep(RECONNECT_RETRY_INTERVAL);
    }
}

fn prioritize_base_urls(current: &str, base_urls: &[String]) -> Vec<String> {
    let mut ordered = Vec::new();
    let current = current.trim_end_matches('/').to_string();
    if !current.is_empty() {
        ordered.push(current);
    }
    for base_url in base_urls {
        let normalized = base_url.trim_end_matches('/').to_string();
        if !normalized.is_empty() && !ordered.contains(&normalized) {
            ordered.push(normalized);
        }
    }
    ordered
}

fn provision_query_api_key_with_client(
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

fn provision_query_api_key_first_available(
    base_urls: &[String],
    management_token: &str,
    username: &str,
) -> Result<(FsMetaApiClient, String), String> {
    let deadline = std::time::Instant::now() + RECONNECT_RETRY_WINDOW;
    let mut last_err = String::new();
    loop {
        let mut saw_transport_error = false;
        for base in base_urls {
            let client = FsMetaApiClient::new(base.clone())?;
            match provision_query_api_key_with_client(&client, management_token, username) {
                Ok(query_api_key) => return Ok((client, query_api_key)),
                Err(err) if is_transport_error(&err) => {
                    saw_transport_error = true;
                    last_err = format!("{base}: {err}");
                }
                Err(err) => return Err(format!("{base}: {err}")),
            }
        }
        if !saw_transport_error || std::time::Instant::now() >= deadline {
            return Err(format!(
                "no reachable facade base URL for query api key provision: {last_err}"
            ));
        }
        std::thread::sleep(RECONNECT_RETRY_INTERVAL);
    }
}

fn unique_suffix() -> String {
    let micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_micros())
        .unwrap_or_default();
    format!("{}-{micros}", std::process::id())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn tree_and_stats_timeouts_are_not_shorter_than_materialized_query_budget() {
        assert!(
            HTTP_TIMEOUT_PROJECTION >= Duration::from_secs(60),
            "projection requests must not time out before the server-side tree/stats query budget"
        );
        assert!(
            HTTP_TIMEOUT_PROJECTION >= HTTP_TIMEOUT_DEFAULT,
            "projection requests should allow at least as much time as ordinary management requests"
        );
    }

    #[test]
    fn tree_failsover_to_next_candidate_on_retryable_internal_grant_attachment_error() {
        let first = TcpListener::bind("127.0.0.1:0").expect("bind first tree listener");
        let first_addr = first.local_addr().expect("first listener addr");
        let second = TcpListener::bind("127.0.0.1:0").expect("bind second tree listener");
        let second_addr = second.local_addr().expect("second listener addr");

        let first_server = std::thread::spawn(move || {
            let (mut stream, _) = first.accept().expect("accept first tree request");
            let mut buf = [0_u8; 4096];
            let _ = stream.read(&mut buf);
            let body = r#"{"code":"INTERNAL_ERROR","error":"access denied: pid Pid(1) is drained/fenced and cannot obtain new grant attachments","path":"/"}"#;
            let response = format!(
                "HTTP/1.1 500 Internal Server Error
content-type: application/json
content-length: {}
connection: close

{}",
                body.len(),
                body
            );
            stream
                .write_all(response.as_bytes())
                .expect("write first tree response");
            stream.flush().expect("flush first tree response");
        });

        let second_server = std::thread::spawn(move || {
            let (mut stream, _) = second.accept().expect("accept second tree request");
            let mut buf = [0_u8; 4096];
            let _ = stream.read(&mut buf);
            let body = r#"{"status":"ok","groups":[],"group_page":{"returned_groups":0,"has_more_groups":false,"next_cursor":null,"next_entry_after":null}}"#;
            let response = format!(
                "HTTP/1.1 200 OK
content-type: application/json
content-length: {}
connection: close

{}",
                body.len(),
                body
            );
            stream
                .write_all(response.as_bytes())
                .expect("write second tree response");
            stream.flush().expect("flush second tree response");
        });

        let mut session = OperatorSession {
            client: FsMetaApiClient::new(format!("http://{}", first_addr)).expect("client"),
            candidate_base_urls: vec![
                format!("http://{}", first_addr),
                format!("http://{}", second_addr),
            ],
            username: "operator".to_string(),
            password: "operator123".to_string(),
            management_token: "ignored".to_string(),
            query_api_key: "ignored".to_string(),
        };

        let tree = session
            .tree(&[("path", "/".to_string()), ("recursive", "true".to_string())])
            .expect("tree should fail over to the next candidate on retryable internal grant-attachment errors");

        assert_eq!(tree.get("status").and_then(Value::as_str), Some("ok"));
        assert_eq!(session.client.base_url(), format!("http://{}", second_addr));
        first_server.join().expect("join first server");
        second_server.join().expect("join second server");
    }

    #[test]
    fn management_request_failsover_when_runtime_control_is_initializing() {
        let first = TcpListener::bind("127.0.0.1:0").expect("bind first facade listener");
        let first_addr = first.local_addr().expect("first listener addr");
        let second = TcpListener::bind("127.0.0.1:0").expect("bind second facade listener");
        let second_addr = second.local_addr().expect("second listener addr");

        let first_server = std::thread::spawn(move || {
            let (mut stream, _) = first.accept().expect("accept first rescan request");
            let mut buf = [0_u8; 4096];
            let _ = stream.read(&mut buf);
            let body = r#"{"error":"fs-meta management request handling is unavailable until runtime control initializes the app"}"#;
            let response = format!(
                "HTTP/1.1 503 Service Unavailable
content-type: application/json
content-length: {}
connection: close

{}",
                body.len(),
                body
            );
            stream
                .write_all(response.as_bytes())
                .expect("write first rescan response");
            stream.flush().expect("flush first rescan response");
        });

        let second_server = std::thread::spawn(move || {
            let (mut stream, _) = second.accept().expect("accept second rescan request");
            let mut buf = [0_u8; 4096];
            let _ = stream.read(&mut buf);
            let body = r#"{"status":"ok"}"#;
            let response = format!(
                "HTTP/1.1 200 OK
content-type: application/json
content-length: {}
connection: close

{}",
                body.len(),
                body
            );
            stream
                .write_all(response.as_bytes())
                .expect("write second rescan response");
            stream.flush().expect("flush second rescan response");
        });

        let mut session = OperatorSession {
            client: FsMetaApiClient::new(format!("http://{}", first_addr)).expect("client"),
            candidate_base_urls: vec![
                format!("http://{}", first_addr),
                format!("http://{}", second_addr),
            ],
            username: "operator".to_string(),
            password: "operator123".to_string(),
            management_token: "ignored".to_string(),
            query_api_key: "ignored".to_string(),
        };

        let response = session
            .with_management_reauth(|client, token| client.rescan(token))
            .expect("management request should fail over while one facade initializes control");

        assert_eq!(response.get("status").and_then(Value::as_str), Some("ok"));
        assert_eq!(session.client.base_url(), format!("http://{}", second_addr));
        first_server.join().expect("join first facade server");
        second_server.join().expect("join second facade server");
    }

    #[test]
    fn login_many_fails_over_query_api_key_provision_after_transient_transport_error() {
        let first = TcpListener::bind("127.0.0.1:0").expect("bind first facade listener");
        let first_addr = first.local_addr().expect("first listener addr");
        let second = TcpListener::bind("127.0.0.1:0").expect("bind second facade listener");
        let second_addr = second.local_addr().expect("second listener addr");

        let first_server = std::thread::spawn(move || {
            let (mut stream, _) = first.accept().expect("accept first login request");
            let mut buf = [0_u8; 4096];
            let _ = stream.read(&mut buf);
            let login_body = r#"{"token":"shared-token"}"#;
            let login_response = format!(
                "HTTP/1.1 200 OK
content-type: application/json
content-length: {}
connection: close

{}",
                login_body.len(),
                login_body
            );
            stream
                .write_all(login_response.as_bytes())
                .expect("write first login response");
            stream.flush().expect("flush first login response");

            let (mut stream, _) = first.accept().expect("accept first query-api-key request");
            let mut buf = [0_u8; 4096];
            let _ = stream.read(&mut buf);
            drop(stream);
        });

        let second_server = std::thread::spawn(move || {
            let (mut stream, _) = second
                .accept()
                .expect("accept second query-api-key request");
            let mut buf = [0_u8; 4096];
            let _ = stream.read(&mut buf);
            let body = r#"{"api_key":"query-key-2"}"#;
            let response = format!(
                "HTTP/1.1 200 OK
content-type: application/json
content-length: {}
connection: close

{}",
                body.len(),
                body
            );
            stream
                .write_all(response.as_bytes())
                .expect("write second query-api-key response");
            stream.flush().expect("flush second query-api-key response");
        });

        let session = OperatorSession::login_many(
            vec![
                format!("http://{}", first_addr),
                format!("http://{}", second_addr),
            ],
            "operator",
            "operator123",
        )
        .expect(
            "login_many should fail over query-api-key provisioning after a transient transport error",
        );

        assert_eq!(session.client.base_url(), format!("http://{}", second_addr));
        assert_eq!(session.query_api_key(), "query-key-2");
        first_server.join().expect("join first facade server");
        second_server.join().expect("join second facade server");
    }

    #[test]
    fn login_first_available_retries_transient_transport_error_for_same_base_url() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind flaky login listener");
        let addr = listener.local_addr().expect("listener addr");
        let request_count = Arc::new(AtomicUsize::new(0));
        let request_count_bg = request_count.clone();
        let server = std::thread::spawn(move || {
            for attempt in 0..2 {
                let (mut stream, _) = listener.accept().expect("accept login request");
                request_count_bg.fetch_add(1, Ordering::SeqCst);
                let mut buf = [0_u8; 4096];
                let _ = stream.read(&mut buf);
                if attempt == 0 {
                    drop(stream);
                    continue;
                }
                let body = r#"{"token":"test-token"}"#;
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                stream
                    .write_all(response.as_bytes())
                    .expect("write login response");
                stream.flush().expect("flush login response");
            }
        });

        let base_urls = vec![format!("http://{}", addr)];
        let (client, token) = login_first_available(&base_urls, "operator", "operator123")
            .expect("login should survive one transient transport flap on the same facade");

        assert_eq!(client.base_url(), format!("http://{}", addr));
        assert_eq!(token, "test-token");
        assert!(request_count.load(Ordering::SeqCst) >= 2);
        server.join().expect("join flaky login server");
    }
}

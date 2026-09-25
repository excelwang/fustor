use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use capanix_app_sdk::{CnxError, Result};
use es_source::shared_types::{
    EsDocumentEvent, EsDocumentEventKind, EsFieldCatalog, EsFieldDescriptor, EsPollCursor,
    EsProbeStatus, EsSnapshotCursor, EsSourceBatch, EsSourceCursor, EsSourceOperation,
    EsSourcePayload, EsSourceRequest,
};
use es_source::{ResolvedCredential, ResolvedEsEndpoint};
use reqwest::blocking::{Client, RequestBuilder};
use serde_json::{Map, Value, json};
use source_kit::{SourceOperationBudget, source_native_revision_digest};

pub trait EsSourceDriver: Send + Sync {
    fn execute(
        &self,
        endpoint: &ResolvedEsEndpoint,
        credential: &ResolvedCredential,
        request: &EsSourceRequest,
        limit: usize,
    ) -> Result<EsSourcePayload>;

    fn execute_with_timeout(
        &self,
        endpoint: &ResolvedEsEndpoint,
        credential: &ResolvedCredential,
        request: &EsSourceRequest,
        limit: usize,
        timeout: Option<Duration>,
    ) -> Result<EsSourcePayload> {
        let _ = timeout;
        self.execute(endpoint, credential, request, limit)
    }

    fn execute_with_budget(
        &self,
        endpoint: &ResolvedEsEndpoint,
        credential: &ResolvedCredential,
        request: &EsSourceRequest,
        limit: usize,
        budget: &mut SourceOperationBudget,
    ) -> Result<EsSourcePayload> {
        budget.observe_fanout(1)?;
        budget.consume_call(1)?;
        self.execute_with_timeout(
            endpoint,
            credential,
            request,
            limit,
            budget.remaining_timeout()?,
        )
    }

    fn close_snapshot(
        &self,
        endpoint: &ResolvedEsEndpoint,
        credential: &ResolvedCredential,
        pit_id: &str,
    ) -> Result<()> {
        let _ = (endpoint, credential, pit_id);
        Err(CnxError::NotSupported(
            "elasticsearch snapshot close is not supported by this driver".into(),
        ))
    }

    fn close_snapshot_with_timeout(
        &self,
        endpoint: &ResolvedEsEndpoint,
        credential: &ResolvedCredential,
        pit_id: &str,
        timeout: Option<Duration>,
    ) -> Result<()> {
        let _ = timeout;
        self.close_snapshot(endpoint, credential, pit_id)
    }

    fn close_snapshot_with_budget(
        &self,
        endpoint: &ResolvedEsEndpoint,
        credential: &ResolvedCredential,
        pit_id: &str,
        budget: &mut SourceOperationBudget,
    ) -> Result<()> {
        budget.observe_fanout(1)?;
        budget.consume_call(1)?;
        self.close_snapshot_with_timeout(endpoint, credential, pit_id, budget.remaining_timeout()?)
    }
}

pub struct HttpEsSourceDriver {
    client: Option<Client>,
    pit_keep_alive: String,
}

impl Default for HttpEsSourceDriver {
    fn default() -> Self {
        Self {
            client: Some(
                run_es_thread("build Elasticsearch HTTP client", || {
                    Client::builder()
                        .timeout(Duration::from_secs(30))
                        .build()
                        .map_err(|err| {
                            CnxError::Internal(format!("build Elasticsearch HTTP client: {err}"))
                        })
                })
                .expect("build reqwest blocking client"),
            ),
            pit_keep_alive: "1m".into(),
        }
    }
}

impl EsSourceDriver for HttpEsSourceDriver {
    fn execute(
        &self,
        endpoint: &ResolvedEsEndpoint,
        credential: &ResolvedCredential,
        request: &EsSourceRequest,
        limit: usize,
    ) -> Result<EsSourcePayload> {
        self.execute_with_timeout(endpoint, credential, request, limit, None)
    }

    fn execute_with_timeout(
        &self,
        endpoint: &ResolvedEsEndpoint,
        credential: &ResolvedCredential,
        request: &EsSourceRequest,
        limit: usize,
        timeout: Option<Duration>,
    ) -> Result<EsSourcePayload> {
        let mut budget = SourceOperationBudget::new(timeout, None);
        budget.observe_fanout(1)?;
        self.execute_with_budget(endpoint, credential, request, limit, &mut budget)
    }

    fn execute_with_budget(
        &self,
        endpoint: &ResolvedEsEndpoint,
        credential: &ResolvedCredential,
        request: &EsSourceRequest,
        limit: usize,
        budget: &mut SourceOperationBudget,
    ) -> Result<EsSourcePayload> {
        budget.observe_fanout(1)?;
        run_es_thread("execute Elasticsearch source request", || {
            validate_driver_request(request)?;
            match request.operation {
                EsSourceOperation::ProbeConnection => self.probe(endpoint, credential, budget),
                EsSourceOperation::DiscoverFields => {
                    self.discover_fields(endpoint, credential, request, budget)
                }
                EsSourceOperation::SnapshotPage => {
                    self.snapshot_page(endpoint, credential, request, limit, budget)
                }
                EsSourceOperation::SnapshotClose => Err(CnxError::InvalidInput(
                    "snapshot close must be handled by the source session boundary".into(),
                )),
                EsSourceOperation::PollSince => {
                    self.poll_since(endpoint, credential, request, limit, budget)
                }
            }
        })
    }

    fn close_snapshot(
        &self,
        endpoint: &ResolvedEsEndpoint,
        credential: &ResolvedCredential,
        pit_id: &str,
    ) -> Result<()> {
        self.close_snapshot_with_timeout(endpoint, credential, pit_id, None)
    }

    fn close_snapshot_with_timeout(
        &self,
        endpoint: &ResolvedEsEndpoint,
        credential: &ResolvedCredential,
        pit_id: &str,
        timeout: Option<Duration>,
    ) -> Result<()> {
        let mut budget = SourceOperationBudget::new(timeout, None);
        budget.observe_fanout(1)?;
        self.close_snapshot_with_budget(endpoint, credential, pit_id, &mut budget)
    }

    fn close_snapshot_with_budget(
        &self,
        endpoint: &ResolvedEsEndpoint,
        credential: &ResolvedCredential,
        pit_id: &str,
        budget: &mut SourceOperationBudget,
    ) -> Result<()> {
        budget.observe_fanout(1)?;
        budget.ensure_call_capacity(1, 1)?;
        run_es_thread("close Elasticsearch snapshot", || {
            self.close_pit(endpoint, credential, pit_id, budget)
        })
    }
}

impl Drop for HttpEsSourceDriver {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            let _ = run_es_thread("drop Elasticsearch HTTP client", || {
                drop(client);
                Ok(())
            });
        }
    }
}

impl HttpEsSourceDriver {
    fn client(&self) -> &Client {
        self.client
            .as_ref()
            .expect("Elasticsearch HTTP client is available")
    }

    fn probe(
        &self,
        endpoint: &ResolvedEsEndpoint,
        credential: &ResolvedCredential,
        budget: &mut SourceOperationBudget,
    ) -> Result<EsSourcePayload> {
        budget.ensure_call_capacity(1, 1)?;
        let value = self.request_json(
            self.apply_auth(self.client().get(base_url(endpoint)), credential),
            "probe es endpoint",
            budget,
        )?;
        Ok(EsSourcePayload::Probe(EsProbeStatus {
            object_ref: endpoint.object_ref.clone(),
            endpoint_uri: endpoint.endpoint_uri.clone(),
            reachable: true,
            cluster_name: value
                .get("cluster_name")
                .and_then(Value::as_str)
                .map(str::to_string),
            diagnostics: value
                .get("version")
                .and_then(|version| version.get("number"))
                .and_then(Value::as_str)
                .map(|version| format!("elasticsearch version {version}")),
        }))
    }

    fn discover_fields(
        &self,
        endpoint: &ResolvedEsEndpoint,
        credential: &ResolvedCredential,
        request: &EsSourceRequest,
        budget: &mut SourceOperationBudget,
    ) -> Result<EsSourcePayload> {
        budget.ensure_call_capacity(1, 1)?;
        let url = format!(
            "{}/{}/_mapping",
            base_url(endpoint),
            clean_index(&request.index)
        );
        let mapping = self.request_json(
            self.apply_auth(self.client().get(url), credential),
            "discover es fields",
            budget,
        )?;
        let mut fields = Vec::new();
        collect_mapping_fields(&mapping, &mut fields);
        fields.push(EsFieldDescriptor {
            name: "_id".into(),
            field_type: Some("metadata".into()),
        });
        fields.push(EsFieldDescriptor {
            name: "_index".into(),
            field_type: Some("metadata".into()),
        });
        fields.sort_by(|left, right| left.name.cmp(&right.name));
        fields.dedup_by(|left, right| left.name == right.name);
        Ok(EsSourcePayload::Fields(EsFieldCatalog {
            object_ref: endpoint.object_ref.clone(),
            index: request.index.clone(),
            fields,
        }))
    }

    fn snapshot_page(
        &self,
        endpoint: &ResolvedEsEndpoint,
        credential: &ResolvedCredential,
        request: &EsSourceRequest,
        limit: usize,
        budget: &mut SourceOperationBudget,
    ) -> Result<EsSourcePayload> {
        let opened_pit = request.cursor.is_none();
        let maximum_calls = if opened_pit { 3 } else { 2 };
        budget.ensure_call_capacity(maximum_calls, maximum_calls)?;
        let (pit_id, search_after) = match request.cursor.as_ref() {
            Some(EsSourceCursor::Snapshot(cursor)) => {
                (cursor.pit_id.clone(), cursor.search_after.clone())
            }
            Some(_) => {
                return Err(CnxError::InvalidInput(
                    "snapshot request received non-snapshot cursor".into(),
                ));
            }
            None => (
                self.open_pit(endpoint, credential, &request.index, budget)?,
                Vec::new(),
            ),
        };

        let mut body = json!({
            "size": limit,
            "seq_no_primary_term": true,
            "pit": {
                "id": pit_id,
                "keep_alive": self.pit_keep_alive.clone(),
            },
            "sort": [
                sort_clause(&endpoint.timestamp_field),
                { "_shard_doc": "asc" }
            ],
            "query": request.query_filter.clone().unwrap_or_else(|| json!({"match_all": {}})),
        });
        apply_source_filter(&mut body, request);
        if !search_after.is_empty() {
            body["search_after"] = Value::Array(search_after);
        }

        let search = match self.request_json(
            self.apply_auth(
                self.client()
                    .post(format!("{}/_search", base_url(endpoint)))
                    .json(&body),
                credential,
            ),
            "snapshot es page",
            budget,
        ) {
            Ok(search) => search,
            Err(err) => {
                if opened_pit {
                    let _ = self.close_pit(endpoint, credential, &pit_id, budget);
                }
                return Err(err);
            }
        };
        let hits = match extract_hits(&search) {
            Ok(hits) => hits,
            Err(err) => {
                if opened_pit {
                    let _ = self.close_pit(endpoint, credential, &pit_id, budget);
                }
                return Err(err);
            }
        };
        let documents = hits_to_events(
            &hits,
            EsDocumentEventKind::SnapshotInsert,
            &endpoint.timestamp_field,
        );
        let has_more = documents.len() == limit && !documents.is_empty();
        let mut diagnostics = None;
        let next_cursor = if has_more {
            documents.last().map(|last| {
                EsSourceCursor::Snapshot(EsSnapshotCursor {
                    pit_id: pit_id.clone(),
                    search_after: last.sort.clone(),
                    expires_at_unix_ms: Some(now_ms() + 60_000),
                })
            })
        } else {
            if let Err(err) = self.close_pit(endpoint, credential, &pit_id, budget) {
                diagnostics = Some(format!("close PIT failed: {err}"));
            }
            None
        };

        Ok(EsSourcePayload::Batch(EsSourceBatch {
            object_ref: endpoint.object_ref.clone(),
            index: request.index.clone(),
            source_revision: source_native_revision_digest("es-pit", &[pit_id.as_bytes()]),
            documents,
            next_cursor,
            has_more,
            grant_epoch: endpoint.grant_epoch,
            diagnostics,
        }))
    }

    fn poll_since(
        &self,
        endpoint: &ResolvedEsEndpoint,
        credential: &ResolvedCredential,
        request: &EsSourceRequest,
        limit: usize,
        budget: &mut SourceOperationBudget,
    ) -> Result<EsSourcePayload> {
        budget.ensure_call_capacity(1, 1)?;
        let cursor = match request.cursor.as_ref() {
            Some(EsSourceCursor::Poll(cursor)) => Some(cursor),
            Some(_) => {
                return Err(CnxError::InvalidInput(
                    "poll request received non-poll cursor".into(),
                ));
            }
            None => None,
        };
        let range_start = cursor
            .and_then(|cursor| cursor.timestamp_after.clone())
            .or_else(|| request.timestamp_after.clone());
        let mut body = json!({
            "size": limit,
            "seq_no_primary_term": true,
            "sort": [
                sort_clause(&endpoint.timestamp_field),
                sort_clause(&endpoint.tie_breaker_field)
            ],
            "query": poll_query(&endpoint.timestamp_field, range_start.as_ref(), request),
        });
        apply_source_filter(&mut body, request);
        if let Some(cursor) = cursor
            && !cursor.search_after.is_empty()
        {
            body["search_after"] = Value::Array(cursor.search_after.clone());
        }

        let search = self.request_json(
            self.apply_auth(
                self.client()
                    .post(format!(
                        "{}/{}/_search",
                        base_url(endpoint),
                        clean_index(&request.index)
                    ))
                    .json(&body),
                credential,
            ),
            "poll es page",
            budget,
        )?;
        let hits = extract_hits(&search)?;
        let documents = hits_to_events(
            &hits,
            EsDocumentEventKind::PollUpdate,
            &endpoint.timestamp_field,
        );
        let has_more = documents.len() == limit && !documents.is_empty();
        let next_cursor = documents.last().map(|last| {
            EsSourceCursor::Poll(EsPollCursor {
                timestamp_after: last.timestamp_value.clone(),
                search_after: last.sort.clone(),
            })
        });
        let source_revision = es_poll_source_revision(&request.index, &documents)?;

        Ok(EsSourcePayload::Batch(EsSourceBatch {
            object_ref: endpoint.object_ref.clone(),
            index: request.index.clone(),
            source_revision,
            documents,
            next_cursor,
            has_more,
            grant_epoch: endpoint.grant_epoch,
            diagnostics: None,
        }))
    }

    fn open_pit(
        &self,
        endpoint: &ResolvedEsEndpoint,
        credential: &ResolvedCredential,
        index: &str,
        budget: &mut SourceOperationBudget,
    ) -> Result<String> {
        let url = format!(
            "{}/{}/_pit?keep_alive={}",
            base_url(endpoint),
            clean_index(index),
            self.pit_keep_alive
        );
        let value = self.request_json(
            self.apply_auth(self.client().post(url), credential),
            "open es point-in-time",
            budget,
        )?;
        value
            .get("id")
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| CnxError::PeerError("open PIT response missing id".into()))
    }

    fn close_pit(
        &self,
        endpoint: &ResolvedEsEndpoint,
        credential: &ResolvedCredential,
        pit_id: &str,
        budget: &mut SourceOperationBudget,
    ) -> Result<()> {
        let value = self.request_json(
            self.apply_auth(
                self.client()
                    .delete(format!("{}/_pit", base_url(endpoint)))
                    .json(&json!({ "id": pit_id })),
                credential,
            ),
            "close es point-in-time",
            budget,
        )?;
        if value.get("succeeded").and_then(Value::as_bool) == Some(false) {
            return Err(CnxError::PeerError(
                "close PIT returned succeeded=false".into(),
            ));
        }
        Ok(())
    }

    fn request_json(
        &self,
        request: RequestBuilder,
        context: &str,
        budget: &mut SourceOperationBudget,
    ) -> Result<Value> {
        budget.consume_call(1)?;
        let timeout = effective_timeout(budget.remaining_timeout()?);
        let response = request
            .timeout(timeout)
            .send()
            .map_err(|err| reqwest_error(context, err))?;
        let status = response.status();
        let text = response.text().map_err(|err| reqwest_error(context, err))?;
        if !status.is_success() {
            return Err(classify_http_status_error(context, status.as_u16(), &text));
        }
        if text.trim().is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_str(&text).map_err(|err| {
            CnxError::PeerError(format!(
                "{context}: invalid json response: {err}; body={}",
                truncate(&text, 512)
            ))
        })
    }

    fn apply_auth(
        &self,
        request: RequestBuilder,
        credential: &ResolvedCredential,
    ) -> RequestBuilder {
        match credential {
            ResolvedCredential::None => request,
            ResolvedCredential::Basic { username, password } => {
                request.basic_auth(username, Some(password))
            }
            ResolvedCredential::ApiKey { api_key } => {
                request.header(reqwest::header::AUTHORIZATION, format!("ApiKey {api_key}"))
            }
            ResolvedCredential::Bearer { token } => request.bearer_auth(token),
        }
    }
}

fn es_poll_source_revision(index: &str, documents: &[EsDocumentEvent]) -> Result<String> {
    let native_observations = documents
        .iter()
        .map(|document| {
            json!({
                "index": document.index,
                "id": document.id,
                "sort": document.sort,
                "seq_no": document.seq_no,
                "primary_term": document.primary_term,
            })
        })
        .collect::<Vec<_>>();
    let encoded = serde_json::to_vec(&native_observations).map_err(|error| {
        CnxError::Internal(format!(
            "encode Elasticsearch poll source-native observations: {error}"
        ))
    })?;
    Ok(source_native_revision_digest(
        "es-poll-observation",
        &[index.as_bytes(), &encoded],
    ))
}

fn effective_timeout(timeout: Option<Duration>) -> Duration {
    timeout
        .unwrap_or_else(|| Duration::from_secs(30))
        .max(Duration::from_millis(1))
}

fn reqwest_error(context: &str, error: reqwest::Error) -> CnxError {
    if error.is_timeout() {
        CnxError::Timeout
    } else {
        CnxError::LinkError(format!("{context}: {error}"))
    }
}

fn base_url(endpoint: &ResolvedEsEndpoint) -> String {
    endpoint.endpoint_uri.trim_end_matches('/').to_string()
}

fn run_es_thread<T, F>(context: &'static str, operation: F) -> Result<T>
where
    T: Send,
    F: FnOnce() -> Result<T> + Send,
{
    std::thread::scope(|scope| {
        scope
            .spawn(operation)
            .join()
            .map_err(|_| CnxError::Internal(format!("{context}: HTTP thread panicked")))?
    })
}

fn clean_index(index: &str) -> &str {
    index.trim().trim_start_matches('/')
}

fn validate_driver_request(request: &EsSourceRequest) -> Result<()> {
    validate_index(&request.index)?;
    if let Some(EsSourceCursor::Snapshot(cursor)) = &request.cursor
        && cursor.pit_id.trim().is_empty()
    {
        return Err(CnxError::InvalidInput(
            "snapshot cursor pit_id must be non-empty".into(),
        ));
    }
    Ok(())
}

fn validate_index(index: &str) -> Result<()> {
    let trimmed = index.trim();
    if trimmed.is_empty() {
        return Err(CnxError::InvalidInput("index must be non-empty".into()));
    }
    if trimmed.contains('/')
        || trimmed.contains('\\')
        || trimmed.contains('?')
        || trimmed.contains('#')
    {
        return Err(CnxError::InvalidInput(format!(
            "index '{index}' contains unsupported path or query characters"
        )));
    }
    Ok(())
}

fn classify_http_status_error(context: &str, status: u16, text: &str) -> CnxError {
    let message = format!("{context}: status={status} body={}", truncate(text, 512));
    match status {
        400 => CnxError::InvalidInput(message),
        401 | 403 => CnxError::AccessDenied(message),
        408 | 429 | 500..=599 => CnxError::LinkError(message),
        404 => CnxError::PeerError(message),
        _ => CnxError::PeerError(message),
    }
}

fn apply_source_filter(body: &mut Value, request: &EsSourceRequest) {
    if request.field_filter.is_empty() {
        return;
    }
    body["_source"] = json!({ "includes": request.field_filter });
}

fn sort_clause(field: &str) -> Value {
    let mut map = Map::new();
    map.insert(field.to_string(), json!({ "order": "asc" }));
    Value::Object(map)
}

fn range_clause(field: &str, operator: &str, value: &Value) -> Value {
    let mut op = Map::new();
    op.insert(operator.to_string(), value.clone());
    let mut field_map = Map::new();
    field_map.insert(field.to_string(), Value::Object(op));
    let mut range = Map::new();
    range.insert("range".to_string(), Value::Object(field_map));
    Value::Object(range)
}

fn poll_query(
    timestamp_field: &str,
    timestamp_after: Option<&Value>,
    request: &EsSourceRequest,
) -> Value {
    let base_query = request
        .query_filter
        .clone()
        .unwrap_or_else(|| json!({"match_all": {}}));
    let Some(timestamp_after) = timestamp_after else {
        return base_query;
    };
    json!({
        "bool": {
            "filter": [
                base_query,
                range_clause(timestamp_field, "gte", timestamp_after)
            ]
        }
    })
}

fn extract_hits(search: &Value) -> Result<Vec<Value>> {
    search
        .get("hits")
        .and_then(|hits| hits.get("hits"))
        .and_then(Value::as_array)
        .cloned()
        .ok_or_else(|| CnxError::PeerError("search response missing hits.hits".into()))
}

fn hits_to_events(
    hits: &[Value],
    kind: EsDocumentEventKind,
    timestamp_field: &str,
) -> Vec<EsDocumentEvent> {
    hits.iter()
        .map(|hit| {
            let source = hit.get("_source").cloned().unwrap_or(Value::Null);
            let timestamp_value = extract_path_value(&source, timestamp_field).cloned();
            let mut metadata = BTreeMap::new();
            if let Some(value) = hit.get("_routing") {
                metadata.insert("_routing".into(), value.clone());
            }
            EsDocumentEvent {
                event_kind: kind,
                index: hit
                    .get("_index")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                id: hit
                    .get("_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                timestamp_value,
                sort: hit
                    .get("sort")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default(),
                seq_no: hit.get("_seq_no").and_then(Value::as_i64),
                primary_term: hit.get("_primary_term").and_then(Value::as_i64),
                source,
                metadata,
            }
        })
        .collect()
}

fn extract_path_value<'a>(source: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = source;
    for part in path.split('.') {
        let Value::Object(map) = current else {
            return None;
        };
        current = map.get(part)?;
    }
    Some(current)
}

fn collect_mapping_fields(mapping: &Value, out: &mut Vec<EsFieldDescriptor>) {
    match mapping {
        Value::Object(map) => {
            if let Some(properties) = map.get("properties").and_then(Value::as_object) {
                collect_properties("", properties, out);
            }
            for value in map.values() {
                collect_mapping_fields(value, out);
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_mapping_fields(item, out);
            }
        }
        _ => {}
    }
}

fn collect_properties(
    prefix: &str,
    properties: &Map<String, Value>,
    out: &mut Vec<EsFieldDescriptor>,
) {
    for (name, descriptor) in properties {
        let full_name = if prefix.is_empty() {
            name.clone()
        } else {
            format!("{prefix}.{name}")
        };
        let field_type = descriptor
            .get("type")
            .and_then(Value::as_str)
            .map(str::to_string);
        if field_type.is_some() {
            out.push(EsFieldDescriptor {
                name: full_name.clone(),
                field_type,
            });
        }
        if let Some(children) = descriptor.get("properties").and_then(Value::as_object) {
            collect_properties(&full_name, children, out);
        }
        if let Some(fields) = descriptor.get("fields").and_then(Value::as_object) {
            collect_properties(&full_name, fields, out);
        }
    }
}

fn truncate(text: &str, max: usize) -> String {
    if text.len() <= max {
        return text.to_string();
    }
    format!("{}...", text.chars().take(max).collect::<String>())
}

fn now_ms() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as u64,
        Err(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn mapping_field_discovery_flattens_properties_and_multifields() {
        let mapping = json!({
            "logs": {
                "mappings": {
                    "properties": {
                        "@timestamp": {"type": "date"},
                        "user": {
                            "properties": {
                                "id": {"type": "keyword"}
                            }
                        },
                        "message": {
                            "type": "text",
                            "fields": {
                                "keyword": {"type": "keyword"}
                            }
                        }
                    }
                }
            }
        });
        let mut fields = Vec::new();

        collect_mapping_fields(&mapping, &mut fields);
        fields.sort_by(|left, right| left.name.cmp(&right.name));

        assert!(fields.iter().any(|field| field.name == "@timestamp"));
        assert!(fields.iter().any(|field| field.name == "user.id"));
        assert!(fields.iter().any(|field| field.name == "message"));
        assert!(fields.iter().any(|field| field.name == "message.keyword"));
    }

    #[test]
    fn hit_conversion_preserves_raw_source_and_sort_cursor() {
        let hits = vec![json!({
            "_index": "logs",
            "_id": "doc-1",
            "_seq_no": 3,
            "_primary_term": 1,
            "_source": {
                "@timestamp": "2026-05-08T00:00:00Z",
                "message": "hello"
            },
            "sort": ["2026-05-08T00:00:00Z", "doc-1"]
        })];

        let events = hits_to_events(&hits, EsDocumentEventKind::PollUpdate, "@timestamp");

        assert_eq!(events[0].id, "doc-1");
        assert_eq!(
            events[0].timestamp_value,
            Some(json!("2026-05-08T00:00:00Z"))
        );
        assert_eq!(
            events[0].sort,
            vec![json!("2026-05-08T00:00:00Z"), json!("doc-1")]
        );
        assert_eq!(events[0].source["message"], json!("hello"));
    }

    #[test]
    fn poll_query_combines_user_filter_with_timestamp_floor() {
        let request = EsSourceRequest {
            object_ref: "es-prod".into(),
            index: "logs".into(),
            operation: EsSourceOperation::PollSince,
            limit: None,
            cursor: None,
            timestamp_after: Some(json!("2026-05-08T00:00:00Z")),
            field_filter: Vec::new(),
            query_filter: Some(json!({"term": {"level": "warn"}})),
        };

        let query = poll_query("@timestamp", request.timestamp_after.as_ref(), &request);

        assert_eq!(
            query["bool"]["filter"][1]["range"]["@timestamp"]["gte"],
            json!("2026-05-08T00:00:00Z")
        );
        assert_eq!(query["bool"]["filter"][0]["term"]["level"], json!("warn"));
    }
}

use crate::{GraphEndpoint, GraphObservation, GraphQuery, GraphSourceRequest};
use capanix_app_sdk::{CnxError, Result};
use serde_json::Value;
use sha2::{Digest, Sha256};
use source_kit::{ResolvedCredential, SourceOperationBudget};
use std::{io::Read, time::Duration};

const MAX_GRAPH_STATUS_BYTES: u64 = 1024 * 1024;

pub trait GraphSourceDriver: Send + Sync {
    fn observe(
        &self,
        endpoint: &GraphEndpoint,
        query: Option<&GraphQuery>,
        request: &GraphSourceRequest,
        credential: &ResolvedCredential,
        status_credential: &ResolvedCredential,
        epoch: u64,
        budget: &mut SourceOperationBudget,
    ) -> Result<GraphObservation>;
}
#[derive(Default)]
pub struct NativeGraphSourceDriver;
fn peer() -> CnxError {
    CnxError::PeerError("graph provider request failed (details withheld)".into())
}

impl NativeGraphSourceDriver {
    fn revision(
        &self,
        endpoint: &GraphEndpoint,
        credential: &ResolvedCredential,
        budget: &mut SourceOperationBudget,
    ) -> Result<Option<(String, String)>> {
        let Some(url) = &endpoint.status_url else {
            return Ok(None);
        };
        budget.consume_call(1)?;
        let client = reqwest::blocking::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(budget.remaining_timeout()?.ok_or(CnxError::Timeout)?)
            .build()
            .map_err(|_| peer())?;
        let request = client.get(url);
        let request = match credential {
            ResolvedCredential::Basic { username, password } => {
                request.basic_auth(username, Some(password))
            }
            _ => {
                return Err(CnxError::AccessDenied(
                    "graph requires a basic environment credential".into(),
                ));
            }
        };
        let response = request
            .send()
            .map_err(|_| peer())?
            .error_for_status()
            .map_err(|_| peer())?;
        let bound = budget
            .remaining_bytes()
            .unwrap_or(MAX_GRAPH_STATUS_BYTES)
            .min(MAX_GRAPH_STATUS_BYTES);
        let mut bytes = Vec::new();
        response
            .take(bound + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| peer())?;
        if bytes.len() as u64 > bound {
            return Err(CnxError::ResourceExhausted(
                "graph status max_bytes exhausted".into(),
            ));
        }
        budget.consume_bytes(bytes.len() as u64)?;
        let doc: Value = serde_json::from_slice(&bytes).map_err(|_| peer())?;
        if doc.pointer("/NodeStatus/Healthy") != Some(&Value::Bool(true))
            || doc.pointer("/GremlinServer/Healthy") != Some(&Value::Bool(true))
            || doc.pointer("/Schema/Exists") != Some(&Value::Bool(true))
            || doc.pointer("/Schema/Healthy") != Some(&Value::Bool(true))
        {
            return Err(CnxError::NotReady("graph provider is not healthy".into()));
        }
        let revision = doc
            .pointer("/Schema/ModTime")
            .filter(|value| !value.is_null())
            .ok_or_else(peer)?;
        let revision = revision
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| revision.to_string());
        Ok(Some((
            format!("puppygraph-schema:{revision}"),
            format!("sha256:{:x}", Sha256::digest(&bytes)),
        )))
    }
    fn query(
        &self,
        endpoint: &GraphEndpoint,
        query: &str,
        request: &GraphSourceRequest,
        credential: &ResolvedCredential,
        budget: &mut SourceOperationBudget,
    ) -> Result<(Vec<Value>, bool)> {
        let ResolvedCredential::Basic { username, password } = credential else {
            return Err(CnxError::AccessDenied(
                "graph basic environment credential required".into(),
            ));
        };
        budget.consume_call(1)?;
        let timeout = budget.remaining_timeout()?.ok_or(CnxError::Timeout)?;
        let byte_limit = budget.remaining_bytes().unwrap_or(0);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| peer())?;
        let output = runtime
            .block_on(async {
                tokio::time::timeout(
                    timeout,
                    crate::bolt::query(
                        endpoint, query, request, username, password, timeout, byte_limit,
                    ),
                )
                .await
            })
            .map_err(|_| CnxError::Timeout)?;
        runtime.shutdown_timeout(Duration::from_millis(100));
        let (rows, bytes) = output?;
        budget.consume_bytes(bytes)?;
        Ok((rows, true))
    }
}
impl GraphSourceDriver for NativeGraphSourceDriver {
    fn observe(
        &self,
        endpoint: &GraphEndpoint,
        query: Option<&GraphQuery>,
        request: &GraphSourceRequest,
        credential: &ResolvedCredential,
        status_credential: &ResolvedCredential,
        epoch: u64,
        budget: &mut SourceOperationBudget,
    ) -> Result<GraphObservation> {
        budget.observe_fanout(1)?;
        let calls = if query.is_some() {
            if endpoint.status_url.is_some() { 3 } else { 1 }
        } else {
            1
        };
        budget.ensure_call_capacity(calls, calls)?;
        let before = self.revision(endpoint, status_credential, budget)?;
        let (rows, complete) = if let Some(query) = query {
            self.query(endpoint, &query.statement, request, credential, budget)?
        } else if before.is_none() {
            self.query(
                endpoint,
                "RETURN 1 AS reachable",
                request,
                credential,
                budget,
            )?;
            (vec![], true)
        } else {
            (vec![], true)
        };
        let after = if query.is_some() {
            self.revision(endpoint, status_credential, budget)?
        } else {
            before.clone()
        };
        if before.as_ref().map(|v| &v.0) != after.as_ref().map(|v| &v.0) {
            return Err(CnxError::NotReady(
                "graph provider revision drift; observation rejected".into(),
            ));
        }
        budget.check_deadline()?;
        Ok(GraphObservation {
            fustor_read_only_enforcement: "native-object-query-epoch-parameter-budget-gates".into(),
            backend_account_permissions: "unverified".into(),
            object_ref: endpoint.object_ref.clone(),
            source_revision: after
                .as_ref()
                .map(|v| v.0.clone())
                .unwrap_or_else(|| "unknown".into()),
            provider_revision: after.as_ref().map(|v| v.0.clone()),
            provider_revision_evidence_digest: after.map(|v| v.1),
            configured_query_id: query.map(|q| q.query_id.clone()),
            result_schema_ref: query.map(|q| q.result_schema_ref.clone()),
            rows,
            complete,
            reachable: true,
            grant_epoch: epoch,
            generation_alignment: "unknown".into(),
        })
    }
}

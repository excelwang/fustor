use crate::*;
use capanix_app_sdk::{
    CnxError, Event, Result,
    runtime::{EventMetadata, NodeId},
};
use source_kit::*;
use std::{sync::Arc, time::Instant};

pub struct GraphSourceService {
    control: SourceControlState<GraphSourceRuntimeConfig>,
    driver: Arc<dyn GraphSourceDriver>,
    origin: NodeId,
}
impl GraphSourceService {
    pub fn new(
        config: GraphSourceRuntimeConfig,
        driver: Arc<dyn GraphSourceDriver>,
        origin: NodeId,
    ) -> Self {
        Self::with_control_state(SourceControlState::new(config), driver, origin)
    }
    pub fn with_control_state(
        control: SourceControlState<GraphSourceRuntimeConfig>,
        driver: Arc<dyn GraphSourceDriver>,
        origin: NodeId,
    ) -> Self {
        Self {
            control,
            driver,
            origin,
        }
    }
    fn reply(&self, bytes: Vec<u8>) -> Event {
        Event::new(
            EventMetadata {
                origin_id: self.origin.clone(),
                timestamp_us: source_now_ms() * 1000,
                logical_ts: None,
                correlation_id: None,
                ingress_auth: None,
                trace: None,
            },
            bytes.into(),
        )
    }
    pub fn handle_event(&self, event: Event) -> Event {
        let start = Instant::now();
        let decoded =
            decode_source_request::<GraphSourceRequest>(event.payload_bytes(), source_now_ms());
        let failure =
            |error| GraphSourcePayload::Error(error_payload_from_cnx(error, "graph-transport"));
        let encoded = match decoded {
            Ok(decoded) => {
                let result = self.observe(&decoded);
                encode_accounted_source_result(
                    &decoded,
                    result,
                    start.elapsed().as_millis() as u64,
                    source_now_ms(),
                    failure,
                )
            }
            Err(error) => rmp_serde::to_vec_named(&failure(error))
                .map_err(|_| CnxError::Internal("graph encoding".into())),
        };
        self.reply(encoded.unwrap_or_else(|_| {
            rmp_serde::to_vec_named(&failure(CnxError::Internal(
                "graph response encoding".into(),
            )))
            .expect("serializable error")
        }))
    }
    fn observe(
        &self,
        decoded: &DecodedSourceRequest<GraphSourceRequest>,
    ) -> Result<(GraphSourcePayload, SourceOperationAccounting)> {
        let context = decoded.context.as_ref().ok_or_else(|| {
            crate::config::invalid("graph requires a versioned source execution envelope")
        })?;
        let limits = &context.limits;
        if limits.max_rows > 10000
            || limits.max_bytes > 4194304
            || limits.max_latency_ms > 30000
            || limits.max_calls > 3
            || limits.max_cost_units > 3
            || limits.max_fanout != 1
        {
            return Err(CnxError::ResourceExhausted(
                "graph global inspection hard cap exceeded".into(),
            ));
        }
        let snapshot = self.control.snapshot()?;
        if !snapshot.fetch_route_active {
            return Err(CnxError::NotReady("graph fetch route inactive".into()));
        }
        let request = &decoded.request;
        if request.cursor.is_some() || request.limit.is_none() {
            return Err(crate::config::invalid(
                "graph requires an explicit limit and no continuation",
            ));
        }
        validate_source_request_limit(Some(context), request.limit)?;
        let (endpoint, grant) = snapshot
            .config
            .endpoint(&request.object_ref, context.grant_epoch)?;
        let query = match request.operation {
            GraphOperation::ProbeConnection => None,
            GraphOperation::ConfiguredQuery => {
                let query = endpoint
                    .queries
                    .iter()
                    .find(|query| {
                        Some(&query.query_id) == request.configured_query_id.as_ref()
                            && grant.query_scopes.contains(&query.query_id)
                    })
                    .ok_or_else(|| {
                        CnxError::ScopeDenied("graph configured query not granted".into())
                    })?;
                if request.expected_result_schema_ref.as_ref() != Some(&query.result_schema_ref) {
                    return Err(crate::config::invalid("graph result schema mismatch"));
                }
                crate::config::validate_parameters(&request.parameters, query)?;
                let allocated = &context.limits;
                let cap = &query.limits;
                if allocated.max_rows > cap.max_rows
                    || allocated.max_bytes > cap.max_bytes
                    || allocated.max_calls > cap.max_calls
                    || allocated.max_fanout > cap.max_fanout
                    || allocated.max_latency_ms > cap.max_latency_ms
                    || allocated.max_cost_units > cap.max_cost_units
                {
                    return Err(CnxError::ResourceExhausted(
                        "graph configured hard cap exceeded".into(),
                    ));
                }
                Some(query)
            }
        };
        let credential =
            resolve_credential(&snapshot.config.credentials, Some(&endpoint.credential_ref))?;
        let status_credential = resolve_credential(
            &snapshot.config.credentials,
            Some(
                endpoint
                    .status_credential_ref
                    .as_deref()
                    .unwrap_or(&endpoint.credential_ref),
            ),
        )?;
        let mut budget = SourceOperationBudget::from_context(Some(context), source_now_ms())?;
        let value = self.driver.observe(
            endpoint,
            query,
            request,
            &credential,
            &status_credential,
            grant.grant_epoch,
            &mut budget,
        )?;
        let after = self.control.snapshot()?;
        if !after.fetch_route_active || after.control_version != snapshot.control_version {
            return Err(CnxError::AccessDenied(
                "graph grant/control changed during observation".into(),
            ));
        }
        after
            .config
            .endpoint(&request.object_ref, context.grant_epoch)?;
        let payload = if query.is_some() {
            GraphSourcePayload::Rows(value)
        } else {
            GraphSourcePayload::Probe(value)
        };
        Ok((payload, budget.accounting()))
    }
    pub fn handle_status_events(
        &self,
        events: Vec<Event>,
        _fetch: &str,
        _status: &str,
    ) -> Vec<Event> {
        let state = self.control.snapshot();
        let mut status = match state {
            Ok(state) => {
                let mut status = SourceRuntimeStatus::new(
                    "graph",
                    state.config.endpoints.len(),
                    state
                        .config
                        .grants
                        .iter()
                        .filter(|grant| grant.grant_state == "active")
                        .count(),
                    vec![
                        "configured read-only queries; one-shot; generation alignment not implied"
                            .into(),
                    ],
                );
                status.ready &= state.fetch_route_active && state.status_route_active;
                status
            }
            Err(_) => SourceRuntimeStatus::new("graph", 0, 0, vec!["control unavailable".into()]),
        };
        status.capabilities = SourceCapabilityReport::new(
            "graph",
            vec![
                SourceOperationCapability::Probe,
                SourceOperationCapability::BoundedSnapshot,
            ],
            SourceConsistencyCapability::StatelessPage,
            SourceContinuationCapability::None,
        );
        events
            .into_iter()
            .map(|_| {
                self.reply(
                    rmp_serde::to_vec_named(&GraphSourcePayload::Status(status.clone()))
                        .expect("serializable status"),
                )
            })
            .collect()
    }
}

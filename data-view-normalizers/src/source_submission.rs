use std::collections::BTreeSet;

use data_view::{
    ActionExecutionContext, ActionResultSubmission, DATA_VIEW_WIRE_VERSION, DataViewContractError,
    DerivationEvidence, FreshnessState, NativeAnchor, PayloadEncoding, ProducerKind,
    SchemaVersionRef, SourceAuthorityRef,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use source_kit::{
    DecodedSourceResponse, SourcePayloadMetadata, decode_source_response,
    source_continuation_digest,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceActionSubmissionContext {
    pub submission_id: String,
    pub execution: ActionExecutionContext,
    pub source_authority_ref: SourceAuthorityRef,
    pub producer_id: String,
    pub schema: SchemaVersionRef,
    pub policy_labels: BTreeSet<String>,
    pub freshness: FreshnessState,
    pub derivation: DerivationEvidence,
}

/// Dispatch only registered builtin schemas; the original wire carrier is never
/// reconstructed from its JSON presentation and native receipt checks are shared.
pub fn map_builtin_source_action_submission(
    context: SourceActionSubmissionContext,
    encoded_response: Vec<u8>,
) -> Result<ActionResultSubmission, DataViewContractError> {
    if context.schema.schema_version != crate::BUILTIN_SCHEMA_VERSION {
        return Err(DataViewContractError::UnsupportedSchema);
    }
    match context.schema.schema_ref.as_str() {
        crate::PG_SCHEMA_REF => map_source_action_submission::<pg_source::PgSourcePayload>(
            context,
            "pg",
            encoded_response,
        ),
        crate::MYSQL_SCHEMA_REF => {
            map_source_action_submission::<mysql_source::MysqlSourcePayload>(
                context,
                "mysql",
                encoded_response,
            )
        }
        crate::ES_SCHEMA_REF => map_source_action_submission::<es_source::EsSourcePayload>(
            context,
            "elasticsearch",
            encoded_response,
        ),
        crate::S3_SCHEMA_REF => map_source_action_submission::<s3_source::S3SourcePayload>(
            context,
            "s3",
            encoded_response,
        ),
        crate::LOCAL_FS_SCHEMA_REF => map_source_action_submission::<
            local_fs_source::LocalFsSourcePayload,
        >(context, "local-fs", encoded_response),
        crate::CSV_SCHEMA_REF => map_source_action_submission::<csv_source::CsvSourcePayload>(
            context,
            "csv",
            encoded_response,
        ),
        crate::JSONL_SCHEMA_REF => {
            map_source_action_submission::<jsonl_source::JsonlSourcePayload>(
                context,
                "jsonl",
                encoded_response,
            )
        }
        crate::GRAPH_SCHEMA_REF => {
            map_source_action_submission::<graph_source::GraphSourcePayload>(
                context,
                "graph",
                encoded_response,
            )
        }
        _ => Err(DataViewContractError::UnsupportedSchema),
    }
}

pub fn map_source_action_submission<P>(
    context: SourceActionSubmissionContext,
    expected_source_kind: &str,
    encoded_response: Vec<u8>,
) -> Result<ActionResultSubmission, DataViewContractError>
where
    P: DeserializeOwned + Serialize + SourcePayloadMetadata,
{
    if context.submission_id.trim().is_empty()
        || context.producer_id.trim().is_empty()
        || expected_source_kind.trim().is_empty()
    {
        return Err(DataViewContractError::InvalidContract(
            "source action submission context",
        ));
    }
    context.schema.validate()?;
    let DecodedSourceResponse::Envelope(response) = decode_source_response::<P>(&encoded_response)
        .map_err(|_| DataViewContractError::InvalidContract("source response envelope"))?
    else {
        return Err(DataViewContractError::InvalidContract(
            "legacy source payload is not materializable",
        ));
    };
    let receipt = response
        .receipt
        .ok_or(DataViewContractError::InvalidContract(
            "source observation receipt",
        ))?;
    let observation =
        response
            .payload
            .source_observation()
            .ok_or(DataViewContractError::InvalidContract(
                "source payload observation",
            ))?;
    let continuation_digest = observation
        .continuation
        .as_deref()
        .map(source_continuation_digest);
    if response.request_id != context.execution.execution_id
        || receipt.source_kind != expected_source_kind
        || receipt.authority_object_ref != observation.authority_object_ref
        || receipt.native_anchors != observation.native_anchors
        || receipt.grant_epoch != context.execution.grant_epoch
        || observation
            .grant_epoch
            .is_some_and(|grant_epoch| grant_epoch != receipt.grant_epoch)
        || receipt.observed_at_ms < context.execution.issued_at_ms
        || receipt.observed_at_ms > context.execution.expires_at_ms
        || receipt.continuation_state != observation.continuation_state
        || receipt.continuation_digest != continuation_digest
        || receipt.actual.rows != observation.actual_rows
        || receipt.actual.bytes != encoded_response.len() as u64
    {
        return Err(DataViewContractError::InvalidContract(
            "source observation receipt context",
        ));
    }
    Ok(ActionResultSubmission {
        wire_version: DATA_VIEW_WIRE_VERSION,
        submission_id: context.submission_id,
        execution: context.execution,
        source_authority_ref: context.source_authority_ref,
        authority_object_ref: receipt.authority_object_ref,
        producer_id: context.producer_id,
        producer_kind: ProducerKind::Source,
        projection_evidence: None,
        schema: context.schema,
        encoding: PayloadEncoding::MessagePack,
        payload: encoded_response,
        native_anchors: receipt
            .native_anchors
            .into_iter()
            .map(|anchor| NativeAnchor {
                anchor_kind: anchor.anchor_kind,
                native_ref: anchor.native_ref,
                native_pointer: anchor.native_pointer,
                native_version: anchor.native_version,
            })
            .collect(),
        source_revision: receipt.source_revision,
        evidence_refs: receipt.evidence_refs,
        observed_at_ms: receipt.observed_at_ms,
        policy_labels: context.policy_labels,
        grant_epoch: receipt.grant_epoch,
        freshness: context.freshness,
        derivation: context.derivation,
        continuation: observation.continuation,
        actual_rows: receipt.actual.rows,
        actual_bytes: receipt.actual.bytes,
        actual_calls: receipt.actual.calls,
        actual_fanout: receipt.actual.fanout,
        actual_latency_ms: receipt.actual.latency_ms,
        actual_cost_units: receipt.actual.cost_units,
    })
}

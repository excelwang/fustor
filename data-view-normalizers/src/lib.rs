#![forbid(unsafe_code)]

mod fs_meta_mapping;
mod source_submission;

use std::collections::BTreeMap;
use std::sync::Arc;

use data_view::{
    ActionResultSubmission, CanonicalIdentity, DataViewContractError, NativeAnchor,
    NormalizedDataViewCommit, PayloadEncoding, RelationshipAssertion, RelationshipConfidence,
    ResultNormalizer, ResultNormalizerRegistry, SchemaVersionRef, SourceAuthorityRef,
    StreamingRefreshEntry, TrackedResultRef,
};
use ring::digest::{SHA256, digest};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};

pub use fs_meta_mapping::*;
pub use source_submission::*;

pub const PG_SCHEMA_REF: &str = "fustor.pg.source-payload";
pub const GRAPH_SCHEMA_REF: &str = "fustor.graph.source-payload";
pub const MYSQL_SCHEMA_REF: &str = "fustor.mysql.source-payload";
pub const S3_SCHEMA_REF: &str = "fustor.s3.source-payload";
pub const ES_SCHEMA_REF: &str = "fustor.es.source-payload";
pub const LOCAL_FS_SCHEMA_REF: &str = "fustor.local-fs.source-payload";
pub const CSV_SCHEMA_REF: &str = "fustor.csv.source-payload";
pub const JSONL_SCHEMA_REF: &str = "fustor.jsonl.source-payload";
pub const FS_META_DIRECT_SCHEMA_REF: &str = "fustor.fs-meta.force-find";
pub const FS_META_PROJECTION_SCHEMA_REF: &str = "fustor.fs-meta.materialized";
pub const BUILTIN_SCHEMA_VERSION: &str = "1";

pub fn register_builtin_normalizers(
    registry: &mut ResultNormalizerRegistry,
) -> Result<(), DataViewContractError> {
    registry.register(Arc::new(PgResultNormalizer::default()))?;
    registry.register(Arc::new(GraphResultNormalizer::default()))?;
    registry.register(Arc::new(MysqlResultNormalizer::default()))?;
    registry.register(Arc::new(S3ResultNormalizer::default()))?;
    registry.register(Arc::new(EsResultNormalizer::default()))?;
    registry.register(Arc::new(LocalFsResultNormalizer::default()))?;
    registry.register(Arc::new(CsvResultNormalizer::default()))?;
    registry.register(Arc::new(JsonlResultNormalizer::default()))?;
    registry.register(Arc::new(FsMetaResultNormalizer::direct()))?;
    registry.register(Arc::new(FsMetaResultNormalizer::projection_backed()))?;
    Ok(())
}

#[derive(Debug, Clone)]
pub struct GraphResultNormalizer { schema: SchemaVersionRef }
impl Default for GraphResultNormalizer {
    fn default() -> Self { Self { schema: schema(GRAPH_SCHEMA_REF) } }
}
impl ResultNormalizer for GraphResultNormalizer {
    fn schema(&self) -> &SchemaVersionRef { &self.schema }
    fn normalize_action(&self, submission: &ActionResultSubmission) -> Result<NormalizedDataViewCommit, DataViewContractError> {
        let payload: graph_source::GraphSourcePayload = decode_source(submission, "graph")?;
        let observation = match payload {
            graph_source::GraphSourcePayload::Rows(value) | graph_source::GraphSourcePayload::Probe(value) => value,
            _ => return Err(DataViewContractError::InvalidContract("graph observation payload")),
        };
        let mut observed = identity(&submission.source_authority_ref, &self.schema, "graph-query-observation",
            &format!("{}:{}", observation.object_ref, observation.configured_query_id.as_deref().unwrap_or("probe")),
            json!({ "query_id":observation.configured_query_id, "result_schema_ref":observation.result_schema_ref,
                "row_count":observation.rows.len(), "provider_revision":observation.provider_revision,
                "provider_revision_evidence_digest":observation.provider_revision_evidence_digest, "generation_alignment":"unknown" }));
        observed.native_anchors = submission.native_anchors.clone();
        normalized(submission, vec![observed], vec![])
    }
    fn normalize_stream_refresh(&self, _: &SourceAuthorityRef, _: &TrackedResultRef, _: &StreamingRefreshEntry) -> Result<NormalizedDataViewCommit, DataViewContractError> {
        Err(DataViewContractError::InvalidContract("graph one-shot source has no streaming refresh"))
    }
}

#[derive(Debug, Clone)]
pub struct MysqlResultNormalizer {
    schema: SchemaVersionRef,
}

impl Default for MysqlResultNormalizer {
    fn default() -> Self {
        Self {
            schema: schema(MYSQL_SCHEMA_REF),
        }
    }
}

impl ResultNormalizer for MysqlResultNormalizer {
    fn schema(&self) -> &SchemaVersionRef {
        &self.schema
    }

    fn normalize_action(
        &self,
        submission: &ActionResultSubmission,
    ) -> Result<NormalizedDataViewCommit, DataViewContractError> {
        let payload: mysql_source::MysqlSourcePayload = decode_source(submission, "mysql")?;
        let mut identities = Vec::new();
        let mut relationships = Vec::new();
        match payload {
            mysql_source::MysqlSourcePayload::Rows(batch) => identities.push(identity(
                &submission.source_authority_ref,
                &self.schema,
                "mysql-table",
                &batch.object_ref,
                json!({
                    "schema": batch.schema,
                    "table": batch.table,
                    "fields": batch.fields,
                    "row_count": batch.rows.len(),
                    "has_more": batch.has_more,
                }),
            )),
            mysql_source::MysqlSourcePayload::Fields(catalog) => {
                let table_ref = catalog.object_ref.clone();
                let table = identity(
                    &submission.source_authority_ref,
                    &self.schema,
                    "mysql-table",
                    &table_ref,
                    json!({"field_count": catalog.fields.len()}),
                );
                for field in catalog.fields {
                    let native_ref = format!("{}.{}.{}", field.schema, field.table, field.column);
                    let field_identity = identity(
                        &submission.source_authority_ref,
                        &self.schema,
                        "mysql-column",
                        &native_ref,
                        json!({
                            "schema": field.schema,
                            "table": field.table,
                            "column": field.column,
                            "ordinal_position": field.ordinal_position,
                            "column_type": field.column_type,
                        }),
                    );
                    relationships.push(relationship(
                        &table,
                        &field_identity,
                        "has-field",
                        submission,
                    ));
                    identities.push(field_identity);
                }
                identities.push(table);
            }
            mysql_source::MysqlSourcePayload::Probe(probe) => identities.push(identity(
                &submission.source_authority_ref,
                &self.schema,
                "mysql-source",
                &probe.object_ref,
                json!({
                    "endpoint_uri": probe.endpoint_uri,
                    "reachable": probe.reachable,
                    "server_version": probe.server_version,
                }),
            )),
            mysql_source::MysqlSourcePayload::Status(_)
            | mysql_source::MysqlSourcePayload::Error(_) => {
                return Err(DataViewContractError::InvalidContract(
                    "non-materializable mysql payload",
                ));
            }
        }
        normalized(submission, identities, relationships)
    }

    fn normalize_stream_refresh(
        &self,
        source_authority_ref: &SourceAuthorityRef,
        tracked_result_ref: &TrackedResultRef,
        entry: &StreamingRefreshEntry,
    ) -> Result<NormalizedDataViewCommit, DataViewContractError> {
        normalize_stream_identity(
            &self.schema,
            "mysql-result",
            source_authority_ref,
            tracked_result_ref,
            entry,
        )
    }
}

#[derive(Debug, Clone)]
pub struct PgResultNormalizer {
    schema: SchemaVersionRef,
}

impl Default for PgResultNormalizer {
    fn default() -> Self {
        Self {
            schema: schema(PG_SCHEMA_REF),
        }
    }
}

impl ResultNormalizer for PgResultNormalizer {
    fn schema(&self) -> &SchemaVersionRef {
        &self.schema
    }

    fn normalize_action(
        &self,
        submission: &ActionResultSubmission,
    ) -> Result<NormalizedDataViewCommit, DataViewContractError> {
        let payload: pg_source::PgSourcePayload = decode_source(submission, "pg")?;
        let mut identities = Vec::new();
        let mut relationships = Vec::new();
        match payload {
            pg_source::PgSourcePayload::Catalog(catalog) => {
                identities.push(identity(
                    &submission.source_authority_ref,
                    &self.schema,
                    "pg-catalog",
                    &catalog.object_ref,
                    json!({"database": catalog.database, "database_oid": catalog.database_oid,
                        "system_identifier": catalog.system_identifier, "complete": catalog.complete,
                        "read_only": catalog.read_only, "objects": catalog.objects}),
                ));
            }
            pg_source::PgSourcePayload::Rows(batch) => {
                identities.push(identity(
                    &submission.source_authority_ref,
                    &self.schema,
                    "pg-table",
                    &batch.object_ref,
                    json!({
                        "schema": batch.schema,
                        "table": batch.table,
                        "fields": batch.fields,
                        "row_count": batch.rows.len(),
                        "has_more": batch.has_more,
                    }),
                ));
            }
            pg_source::PgSourcePayload::Fields(catalog) => {
                let table_ref = catalog.object_ref.clone();
                let table = identity(
                    &submission.source_authority_ref,
                    &self.schema,
                    "pg-table",
                    &table_ref,
                    json!({"field_count": catalog.fields.len()}),
                );
                for field in catalog.fields {
                    let native_ref = format!("{}.{}.{}", field.schema, field.table, field.column);
                    let field_identity = identity(
                        &submission.source_authority_ref,
                        &self.schema,
                        "pg-column",
                        &native_ref,
                        json!({
                            "schema": field.schema,
                            "table": field.table,
                            "column": field.column,
                            "ordinal_position": field.ordinal_position,
                            "column_type": field.column_type,
                        }),
                    );
                    relationships.push(relationship(
                        &table,
                        &field_identity,
                        "has-field",
                        submission,
                    ));
                    identities.push(field_identity);
                }
                identities.push(table);
            }
            pg_source::PgSourcePayload::Probe(probe) => identities.push(identity(
                &submission.source_authority_ref,
                &self.schema,
                "pg-source",
                &probe.object_ref,
                json!({
                    "endpoint_uri": probe.endpoint_uri,
                    "reachable": probe.reachable,
                    "server_version": probe.server_version,
                }),
            )),
            pg_source::PgSourcePayload::Status(_) | pg_source::PgSourcePayload::Error(_) => {
                return Err(DataViewContractError::InvalidContract(
                    "non-materializable pg payload",
                ));
            }
        }
        normalized(submission, identities, relationships)
    }

    fn normalize_stream_refresh(
        &self,
        source_authority_ref: &SourceAuthorityRef,
        tracked_result_ref: &TrackedResultRef,
        entry: &StreamingRefreshEntry,
    ) -> Result<NormalizedDataViewCommit, DataViewContractError> {
        normalize_stream_identity(
            &self.schema,
            "pg-result",
            source_authority_ref,
            tracked_result_ref,
            entry,
        )
    }
}

#[derive(Debug, Clone)]
pub struct S3ResultNormalizer {
    schema: SchemaVersionRef,
}

impl Default for S3ResultNormalizer {
    fn default() -> Self {
        Self {
            schema: schema(S3_SCHEMA_REF),
        }
    }
}

impl ResultNormalizer for S3ResultNormalizer {
    fn schema(&self) -> &SchemaVersionRef {
        &self.schema
    }

    fn normalize_action(
        &self,
        submission: &ActionResultSubmission,
    ) -> Result<NormalizedDataViewCommit, DataViewContractError> {
        let payload: s3_source::S3SourcePayload = decode_source(submission, "s3")?;
        let mut identities = Vec::new();
        let mut relationships = Vec::new();
        match payload {
            s3_source::S3SourcePayload::Content(sample) => identities.push(identity(
                &submission.source_authority_ref, &self.schema, "s3-content-sample", &sample.native_ref,
                json!({"format": sample.format, "row_count": sample.rows.len(), "prefix_sha256": sample.prefix_sha256,
                    "bytes_read": sample.bytes_read, "complete": sample.complete}),
            )),
            s3_source::S3SourcePayload::Objects(batch) => {
                let scope_ref = format!("s3://{}/{}", batch.bucket, batch.prefix);
                let scope = identity(
                    &submission.source_authority_ref,
                    &self.schema,
                    "s3-prefix",
                    &scope_ref,
                    json!({
                        "bucket": batch.bucket,
                        "prefix": batch.prefix,
                        "object_count": batch.objects.len(),
                        "has_more": batch.has_more,
                    }),
                );
                for object in batch.objects {
                    let native_ref = format!("s3://{}/{}", batch.bucket, object.key);
                    let object_identity = identity(
                        &submission.source_authority_ref,
                        &self.schema,
                        "s3-object",
                        &native_ref,
                        json!({
                            "key": object.key,
                            "size": object.size,
                            "last_modified_unix_ms": object.last_modified_unix_ms,
                            "etag": object.etag,
                            "storage_class": object.storage_class,
                            "metadata": object.metadata,
                        }),
                    );
                    relationships.push(relationship(
                        &scope,
                        &object_identity,
                        "contains",
                        submission,
                    ));
                    identities.push(object_identity);
                }
                identities.push(scope);
            }
            s3_source::S3SourcePayload::Fields(catalog) => {
                let object = identity(
                    &submission.source_authority_ref,
                    &self.schema,
                    "s3-object-schema",
                    &catalog.object_ref,
                    json!({"field_count": catalog.fields.len()}),
                );
                for field in catalog.fields {
                    let native_ref = format!("{}#{}", catalog.object_ref, field.name);
                    let field_identity = identity(
                        &submission.source_authority_ref,
                        &self.schema,
                        "s3-field",
                        &native_ref,
                        json!({"name": field.name, "field_type": field.field_type}),
                    );
                    relationships.push(relationship(
                        &object,
                        &field_identity,
                        "has-field",
                        submission,
                    ));
                    identities.push(field_identity);
                }
                identities.push(object);
            }
            s3_source::S3SourcePayload::Probe(probe) => identities.push(identity(
                &submission.source_authority_ref,
                &self.schema,
                "s3-source",
                &probe.object_ref,
                json!({
                    "endpoint_uri": probe.endpoint_uri,
                    "bucket": probe.bucket,
                    "reachable": probe.reachable,
                }),
            )),
            s3_source::S3SourcePayload::Status(_) | s3_source::S3SourcePayload::Error(_) => {
                return Err(DataViewContractError::InvalidContract(
                    "non-materializable s3 payload",
                ));
            }
        }
        normalized(submission, identities, relationships)
    }

    fn normalize_stream_refresh(
        &self,
        source_authority_ref: &SourceAuthorityRef,
        tracked_result_ref: &TrackedResultRef,
        entry: &StreamingRefreshEntry,
    ) -> Result<NormalizedDataViewCommit, DataViewContractError> {
        normalize_stream_identity(
            &self.schema,
            "s3-result",
            source_authority_ref,
            tracked_result_ref,
            entry,
        )
    }
}

#[derive(Debug, Clone)]
pub struct EsResultNormalizer {
    schema: SchemaVersionRef,
}

impl Default for EsResultNormalizer {
    fn default() -> Self {
        Self {
            schema: schema(ES_SCHEMA_REF),
        }
    }
}

impl ResultNormalizer for EsResultNormalizer {
    fn schema(&self) -> &SchemaVersionRef {
        &self.schema
    }

    fn normalize_action(
        &self,
        submission: &ActionResultSubmission,
    ) -> Result<NormalizedDataViewCommit, DataViewContractError> {
        let payload: es_source::EsSourcePayload = decode_source(submission, "elasticsearch")?;
        let mut identities = Vec::new();
        let mut relationships = Vec::new();
        match payload {
            es_source::EsSourcePayload::Batch(batch) => {
                let index = identity(
                    &submission.source_authority_ref,
                    &self.schema,
                    "es-index",
                    &batch.object_ref,
                    json!({
                        "index": batch.index,
                        "document_count": batch.documents.len(),
                        "has_more": batch.has_more,
                    }),
                );
                for document in batch.documents {
                    let native_ref = format!("{}/{}", document.index, document.id);
                    let document_identity = identity(
                        &submission.source_authority_ref,
                        &self.schema,
                        "es-document",
                        &native_ref,
                        json!({
                            "index": document.index,
                            "id": document.id,
                            "seq_no": document.seq_no,
                            "primary_term": document.primary_term,
                            "timestamp_value": document.timestamp_value,
                            "metadata": document.metadata,
                        }),
                    );
                    relationships.push(relationship(
                        &index,
                        &document_identity,
                        "contains",
                        submission,
                    ));
                    identities.push(document_identity);
                }
                identities.push(index);
            }
            es_source::EsSourcePayload::Fields(catalog) => {
                let index = identity(
                    &submission.source_authority_ref,
                    &self.schema,
                    "es-index",
                    &catalog.object_ref,
                    json!({"index": catalog.index, "field_count": catalog.fields.len()}),
                );
                for field in catalog.fields {
                    let native_ref = format!("{}#{}", catalog.object_ref, field.name);
                    let field_identity = identity(
                        &submission.source_authority_ref,
                        &self.schema,
                        "es-field",
                        &native_ref,
                        json!({"name": field.name, "field_type": field.field_type}),
                    );
                    relationships.push(relationship(
                        &index,
                        &field_identity,
                        "has-field",
                        submission,
                    ));
                    identities.push(field_identity);
                }
                identities.push(index);
            }
            es_source::EsSourcePayload::Probe(probe) => identities.push(identity(
                &submission.source_authority_ref,
                &self.schema,
                "es-source",
                &probe.object_ref,
                json!({
                    "endpoint_uri": probe.endpoint_uri,
                    "reachable": probe.reachable,
                    "cluster_name": probe.cluster_name,
                }),
            )),
            es_source::EsSourcePayload::Status(_) | es_source::EsSourcePayload::Error(_) => {
                return Err(DataViewContractError::InvalidContract(
                    "non-materializable es payload",
                ));
            }
        }
        normalized(submission, identities, relationships)
    }

    fn normalize_stream_refresh(
        &self,
        source_authority_ref: &SourceAuthorityRef,
        tracked_result_ref: &TrackedResultRef,
        entry: &StreamingRefreshEntry,
    ) -> Result<NormalizedDataViewCommit, DataViewContractError> {
        normalize_stream_identity(
            &self.schema,
            "es-result",
            source_authority_ref,
            tracked_result_ref,
            entry,
        )
    }
}

#[derive(Debug, Clone)]
pub struct LocalFsResultNormalizer {
    schema: SchemaVersionRef,
}

impl Default for LocalFsResultNormalizer {
    fn default() -> Self {
        Self {
            schema: schema(LOCAL_FS_SCHEMA_REF),
        }
    }
}

impl ResultNormalizer for LocalFsResultNormalizer {
    fn schema(&self) -> &SchemaVersionRef {
        &self.schema
    }

    fn normalize_action(
        &self,
        submission: &ActionResultSubmission,
    ) -> Result<NormalizedDataViewCommit, DataViewContractError> {
        let payload: local_fs_source::LocalFsSourcePayload = decode_source(submission, "local-fs")?;
        let mut identities = Vec::new();
        let mut relationships = Vec::new();
        match payload {
            local_fs_source::LocalFsSourcePayload::Content(sample) => identities.push(identity(
                &submission.source_authority_ref, &self.schema, "local-fs-content-sample", &sample.native_ref,
                json!({"format": sample.format, "row_count": sample.rows.len(), "prefix_sha256": sample.prefix_sha256,
                    "bytes_read": sample.bytes_read, "complete": sample.complete}),
            )),
            local_fs_source::LocalFsSourcePayload::Entries(batch) => {
                let scope_ref = join_native_path(&batch.root_path, &batch.path);
                let scope = identity(
                    &submission.source_authority_ref,
                    &self.schema,
                    "local-fs-scope",
                    &scope_ref,
                    json!({
                        "root_path": batch.root_path,
                        "path": batch.path,
                        "entry_count": batch.entries.len(),
                        "has_more": batch.has_more,
                    }),
                );
                for entry in batch.entries {
                    let entry_identity = identity(
                        &submission.source_authority_ref,
                        &self.schema,
                        if entry.is_dir {
                            "local-fs-directory"
                        } else {
                            "local-fs-file"
                        },
                        &entry.path,
                        json!({
                            "file_name": entry.file_name,
                            "is_dir": entry.is_dir,
                            "size": entry.size,
                            "modified_unix_ms": entry.modified_unix_ms,
                        }),
                    );
                    relationships.push(relationship(
                        &scope,
                        &entry_identity,
                        "contains",
                        submission,
                    ));
                    identities.push(entry_identity);
                }
                identities.push(scope);
            }
            local_fs_source::LocalFsSourcePayload::Fields(catalog) => {
                let source = identity(
                    &submission.source_authority_ref,
                    &self.schema,
                    "local-fs-schema",
                    &catalog.object_ref,
                    json!({"field_count": catalog.fields.len()}),
                );
                for field in catalog.fields {
                    let native_ref = format!("{}#{}", catalog.object_ref, field.name);
                    let field_identity = identity(
                        &submission.source_authority_ref,
                        &self.schema,
                        "local-fs-field",
                        &native_ref,
                        json!({"name": field.name, "field_type": field.field_type}),
                    );
                    relationships.push(relationship(
                        &source,
                        &field_identity,
                        "has-field",
                        submission,
                    ));
                    identities.push(field_identity);
                }
                identities.push(source);
            }
            local_fs_source::LocalFsSourcePayload::Probe(probe) => identities.push(identity(
                &submission.source_authority_ref,
                &self.schema,
                "local-fs-source",
                &probe.object_ref,
                json!({
                    "root_path": probe.root_path,
                    "reachable": probe.reachable,
                }),
            )),
            local_fs_source::LocalFsSourcePayload::Status(_)
            | local_fs_source::LocalFsSourcePayload::Error(_) => {
                return Err(DataViewContractError::InvalidContract(
                    "non-materializable local-fs payload",
                ));
            }
        }
        normalized(submission, identities, relationships)
    }

    fn normalize_stream_refresh(
        &self,
        source_authority_ref: &SourceAuthorityRef,
        tracked_result_ref: &TrackedResultRef,
        entry: &StreamingRefreshEntry,
    ) -> Result<NormalizedDataViewCommit, DataViewContractError> {
        normalize_stream_identity(
            &self.schema,
            "local-fs-result",
            source_authority_ref,
            tracked_result_ref,
            entry,
        )
    }
}

#[derive(Debug, Clone)]
pub struct CsvResultNormalizer {
    schema: SchemaVersionRef,
}

impl Default for CsvResultNormalizer {
    fn default() -> Self {
        Self {
            schema: schema(CSV_SCHEMA_REF),
        }
    }
}

impl ResultNormalizer for CsvResultNormalizer {
    fn schema(&self) -> &SchemaVersionRef {
        &self.schema
    }

    fn normalize_action(
        &self,
        submission: &ActionResultSubmission,
    ) -> Result<NormalizedDataViewCommit, DataViewContractError> {
        let payload: csv_source::CsvSourcePayload = decode_source(submission, "csv")?;
        let mut identities = Vec::new();
        let mut relationships = Vec::new();
        match payload {
            csv_source::CsvSourcePayload::Records(batch) => {
                let file_ref = join_native_path(&batch.root_path, &batch.path);
                let file = identity(
                    &submission.source_authority_ref,
                    &self.schema,
                    "csv-file",
                    &file_ref,
                    json!({
                        "root_path": batch.root_path,
                        "path": batch.path,
                        "fields": batch.fields,
                        "record_count": batch.records.len(),
                        "has_more": batch.has_more,
                    }),
                );
                for record in batch.records {
                    let native_ref = format!("{file_ref}#row:{}", record.row_number);
                    let record_identity = identity(
                        &submission.source_authority_ref,
                        &self.schema,
                        "csv-record",
                        &native_ref,
                        json!({
                            "row_number": record.row_number,
                            "values": record.values,
                        }),
                    );
                    relationships.push(relationship(
                        &file,
                        &record_identity,
                        "contains-record",
                        submission,
                    ));
                    identities.push(record_identity);
                }
                identities.push(file);
            }
            csv_source::CsvSourcePayload::Fields(catalog) => {
                let file = identity(
                    &submission.source_authority_ref,
                    &self.schema,
                    "csv-file",
                    &catalog.path,
                    json!({"field_count": catalog.fields.len()}),
                );
                for field in catalog.fields {
                    let native_ref = format!("{}#{}", catalog.path, field.name);
                    let field_identity = identity(
                        &submission.source_authority_ref,
                        &self.schema,
                        "csv-field",
                        &native_ref,
                        json!({"name": field.name, "field_type": field.field_type}),
                    );
                    relationships.push(relationship(
                        &file,
                        &field_identity,
                        "has-field",
                        submission,
                    ));
                    identities.push(field_identity);
                }
                identities.push(file);
            }
            csv_source::CsvSourcePayload::Probe(probe) => identities.push(identity(
                &submission.source_authority_ref,
                &self.schema,
                "csv-source",
                &probe.object_ref,
                json!({
                    "root_path": probe.root_path,
                    "reachable": probe.reachable,
                }),
            )),
            csv_source::CsvSourcePayload::Status(_) | csv_source::CsvSourcePayload::Error(_) => {
                return Err(DataViewContractError::InvalidContract(
                    "non-materializable csv payload",
                ));
            }
        }
        normalized(submission, identities, relationships)
    }

    fn normalize_stream_refresh(
        &self,
        source_authority_ref: &SourceAuthorityRef,
        tracked_result_ref: &TrackedResultRef,
        entry: &StreamingRefreshEntry,
    ) -> Result<NormalizedDataViewCommit, DataViewContractError> {
        normalize_stream_identity(
            &self.schema,
            "csv-result",
            source_authority_ref,
            tracked_result_ref,
            entry,
        )
    }
}

#[derive(Debug, Clone)]
pub struct JsonlResultNormalizer {
    schema: SchemaVersionRef,
}

impl Default for JsonlResultNormalizer {
    fn default() -> Self {
        Self {
            schema: schema(JSONL_SCHEMA_REF),
        }
    }
}

impl ResultNormalizer for JsonlResultNormalizer {
    fn schema(&self) -> &SchemaVersionRef {
        &self.schema
    }

    fn normalize_action(
        &self,
        submission: &ActionResultSubmission,
    ) -> Result<NormalizedDataViewCommit, DataViewContractError> {
        let payload: jsonl_source::JsonlSourcePayload = decode_source(submission, "jsonl")?;
        let mut identities = Vec::new();
        let mut relationships = Vec::new();
        match payload {
            jsonl_source::JsonlSourcePayload::Records(batch) => {
                let file_ref = join_native_path(&batch.root_path, &batch.path);
                let file = identity(
                    &submission.source_authority_ref,
                    &self.schema,
                    "jsonl-file",
                    &file_ref,
                    json!({
                        "root_path": batch.root_path,
                        "path": batch.path,
                        "record_count": batch.records.len(),
                        "has_more": batch.has_more,
                    }),
                );
                for record in batch.records {
                    let native_ref = format!("{file_ref}#line:{}", record.line_number);
                    let record_identity = identity(
                        &submission.source_authority_ref,
                        &self.schema,
                        "jsonl-record",
                        &native_ref,
                        json!({
                            "line_number": record.line_number,
                            "values": record.values,
                        }),
                    );
                    relationships.push(relationship(
                        &file,
                        &record_identity,
                        "contains-record",
                        submission,
                    ));
                    identities.push(record_identity);
                }
                identities.push(file);
            }
            jsonl_source::JsonlSourcePayload::Fields(catalog) => {
                let file = identity(
                    &submission.source_authority_ref,
                    &self.schema,
                    "jsonl-file",
                    &catalog.path,
                    json!({"field_count": catalog.fields.len()}),
                );
                for field in catalog.fields {
                    let native_ref = format!("{}#{}", catalog.path, field.name);
                    let field_identity = identity(
                        &submission.source_authority_ref,
                        &self.schema,
                        "jsonl-field",
                        &native_ref,
                        json!({"name": field.name, "field_type": field.field_type}),
                    );
                    relationships.push(relationship(
                        &file,
                        &field_identity,
                        "has-field",
                        submission,
                    ));
                    identities.push(field_identity);
                }
                identities.push(file);
            }
            jsonl_source::JsonlSourcePayload::Probe(probe) => identities.push(identity(
                &submission.source_authority_ref,
                &self.schema,
                "jsonl-source",
                &probe.object_ref,
                json!({
                    "root_path": probe.root_path,
                    "reachable": probe.reachable,
                }),
            )),
            jsonl_source::JsonlSourcePayload::Status(_)
            | jsonl_source::JsonlSourcePayload::Error(_) => {
                return Err(DataViewContractError::InvalidContract(
                    "non-materializable jsonl payload",
                ));
            }
        }
        normalized(submission, identities, relationships)
    }

    fn normalize_stream_refresh(
        &self,
        source_authority_ref: &SourceAuthorityRef,
        tracked_result_ref: &TrackedResultRef,
        entry: &StreamingRefreshEntry,
    ) -> Result<NormalizedDataViewCommit, DataViewContractError> {
        normalize_stream_identity(
            &self.schema,
            "jsonl-result",
            source_authority_ref,
            tracked_result_ref,
            entry,
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FsMetaPath {
    Direct,
    ProjectionBacked,
}

#[derive(Debug, Clone)]
pub struct FsMetaResultNormalizer {
    schema: SchemaVersionRef,
    path: FsMetaPath,
}

impl FsMetaResultNormalizer {
    pub fn direct() -> Self {
        Self {
            schema: schema(FS_META_DIRECT_SCHEMA_REF),
            path: FsMetaPath::Direct,
        }
    }

    pub fn projection_backed() -> Self {
        Self {
            schema: schema(FS_META_PROJECTION_SCHEMA_REF),
            path: FsMetaPath::ProjectionBacked,
        }
    }
}

impl ResultNormalizer for FsMetaResultNormalizer {
    fn schema(&self) -> &SchemaVersionRef {
        &self.schema
    }

    fn normalize_action(
        &self,
        submission: &ActionResultSubmission,
    ) -> Result<NormalizedDataViewCommit, DataViewContractError> {
        let payload = match self.path {
            FsMetaPath::Direct => {
                let payload: fs_meta::query::ForceFindQueryPayload = decode(submission)?;
                match payload {
                    fs_meta::query::ForceFindQueryPayload::Tree(tree) => FsMetaPayload::Tree(tree),
                    fs_meta::query::ForceFindQueryPayload::Stats(stats) => {
                        FsMetaPayload::Stats(stats)
                    }
                }
            }
            FsMetaPath::ProjectionBacked => {
                let payload: fs_meta::query::MaterializedQueryPayload = decode(submission)?;
                match payload {
                    fs_meta::query::MaterializedQueryPayload::Tree(tree) => {
                        FsMetaPayload::Tree(tree)
                    }
                    fs_meta::query::MaterializedQueryPayload::Stats(stats) => {
                        FsMetaPayload::Stats(stats)
                    }
                }
            }
        };
        let mut identities = Vec::new();
        let mut relationships = Vec::new();
        match payload {
            FsMetaPayload::Tree(tree) => {
                let root_ref = path_ref(&tree.root.path);
                let root = identity(
                    &submission.source_authority_ref,
                    &self.schema,
                    if tree.root.is_dir {
                        "fs-directory"
                    } else {
                        "fs-file"
                    },
                    &root_ref,
                    json!({
                        "size": tree.root.size,
                        "modified_time_us": tree.root.modified_time_us,
                        "exists": tree.root.exists,
                        "has_children": tree.root.has_children,
                        "reliability": serde_json::to_value(tree.reliability).unwrap_or(Value::Null),
                        "stability": serde_json::to_value(tree.stability).unwrap_or(Value::Null),
                    }),
                );
                for entry in tree.entries {
                    let native_ref = path_ref(&entry.path);
                    let entry_identity = identity(
                        &submission.source_authority_ref,
                        &self.schema,
                        if entry.is_dir {
                            "fs-directory"
                        } else {
                            "fs-file"
                        },
                        &native_ref,
                        json!({
                            "depth": entry.depth,
                            "size": entry.size,
                            "modified_time_us": entry.modified_time_us,
                            "has_children": entry.has_children,
                        }),
                    );
                    relationships.push(relationship(
                        &root,
                        &entry_identity,
                        "contains",
                        submission,
                    ));
                    identities.push(entry_identity);
                }
                identities.push(root);
            }
            FsMetaPayload::Stats(stats) => identities.push(identity(
                &submission.source_authority_ref,
                &self.schema,
                "fs-subtree-stats",
                submission
                    .native_anchors
                    .first()
                    .map(|anchor| anchor.native_ref.as_str())
                    .unwrap_or("/"),
                serde_json::to_value(stats).unwrap_or(Value::Null),
            )),
        }
        normalized(submission, identities, relationships)
    }

    fn normalize_stream_refresh(
        &self,
        source_authority_ref: &SourceAuthorityRef,
        tracked_result_ref: &TrackedResultRef,
        entry: &StreamingRefreshEntry,
    ) -> Result<NormalizedDataViewCommit, DataViewContractError> {
        normalize_stream_identity(
            &self.schema,
            "fs-result",
            source_authority_ref,
            tracked_result_ref,
            entry,
        )
    }
}

enum FsMetaPayload {
    Tree(fs_meta::query::TreeGroupPayload),
    Stats(fs_meta::query::SubtreeStats),
}

fn schema(schema_ref: &str) -> SchemaVersionRef {
    SchemaVersionRef {
        schema_ref: schema_ref.into(),
        schema_version: BUILTIN_SCHEMA_VERSION.into(),
    }
}

fn decode<T: DeserializeOwned>(
    submission: &ActionResultSubmission,
) -> Result<T, DataViewContractError> {
    match submission.encoding {
        PayloadEncoding::Json => serde_json::from_slice(&submission.payload)
            .map_err(|_| DataViewContractError::InvalidContract("source payload")),
        PayloadEncoding::MessagePack => rmp_serde::from_slice(&submission.payload)
            .map_err(|_| DataViewContractError::InvalidContract("source payload")),
        _ => Err(DataViewContractError::InvalidContract(
            "source payload encoding",
        )),
    }
}

fn decode_source<T: DeserializeOwned + Serialize + source_kit::SourcePayloadMetadata>(
    submission: &ActionResultSubmission,
    expected_source_kind: &str,
) -> Result<T, DataViewContractError> {
    if submission.encoding != PayloadEncoding::MessagePack {
        return Err(DataViewContractError::InvalidContract(
            "source response envelope encoding",
        ));
    }
    let source_kit::DecodedSourceResponse::Envelope(envelope) =
        source_kit::decode_source_response::<T>(&submission.payload)
            .map_err(|_| DataViewContractError::InvalidContract("source response envelope"))?
    else {
        return Err(DataViewContractError::InvalidContract(
            "legacy source payload is not materializable",
        ));
    };
    let receipt = envelope
        .receipt
        .ok_or(DataViewContractError::InvalidContract(
            "source observation receipt",
        ))?;
    let observation =
        envelope
            .payload
            .source_observation()
            .ok_or(DataViewContractError::InvalidContract(
                "source payload observation",
            ))?;
    let continuation_digest = submission
        .continuation
        .as_deref()
        .map(source_kit::source_continuation_digest);
    if envelope.request_id != submission.execution.execution_id
        || receipt.source_kind != expected_source_kind
        || receipt.authority_object_ref != submission.authority_object_ref
        || receipt.source_revision != submission.source_revision
        || receipt.evidence_refs != submission.evidence_refs
        || receipt.observed_at_ms != submission.observed_at_ms
        || receipt.grant_epoch != submission.grant_epoch
        || receipt.actual.rows != submission.actual_rows
        || receipt.actual.bytes != submission.actual_bytes
        || receipt.actual.calls != submission.actual_calls
        || receipt.actual.fanout != submission.actual_fanout
        || receipt.actual.latency_ms != submission.actual_latency_ms
        || receipt.actual.cost_units != submission.actual_cost_units
        || receipt.continuation_digest != continuation_digest
        || observation.authority_object_ref != receipt.authority_object_ref
        || observation.native_anchors != receipt.native_anchors
        || observation
            .grant_epoch
            .is_some_and(|grant_epoch| grant_epoch != receipt.grant_epoch)
        || observation.continuation != submission.continuation
        || observation.continuation_state != receipt.continuation_state
        || observation.actual_rows != receipt.actual.rows
    {
        return Err(DataViewContractError::InvalidContract(
            "source observation receipt mismatch",
        ));
    }
    let receipt_anchors = receipt
        .native_anchors
        .into_iter()
        .map(|anchor| NativeAnchor {
            anchor_kind: anchor.anchor_kind,
            native_ref: anchor.native_ref,
            native_pointer: anchor.native_pointer,
            native_version: anchor.native_version,
        })
        .collect::<Vec<_>>();
    if receipt_anchors != submission.native_anchors {
        return Err(DataViewContractError::InvalidContract(
            "source observation anchors mismatch",
        ));
    }
    Ok(envelope.payload)
}

fn normalized(
    submission: &ActionResultSubmission,
    identities: Vec<CanonicalIdentity>,
    relationships: Vec<RelationshipAssertion>,
) -> Result<NormalizedDataViewCommit, DataViewContractError> {
    if identities.is_empty() {
        return Err(DataViewContractError::InvalidContract(
            "normalized identities",
        ));
    }
    Ok(NormalizedDataViewCommit {
        complete_payload_attachment: submission.payload.clone(),
        canonical_identities: identities,
        relationship_assertions: relationships,
        dependency_refs: submission.derivation.dependency_refs.clone(),
        normalization_evidence: vec![format!(
            "normalizer:{}:{}",
            submission.schema.schema_ref, submission.schema.schema_version
        )],
        source_revision_order_key: submission.source_revision.as_bytes().to_vec(),
    })
}

fn identity(
    source_authority_ref: &SourceAuthorityRef,
    schema: &SchemaVersionRef,
    entity_kind: &str,
    native_ref: &str,
    properties: Value,
) -> CanonicalIdentity {
    let result_ref = result_ref(source_authority_ref, schema, entity_kind, native_ref);
    CanonicalIdentity {
        canonical_id: result_ref.0.clone(),
        result_ref,
        entity_kind: entity_kind.into(),
        source_authority_ref: source_authority_ref.clone(),
        native_anchors: vec![NativeAnchor {
            anchor_kind: entity_kind.into(),
            native_ref: native_ref.into(),
            native_pointer: None,
            native_version: None,
        }],
        properties: value_properties(properties),
    }
}

fn relationship(
    source: &CanonicalIdentity,
    target: &CanonicalIdentity,
    kind: &str,
    submission: &ActionResultSubmission,
) -> RelationshipAssertion {
    let relationship_id = opaque_hash(&format!(
        "{}\u{0}{}\u{0}{}",
        source.result_ref.0, kind, target.result_ref.0
    ));
    RelationshipAssertion {
        relationship_id: format!("relationship:{relationship_id}"),
        source_result_ref: source.result_ref.clone(),
        target_result_ref: target.result_ref.clone(),
        relationship_kind: kind.into(),
        authority_refs: vec![submission.source_authority_ref.clone()],
        evidence_refs: submission.evidence_refs.clone(),
        derivation_rule: Some(format!("normalizer:{}", submission.schema.schema_ref)),
        confidence: RelationshipConfidence::Observed,
        observed_revision: submission.source_revision.clone(),
        freshness: submission.freshness.clone(),
    }
}

fn result_ref(
    source_authority_ref: &SourceAuthorityRef,
    _schema: &SchemaVersionRef,
    entity_kind: &str,
    native_ref: &str,
) -> TrackedResultRef {
    TrackedResultRef(format!(
        "result:{}",
        opaque_hash(&format!(
            "{}\u{0}{}\u{0}{}",
            source_authority_ref.0, entity_kind, native_ref
        ))
    ))
}

fn normalize_stream_identity(
    schema: &SchemaVersionRef,
    entity_kind: &str,
    source_authority_ref: &SourceAuthorityRef,
    tracked_result_ref: &TrackedResultRef,
    entry: &StreamingRefreshEntry,
) -> Result<NormalizedDataViewCommit, DataViewContractError> {
    let payload = entry
        .payload
        .clone()
        .ok_or(DataViewContractError::InvalidContract("refresh payload"))?;
    Ok(NormalizedDataViewCommit {
        complete_payload_attachment: payload,
        canonical_identities: vec![CanonicalIdentity {
            result_ref: tracked_result_ref.clone(),
            canonical_id: tracked_result_ref.0.clone(),
            entity_kind: entity_kind.into(),
            source_authority_ref: source_authority_ref.clone(),
            native_anchors: entry.native_anchors.clone(),
            properties: BTreeMap::new(),
        }],
        relationship_assertions: Vec::new(),
        dependency_refs: Vec::new(),
        normalization_evidence: vec![format!(
            "normalizer:{}:{}",
            schema.schema_ref, schema.schema_version
        )],
        source_revision_order_key: entry.source_revision.as_bytes().to_vec(),
    })
}

fn value_properties(value: Value) -> BTreeMap<String, Value> {
    match value {
        Value::Object(map) => map.into_iter().collect(),
        other => BTreeMap::from([("value".into(), other)]),
    }
}

fn path_ref(path: &[u8]) -> String {
    if let Ok(path) = std::str::from_utf8(path) {
        return path.to_string();
    }
    let mut encoded = String::from("bytes:");
    for byte in path {
        encoded.push_str(&format!("{byte:02x}"));
    }
    encoded
}

fn join_native_path(root: &str, path: &str) -> String {
    if path.trim().is_empty() {
        return root.to_string();
    }
    format!(
        "{}/{}",
        root.trim_end_matches(['/', '\\']),
        path.trim_start_matches(['/', '\\'])
    )
}

fn opaque_hash(value: &str) -> String {
    let hash = digest(&SHA256, value.as_bytes());
    let mut encoded = String::with_capacity(hash.as_ref().len() * 2);
    for byte in hash.as_ref() {
        encoded.push_str(&format!("{byte:02x}"));
    }
    encoded
}

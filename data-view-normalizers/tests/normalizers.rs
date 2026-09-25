use std::collections::BTreeSet;

use data_view::{
    ActionExecutionContext, ActionResultSubmission, DATA_VIEW_WIRE_VERSION, DataViewRef,
    DerivationEvidence, FreshnessState, NativeAnchor, PayloadEncoding, ProducerKind,
    ResultNormalizer, ResultNormalizerRegistry, SchemaVersionRef, SourceAuthorityRef,
};
use data_view_normalizers::{
    BUILTIN_SCHEMA_VERSION, CSV_SCHEMA_REF, CsvResultNormalizer, ES_SCHEMA_REF, EsResultNormalizer,
    FS_META_DIRECT_SCHEMA_REF, FS_META_PROJECTION_SCHEMA_REF, FsMetaResultNormalizer,
    JSONL_SCHEMA_REF, JsonlResultNormalizer, LOCAL_FS_SCHEMA_REF, LocalFsResultNormalizer,
    MYSQL_SCHEMA_REF, MysqlResultNormalizer, PG_SCHEMA_REF, PgResultNormalizer, S3_SCHEMA_REF,
    S3ResultNormalizer, SourceActionSubmissionContext, map_source_action_submission,
    register_builtin_normalizers,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use source_kit::{
    SourceExecutionContext as SourceWireExecutionContext, SourceExecutionEnvelope,
    SourceExecutionLimits, SourcePayloadMetadata, SourceRequestMetadata, decode_source_request,
    encode_source_request, encode_source_response,
};

fn submission(schema_ref: &str, payload: Vec<u8>) -> ActionResultSubmission {
    ActionResultSubmission {
        wire_version: DATA_VIEW_WIRE_VERSION,
        submission_id: format!("submission:{schema_ref}"),
        execution: ActionExecutionContext {
            wire_version: DATA_VIEW_WIRE_VERSION,
            action_id: "action:test".into(),
            execution_id: "execution:test".into(),
            base_revision: DataViewRef {
                data_view_id: "global".into(),
                revision_id: "revision:genesis".into(),
            },
            action_contract_digest: "digest".into(),
            policy_context_digest: "policy-a".into(),
            grant_epoch: 4,
            issued_at_ms: 100,
            expires_at_ms: 200,
            idempotency_key: "idempotency:test".into(),
        },
        source_authority_ref: SourceAuthorityRef("source:test".into()),
        authority_object_ref: "source:test".into(),
        producer_id: "source:test".into(),
        producer_kind: ProducerKind::Source,
        projection_evidence: None,
        schema: SchemaVersionRef {
            schema_ref: schema_ref.into(),
            schema_version: BUILTIN_SCHEMA_VERSION.into(),
        },
        encoding: PayloadEncoding::MessagePack,
        actual_bytes: payload.len() as u64,
        payload,
        native_anchors: vec![NativeAnchor {
            anchor_kind: "scope".into(),
            native_ref: "/".into(),
            native_pointer: None,
            native_version: Some("rev-1".into()),
        }],
        source_revision: "rev-1".into(),
        evidence_refs: vec!["evidence:test".into()],
        observed_at_ms: 120,
        policy_labels: BTreeSet::from(["tenant-a".into()]),
        grant_epoch: 4,
        freshness: FreshnessState::Fresh,
        derivation: DerivationEvidence {
            derivation_kind: "observed".into(),
            rule_ref: None,
            dependency_refs: vec![],
        },
        continuation: None,
        actual_rows: 1,
        actual_calls: 1,
        actual_fanout: 1,
        actual_latency_ms: 10,
        actual_cost_units: 1,
    }
}

fn source_submission<R, P>(schema_ref: &str, request: R, payload: P) -> ActionResultSubmission
where
    R: SourceRequestMetadata + Serialize + DeserializeOwned,
    P: SourcePayloadMetadata + Serialize + DeserializeOwned,
{
    let source_request = SourceExecutionEnvelope::new(
        SourceWireExecutionContext {
            request_id: "execution:test".into(),
            grant_epoch: 4,
            issued_at_ms: 100,
            deadline_ms: 200,
            limits: SourceExecutionLimits {
                max_rows: 100,
                max_bytes: 1024 * 1024,
                max_calls: 1,
                max_fanout: 1,
                max_latency_ms: 100,
                max_cost_units: 1,
            },
        },
        request,
    );
    let request_bytes = encode_source_request(&source_request).expect("encode source request");
    let decoded_request =
        decode_source_request::<R>(&request_bytes, 110).expect("decode source request");
    let response_bytes =
        encode_source_response(&decoded_request, payload, 10, 120).expect("source response");
    let template = submission(schema_ref, Vec::new());
    let authority_object_ref = decoded_request.request.authority_object_ref().to_string();
    map_source_action_submission::<P>(
        SourceActionSubmissionContext {
            submission_id: template.submission_id,
            execution: template.execution,
            source_authority_ref: SourceAuthorityRef(authority_object_ref.clone()),
            producer_id: authority_object_ref,
            schema: template.schema,
            policy_labels: template.policy_labels,
            freshness: template.freshness,
            derivation: template.derivation,
        },
        decoded_request.request.source_kind(),
        response_bytes,
    )
    .expect("map source action submission")
}

#[test]
fn builtin_registry_is_schema_version_driven() {
    let mut registry = ResultNormalizerRegistry::default();
    register_builtin_normalizers(&mut registry).expect("register builtins");
    for schema_ref in [
        PG_SCHEMA_REF,
        MYSQL_SCHEMA_REF,
        S3_SCHEMA_REF,
        ES_SCHEMA_REF,
        LOCAL_FS_SCHEMA_REF,
        CSV_SCHEMA_REF,
        JSONL_SCHEMA_REF,
        FS_META_DIRECT_SCHEMA_REF,
        FS_META_PROJECTION_SCHEMA_REF,
    ] {
        assert!(registry.contains(&SchemaVersionRef {
            schema_ref: schema_ref.into(),
            schema_version: BUILTIN_SCHEMA_VERSION.into(),
        }));
    }
}

#[test]
fn source_normalizers_reject_legacy_and_tampered_receipt_metadata() {
    let payload = csv_source::CsvSourcePayload::Records(csv_source::CsvRecordBatch {
        object_ref: "csv-reports".into(),
        root_path: "/srv/private".into(),
        path: "reports/q1.csv".into(),
        source_revision: "fs:csv-q1".into(),
        fields: vec!["id".into()],
        records: vec![csv_source::CsvRecord {
            row_number: 2,
            values: serde_json::json!({"id": "1"}),
        }],
        next_cursor: None,
        has_more: false,
        grant_epoch: Some(4),
        diagnostics: None,
    });
    let legacy = submission(
        CSV_SCHEMA_REF,
        rmp_serde::to_vec_named(&payload).expect("legacy payload"),
    );
    assert!(
        CsvResultNormalizer::default()
            .normalize_action(&legacy)
            .is_err()
    );

    let accepted = source_submission(
        CSV_SCHEMA_REF,
        csv_source::CsvSourceRequest {
            object_ref: "csv-reports".into(),
            path: Some("reports/q1.csv".into()),
            operation: csv_source::CsvSourceOperation::SnapshotPage,
            limit: Some(10),
            cursor: None,
        },
        payload,
    );
    for tampered in [
        {
            let mut value = accepted.clone();
            value.source_revision.push_str(":tampered");
            value
        },
        {
            let mut value = accepted.clone();
            value.authority_object_ref.push_str(":tampered");
            value
        },
        {
            let mut value = accepted.clone();
            value.native_anchors[0].native_ref.push_str("/other");
            value
        },
        {
            let mut value = accepted.clone();
            value.actual_rows += 1;
            value
        },
    ] {
        assert!(
            CsvResultNormalizer::default()
                .normalize_action(&tampered)
                .is_err(),
            "tampered receipt projection must fail"
        );
    }
}

#[test]
fn mysql_fields_derive_table_and_column_identities_without_losing_payload() {
    let payload = mysql_source::MysqlSourcePayload::Fields(mysql_source::MysqlFieldCatalog {
        object_ref: "mysql://db/sales.orders".into(),
        fields: vec![mysql_source::MysqlFieldDescriptor {
            schema: "sales".into(),
            table: "orders".into(),
            column: "id".into(),
            ordinal_position: Some(1),
            column_type: Some("bigint".into()),
        }],
    });
    let request = source_submission(
        MYSQL_SCHEMA_REF,
        mysql_source::MysqlSourceRequest {
            object_ref: "mysql://db/sales.orders".into(),
            schema: None,
            table: None,
            operation: mysql_source::MysqlSourceOperation::DiscoverFields,
            limit: None,
            cursor: None,
            field_filter: Vec::new(),
        },
        payload,
    );
    let normalized = MysqlResultNormalizer::default()
        .normalize_action(&request)
        .expect("normalize mysql");

    assert_eq!(normalized.complete_payload_attachment, request.payload);
    assert_eq!(normalized.canonical_identities.len(), 2);
    assert_eq!(normalized.relationship_assertions.len(), 1);
    assert!(
        normalized
            .canonical_identities
            .iter()
            .any(|identity| identity.entity_kind == "mysql-column")
    );
}

#[test]
fn local_fs_entries_derive_scope_and_entry_relationships() {
    let payload =
        local_fs_source::LocalFsSourcePayload::Entries(local_fs_source::LocalFsEntryBatch {
            object_ref: "local-reports".into(),
            root_path: "/srv/private".into(),
            path: "reports".into(),
            source_revision: "fs:reports".into(),
            entries: vec![local_fs_source::LocalFsEntry {
                path: "reports/q1.csv".into(),
                file_name: "q1.csv".into(),
                is_dir: false,
                size: 128,
                modified_unix_ms: Some(100),
            }],
            next_cursor: None,
            has_more: false,
            grant_epoch: Some(4),
            diagnostics: None,
        });
    let request = source_submission(
        LOCAL_FS_SCHEMA_REF,
        local_fs_source::LocalFsSourceRequest {
            object_ref: "local-reports".into(),
            path: Some("reports".into()),
            operation: local_fs_source::LocalFsSourceOperation::SnapshotPage,
            limit: Some(10),
            cursor: None,
        },
        payload,
    );
    let normalized = LocalFsResultNormalizer::default()
        .normalize_action(&request)
        .expect("normalize local-fs");

    assert_eq!(normalized.complete_payload_attachment, request.payload);
    assert_eq!(normalized.canonical_identities.len(), 2);
    assert_eq!(normalized.relationship_assertions.len(), 1);
    assert_eq!(
        normalized.relationship_assertions[0].relationship_kind,
        "contains"
    );
}

#[test]
fn csv_records_derive_file_and_stable_row_identities() {
    let payload = csv_source::CsvSourcePayload::Records(csv_source::CsvRecordBatch {
        object_ref: "csv-reports".into(),
        root_path: "/srv/private".into(),
        path: "reports/q1.csv".into(),
        source_revision: "fs:csv-q1".into(),
        fields: vec!["id".into(), "amount".into()],
        records: vec![csv_source::CsvRecord {
            row_number: 2,
            values: serde_json::json!({"id": "1", "amount": "10"}),
        }],
        next_cursor: None,
        has_more: false,
        grant_epoch: Some(4),
        diagnostics: None,
    });
    let request = source_submission(
        CSV_SCHEMA_REF,
        csv_source::CsvSourceRequest {
            object_ref: "csv-reports".into(),
            path: Some("reports/q1.csv".into()),
            operation: csv_source::CsvSourceOperation::SnapshotPage,
            limit: Some(10),
            cursor: None,
        },
        payload,
    );
    let normalized = CsvResultNormalizer::default()
        .normalize_action(&request)
        .expect("normalize csv");

    assert_eq!(normalized.complete_payload_attachment, request.payload);
    assert_eq!(normalized.canonical_identities.len(), 2);
    assert_eq!(normalized.relationship_assertions.len(), 1);
    assert!(normalized.canonical_identities.iter().any(|identity| {
        identity.entity_kind == "csv-record"
            && identity.native_anchors[0]
                .native_ref
                .ends_with("q1.csv#row:2")
    }));
}

#[test]
fn jsonl_records_derive_file_and_stable_line_identities() {
    let payload = jsonl_source::JsonlSourcePayload::Records(jsonl_source::JsonlRecordBatch {
        object_ref: "jsonl-events".into(),
        root_path: "/srv/private".into(),
        path: "events/2026.jsonl".into(),
        source_revision: "fs:events-2026".into(),
        records: vec![jsonl_source::JsonlRecord {
            line_number: 7,
            values: serde_json::json!({"id": 7}),
        }],
        next_cursor: None,
        has_more: false,
        grant_epoch: Some(4),
        diagnostics: None,
    });
    let request = source_submission(
        JSONL_SCHEMA_REF,
        jsonl_source::JsonlSourceRequest {
            object_ref: "jsonl-events".into(),
            path: Some("events/2026.jsonl".into()),
            operation: jsonl_source::JsonlSourceOperation::SnapshotPage,
            limit: Some(10),
            cursor: None,
        },
        payload,
    );
    let normalized = JsonlResultNormalizer::default()
        .normalize_action(&request)
        .expect("normalize jsonl");

    assert_eq!(normalized.complete_payload_attachment, request.payload);
    assert_eq!(normalized.canonical_identities.len(), 2);
    assert_eq!(normalized.relationship_assertions.len(), 1);
    assert!(normalized.canonical_identities.iter().any(|identity| {
        identity.entity_kind == "jsonl-record"
            && identity.native_anchors[0]
                .native_ref
                .ends_with("2026.jsonl#line:7")
    }));
}

#[test]
fn pg_fields_derive_table_and_column_identities_without_losing_payload() {
    let payload = pg_source::PgSourcePayload::Fields(pg_source::PgFieldCatalog {
        object_ref: "postgres://db/public.orders".into(),
        fields: vec![
            pg_source::PgFieldDescriptor {
                schema: "public".into(),
                table: "orders".into(),
                column: "id".into(),
                ordinal_position: Some(1),
                column_type: Some("bigint".into()),
            },
            pg_source::PgFieldDescriptor {
                schema: "public".into(),
                table: "orders".into(),
                column: "amount".into(),
                ordinal_position: Some(2),
                column_type: Some("numeric".into()),
            },
        ],
    });
    let request = source_submission(
        PG_SCHEMA_REF,
        pg_source::PgSourceRequest {
            object_ref: "postgres://db/public.orders".into(),
            schema: None,
            table: None,
            operation: pg_source::PgSourceOperation::DiscoverFields,
            limit: None,
            cursor: None,
            field_filter: Vec::new(),
        },
        payload,
    );
    let normalized = PgResultNormalizer::default()
        .normalize_action(&request)
        .expect("normalize pg");

    assert_eq!(normalized.complete_payload_attachment, request.payload);
    assert_eq!(normalized.canonical_identities.len(), 3);
    assert_eq!(normalized.relationship_assertions.len(), 2);
    assert!(
        normalized
            .canonical_identities
            .iter()
            .any(|identity| identity.entity_kind == "pg-table")
    );
}

#[test]
fn pg_catalog_metadata_remains_one_observation_not_business_entities() {
    let payload = pg_source::PgSourcePayload::Catalog(pg_source::PgCatalogMetadata {
        object_ref: "pg-catalog".into(), database: "test".into(), database_oid: 5,
        system_identifier: None, read_only: true, complete: true, grant_epoch: Some(4),
        objects: vec![pg_source::PgCatalogObject { schema: "public".into(), name: "records".into(), oid: 42,
            kind: "r".into(), estimated_rows: None, row_count_method: "estimate".into(), view_definition_md5: None }],
    });
    let request = source_submission(PG_SCHEMA_REF, pg_source::PgSourceRequest {
        object_ref: "pg-catalog".into(), operation: pg_source::PgSourceOperation::CatalogMetadata,
        schema: None, table: None, limit: Some(10), cursor: None, field_filter: Vec::new(),
    }, payload);
    let normalized = PgResultNormalizer::default().normalize_action(&request).expect("normalize catalog");
    assert_eq!(normalized.complete_payload_attachment, request.payload);
    assert_eq!(normalized.canonical_identities.len(), 1);
    assert_eq!(normalized.canonical_identities[0].entity_kind, "pg-catalog");
    assert!(normalized.relationship_assertions.is_empty());
}

#[test]
fn s3_objects_derive_prefix_and_object_relationships() {
    let payload = s3_source::S3SourcePayload::Objects(s3_source::S3ObjectBatch {
        object_ref: "s3://finance/reports/".into(),
        bucket: "finance".into(),
        prefix: "reports/".into(),
        source_revision: "s3-list-observation:reports".into(),
        objects: vec![s3_source::S3ObjectEvent {
            key: "reports/q1.csv".into(),
            size: 128,
            last_modified_unix_ms: 100,
            etag: Some("etag-1".into()),
            storage_class: Some("STANDARD".into()),
            owner_id: None,
            owner_display_name: None,
            metadata: serde_json::json!({"team": "finance"}),
        }],
        next_cursor: None,
        has_more: false,
        grant_epoch: Some(4),
        diagnostics: None,
    });
    let request = source_submission(
        S3_SCHEMA_REF,
        s3_source::S3SourceRequest {
            object_ref: "s3://finance/reports/".into(),
            bucket: Some("finance".into()),
            prefix: Some("reports/".into()),
            operation: s3_source::S3SourceOperation::SnapshotPage,
            limit: Some(10),
            cursor: None,
        },
        payload,
    );
    let normalized = S3ResultNormalizer::default()
        .normalize_action(&request)
        .expect("normalize s3");

    assert_eq!(normalized.complete_payload_attachment, request.payload);
    assert_eq!(normalized.canonical_identities.len(), 2);
    assert_eq!(normalized.relationship_assertions.len(), 1);
    assert_eq!(
        normalized.relationship_assertions[0].relationship_kind,
        "contains"
    );
}

#[test]
fn es_fields_derive_index_and_field_identities() {
    let payload = es_source::EsSourcePayload::Fields(es_source::EsFieldCatalog {
        object_ref: "es://cluster/orders".into(),
        index: "orders".into(),
        fields: vec![es_source::EsFieldDescriptor {
            name: "customer_id".into(),
            field_type: Some("keyword".into()),
        }],
    });
    let request = source_submission(
        ES_SCHEMA_REF,
        es_source::EsSourceRequest {
            object_ref: "es://cluster/orders".into(),
            index: "orders".into(),
            operation: es_source::EsSourceOperation::DiscoverFields,
            limit: None,
            cursor: None,
            timestamp_after: None,
            field_filter: Vec::new(),
            query_filter: None,
        },
        payload,
    );
    let normalized = EsResultNormalizer::default()
        .normalize_action(&request)
        .expect("normalize es");

    assert_eq!(normalized.canonical_identities.len(), 2);
    assert_eq!(normalized.relationship_assertions.len(), 1);
    assert!(
        normalized
            .canonical_identities
            .iter()
            .any(|identity| identity.entity_kind == "es-index")
    );
}

#[test]
fn fs_meta_direct_and_projection_payloads_preserve_native_identity_semantics() {
    let stats = fs_meta::query::SubtreeStats {
        total_nodes: 3,
        total_files: 2,
        total_dirs: 1,
        total_size: 42,
        attested_count: 3,
        blind_spot_count: 0,
        latest_file_mtime_us: Some(100),
    };
    let direct_payload = fs_meta::query::ForceFindQueryPayload::Stats(stats.clone());
    let projection_payload = fs_meta::query::MaterializedQueryPayload::Stats(stats);
    let direct = FsMetaResultNormalizer::direct()
        .normalize_action(&submission(
            FS_META_DIRECT_SCHEMA_REF,
            rmp_serde::to_vec_named(&direct_payload).expect("encode direct"),
        ))
        .expect("normalize direct");
    let projection = FsMetaResultNormalizer::projection_backed()
        .normalize_action(&submission(
            FS_META_PROJECTION_SCHEMA_REF,
            rmp_serde::to_vec_named(&projection_payload).expect("encode projection"),
        ))
        .expect("normalize projection");

    assert_eq!(direct.canonical_identities.len(), 1);
    assert_eq!(projection.canonical_identities.len(), 1);
    assert_eq!(
        direct.canonical_identities[0].entity_kind,
        projection.canonical_identities[0].entity_kind
    );
    assert_eq!(
        direct.canonical_identities[0].native_anchors[0].native_ref,
        projection.canonical_identities[0].native_anchors[0].native_ref
    );
    assert_eq!(
        direct.canonical_identities[0].result_ref, projection.canonical_identities[0].result_ref,
        "direct and projection-backed paths must preserve the same source-native identity"
    );
}

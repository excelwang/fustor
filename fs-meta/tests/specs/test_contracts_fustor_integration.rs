use capanix_host_fs_types::UnixStat;
use fs_meta::fustor_integration::{
    FS_META_DIRECT_SCHEMA_REF, FS_META_PROJECTION_SCHEMA_REF, FsMetaAuthorityCursor,
    FsMetaAuthorityEpochEvidence, FsMetaExportLimits, FsMetaProjectionCoverage,
    FsMetaProjectionCoverageState, FsMetaProjectionCursor, FsMetaProjectionReadiness,
    FsMetaStreamingChange, FsMetaStreamingOperation, FsMetaTrackedResultBinding,
    export_direct_result, export_projection_result, export_tracked_stream_batch,
};
use fs_meta::query::{
    ForceFindQueryPayload, GroupReliability, MaterializedQueryPayload, QueryScope, ReadClass,
    TreeGroupPayload, TreePageEntry, TreePageRoot, TreeStability,
};
use fs_meta::shared_types::FileMetaRecord;

fn authority() -> FsMetaAuthorityCursor {
    FsMetaAuthorityCursor {
        source_authority_ref: "source:fs-meta:nfs1".into(),
        change_id: "change:42".into(),
        source_revision: "pit:force-find:42".into(),
        authority_epoch: FsMetaAuthorityEpochEvidence {
            roots_signature: "roots:1".into(),
            grants_signature: "grants:7".into(),
            source_stream_generation: Some(3),
            journal_sequence: 42,
        },
        observed_at_us: 1000,
    }
}

fn scope() -> QueryScope {
    QueryScope {
        path: b"/reports".to_vec(),
        recursive: true,
        max_depth: Some(2),
        selected_group: Some("nfs1".into()),
    }
}

fn tree() -> TreeGroupPayload {
    TreeGroupPayload {
        reliability: GroupReliability {
            reliable: true,
            unreliable_reason: None,
        },
        stability: TreeStability::not_evaluated(),
        root: TreePageRoot {
            path: b"/reports".to_vec(),
            size: 0,
            modified_time_us: 10,
            is_dir: true,
            exists: true,
            has_children: true,
        },
        entries: vec![TreePageEntry {
            path: b"/reports/q1.csv".to_vec(),
            depth: 1,
            size: 128,
            modified_time_us: 11,
            is_dir: false,
            has_children: false,
        }],
    }
}

fn limits() -> FsMetaExportLimits {
    FsMetaExportLimits {
        max_rows: 10,
        max_payload_bytes: 64 * 1024,
    }
}

// @verify_spec("CONTRACTS.DATA_BOUNDARY.SOURCE_PROVIDER_EVIDENCE_EXPORT", mode="system")
#[test]
fn direct_export_preserves_force_find_payload_and_authority_evidence() {
    let export = export_direct_result(
        ForceFindQueryPayload::Tree(tree()),
        &scope(),
        authority(),
        &limits(),
    )
    .expect("direct export");

    assert_eq!(export.schema_ref, FS_META_DIRECT_SCHEMA_REF);
    assert_eq!(export.actual_rows, 2);
    assert_eq!(export.actual_bytes, export.payload.len() as u64);
    assert_eq!(export.authority_epoch.journal_sequence, 42);
    assert!(
        export
            .evidence_refs
            .iter()
            .any(|value| value == "fs-meta:change:change:42")
    );
    let restored: ForceFindQueryPayload =
        rmp_serde::from_slice(&export.payload).expect("decode direct payload");
    let ForceFindQueryPayload::Tree(restored) = restored else {
        panic!("expected tree payload");
    };
    assert_eq!(restored.root.path, b"/reports");
    assert_eq!(restored.entries[0].path, b"/reports/q1.csv");
}

// @verify_spec("CONTRACTS.DATA_BOUNDARY.SOURCE_PROJECTION_EVIDENCE_EXPORT", mode="system")
#[test]
fn projection_export_requires_read_class_consistent_apply_evidence() {
    let mut cursor = FsMetaProjectionCursor {
        authority: authority(),
        projection_ref: "projection:fs-meta:nfs1".into(),
        projection_revision: "materialized:9".into(),
        applied_change_id: "change:42".into(),
        coverage: FsMetaProjectionCoverage {
            state: FsMetaProjectionCoverageState::CompleteForScope,
            scope_ref: "nfs1:/reports".into(),
            limiting_reasons: vec![],
        },
        readiness: FsMetaProjectionReadiness::MaterializedUntrusted,
        read_class: ReadClass::TrustedMaterialized,
    };
    assert!(
        export_projection_result(
            MaterializedQueryPayload::Tree(tree()),
            &scope(),
            cursor.clone(),
            &limits(),
        )
        .is_err()
    );

    cursor.readiness = FsMetaProjectionReadiness::Ready;
    let export = export_projection_result(
        MaterializedQueryPayload::Tree(tree()),
        &scope(),
        cursor,
        &limits(),
    )
    .expect("projection export");
    assert_eq!(
        export.projection_evidence.schema_ref,
        FS_META_PROJECTION_SCHEMA_REF
    );
    assert_eq!(export.projection_evidence.applied_change_id, "change:42");
    assert_eq!(
        export.projection_evidence.read_class,
        ReadClass::TrustedMaterialized
    );
}

// @verify_spec("CONTRACTS.DATA_BOUNDARY.TRACKED_RESULT_STREAM_REFRESH_ONLY", mode="system")
#[test]
fn streaming_export_emits_only_prebound_tracked_results() {
    let stat = UnixStat {
        is_dir: false,
        size: 128,
        ..UnixStat::default()
    };
    let tracked = FileMetaRecord::realtime_update(
        b"/reports/q1.csv".to_vec(),
        b"q1.csv".to_vec(),
        stat.clone(),
        true,
    );
    let untracked = FileMetaRecord::realtime_update(
        b"/reports/new.csv".to_vec(),
        b"new.csv".to_vec(),
        stat,
        true,
    );
    let changes = vec![tracked, untracked]
        .into_iter()
        .enumerate()
        .map(|(index, record)| FsMetaStreamingChange {
            record,
            change_id: format!("change:{}", index + 43),
            source_revision: format!("journal:{}", index + 43),
            authority_epoch: authority().authority_epoch,
            observed_at_us: 2000 + index as u64,
        })
        .collect();
    let batch = export_tracked_stream_batch(
        "subscription:1",
        "batch:1",
        1,
        "wm:42",
        "wm:44",
        &[FsMetaTrackedResultBinding {
            tracked_result_ref: "result:q1".into(),
            native_path: b"/reports/q1.csv".to_vec(),
        }],
        changes,
        None,
        4,
        64 * 1024,
    )
    .expect("stream export")
    .expect("tracked batch");

    assert_eq!(batch.entries.len(), 1);
    assert_eq!(batch.entries[0].tracked_result_ref, "result:q1");
    assert_eq!(
        batch.entries[0].operation,
        FsMetaStreamingOperation::Refresh
    );
    assert!(batch.entries[0].payload.is_some());

    let no_match = export_tracked_stream_batch(
        "subscription:1",
        "batch:2",
        2,
        "wm:44",
        "wm:45",
        &[FsMetaTrackedResultBinding {
            tracked_result_ref: "result:q1".into(),
            native_path: b"/reports/q1.csv".to_vec(),
        }],
        vec![FsMetaStreamingChange {
            record: FileMetaRecord::realtime_delete(
                b"/reports/other.csv".to_vec(),
                b"other.csv".to_vec(),
                false,
            ),
            change_id: "change:45".into(),
            source_revision: "journal:45".into(),
            authority_epoch: authority().authority_epoch,
            observed_at_us: 2045,
        }],
        None,
        4,
        64 * 1024,
    )
    .expect("untracked export");
    assert!(no_match.is_none());
}

use std::fs;
use std::path::PathBuf;

// @verify_spec("CONTRACTS.SOURCE.HETEROGENEOUS_SOURCE_RESULTS", mode="logic")
// @verify_spec("CONTRACTS.SOURCE.SOURCE_AUTHORITY", mode="logic")
// @verify_spec("CONTRACTS.SOURCE.BOUNDED_SOURCE_ACTIONS", mode="logic")
// @verify_spec("CONTRACTS.SOURCE.EVIDENCE_AND_PROVENANCE", mode="logic")
// @verify_spec("CONTRACTS.SOURCE.SOURCE_EXECUTION_ENVELOPE", mode="logic")
// @verify_spec("CONTRACTS.SOURCE.SOURCE_OBSERVATION_RECEIPT", mode="logic")
// @verify_spec("CONTRACTS.SOURCE.SOURCE_SESSION_LIFECYCLE", mode="logic")
// @verify_spec("CONTRACTS.SOURCE.SOURCE_INCREMENTAL_CONTINUITY", mode="logic")
// @verify_spec("CONTRACTS.SOURCE.SOURCE_CREDENTIAL_AND_SCOPE_ISOLATION", mode="logic")
// @verify_spec("CONTRACTS.SOURCE.SOURCE_CONNECTOR_PARITY", mode="logic")
// @verify_spec("CONTRACTS.SOURCE.GRAPH_SOURCE_PARITY", mode="skeleton")
// @verify_spec("CONTRACTS.SOURCE_PROJECTION.OPTIONAL_SOURCE_ACCELERATION", mode="logic")
// @verify_spec("CONTRACTS.SOURCE_PROJECTION.PROJECTION_SEMANTIC_CONTINUITY", mode="logic")
// @verify_spec("CONTRACTS.SOURCE_PROJECTION.PROJECTION_AUTHORITY_SEPARATION", mode="logic")
// @verify_spec("CONTRACTS.SOURCE_PROJECTION.PROJECTION_PATH_EXPLICITNESS", mode="logic")
// @verify_spec("CONTRACTS.DATA_VIEW.GLOBAL_DATA_VIEW", mode="logic")
// @verify_spec("CONTRACTS.DATA_VIEW.DEMAND_MATERIALIZATION", mode="logic")
// @verify_spec("CONTRACTS.DATA_VIEW.DECLARED_EXTERNAL_RESULT_ADMISSION", mode="skeleton")
// @verify_spec("CONTRACTS.DATA_VIEW.RESULT_NORMALIZATION_OWNERSHIP", mode="logic")
// @verify_spec("CONTRACTS.DATA_VIEW.FULL_BOUNDED_RESULT_MATERIALIZATION", mode="logic")
// @verify_spec("CONTRACTS.DATA_VIEW.EVIDENCE_GATED_ATOMIC_COMMIT", mode="logic")
// @verify_spec("CONTRACTS.DATA_VIEW.CONCURRENT_REVISION_SAFETY", mode="logic")
// @verify_spec("CONTRACTS.DATA_VIEW.OPAQUE_REVISION_IDENTITY", mode="logic")
// @verify_spec("CONTRACTS.DATA_VIEW.REVISION_PINNED_PROJECTION", mode="logic")
// @verify_spec("CONTRACTS.DATA_VIEW.REALTIME_EXISTING_RESULT_FRESHNESS", mode="logic")
// @verify_spec("CONTRACTS.DATA_VIEW.STREAMING_SUBSCRIPTION_BOUNDARY", mode="logic")
// @verify_spec("CONTRACTS.DATA_VIEW.RELATIONSHIP_EVIDENCE_PRESERVATION", mode="logic")
// @verify_spec("CONTRACTS.DATA_VIEW.RETENTION_AND_COMPLIANCE", mode="logic")
// @verify_spec("CONTRACTS.PRIVATE_DISCOVERY.PRIVATE_DATA_DISCOVERY", mode="logic")
// @verify_spec("CONTRACTS.PRIVATE_DISCOVERY.SCOPED_DATA_VIEW", mode="logic")
// @verify_spec("CONTRACTS.PRIVATE_DISCOVERY.EXISTENCE_NON_COMMITMENT", mode="logic")
// @verify_spec("CONTRACTS.PRIVATE_DISCOVERY.BUDGETED_BREADTH_FIRST_DISCOVERY", mode="logic")
// @verify_spec("CONTRACTS.PRIVATE_DISCOVERY.COVERAGE_STATE", mode="logic")
// @verify_spec("CONTRACTS.BOUNDARY.CALLER_AGNOSTIC_TOOLING", mode="logic")
// @verify_spec("CONTRACTS.BOUNDARY.EXTERNAL_ORCHESTRATION_DECOUPLING", mode="logic")
// @verify_spec("CONTRACTS.BOUNDARY.PULL_BASED_REPROJECTION", mode="logic")
// @verify_spec("CONTRACTS.BOUNDARY.CAPANIX_NEUTRALITY", mode="logic")
// @verify_spec("CONTRACTS.BOUNDARY.LOCAL_ACCEPTANCE", mode="logic")
// @verify_spec("CONTRACTS.BOUNDARY.DISPOSABLE_SOURCE_ACCEPTANCE", mode="logic")
// @verify_spec("CONTRACTS.NON_GOALS.NO_MANDATORY_SOURCE_PROJECTION", mode="logic")
// @verify_spec("CONTRACTS.NON_GOALS.NO_UNBOUNDED_INGESTION", mode="logic")
// @verify_spec("CONTRACTS.NON_GOALS.NO_SOURCE_SPECIFIC_DATA_VIEW_API", mode="logic")
// @verify_spec("CONTRACTS.NON_GOALS.NO_UPPER_LAYER_CALLBACK", mode="logic")
// @verify_spec("CONTRACTS.NON_GOALS.NO_SOURCE_RUNTIME_ORCHESTRATION", mode="logic")
// @verify_spec("CONTRACTS.NON_GOALS.NO_HISTORICAL_BRANCH_EXECUTION", mode="logic")

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn read(path: &str) -> String {
    fs::read_to_string(repo_root().join(path)).unwrap_or_else(|err| {
        panic!("read {path}: {err}");
    })
}

fn normalized(path: &str) -> String {
    read(path).split_whitespace().collect::<Vec<_>>().join(" ")
}

fn all_root_specs() -> String {
    [
        "specs/L0-VISION.md",
        "specs/L1-CONTRACTS.md",
        "specs/L2-ARCHITECTURE.md",
        "specs/L3-RUNTIME/interfaces.md",
        "specs/L3-RUNTIME/algorithms.md",
        "specs/L3-RUNTIME/workflows.md",
        "specs/L3-RUNTIME/decisions.md",
    ]
    .into_iter()
    .map(normalized)
    .collect::<Vec<_>>()
    .join(" ")
}

#[test]
fn fustor_has_one_l2_and_traceable_l3_categories() {
    let expected = [
        "specs/L0-VISION.md",
        "specs/L1-CONTRACTS.md",
        "specs/L2-ARCHITECTURE.md",
        "specs/L3-RUNTIME/interfaces.md",
        "specs/L3-RUNTIME/algorithms.md",
        "specs/L3-RUNTIME/workflows.md",
        "specs/L3-RUNTIME/decisions.md",
    ];
    for path in expected {
        assert!(
            repo_root().join(path).is_file(),
            "missing formal spec: {path}"
        );
        assert!(
            read(path).contains("version: 0.5.0"),
            "spec is not on v0.5: {path}"
        );
    }

    let l2_files = fs::read_dir(repo_root().join("specs"))
        .expect("read specs")
        .filter_map(Result::ok)
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter(|name| name.starts_with("L2-") && name.ends_with(".md"))
        .collect::<Vec<_>>();
    assert_eq!(l2_files, vec!["L2-ARCHITECTURE.md"]);

    for removed in [
        "specs/L2-PRIVATE-DISCOVERY.md",
        "specs/L2-SOURCE-UNION-GRAPH.md",
        "specs/L3-RUNTIME/PRIVATE_DISCOVERY.md",
        "specs/L3-RUNTIME/SOURCE_UNION_GRAPH.md",
    ] {
        assert!(
            !repo_root().join(removed).exists(),
            "stale spec remains: {removed}"
        );
    }
}

#[test]
fn l1_contracts_are_atomic_traceable_and_testable() {
    let spec = read("specs/L1-CONTRACTS.md");
    let contract_count = spec
        .lines()
        .filter(|line| line.starts_with("- **") && line.contains("**: System "))
        .count();
    assert_eq!(contract_count, 45, "unexpected L1 contract count");
    assert_eq!(spec.matches("> Responsibility:").count(), contract_count);
    assert_eq!(spec.matches("> Verification:").count(), contract_count);
    assert_eq!(spec.matches("> Covers L0:").count(), contract_count);

    for section in [
        "CONTRACTS.SOURCE",
        "CONTRACTS.SOURCE_PROJECTION",
        "CONTRACTS.DATA_VIEW",
        "CONTRACTS.PRIVATE_DISCOVERY",
        "CONTRACTS.BOUNDARY",
        "CONTRACTS.NON_GOALS",
    ] {
        assert!(spec.contains(section), "missing L1 section: {section}");
    }
}

#[test]
fn architecture_separates_sources_projections_dataview_and_access_plane() {
    let spec = normalized("specs/L2-ARCHITECTURE.md");
    for required in [
        "SourceProvider -> SourceProjection?",
        "DeclaredResultAdmissionRegistry",
        "`SourceProjection` is an optional source-local acceleration role",
        "Private discovery and scoped projection form a control/access plane",
        "Components.SourceProvider",
        "Components.SourceProjection",
        "Components.BoundedActionRegistry",
        "Components.StreamingSubscriptionRegistry",
        "Components.ResultNormalizerRegistry",
        "Components.DataViewCommitGate",
        "Components.DataViewStore",
        "Components.ScopedDataViewProjector",
        "Components.PrivateDiscoveryService",
        "Components.RetentionController",
        "opaque `DataViewRef` values",
        "an empty genesis revision",
        "`historical-replay` projection",
        "does not issue executable action contexts from that projection",
        "New source objects may update a SourceProjection but cannot enter DataView through the subscription",
        "Fustor never calls the caller",
        "DataView core does not depend on source product crates",
        "source-owned permission and capability facts",
        "effective DataView actionability",
    ] {
        assert!(spec.contains(required), "missing architecture rule: {required}");
    }
}

#[test]
fn graph_sources_are_peer_read_only_sources_not_dataview_backends() {
    let specs = all_root_specs();
    for required in [
        "GRAPH_SOURCE_PARITY",
        "GraphSourceAdapter",
        "GraphSourceRequest",
        "configured_query_id",
        "raw unbounded query text",
        "GraphSourceAndDataViewSeparation",
        "Graph observation does not imply DataView admission",
        "Domain graph revision, source observation receipt, and DataView revision",
        "GraphSourceReadOnlyObservation",
        "no graph-specific admission path",
    ] {
        assert!(specs.contains(required), "missing graph-source boundary: {required}");
    }
}

#[test]
fn declared_external_results_have_a_generic_independent_admission_branch() {
    let specs = all_root_specs();
    for required in [
        "DECLARED_EXTERNAL_RESULT_ADMISSION",
        "DeclaredResultAdmissionRegistry",
        "DeclaredResultAdmissionContext",
        "ExternalResultManifest",
        "DeclaredResultSubmission",
        "MUST NOT require or fabricate a `BoundedSourceAction`",
        "without copying referenced product bytes",
        "DeclaredExternalResultCommit",
        "DeclaredExternalResultAdmission",
        "no source receipt, source anchors, DataView identities, or graph assertions",
    ] {
        assert!(
            specs.contains(required),
            "missing declared-result boundary: {required}"
        );
    }
}

#[test]
fn runtime_specs_define_one_shot_streaming_and_pull_only_contracts() {
    let spec = [
        normalized("specs/L3-RUNTIME/interfaces.md"),
        normalized("specs/L3-RUNTIME/algorithms.md"),
        normalized("specs/L3-RUNTIME/workflows.md"),
        normalized("specs/L3-RUNTIME/decisions.md"),
    ]
    .join(" ");

    for required in [
        "`BoundedSourceAction` is one-shot",
        "ActionExecutionContext",
        "ActionResultSubmission",
        "ProjectionApplyEvidence",
        "StreamingSubscription",
        "StreamingRefreshSubmission",
        "tracked_result_refs",
        "An entry whose native identity is not already represented by a tracked result ref",
        "DataViewCommitOutcome",
        "DataViewRevisionSelector = latest | exact(DataViewRef)",
        "projection_mode: current-actionable | historical-replay",
        "revision-not-available",
        "ExactRevisionReadOnlyProjection",
        "OpaqueRevisionBoundary",
        "HistoricalProjectionBoundary",
        "performs no source call, streaming registration, result commit, or head mutation",
        "ResultNormalizerRegistry.resolve",
        "DirectOneShotMaterialization",
        "ProjectionBackedOneShotMaterialization",
        "DirectStreamingRefresh",
        "ProjectionBackedStreamingRefresh",
        "PullBasedHeadRefresh",
        "Fustor embeds no external orchestration or experience-graph runtime",
        "Fustor never invokes the adapter for recovery or streaming events",
    ] {
        assert!(
            spec.contains(required),
            "missing runtime contract: {required}"
        );
    }
}

#[test]
fn active_specs_forbid_upper_layer_and_source_specific_dataview_ownership() {
    let spec = all_root_specs();
    for forbidden in [
        "agentic-map",
        "Agentic Map",
        "Coox",
        "agent-facing",
        "agent-visible",
        "human-agent",
        "research-plan",
        "IngestS3Batch",
        "IngestPgFields",
        "IngestMysqlFields",
        "IngestEsBatch",
        "IngestCsvBatch",
        "IngestJsonlBatch",
        "typed source payloads to become graph skeletons",
    ] {
        assert!(
            !spec.contains(forbidden),
            "forbidden spec ownership: {forbidden}"
        );
    }
}

#[test]
fn data_view_core_dependency_direction_is_source_and_caller_agnostic() {
    let manifest = read("data-view/Cargo.toml");
    let source = [
        read("data-view/src/lib.rs"),
        read("data-view/src/model.rs"),
        read("data-view/src/registry.rs"),
    ]
    .join("\n");

    for forbidden in [
        "pg-source",
        "mysql-source",
        "s3-source",
        "es-source",
        "fs-meta",
        "capanix-",
    ] {
        assert!(
            !manifest.contains(forbidden),
            "DataView core manifest owns forbidden dependency: {forbidden}"
        );
    }
    for forbidden in ["AgenticMap", "agentic-map", "Coox", "coox"] {
        assert!(
            !source.contains(forbidden),
            "DataView core source owns upper-layer term: {forbidden}"
        );
    }
}

#[test]
fn container_staging_runbook_matches_gate_sentinels() {
    let runbook = normalized("fs-meta/docs/CONTAINER_STAGING_GATE.md");
    let script = normalized("fs-meta/docs/examples/container-staging-gate.sh");
    let cleanup = normalized("fs-meta/docs/examples/container-staging-nfs-cleanup.sh");
    let cleanup_test = normalized("scripts/test-container-staging-nfs-cleanup.sh");
    let runtime_nfs_lab = normalized("fs-meta/app/src/runtime_app_real_nfs_lab.rs");

    for required in [
        "fs-meta/scripts/fs-meta-container-staging-gate.sh run active-three",
        "fs-meta/scripts/fs-meta-container-staging-gate.sh run all",
        "fs-meta/scripts/fs-meta-container-staging-gate.sh cleanup-stale",
        "CONTAINER_STAGING_GATE_PASS",
        "CONTAINER_STAGING_GATE_FAIL",
        "CONTAINER_STAGING_CLEANUP_BLOCKED",
        "CONTAINER_STAGING_PREFLIGHT_BLOCKED",
        "CONTAINER_STAGING_NFS_CLEANUP_TEST_PASS",
        "CONTAINER_STAGING_NFS_CLEANUP_DEFERRED_SERVER_STOP",
        "198.51.100.144",
        "198.51.100.145",
        "198.51.100.146",
        "D-state",
        "does not prove",
    ] {
        assert!(runbook.contains(required), "runbook missing: {required}");
    }

    for required in [
        "CONTAINER_STAGING_GATE_PASS",
        "CONTAINER_STAGING_GATE_FAIL",
        "CONTAINER_STAGING_CLEANUP_BLOCKED",
        "CONTAINER_STAGING_PREFLIGHT_BLOCKED",
        "FSMETA_CONTAINER_STAGING_REQUIRE_MINI",
    ] {
        assert!(script.contains(required), "script missing: {required}");
    }

    let run_container = script
        .split_once("run_container() {")
        .expect("staging script must define run_container")
        .1
        .split_once("run_all_container() {")
        .expect("run_container must precede run_all_container")
        .0;
    let resource_check = run_container
        .find("require_resource_limit_configuration")
        .expect("run_container must validate resource limits");
    let slot_check = run_container
        .find("assert_staging_slot_available")
        .expect("run_container must validate the staging slot");
    let image_build = run_container
        .find("build_image")
        .expect("run_container must build the staging image");
    let sysctl_tuning = run_container
        .find("prepare_host_kernel_limits")
        .expect("run_container must prepare host kernel limits");
    assert!(
        resource_check < slot_check && slot_check < image_build && image_build < sysctl_tuning,
        "resource and staging-slot preflight must precede image build and host sysctl tuning"
    );

    for required in [
        "thread-limit|signal-*",
        "CONTAINER_STAGING_NFS_CLEANUP_DEFERRED_SERVER_STOP",
        "if [[ \"${stop_server}\" != \"1\" ]]",
        "run_cleanup_command stop-nfsd",
        "run_cleanup_command stop-rpcbind",
    ] {
        assert!(
            cleanup.contains(required),
            "NFS cleanup missing: {required}"
        );
    }
    assert!(
        cleanup.find("if [[ \"${stop_server}\" != \"1\" ]]")
            < cleanup.find("run_cleanup_command stop-nfsd"),
        "pre-termination cleanup must return before stopping the NFS server"
    );
    assert!(
        cleanup_test.contains("CONTAINER_STAGING_NFS_CLEANUP_TEST_PASS")
            && cleanup_test.contains("pre-terminate-stopped-server"),
        "Docker-free cleanup test must fail if pre-termination stops NFS services"
    );
    for required in ["soft", "timeo=50", "retrans=1", "retry=0"] {
        assert!(
            runtime_nfs_lab.contains(required),
            "runtime-app disposable NFS mount missing bounded option: {required}"
        );
    }
}

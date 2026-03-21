//! L1 Contract Tests — fs-meta app package boundary.

use std::fs;
use std::path::PathBuf;

use crate::app_support::combined_source_text;

fn fs_meta_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(PathBuf::from)
        .expect("fs-meta container root")
}

fn workspace_root() -> PathBuf {
    fs_meta_root()
        .parent()
        .map(PathBuf::from)
        .expect("workspace root")
}

fn read_app_spec(rel: &str) -> String {
    let root = fs_meta_root();
    let normalized = rel.strip_prefix("fs-meta/").unwrap_or(rel);
    let path = match normalized {
        "Cargo.toml" => root.join("app/Cargo.toml"),
        _ if normalized.starts_with("src/") => root.join("app").join(normalized),
        _ => root.join(normalized),
    };
    fs::read_to_string(path).expect("read app spec file")
}

fn read_workspace_manifest() -> String {
    fs::read_to_string(workspace_root().join("Cargo.toml")).expect("read workspace manifest")
}

fn collect_rust_files(dir: &PathBuf, base: &PathBuf, out: &mut Vec<(String, String)>) {
    let mut entries = fs::read_dir(dir)
        .expect("read source directory")
        .map(|entry| entry.expect("dir entry"))
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.path());
    for entry in entries {
        let path = entry.path();
        if path.is_dir() {
            collect_rust_files(&path, base, out);
            continue;
        }
        if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
            continue;
        }
        let rel = path
            .strip_prefix(base)
            .expect("relative path")
            .to_string_lossy()
            .replace('\\', "/");
        out.push((rel, fs::read_to_string(&path).expect("read rust source")));
    }
}

fn read_app_source_files() -> Vec<(String, String)> {
    let app_src = fs_meta_root().join("app/src");
    let mut out = Vec::new();
    collect_rust_files(&app_src, &app_src, &mut out);
    out
}

fn is_dependency_section(section: &str) -> bool {
    matches!(
        section,
        "dependencies" | "dev-dependencies" | "build-dependencies"
    ) || section.starts_with("target.")
        && (section.ends_with(".dependencies")
            || section.ends_with(".dev-dependencies")
            || section.ends_with(".build-dependencies"))
}

fn manifest_has_dependency(manifest: &str, dep_name: &str) -> bool {
    let mut current_section = None::<String>;
    for line in manifest.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') && trimmed.ends_with(']') {
            current_section = Some(trimmed[1..trimmed.len() - 1].to_string());
            continue;
        }
        let Some(section) = current_section.as_deref() else {
            continue;
        };
        if !is_dependency_section(section) || trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let Some((name, _)) = trimmed.split_once('=') else {
            continue;
        };
        if name.trim() == dep_name {
            return true;
        }
    }
    false
}

#[test]
// @verify_spec("CONTRACTS.APP_SCOPE.DOMAIN_CONTRACT_CONSUMPTION_ONLY", mode="system")
fn domain_contract_consumption_only() {
    let source = combined_source_text();
    let l0 = read_app_spec("specs/L0-VISION.md");
    let l1 = read_app_spec("specs/L1-CONTRACTS.md");
    assert!(source.contains("capanix_app_sdk"));
    assert!(l0.contains("consume fs-meta domain specs and root Convergence Vocabulary"));
    assert!(l1.contains("trace root/domain Convergence Vocabulary"));
    assert!(l1.contains("FS_META_SOURCE_SCAN_WORKERS"));
    assert!(l1.contains("FS_META_SOURCE_AUDIT_DEEP_INTERVAL_ROUNDS"));
}

#[test]
// @verify_spec("CONTRACTS.APP_SCOPE.DOMAIN_CONTRACT_CONSUMPTION_ONLY", mode="system")
fn app_sdk_authoring_path_is_primary() {
    let spec = read_app_spec("specs/L2-ARCHITECTURE.md");
    let l1 = read_app_spec("specs/L1-CONTRACTS.md");
    let cutover = read_app_spec("specs/L3-RUNTIME/OBSERVATION_CUTOVER.md");
    let app_manifest = read_app_spec("app/Cargo.toml");
    let app_lib = read_app_spec("app/src/lib.rs");
    let source_mod = read_app_spec("src/source/mod.rs");
    let sink_mod = read_app_spec("src/sink/mod.rs");
    let query_api = read_app_spec("src/query/api.rs");
    let seam = read_app_spec("src/runtime/seam.rs");
    let runtime_app = read_app_spec("src/runtime_app.rs");
    let state_cell = read_app_spec("src/state/cell.rs");
    assert!(spec.contains(
        "Ordinary app-facing business modules use the SDK family rooted in `capanix-app-sdk`"
    ));
    assert!(l1.contains(
        "narrow runtime glue and boundary-conversion seams MAY directly consume `capanix-runtime-api`"
    ));
    assert!(l1.contains("`capanix-managed-state-sdk` supplies shared stateful observation authoring"));
    assert!(l1.contains("`capanix-service-sdk`"));
    assert!(l1.contains("service-first runtime wrapper/lowering path"));
    assert!(l1.contains(
        "MUST NOT directly depend on or reference `capanix-kernel-api`, `capanix-unit-entry-macros`, or `capanix-unit-sidecar`"
    ));
    assert!(cutover.contains(
        "runtime-glue and\nboundary-conversion seams MAY consume `capanix-runtime-api` directly"
    ));
    assert!(app_manifest.contains("publish = false"));
    assert!(manifest_has_dependency(&app_manifest, "capanix-app-sdk"));
    assert!(manifest_has_dependency(
        &app_manifest,
        "capanix-managed-state-sdk"
    ));
    assert!(manifest_has_dependency(
        &app_manifest,
        "capanix-service-sdk"
    ));
    assert!(!manifest_has_dependency(
        &app_manifest,
        "capanix-kernel-api"
    ));
    assert!(!manifest_has_dependency(
        &app_manifest,
        "capanix-unit-sidecar"
    ));
    assert!(!manifest_has_dependency(
        &app_manifest,
        "capanix-unit-entry-macros"
    ));
    assert!(!manifest_has_dependency(
        &app_manifest,
        "capanix-app-fs-meta-worker-source"
    ));
    assert!(!manifest_has_dependency(
        &app_manifest,
        "capanix-app-fs-meta-worker-sink"
    ));
    assert!(!manifest_has_dependency(
        &app_manifest,
        "capanix-app-fs-meta-worker-scan"
    ));
    assert!(!manifest_has_dependency(
        &app_manifest,
        "capanix-app-fs-meta-worker-facade"
    ));
    assert!(app_lib.contains("pub struct FSMetaConfig"));
    assert!(app_lib.contains("capanix_app_sdk"));
    assert!(!app_lib.contains("capanix_kernel_api"));
    assert!(!app_lib.contains("capanix_unit_sidecar"));
    assert!(!app_lib.contains("capanix_unit_entry_macros"));
    assert!(!app_lib.contains("SourceWorkerClient"));
    assert!(!app_lib.contains("SinkWorkerClient"));
    assert!(!app_lib.contains("UnitRuntimeIpcBoundary::bind_and_accept"));
    assert!(!source_mod.contains("use capanix_runtime_api"));
    assert!(!source_mod.contains("capanix_kernel_api"));
    assert!(!sink_mod.contains("use capanix_runtime_api"));
    assert!(!sink_mod.contains("capanix_kernel_api"));
    assert!(!query_api.contains("use capanix_runtime_api"));
    assert!(!query_api.contains("capanix_kernel_api"));
    assert!(!state_cell.contains("use capanix_runtime_api"));
    assert!(!state_cell.contains("capanix_kernel_api"));
    assert!(seam.contains("capanix_app_sdk::raw::{ChannelIoSubset, channel_boundary_into_kernel}"));
    assert!(seam.contains("capanix_app_sdk::runtime::NodeId"));
    assert!(seam.contains("Keep runtime-api boundary conversion out of business modules."));
    assert!(runtime_app.contains("direct runtime-api use"));
    assert!(runtime_app.contains("capanix_service_sdk"));
    assert!(runtime_app.contains("capanix_managed_state_sdk"));
    assert!(runtime_app.contains("impl ManagedStateProfile for FSMetaApp"));
    assert!(runtime_app.contains("RuntimeLoadedManagedState::from_loader"));
    assert!(runtime_app.contains("ManagedStateBuilder::from_profile"));
    assert!(!runtime_app.contains("ServiceApp::new(context, hooks)"));
    assert!(!runtime_app.contains("impl RuntimeBoundaryApp for FSMetaApp"));
    assert!(!runtime_app.contains("define_service_hook_provider!"));
    assert!(!source_mod.contains("define_service_hook_provider!"));
    assert!(!source_mod.contains("FSMetaSourceRuntimeApp"));
    assert!(!source_mod.contains("ServiceHooks::new()"));
    assert!(!source_mod.contains("impl RuntimeBoundaryApp for FSMetaSource {"));
    assert!(!sink_mod.contains("define_service_hook_provider!"));
    assert!(!sink_mod.contains("SinkFileMetaRuntimeApp"));
    assert!(!sink_mod.contains("ServiceHooks::new()"));
    assert!(!sink_mod.contains("impl RuntimeBoundaryApp for SinkFileMeta {"));
    assert!(!seam.contains("capanix_runtime_api::channel_boundary_into_kernel"));
    assert!(!seam.contains("capanix_kernel_api"));
}

#[test]
// @verify_spec("CONTRACTS.APP_SCOPE.WORKER_MODE_MODEL", mode="system")
fn worker_process_unit_split_is_explicit() {
    let l2 = read_app_spec("specs/L2-ARCHITECTURE.md");
    assert!(l2.contains("root `worker / process / unit` split"));
    assert!(l2.contains("`worker` names the product-facing role"));
    assert!(l2.contains("`process` names the hosting container"));
    assert!(l2.contains("`unit` remains the runtime-owned finest bind/run, activation, tick, and state-boundary identity"));
}

#[test]
fn direct_runtime_api_use_is_confined_to_runtime_glue() {
    let files = read_app_source_files();
    for (rel, src) in files {
        if !src.contains("capanix_runtime_api") {
            continue;
        }
        assert!(
            rel.starts_with("runtime/") || rel == "runtime_app.rs",
            "direct runtime-api use must stay confined to runtime glue, found in {rel}"
        );
    }
}

#[test]
// @verify_spec("CONTRACTS.APP_SCOPE.DOMAIN_CONTRACT_CONSUMPTION_ONLY", mode="system")
fn public_operational_support_surfaces_are_explicitly_non_authoritative() {
    let l1 = read_app_spec("specs/L1-CONTRACTS.md");
    let l2 = read_app_spec("specs/L2-ARCHITECTURE.md");
    let lib = read_app_spec("src/lib.rs");
    assert!(l1.contains("bounded `product` remains the product-facing CLI/tooling namespace"));
    assert!(l2.contains("`product` remains the bounded product-facing CLI/tooling namespace"));
    assert!(l2.contains("`query`, `product::release_doc`, and `workers`"));
    assert!(lib.contains("pub mod product;"));
    assert!(lib.contains("pub mod query;"));
    assert!(lib.contains("pub mod workers;"));
}

#[test]
// @verify_spec("CONTRACTS.APP_SCOPE.WORKER_ROLE_MODEL", mode="system")
// @verify_spec("CONTRACTS.APP_SCOPE.WORKER_MODE_MODEL", mode="system")
fn worker_model_is_product_facing() {
    let l0 = read_app_spec("specs/L0-VISION.md");
    let l1 = read_app_spec("specs/L1-CONTRACTS.md");
    let l2 = read_app_spec("specs/L2-ARCHITECTURE.md");
    for spec in [&l1, &l2] {
        assert!(spec.contains("facade-worker"));
        assert!(spec.contains("source-worker"));
        assert!(spec.contains("scan-worker"));
        assert!(spec.contains("sink-worker"));
        assert!(spec.contains("embedded"));
        assert!(spec.contains("external"));
    }
    assert!(l0.contains("WORKER_ROLE_MODEL"));
    assert!(l0.contains("WORKER_MODE_MODEL"));
    assert!(l0.contains("facade/query ingress"));
    assert!(l0.contains("shared-process versus dedicated-process isolation choices"));
    assert!(!l0.contains("facade-worker"));
    assert!(!l0.contains("source-worker"));
    assert!(!l0.contains("scan-worker"));
    assert!(!l0.contains("sink-worker"));
    assert!(!l0.contains("`embedded | external`"));
    assert!(l1.contains("split as `4`, not `3`"));
    assert!(l1.contains("split as `4`, not `5`"));
    assert!(l1.contains("workers.facade.mode"));
    assert!(l2.contains("workers.facade.mode"));
    assert!(l2.contains("workers.scan.mode"));
    assert!(l2.contains("Current baseline defaults: `facade-worker=embedded`"));
}

#[test]
fn worker_responsibility_split_is_explicit() {
    let l2 = read_app_spec("specs/L2-ARCHITECTURE.md");
    let api_server = read_app_spec("src/api/server.rs");
    let query_api = read_app_spec("src/query/api.rs");
    let source_mod = read_app_spec("src/source/mod.rs");
    let source_scanner = read_app_spec("src/source/scanner.rs");
    let sink_mod = read_app_spec("src/sink/mod.rs");

    assert!(l2.contains("`facade-worker` owns HTTP/API ingress, auth, PIT lifecycle, response shaping, and current query orchestration"));
    assert!(l2.contains(
        "`source-worker` owns live watch, live force-find, and source-side host/grant interaction"
    ));
    assert!(l2.contains(
        "`scan-worker` owns initial full scan, periodic audit, and overflow/recovery rescans"
    ));
    assert!(l2.contains("`sink-worker` owns materialized tree/index maintenance plus `/tree` and `/stats` materialized-query backend duties"));
    assert!(l2.contains("This split is intentionally `4`, not `3`"));
    assert!(l2.contains("This split is intentionally `4`, not `5`"));

    assert!(api_server.contains("create_inprocess_router"));
    assert!(query_api.contains("pit"));
    assert!(source_mod.contains("WatchManager"));
    assert!(source_mod.contains("force_find"));
    assert!(source_scanner.contains("audit"));
    assert!(sink_mod.contains("MaterializedTree"));
    assert!(sink_mod.contains("QueryOp::Tree"));
    assert!(sink_mod.contains("QueryOp::Stats"));
}

#[test]
// @verify_spec("CONTRACTS.APP_SCOPE.WORKER_MODE_MODEL", mode="system")
fn product_specs_avoid_realization_vocabulary() {
    let l0 = read_app_spec("specs/L0-VISION.md");
    let l1 = read_app_spec("specs/L1-CONTRACTS.md");
    let l2 = read_app_spec("specs/L2-ARCHITECTURE.md");
    let l0_lowered = l0.to_ascii_lowercase();
    assert!(!l0_lowered.contains("sidecar"));
    for spec in [&l1, &l2] {
        let lowered = spec.to_ascii_lowercase();
        assert!(!lowered.contains("cdylib"));
        assert!(!lowered.contains("create_unit"));
        assert!(!lowered.contains("proc-macro"));
        assert!(!lowered.contains("proc macro"));
        assert!(!lowered.contains("app-sidecar"));
        assert!(!lowered.contains("capanix_app_sidecar"));
    }
}

#[test]
// @verify_spec("CONTRACTS.APP_SCOPE.OPAQUE_INTERNAL_PORTS_ONLY", mode="system")
fn opaque_internal_ports_only() {
    let source = combined_source_text();
    assert!(source.contains("InvokeRequest") || source.contains("route"));
}

#[test]
// @verify_spec("CONTRACTS.APP_SCOPE.OPAQUE_INTERNAL_PORTS_ONLY", mode="system")
fn realization_bridge_confined_to_worker_module() {
    let combined = combined_source_text();
    let app_cargo = read_app_spec("Cargo.toml");
    let app_manifest = read_app_spec("app/Cargo.toml");
    let tooling_manifest = read_app_spec("tooling/Cargo.toml");
    let facade_manifest = read_app_spec("worker-facade/Cargo.toml");
    let source_manifest = read_app_spec("worker-source/Cargo.toml");
    let scan_manifest = read_app_spec("worker-scan/Cargo.toml");
    let sink_manifest = read_app_spec("worker-sink/Cargo.toml");
    let app_lib = read_app_spec("app/src/lib.rs");
    let source_worker = read_app_spec("src/workers/source.rs");
    let sink_worker = read_app_spec("src/workers/sink.rs");
    let lib = read_app_spec("src/lib.rs");
    assert!(!combined.contains("capanix_app_sidecar::ipc_codec"));
    assert!(!app_cargo.contains("name = \"fsmeta\""));
    assert!(!app_cargo.contains("name = \"capanixd-fs-meta\""));
    assert!(!app_cargo.contains("name = \"fs_meta_api_fixture\""));
    assert!(!app_cargo.contains("name = \"fs_meta_source_worker\""));
    assert!(!app_cargo.contains("name = \"fs_meta_scan_worker\""));
    assert!(!app_cargo.contains("name = \"fs_meta_sink_worker\""));
    assert!(!app_manifest.contains("capanix-unit-sidecar"));
    assert!(!app_manifest.contains("capanix-unit-entry-macros"));
    assert!(!app_lib.contains("capanix_unit_sidecar"));
    assert!(!app_lib.contains("capanix_unit_entry_macros"));
    assert!(tooling_manifest.contains("name = \"capanix-app-fs-meta-tooling\""));
    assert!(tooling_manifest.contains("name = \"fsmeta\""));
    assert!(tooling_manifest.contains("name = \"capanixd-fs-meta\""));
    assert!(facade_manifest.contains("name = \"capanix-app-fs-meta-worker-facade\""));
    assert!(facade_manifest.contains("name = \"fs_meta_api_fixture\""));
    assert!(source_manifest.contains("name = \"capanix-app-fs-meta-worker-source\""));
    assert!(source_manifest.contains("name = \"fs_meta_source_worker\""));
    assert!(scan_manifest.contains("name = \"capanix-app-fs-meta-worker-scan\""));
    assert!(scan_manifest.contains("name = \"fs_meta_scan_worker\""));
    assert!(sink_manifest.contains("name = \"capanix-app-fs-meta-worker-sink\""));
    assert!(sink_manifest.contains("name = \"fs_meta_sink_worker\""));
    assert!(!source_worker.contains("BoundRouteClient"));
    assert!(!source_worker.contains("::open("));
    assert!(!source_worker.contains("Command::new("));
    assert!(!source_worker.contains("OpenOptions::new()"));
    assert!(!source_worker.contains("create_dir_all("));
    assert!(!source_worker.contains(".route\n            .ask("));
    assert!(!source_worker.contains("route.ask("));
    assert!(!source_worker.contains("fn init_with_retry("));
    assert!(!source_worker.contains("fn call_with_timeout("));
    assert!(!source_worker.contains("TypedWorkerClient::spawn("));
    assert!(source_worker.contains("TypedWorkerHandle<SourceWorkerRpc"));
    assert!(source_worker.contains("TypedWorkerHandle::new("));
    assert!(source_worker.contains("WorkerBootstrapSpec::new("));
    assert!(!sink_worker.contains("BoundRouteClient"));
    assert!(!sink_worker.contains("::open("));
    assert!(!sink_worker.contains("Command::new("));
    assert!(!sink_worker.contains("OpenOptions::new()"));
    assert!(!sink_worker.contains("create_dir_all("));
    assert!(!sink_worker.contains(".route\n            .ask("));
    assert!(!sink_worker.contains("route.ask("));
    assert!(!sink_worker.contains("fn init_with_retry("));
    assert!(!sink_worker.contains("fn call_with_timeout("));
    assert!(!sink_worker.contains("TypedWorkerClient::spawn("));
    assert!(sink_worker.contains("TypedWorkerHandle<SinkWorkerRpc"));
    assert!(sink_worker.contains("TypedWorkerHandle::new("));
    assert!(sink_worker.contains("WorkerBootstrapSpec::new("));
    assert!(!lib.contains("mod sink_worker_sidecar_bridge;"));
    assert!(!lib.contains("mod source_worker_sidecar_bridge;"));
}

#[test]
// @verify_spec("CONTRACTS.APP_SCOPE.LOCAL_HOST_RESOURCE_PROGRAMMING_ONLY", mode="system")
fn local_host_resource_programming_only() {
    let source = combined_source_text();
    assert!(
        source.contains("HostFsFacade")
            || source.contains("host_adapter")
            || source.contains("watcher"),
        "fs-meta app should consume local-host programming targets for resource-bound logic"
    );
}

#[test]
// @verify_spec("CONTRACTS.APP_SCOPE.RESOURCE_SCOPED_HTTP_FACADE_ONLY", mode="system")
fn ingress_scoped_http_facade_only() {
    let spec = read_app_spec("specs/L1-CONTRACTS.md");
    let source = combined_source_text();
    assert!(spec.contains("one-cardinality HTTP facade"));
    assert!(
        source.contains("http") || source.contains("axum") || source.contains("router"),
        "fs-meta app should host the resource-scoped HTTP facade"
    );
}

#[test]
// @verify_spec("CONTRACTS.APP_SCOPE.RELEASE_GENERATION_CUTOVER_CONSUMPTION_ONLY", mode="system")
// @verify_spec("CONTRACTS.APP_SCOPE.AUTHORITATIVE_TRUTH_CARRIER_CONSUMPTION_ONLY", mode="system")
fn release_generation_cutover_consumption_only() {
    let workflow = read_app_spec("specs/L3-RUNTIME/OBSERVATION_CUTOVER.md");
    let l1 = read_app_spec("specs/L1-CONTRACTS.md");
    assert!(
        l1.contains("rebuilding in-memory observation/projection state through scan/audit/rescan on the candidate generation")
            && l1.contains("reaching app-owned `observation_eligible` for materialized `/tree` and `/stats`")
            && l1.contains("`/on-demand-force-find` on the freshness path")
            && workflow.contains("AuthoritativeTruthReplay")
            && workflow.contains("ProjectionCatchUp")
            && workflow.contains("query `observation_status`")
            && workflow.contains("trusted-materialized")
            && workflow.contains("`/on-demand-force-find` stays a freshness path"),
        "fs-meta app should replay authoritative truth, rebuild materialized observation, and keep force-find available before tree/stats eligibility"
    );
    assert!(!l1.contains("observed_projection_revision"));
}

#[test]
// @verify_spec("CONTRACTS.APP_SCOPE.OBSERVATION_ELIGIBILITY_GATE_OWNERSHIP", mode="system")
fn observation_eligibility_gate_ownership() {
    let source = read_app_spec("src/runtime_app.rs");
    let contracts = read_app_spec("specs/L1-CONTRACTS.md");
    let workflow = read_app_spec("specs/L3-RUNTIME/OBSERVATION_CUTOVER.md");
    let architecture = read_app_spec("specs/L2-ARCHITECTURE.md");
    let source_config = read_app_spec("app/src/source/config.rs");
    let scanner = read_app_spec("src/source/scanner.rs");
    assert!(
        source.contains("apply_facade_activate")
            && source.contains("facade_gate")
            && source.contains("observation_eligible")
            && source.contains("evaluate_observation_status")
            && source.contains("candidate_group_observation_evidence")
            && source.contains("runtime_exposure_confirmed"),
        "fs-meta app should own a package-local materialized-query gate instead of relying on weak process-readiness proxies"
    );
    assert!(
        (contracts.contains("trusted external exposure ownership in runtime")
            || contracts.contains("runtime-owned trusted external exposure"))
            && workflow
                .contains("HTTP facade process/listener readiness is necessary but not sufficient")
            && workflow.contains("materialized `/tree` and `/stats`")
            && workflow.contains("current observation")
            && workflow.contains("materialized observation evidence")
            && workflow.contains("query `observation_status`")
            && workflow.contains("`/on-demand-force-find` stays a freshness path"),
        "fs-meta app workflow should treat observation_eligible as materialized-query evidence while keeping force-find on the freshness path"
    );
    assert!(
        architecture.contains("Package-local implementation env seams remain bounded tuning knobs")
    );
    assert!(architecture.contains("FS_META_SOURCE_SCAN_WORKERS"));
    assert!(architecture.contains("FS_META_SINK_TOMBSTONE_TOLERANCE_US"));
    assert!(source_config.contains("FS_META_SOURCE_SCAN_WORKERS"));
    assert!(source_config.contains("FS_META_SINK_TOMBSTONE_TTL_MS"));
    assert!(scanner.contains("FS_META_SOURCE_AUDIT_DEEP_INTERVAL_ROUNDS"));
}

#[test]
// @verify_spec("CONTRACTS.APP_SCOPE.STALE_WRITER_FENCE_BEFORE_EXPOSURE", mode="system")
fn stale_writer_fence_before_exposure() {
    let source = read_app_spec("src/runtime_app.rs");
    let workflow = read_app_spec("specs/L3-RUNTIME/OBSERVATION_CUTOVER.md");
    assert!(
        source.contains("RuntimeUnitGate")
            && source.contains("apply_facade_activate")
            && source.contains("generation"),
        "fs-meta app should fence stale generations before re-exposure"
    );
    assert!(
        workflow.contains("older observations are not allowed to reclaim facade/query ownership")
            && workflow.contains("runtime-owned trusted exposure gate"),
        "fs-meta app workflow should fence stale observations before runtime can trust new or stale exposure"
    );
}

#[test]
// @verify_spec("CONTRACTS.APP_SCOPE.WORKER_MODE_FAILURE_BOUNDARY_IS_EXPLICIT", mode="system")
fn worker_mode_failure_boundary_is_explicit() {
    let cargo = read_app_spec("Cargo.toml");
    let facade_manifest = read_app_spec("worker-facade/Cargo.toml");
    let l0 = read_app_spec("specs/L0-VISION.md");
    let contracts = read_app_spec("specs/L1-CONTRACTS.md");
    let architecture = read_app_spec("specs/L2-ARCHITECTURE.md");
    let workflow = read_app_spec("specs/L3-RUNTIME/OBSERVATION_CUTOVER.md");
    let runtime_app = read_app_spec("src/runtime_app.rs");
    let api_server = read_app_spec("src/api/server.rs");
    let app_lib = read_app_spec("src/lib.rs");
    assert!(l0.contains("shared-process versus dedicated-process execution boundaries"));
    assert!(!l0.contains("init_error"));
    assert!(!l0.contains("facade-worker"));
    assert!(contracts.contains("`embedded` workers share a host process"));
    assert!(contracts.contains("`external` workers run in dedicated worker processes"));
    assert!(contracts.contains(
        "upstream bridge-realization seam remains only the low-level external-worker bridge carrier"
    ));
    assert!(contracts.contains(
        "worker child-process bootstrap, log/socket ownership, retry clipping, and lifecycle supervision remain confined to helper-only `capanix-worker-runtime-support`"
    ));
    assert!(contracts.contains("canonical `Timeout` / `TransportClosed` categories"));
    assert!(architecture.contains("Product-facing worker modes are only `embedded | external`"));
    assert!(
        architecture.contains("upstream bridge-realization seam is low-level carrier glue only")
    );
    assert!(architecture.contains(
        "helper-only `capanix-worker-runtime-support` owns worker child-process bootstrap, control/data socket ownership, direct control-plane startup/management, retry clipping, and lifecycle supervision"
    ));
    assert!(architecture.contains(
        "The canonical worker transport contract MUST preserve canonical `Timeout` / `TransportClosed` categories plus wall-clock timeout clipping"
    ));
    assert!(workflow.contains(
        "product-facing failure domains are expressed only as `embedded` versus `external` workers"
    ));
    assert!(workflow.contains("`facade-worker=embedded`"));
    assert!(workflow.contains("`source-worker=external`"));
    assert!(workflow.contains("`scan-worker=external`"));
    assert!(workflow.contains("`sink-worker=external`"));
    assert!(app_lib.contains("pub struct FSMetaRuntimeWorkers"));
    assert!(app_lib.contains("resolve_worker_artifact_binding("));
    assert!(app_lib.contains("workers.<role>.mode/startup.path/socket_dir"));
    assert!(contracts.contains("workers.source.mode"));
    assert!(contracts.contains("workers.scan.mode"));
    assert!(contracts.contains("workers.sink.mode"));
    assert!(!cargo.contains("crate-type = [\"cdylib\", \"rlib\"]"));
    assert!(facade_manifest.contains("crate-type = [\"cdylib\", \"rlib\"]"));
    assert!(runtime_app.contains("init_error"));
    assert!(runtime_app.contains("fs-meta runtime init failed"));
    assert!(api_server.contains("create_inprocess_router"));
    assert!(!api_server.contains("remote projection client"));
    assert!(!api_server.contains("expect(\"fs-meta facade must build remote projection client\")"));
}

#[test]
// @verify_spec("CONTRACTS.APP_SCOPE.NO_PRODUCT_OR_PLATFORM_OWNERSHIP", mode="system")
fn no_product_or_platform_ownership() {
    let source = combined_source_text();
    assert!(!source.contains("capanix-cli") && !source.contains("PlatformAdminContext"));
}

#[test]
fn fixture_authoring_crate_avoids_realization_imports() {
    let fixture_manifest = read_app_spec("fixtures/apps/app-test-runtime/Cargo.toml");
    let fixture_app_manifest = read_app_spec("fixtures/apps/app-test-runtime/app/Cargo.toml");
    let fixture_wrapper = read_app_spec("fixtures/apps/app-test-runtime/src/lib.rs");
    let fixture_authoring = read_app_spec("fixtures/apps/app-test-runtime/app/src/lib.rs");

    assert!(fixture_manifest.contains("capanix-app-test-runtime-app = { path = \"app\" }"));
    assert!(fixture_manifest.contains("capanix-unit-entry-macros"));
    assert!(!fixture_app_manifest.contains("capanix-unit-entry-macros"));
    assert!(!fixture_authoring.contains("capanix_unit_entry_macros"));
    assert!(!fixture_authoring.contains("capanix_unit_sidecar"));
    assert!(fixture_wrapper.contains("pub use capanix_app_test_runtime_app::TestRuntimeApp;"));
    assert!(fixture_wrapper.contains("capanix_unit_entry!(TestRuntimeApp);"));
}

#[test]
fn operator_tooling_prefers_worker_oriented_runtime_wording() {
    let tooling = read_app_spec("tooling/src/bin/fsmeta.rs");
    let harness = read_app_spec("tests/common/harness.rs");
    let runtime_scope = read_app_spec("tests/common/scenarios_runtime_scope.rs");
    let lifecycle = read_app_spec("tests/common/scenarios_lifecycle_orchestration.rs");
    let app_runtime = read_app_spec("tests/common/scenarios_app_runtime.rs");
    let system = read_app_spec("tests/common/scenarios_system_integration.rs");
    let cluster = read_app_spec("tests/e2e/support/cluster5.rs");

    assert!(tooling.contains("CAPANIX_CTL_SK_B64"));
    assert!(tooling.contains("CAPANIX_HOME"));
    assert!(harness.contains("CAPANIX_FS_META_APP_BINARY"));
    assert!(harness.contains("CAPANIX_TEST_APP_BINARY"));

    for source in [&runtime_scope, &lifecycle, &app_runtime, &system, &cluster] {
        assert!(source.contains("runtime path not found"));
        assert!(!source.contains("cdylib not found"));
    }
}

#[test]
fn real_nfs_e2e_entrypoint_stays_aligned_with_support_preflight() {
    let script = read_app_spec("scripts/fs-meta-api-e2e-real-nfs.sh");
    let support = read_app_spec("tests/e2e/support/nfs_lab.rs");

    for needle in [
        "CAPANIX_REAL_NFS_E2E",
        "/proc/fs/nfsd",
        "rpcbind",
        "rpc.nfsd",
        "rpc.mountd",
        "exportfs",
        "mount",
        "umount",
        "pgrep",
        "pkill",
    ] {
        assert!(
            script.contains(needle),
            "real NFS e2e shell entrypoint missing preflight requirement: {needle}"
        );
        assert!(
            support.contains(needle),
            "real NFS e2e Rust support missing preflight requirement: {needle}"
        );
    }
}

#[test]
fn app_authoring_crate_stays_product_specific() {
    let l1 = read_app_spec("specs/L1-CONTRACTS.md");
    let l2 = read_app_spec("specs/L2-ARCHITECTURE.md");
    let app_manifest = read_app_spec("app/Cargo.toml");
    let app_lib = read_app_spec("app/src/lib.rs");

    assert!(l1.contains("`fs-meta/app` remains the only product app package"));
    assert!(l1.contains("does not present a generic reusable fs-meta library API"));
    assert!(l2.contains("`fs-meta/app` is the main product app package"));
    assert!(l2.contains("does not define a generic reusable fs-meta library surface"));
    assert!(app_manifest.contains("name = \"capanix-app-fs-meta\""));
    assert!(app_manifest.contains("publish = false"));
    assert!(app_manifest.contains("description = \"fs-meta application package\""));
    assert!(app_lib.contains("pub struct FSMetaConfig"));
}

#[test]
fn embedded_worker_build_path_stays_internal() {
    let l0 = read_app_spec("specs/L0-VISION.md");
    let l1 = read_app_spec("specs/L1-CONTRACTS.md");
    let l2 = read_app_spec("specs/L2-ARCHITECTURE.md");
    let runtime_manifest = read_app_spec("Cargo.toml");
    let runtime_lib = read_app_spec("src/lib.rs");
    let facade_manifest = read_app_spec("worker-facade/Cargo.toml");
    let facade_lib = read_app_spec("worker-facade/src/lib.rs");
    let app_manifest = read_app_spec("app/Cargo.toml");
    let tooling = read_app_spec("tooling/src/bin/fsmeta.rs");
    let harness = read_app_spec("tests/common/harness.rs");
    let cluster = read_app_spec("tests/e2e/support/cluster5.rs");

    for spec in [&l0, &l1, &l2] {
        let lowered = spec.to_ascii_lowercase();
        assert!(!lowered.contains("proc-macro"));
        assert!(!lowered.contains("proc macro"));
        assert!(!lowered.contains("create_unit"));
    }
    assert!(!runtime_manifest.contains("capanix-unit-entry-macros"));
    assert!(!runtime_lib.contains("capanix_unit_entry!(FSMetaRuntimeApp);"));
    assert!(facade_manifest.contains("crate-type = [\"cdylib\", \"rlib\"]"));
    assert!(facade_manifest.contains("capanix-unit-entry-macros"));
    assert!(facade_lib.contains("Embedded facade-worker entry wiring stays artifact-local"));
    assert!(facade_lib.contains("pub use capanix_app_fs_meta::FSMetaRuntimeApp;"));
    assert!(facade_lib.contains("capanix_unit_entry!(FSMetaRuntimeApp);"));
    assert!(!app_manifest.contains("capanix-unit-entry-macros"));
    assert!(tooling.contains("capanix-app-fs-meta-worker-facade"));
    assert!(harness.contains("capanix-app-fs-meta-worker-facade"));
    assert!(cluster.contains("capanix-app-fs-meta-worker-facade"));
}

#[test]
fn source_and_sink_worker_server_bootstrap_live_in_artifact_crates() {
    let runtime_source = read_app_spec("src/workers/source.rs");
    let runtime_sink = read_app_spec("src/workers/sink.rs");
    let worker_source = read_app_spec("worker-source/src/lib.rs");
    let worker_sink = read_app_spec("worker-sink/src/lib.rs");

    assert!(!runtime_source.contains("pub fn run_source_worker_server"));
    assert!(!runtime_sink.contains("pub fn run_sink_worker_server"));
    assert!(!runtime_source.contains("UnitRuntimeIpcBoundary::bind_and_accept"));
    assert!(!runtime_sink.contains("UnitRuntimeIpcBoundary::bind_and_accept"));

    assert!(worker_source.contains("pub fn run_source_worker_server"));
    assert!(worker_source.contains("TypedWorkerServer::<SourceWorkerRpc, _>::new("));
    assert!(worker_source.contains(".run_with_sidecar("));
    assert!(worker_sink.contains("pub fn run_sink_worker_server"));
    assert!(worker_sink.contains("TypedWorkerServer::<SinkWorkerRpc, _>::new("));
    assert!(worker_sink.contains(".run_with_sidecar("));
}

#[test]
fn crate_ownership_and_dependency_rules_are_explicit() {
    let governance = read_app_spec("docs/ENGINEERING_GOVERNANCE.md");
    let l2 = read_app_spec("specs/L2-ARCHITECTURE.md");
    let root_manifest = read_workspace_manifest();
    let app_manifest = read_app_spec("app/Cargo.toml");
    let tooling_manifest = read_app_spec("tooling/Cargo.toml");
    let worker_source_manifest = read_app_spec("worker-source/Cargo.toml");
    let worker_sink_manifest = read_app_spec("worker-sink/Cargo.toml");
    let scan_manifest = read_app_spec("worker-scan/Cargo.toml");

    assert!(governance.contains("## ENGINEERING_GOVERNANCE.CRATE_OWNERSHIP"));
    assert!(governance.contains(
        "`capanix-worker-runtime-support` is the helper-only upstream crate that owns worker child-process bootstrap"
    ));
    assert!(governance.contains(
        "`fs-meta/worker-scan/` owns the `scan-worker` executable artifact identity and `run_scan_worker_server(...)` entry"
    ));
    assert!(governance.contains("## ENGINEERING_GOVERNANCE.DEPENDENCY_RULES"));
    assert!(governance.contains("`fs-meta/app/` does not depend on `capanix-kernel-api`, worker artifact crates, low-level external-worker bridge crate or embedded-entry macro crate"));
    assert!(l2.contains("## ARCHITECTURE.WORKER_ROLE_TO_ARTIFACT_MAP"));
    assert!(l2.contains("`scan-worker` is a distinct operator-visible worker role"));
    assert!(l2.contains("dedicated `run_scan_worker_server(...)` entry while still sharing lower-level source-runtime helpers internally"));

    assert!(root_manifest.contains("capanix-worker-runtime-support"));
    assert!(app_manifest.contains("capanix-worker-runtime-support"));
    assert!(!tooling_manifest.contains("capanix-worker-runtime-support"));
    assert!(worker_source_manifest.contains("capanix-worker-runtime-support"));
    assert!(worker_sink_manifest.contains("capanix-worker-runtime-support"));
    assert!(scan_manifest.contains("capanix-app-fs-meta-worker-source"));
}

#[test]
fn formal_specs_tree_is_single_and_non_spec_materials_live_outside_specs() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(PathBuf::from)
        .expect("fs-meta container root");
    assert!(!root.join("specs/app").exists());
    assert!(!root.join("specs/cli").exists());
    assert!(
        root.join("specs/L3-RUNTIME/OBSERVATION_CUTOVER.md")
            .exists()
    );
    assert!(root.join("docs/PRODUCT_DEPLOYMENT.md").exists());
    assert!(root.join("docs/ENGINEERING_GOVERNANCE.md").exists());
    assert!(root.join("docs/UPSTREAM_SPEC_ALIGNMENT.md").exists());
    assert!(root.join("docs/examples/fs-meta.yaml").exists());
    assert!(
        root.join("testdata/specs/fs-meta-contract-tests.config.md")
            .exists()
    );
    assert!(root.join("scripts/validate_specs.sh").exists());
}

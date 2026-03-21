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
        "deploy compilation consumes `capanix-deploy-sdk`"
    ));
    assert!(l1.contains(
        "`capanix-managed-state-sdk` supplies shared stateful observation declarations/evaluator only"
    ));
    assert!(l1.contains("`capanix-service-sdk`"));
    assert!(l1.contains("`capanix-runtime-host-sdk`"));
    assert!(l1.contains("service-first authoring primitives"));
    assert!(l1.contains("top-level runtime host/lowering path"));
    assert!(l1.contains("MUST NOT directly depend on or reference `capanix-kernel-api`"));
    assert!(cutover.contains("capanix-service-sdk"));
    assert!(cutover.contains("capanix-runtime-host-sdk"));
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
    assert!(manifest_has_dependency(
        &app_manifest,
        "capanix-runtime-host-sdk"
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
        "fs-meta-worker-source"
    ));
    assert!(!manifest_has_dependency(
        &app_manifest,
        "fs-meta-worker-sink"
    ));
    assert!(!manifest_has_dependency(
        &app_manifest,
        "fs-meta-worker-facade"
    ));
    assert!(app_lib.contains("pub struct FSMetaConfig"));
    assert!(app_lib.contains("capanix_app_sdk"));
    assert!(!app_lib.contains("capanix_kernel_api"));
    assert!(!app_lib.contains("capanix_unit_sidecar"));
    assert!(app_lib.contains("capanix_runtime_host_sdk::entry::capanix_unit_entry"));
    assert!(app_lib.contains("capanix_unit_entry!(FSMetaRuntimeApp);"));
    assert!(combined_source_text().contains("pub extern \"C\" fn capanix_run_worker_module"));
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
    assert!(seam.contains("exchange_host_adapter_from_channel_boundary"));
    assert!(seam.contains("capanix_app_sdk::runtime::NodeId"));
    assert!(seam.contains("Keep runtime-api boundary conversion out of business modules."));
    assert!(runtime_app.contains("direct runtime-api use"));
    assert!(runtime_app.contains("capanix_managed_state_sdk"));
    assert!(runtime_app.contains("service-sdk -> runtime-host-sdk -> app-sdk"));
    assert!(runtime_app.contains("impl ManagedStateProfile for FSMetaApp"));
    assert!(runtime_app.contains("RuntimeLoadedServiceApp::from_runtime_config"));
    assert!(runtime_app.contains("AppBuilder::new()"));
    assert!(runtime_app.contains("initialize_from_control"));
    assert!(runtime_app.contains("should_initialize_from_control"));
    assert!(runtime_app.contains("SourceControlSignal::Activate"));
    assert!(runtime_app.contains("FacadeControlSignal::ExposureConfirmed"));
    assert!(!runtime_app.contains("ServiceApp::new(context, hooks)"));
    assert!(!runtime_app.contains("impl RuntimeBoundaryApp for FSMetaApp"));
    assert!(!runtime_app.contains("with_start("));
    assert!(!runtime_app.contains("default_module_path"));
    assert!(!runtime_app.contains("!matches!(signal, SourceControlSignal::Passthrough(_)"));
    assert!(!runtime_app.contains("define_service_hook_provider!"));
    assert!(!runtime_app.contains("source.module_path = scan.module_path.clone();"));
    assert!(!runtime_app.contains("scan.module_path = source.module_path.clone();"));
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
fn worker_unit_split_is_explicit() {
    let l2 = read_app_spec("specs/L2-ARCHITECTURE.md");
    assert!(l2.contains("runtime-facing architecture keeps only the `worker / unit` split"));
    assert!(l2.contains("`worker` names the product-facing role"));
    assert!(l2.contains("`runtime.exec.scan` remains a source-side unit"));
    assert!(l2.contains("`unit` stays the runtime-owned finest bind/run, activation, tick, and state-boundary identity"));
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
    let authoring_lib = read_app_spec("lib/src/lib.rs");
    let runtime_lib = read_app_spec("app/src/lib.rs");
    let deploy_lib = read_app_spec("deploy/src/lib.rs");
    assert!(l1.contains("bounded `fs-meta-deploy` remains the product-facing CLI/tooling namespace"));
    assert!(l2.contains("`fs-meta/deploy` is the internal product deployment compiler package"));
    assert!(authoring_lib.contains("pub use fs_meta_runtime::"));
    assert!(!authoring_lib.contains("pub mod product;"));
    assert!(runtime_lib.contains("pub mod query;"));
    assert!(runtime_lib.contains("pub mod workers;"));
    assert!(deploy_lib.contains("pub fn build_release_doc_value"));
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
        assert!(spec.contains("sink-worker"));
        assert!(spec.contains("embedded"));
        assert!(spec.contains("external"));
    }
    assert!(l0.contains("WORKER_ROLE_MODEL"));
    assert!(l0.contains("WORKER_MODE_MODEL"));
    assert!(l0.contains("facade/query ingress"));
    assert!(l0.contains("bounded per-worker hosting choices"));
    assert!(!l0.contains("facade-worker"));
    assert!(!l0.contains("source-worker"));
    assert!(!l0.contains("four worker roles"));
    assert!(!l0.contains("sink-worker"));
    assert!(!l0.contains("`embedded | external`"));
    assert!(l1.contains("composed of three worker roles"));
    assert!(l1.contains("split as `3`, not `2`"));
    assert!(l1.contains("workers.facade.mode"));
    assert!(l2.contains("workers.facade.mode"));
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
        "`source-worker` owns live watch, live force-find, initial full scan, periodic audit, overflow/recovery rescans, and source-side host/grant interaction"
    ));
    assert!(l2.contains("`sink-worker` owns materialized tree/index maintenance plus `/tree` and `/stats` materialized-query backend duties"));
    assert!(l2.contains("This split is intentionally `3`, not `2`"));
    assert!(l2.contains("This split is intentionally `3`, not `4`"));

    assert!(api_server.contains("create_local_router"));
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
    let app_lib = read_app_spec("app/src/lib.rs");
    let module_entry = read_app_spec("src/workers/module_entry.rs");
    let source_worker = read_app_spec("src/workers/source.rs");
    let sink_worker = read_app_spec("src/workers/sink.rs");
    let source_server = read_app_spec("src/workers/source_server.rs");
    let sink_server = read_app_spec("src/workers/sink_server.rs");
    let lib = read_app_spec("src/lib.rs");
    assert!(!combined.contains("capanix_app_sidecar::ipc_codec"));
    assert!(!app_cargo.contains("name = \"fsmeta\""));
    assert!(!app_cargo.contains("name = \"fsmeta-locald\""));
    assert!(!app_cargo.contains("name = \"fs_meta_source_worker\""));
    assert!(!app_cargo.contains("name = \"fs_meta_scan_worker\""));
    assert!(!app_cargo.contains("name = \"fs_meta_sink_worker\""));
    assert!(!app_manifest.contains("capanix-unit-sidecar"));
    assert!(!app_manifest.contains("capanix-unit-entry-macros"));
    assert!(app_manifest.contains("crate-type = [\"rlib\", \"cdylib\"]"));
    assert!(app_manifest.contains("name = \"fs_meta_api_fixture\""));
    assert!(!app_lib.contains("capanix_unit_sidecar"));
    assert!(app_lib.contains("capanix_runtime_host_sdk::entry::capanix_unit_entry"));
    assert!(tooling_manifest.contains("name = \"fs-meta-tooling\""));
    assert!(tooling_manifest.contains("name = \"fsmeta\""));
    assert!(tooling_manifest.contains("name = \"fsmeta-locald\""));
    assert!(combined_source_text().contains("pub extern \"C\" fn capanix_run_worker_module"));
    assert!(module_entry.contains("\"source\" => run_source_worker_server"));
    assert!(module_entry.contains("\"sink\" => run_sink_worker_server"));
    assert!(!module_entry.contains("\"scan\" =>"));
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
    assert!(source_worker.contains("TypedRuntimeWorkerClient<SourceWorkerRpc"));
    assert!(source_worker.contains("TypedWorkerInit<SourceConfig>"));
    assert!(source_worker.contains("RuntimeWorkerClientFactory"));
    assert!(source_worker.contains(".shutdown(Duration::from_secs(2))"));
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
    assert!(sink_worker.contains("TypedRuntimeWorkerClient<SinkWorkerRpc"));
    assert!(sink_worker.contains("TypedWorkerInit<SourceConfig>"));
    assert!(sink_worker.contains("RuntimeWorkerClientFactory"));
    assert!(sink_worker.contains(".shutdown(Duration::from_secs(2))"));
    assert!(!lib.contains("mod sink_worker_sidecar_bridge;"));
    assert!(!lib.contains("mod source_worker_sidecar_bridge;"));
    assert!(source_server.contains("run_worker_sidecar_server::<SourceWorkerRpc"));
    assert!(sink_server.contains("run_worker_sidecar_server::<SinkWorkerRpc"));
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
        "fs-meta app should own a package-local materialized-query gate instead of relying on weak hosting-readiness proxies"
    );
    assert!(
        (contracts.contains("trusted external exposure ownership in runtime")
            || contracts.contains("runtime-owned trusted external exposure"))
            && workflow.contains("HTTP facade listener readiness is necessary but not sufficient")
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
    let l0 = read_app_spec("specs/L0-VISION.md");
    let contracts = read_app_spec("specs/L1-CONTRACTS.md");
    let architecture = read_app_spec("specs/L2-ARCHITECTURE.md");
    let workflow = read_app_spec("specs/L3-RUNTIME/OBSERVATION_CUTOVER.md");
    let runtime_app = read_app_spec("src/runtime_app.rs");
    let api_server = read_app_spec("src/api/server.rs");
    let app_lib = read_app_spec("src/lib.rs");
    assert!(l0.contains("`embedded` versus `external` worker hosting boundaries"));
    assert!(!l0.contains("init_error"));
    assert!(!l0.contains("facade-worker"));
    assert!(contracts.contains("`embedded` workers stay inside the shared host boundary"));
    assert!(contracts.contains("`external` workers run through isolated external worker hosting"));
    assert!(contracts.contains(
        "upstream bridge-realization seam remains only the low-level external-worker bridge carrier"
    ));
    assert!(contracts.contains(
        "worker bootstrap, log/socket ownership, retry clipping, and lifecycle supervision remain confined to helper-only upstream support beneath `capanix-runtime-host-sdk`"
    ));
    assert!(contracts.contains("canonical `Timeout` / `TransportClosed` categories"));
    assert!(architecture.contains("Product-facing worker modes are only `embedded | external`"));
    assert!(
        architecture.contains("upstream bridge-realization seam is low-level carrier glue only")
    );
    assert!(architecture.contains(
        "helper-only upstream worker support beneath `capanix-runtime-host-sdk` owns worker bootstrap"
    ));
    assert!(architecture.contains(
        "The canonical worker transport contract MUST preserve canonical `Timeout` / `TransportClosed` categories plus wall-clock timeout clipping"
    ));
    assert!(workflow.contains(
        "product-facing failure domains are expressed only as `embedded` versus `external` workers"
    ));
    assert!(workflow.contains("`facade-worker=embedded`"));
    assert!(workflow.contains("`source-worker=external`"));
    assert!(workflow.contains("`sink-worker=external`"));
    assert!(!app_lib.contains("pub struct FSMetaRuntimeWorkers"));
    assert!(!app_lib.contains("resolve_worker_artifact_binding("));
    assert!(app_lib.contains("compiled fs-meta manifest config must not expose config.workers"));
    assert!(contracts.contains("workers.source.mode"));
    assert!(contracts.contains("workers.sink.mode"));
    assert!(contracts.contains("consumes only compiled `__cnx_runtime.workers` bindings"));
    assert!(architecture.contains("consumes only compiled `__cnx_runtime.workers` bindings emitted by the fs-meta release compiler"));
    assert!(workflow.contains("__cnx_runtime.workers"));
    assert!(cargo.contains("crate-type = [\"rlib\", \"cdylib\"]"));
    assert!(runtime_app.contains("init_error"));
    assert!(runtime_app.contains("fs-meta runtime init failed"));
    assert!(api_server.contains("create_local_router"));
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
    let authoring_manifest = read_app_spec("lib/Cargo.toml");
    let authoring_lib = read_app_spec("lib/src/lib.rs");

    assert!(l1.contains("`fs-meta/lib` is the only developer-facing fs-meta authoring package"));
    assert!(l1.contains("`fs-meta-runtime` and `fs-meta-deploy`"));
    assert!(l2.contains("`fs-meta/lib` is the main product authoring package"));
    assert!(l2.contains("`fs-meta/app` is the internal `fs-meta-runtime` package"));
    assert!(authoring_manifest.contains("name = \"fs-meta\""));
    assert!(authoring_manifest.contains("publish = false"));
    assert!(authoring_manifest.contains("description = \"fs-meta authoring package\""));
    assert!(authoring_lib.contains("pub use fs_meta_runtime::{"));
}

#[test]
fn embedded_worker_build_path_stays_internal() {
    let l0 = read_app_spec("specs/L0-VISION.md");
    let l1 = read_app_spec("specs/L1-CONTRACTS.md");
    let l2 = read_app_spec("specs/L2-ARCHITECTURE.md");
    let app_manifest = read_app_spec("app/Cargo.toml");
    let app_lib = read_app_spec("app/src/lib.rs");
    let tooling = read_app_spec("tooling/src/bin/fsmeta.rs");
    let harness = read_app_spec("tests/common/harness.rs");
    let cluster = read_app_spec("tests/e2e/support/cluster5.rs");

    for spec in [&l0, &l1, &l2] {
        let lowered = spec.to_ascii_lowercase();
        assert!(!lowered.contains("proc-macro"));
        assert!(!lowered.contains("proc macro"));
        assert!(!lowered.contains("create_unit"));
    }
    assert!(!app_manifest.contains("capanix-unit-entry-macros"));
    assert!(app_manifest.contains("crate-type = [\"rlib\", \"cdylib\"]"));
    assert!(app_lib.contains("capanix_unit_entry!(FSMetaRuntimeApp);"));
    assert!(combined_source_text().contains("pub extern \"C\" fn capanix_run_worker_module"));
    assert!(tooling.contains("fs-meta-runtime"));
    assert!(harness.contains("fs-meta-runtime"));
    assert!(cluster.contains("fs-meta-runtime"));
}

#[test]
fn source_and_sink_worker_server_bootstrap_live_in_artifact_crates() {
    let runtime_source = read_app_spec("src/workers/source.rs");
    let runtime_sink = read_app_spec("src/workers/sink.rs");
    let worker_source = read_app_spec("src/workers/source_server.rs");
    let worker_sink = read_app_spec("src/workers/sink_server.rs");

    assert!(!runtime_source.contains("pub fn run_source_worker_server"));
    assert!(!runtime_sink.contains("pub fn run_sink_worker_server"));
    assert!(!runtime_source.contains("UnitRuntimeIpcBoundary::bind_and_accept"));
    assert!(!runtime_sink.contains("UnitRuntimeIpcBoundary::bind_and_accept"));

    assert!(worker_source.contains("pub fn run_source_worker_server"));
    assert!(worker_source.contains("run_worker_sidecar_server::<SourceWorkerRpc"));
    assert!(worker_source.contains("TypedWorkerBootstrapSession<SourceConfig>"));
    assert!(!worker_source.contains("run_scan_worker_server("));
    assert!(worker_sink.contains("pub fn run_sink_worker_server"));
    assert!(worker_sink.contains("run_worker_sidecar_server::<SinkWorkerRpc"));
    assert!(worker_sink.contains("TypedWorkerBootstrapSession<SinkWorkerInitConfig>"));
    assert!(worker_sink.contains("SinkWorkerSession"));
}

#[test]
fn crate_ownership_and_dependency_rules_are_explicit() {
    let governance = read_app_spec("docs/ENGINEERING_GOVERNANCE.md");
    let l2 = read_app_spec("specs/L2-ARCHITECTURE.md");
    let root_manifest = read_workspace_manifest();
    let app_manifest = read_app_spec("app/Cargo.toml");
    let tooling_manifest = read_app_spec("tooling/Cargo.toml");

    assert!(governance.contains("## ENGINEERING_GOVERNANCE.CRATE_OWNERSHIP"));
    assert!(governance.contains(
        "Helper-only upstream worker bootstrap/transport support remains owned beneath `capanix-runtime-host-sdk`"
    ));
    assert!(
        governance.contains("shared external worker module entry `capanix_run_worker_module(...)`")
    );
    assert!(governance.contains("## ENGINEERING_GOVERNANCE.DEPENDENCY_RULES"));
    assert!(governance.contains("`fs-meta/lib/` 不直接依赖 `capanix-kernel-api`"));
    assert!(l2.contains("## ARCHITECTURE.WORKER_ROLE_TO_ARTIFACT_MAP"));
    assert!(l2.contains(
        "`source-worker` and `sink-worker` remain the two operator-visible external worker roles"
    ));
    assert!(l2.contains("shared `fs-meta/app` worker module"));

    assert!(root_manifest.contains("capanix-worker-runtime-support"));
    assert!(!app_manifest.contains("capanix-worker-runtime-support"));
    assert!(!app_manifest.contains("capanix-unit-entry-macros"));
    assert!(!tooling_manifest.contains("capanix-worker-runtime-support"));
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

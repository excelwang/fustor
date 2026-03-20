//! L1 Contract Tests — fs-meta CLI package scope.

use crate::support::{
    combined_source_text, fsmeta_source_text, launcher_source_text, package_manifest_text,
};

#[test]
// @verify_spec("CONTRACTS.CLI_SCOPE.PRODUCT_DEPLOYMENT_CLIENT_ONLY", mode="system")
fn product_deployment_client_only() {
    let source = combined_source_text();
    assert!(
        source.contains("Commands::Deploy")
            && source.contains("Commands::Undeploy")
            && source.contains("Commands::Local")
            && source.contains("Commands::Grants")
            && source.contains("Commands::Roots")
            && !source.contains("Commands::Login")
            && !source.contains("Commands::Rescan")
    );
}

#[test]
// @verify_spec("CONTRACTS.CLI_SCOPE.DOMAIN_BOUNDARY_CONSUMPTION_ONLY", mode="system")
fn domain_boundary_consumption_only() {
    let source = fsmeta_source_text();
    let manifest = package_manifest_text();
    assert!(
        source.contains("build_release_doc_value")
            && source.contains("ScopeWorkerIntentDoc")
            && source.contains("compile_scope_worker_intent_doc")
            && source.contains("ControlClient")
            && source.contains("apply_relation_target_intent")
            && source.contains("clear_relation_target")
            && source.contains("run_cnxctl")
            && source.contains("resolve_cnxctl_bin")
            && source.contains("\"scope-worker-intent-v1\"")
            && manifest.contains("capanix-config = { workspace = true }")
            && manifest.contains("capanix-runtime-api = \"0.1.0\"")
            && !source.contains("CtlCommand::RelationTargetApply")
            && !source.contains("CtlCommand::RelationTargetClear")
    );
}

#[test]
// @verify_spec("CONTRACTS.CLI_SCOPE.AUTH_BOUNDARY_CONSUMPTION_ONLY", mode="system")
fn auth_boundary_consumption_only() {
    let source = combined_source_text();
    assert!(
        source.contains("build_auth_config(")
            && source.contains("login_api(")
            && source.contains("/session/login")
            && source.contains(".bearer_auth(")
            && !source.contains("create_session(")
            && !source.contains("bypass_auth"),
        "fs-meta cli should consume the bounded auth boundary instead of defining auth ownership"
    );
}

#[test]
// @verify_spec("CONTRACTS.CLI_SCOPE.RESOURCE_SCOPED_HTTP_FACADE_CONSUMPTION_ONLY", mode="system")
fn ingress_scoped_http_facade_consumption_only() {
    let source = combined_source_text();
    assert!(
        source.contains("/session/login")
            || source.contains("/monitoring/roots")
            || source.contains("/index/rescan"),
        "fs-meta cli should target the resource-scoped HTTP facade"
    );
}

#[test]
// @verify_spec("CONTRACTS.CLI_SCOPE.RELEASE_GENERATION_DEPLOY_CONSUMPTION_ONLY", mode="system")
fn release_generation_deploy_consumption_only() {
    let source = combined_source_text();
    assert!(
        source.contains("Deploy") || source.contains("release") || source.contains("cutover"),
        "fs-meta cli should drive release-generation deploy/cutover through the product boundary"
    );
}

#[test]
// @verify_spec("CONTRACTS.CLI_SCOPE.NO_RUNTIME_OR_PLATFORM_OWNERSHIP", mode="system")
fn no_runtime_or_platform_ownership() {
    let source = combined_source_text();
    assert!(
        source.contains("does not own `link`")
            && source.contains("`bind/run` realization")
            && source.contains("convergence")
            && source.contains("target selection"),
        "fs-meta cli should remain a non-authority client boundary"
    );
}

#[test]
// @verify_spec("CONTRACTS.CLI_SCOPE.NO_OBSERVATION_PLANE_OWNERSHIP", mode="system")
fn no_observation_plane_ownership() {
    let source = combined_source_text();
    assert!(
        source.contains("state/effect observation plane")
            && source.contains("observation_eligible"),
        "fs-meta cli should not own observation-plane meaning"
    );
}

#[test]
// @verify_spec("CONTRACTS.CLI_SCOPE.LOCAL_DEV_DAEMON_COMPOSITION_ONLY", mode="system")
fn local_dev_daemon_composition_only() {
    let manifest = package_manifest_text();
    let launcher = launcher_source_text();
    let l2 = include_str!("../../specs/L2-ARCHITECTURE.md");

    assert!(
        manifest.contains("name = \"capanix-app-fs-meta-tooling\"")
            && manifest.contains("local-daemon = [\"dep:capanix-daemon\"]")
            && manifest.contains("capanix-daemon = { workspace = true, optional = true }")
            && manifest.contains("capanix-host-adapter-fs-meta = { workspace = true }")
            && manifest.contains("required-features = [\"local-daemon\"]"),
        "fs-meta local-dev launcher should stay in the dedicated tooling package as an explicit optional feature over daemon seams"
    );
    assert!(
        l2.contains("workspace package `capanix-app-fs-meta-tooling`")
            && l2.contains("feature-gated away from the default CLI install")
            && l2.contains("run_with_host_passthrough_bootstrap")
            && l2.contains("spawn_host_passthrough_endpoint")
            && l2.contains("tooling only")
            && l2.contains("do not create new platform authority or alter daemon ownership"),
        "fs-meta architecture must document launcher composition as tooling-only daemon seam usage"
    );
    assert!(
        launcher.contains("run_with_host_passthrough_bootstrap(Some(Arc::new(")
            && launcher.contains("spawn_host_passthrough_endpoint"),
        "fs-meta local-dev launcher must compose generic daemon through the passthrough bootstrap seam"
    );
    assert!(
        !launcher.contains("UnixListener::bind")
            && !launcher.contains("RuntimeControlService")
            && !launcher.contains("start_runtime(")
            && !launcher.contains("control_service.handle_request"),
        "fs-meta local-dev launcher must not reimplement daemon ingress or runtime embedding logic directly"
    );
}

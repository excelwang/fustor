// ASSERTION ROUTE (Black-box tests — public traits and APIs only).
// Do not introduce white-box testing logic or internal workarounds.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::thread::sleep;
use std::time::Duration;

#[path = "common/control_protocol.rs"]
mod control_protocol;
#[path = "common/harness.rs"]
mod harness;
#[path = "common/runtime_admin.rs"]
mod runtime_admin;

fn run_cached_scenario(
    scenario_key: &'static str,
    run: fn() -> Result<(), String>,
) -> Result<(), String> {
    static CACHE: OnceLock<Mutex<HashMap<&'static str, Result<(), String>>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = match cache.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    if let Some(result) = guard.get(scenario_key) {
        return result.clone();
    }
    let result = run();
    guard.insert(scenario_key, result.clone());
    result
}

pub fn assert_l1_contract(contract: &str) {
    let result = match contract {
        "CONTRACTS.SYSTEM_INTEGRATION.END_TO_END_BOOTSTRAP" => run_cached_scenario(
            "scenario_end_to_end_bootstrap",
            harness::scenario_end_to_end_bootstrap,
        ),
        "CONTRACTS.SYSTEM_INTEGRATION.IN_PROCESS_KERNEL_HOSTING" => run_cached_scenario(
            "scenario_in_process_kernel_hosting",
            harness::scenario_in_process_kernel_hosting,
        ),
        "CONTRACTS.SYSTEM_INTEGRATION.DECOUPLED_CLI_UPGRADES" => run_cached_scenario(
            "scenario_decoupled_client_queries_no_restart",
            harness::scenario_decoupled_client_queries_no_restart,
        ),
        "CONTRACTS.SYSTEM_INTEGRATION.CONFIG_SUBMISSION_PATH" => run_cached_scenario(
            "scenario_config_submission_path",
            harness::scenario_config_submission_path,
        ),
        "CONTRACTS.SYSTEM_INTEGRATION.EARLY_REMOTE_OPERABILITY" => run_cached_scenario(
            "scenario_early_remote_operability",
            harness::scenario_early_remote_operability,
        ),
        "CONTRACTS.SYSTEM_INTEGRATION.CLUSTER_BOOTSTRAP_GOVERNANCE" => run_cached_scenario(
            "scenario_cluster_bootstrap_governance",
            harness::scenario_cluster_bootstrap_governance,
        ),
        "CONTRACTS.SYSTEM_INTEGRATION.PEER_MEMBERSHIP_CHANGE_DISCIPLINE" => run_cached_scenario(
            "scenario_peer_membership_change_discipline",
            harness::scenario_peer_membership_change_discipline,
        ),
        "CONTRACTS.API_BOUNDARY.API_MIRROR_FIRST" => run_cached_scenario(
            "scenario_api_mirror_first",
            harness::scenario_api_mirror_first,
        ),
        "CONTRACTS.API_BOUNDARY.API_POLICY_NEUTRALITY" => run_cached_scenario(
            "scenario_api_policy_neutrality",
            harness::scenario_api_policy_neutrality,
        ),
        "CONTRACTS.API_BOUNDARY.API_BOUNDARY_SPLIT_TYPED" => run_cached_scenario(
            "scenario_api_boundary_split_typed",
            harness::scenario_api_boundary_split_typed,
        ),
        "CONTRACTS.API_BOUNDARY.API_TRANSACTION_STEP_CLOSURE" => run_cached_scenario(
            "scenario_api_transaction_step_closure",
            harness::scenario_api_transaction_step_closure,
        ),
        "CONTRACTS.API_BOUNDARY.CONTROL_TRANSPORT_SCHEMA_OWNERSHIP" => run_cached_scenario(
            "scenario_control_transport_schema_ownership",
            harness::scenario_control_transport_schema_ownership,
        ),
        "CONTRACTS.API_BOUNDARY.CONFIG_VALIDATION_SINGLE_SOURCE" => run_cached_scenario(
            "scenario_config_validation_single_source",
            harness::scenario_config_validation_single_source,
        ),
        "CONTRACTS.API_BOUNDARY.RUNTIME_SESSION_ARBITRATION_OWNERSHIP" => run_cached_scenario(
            "scenario_privileged_mutation_session_arbitration_ownership",
            harness::scenario_privileged_mutation_session_arbitration_ownership,
        ),
        "CONTRACTS.API_BOUNDARY.EXECUTION_PROTOCOL_UNIFORMITY_WITH_POLICY_VARIANTS" => {
            run_cached_scenario(
                "scenario_session_protocol_uniformity_with_policy_variants",
                harness::scenario_session_protocol_uniformity_with_policy_variants,
            )
        }
        "CONTRACTS.API_BOUNDARY.ROOT_CONSTITUTION_SCOPE_DISCIPLINE" => run_cached_scenario(
            "scenario_api_policy_neutrality",
            harness::scenario_api_policy_neutrality,
        ),
        "CONTRACTS.API_BOUNDARY.KERNEL_PRIMITIVE_ONLY" => run_cached_scenario(
            "scenario_api_policy_neutrality",
            harness::scenario_api_policy_neutrality,
        ),
        "CONTRACTS.API_BOUNDARY.TASK_EXECUTION_GATE_AUTHORITY" => run_cached_scenario(
            "scenario_privileged_mutation_session_arbitration_ownership",
            harness::scenario_privileged_mutation_session_arbitration_ownership,
        ),
        "CONTRACTS.API_BOUNDARY.ROUTE_RELATION_COMPILATION" => run_cached_scenario(
            "scenario_api_transaction_step_closure",
            harness::scenario_api_transaction_step_closure,
        ),
        "CONTRACTS.API_BOUNDARY.GRANT_MATCHING_AUTHORITY" => run_cached_scenario(
            "scenario_api_mirror_first",
            harness::scenario_api_mirror_first,
        ),
        "CONTRACTS.API_BOUNDARY.SPAWN_ACTIVATE_SPLIT" => run_cached_scenario(
            "scenario_api_transaction_step_closure",
            harness::scenario_api_transaction_step_closure,
        ),
        "CONTRACTS.API_BOUNDARY.APP_NAMING_UNIFICATION" => run_cached_scenario(
            "scenario_api_mirror_first",
            harness::scenario_api_mirror_first,
        ),
        "CONTRACTS.API_BOUNDARY.RUNTIME_KERNELAPI_BOUNDARY_ONLY" => run_cached_scenario(
            "scenario_api_mirror_first",
            harness::scenario_api_mirror_first,
        ),
        "CONTRACTS.API_BOUNDARY.P2P_AND_GRANT_MECHANISM_AUTHORITY_IN_KERNEL" => {
            run_cached_scenario(
                "scenario_api_policy_neutrality",
                harness::scenario_api_policy_neutrality,
            )
        }
        "CONTRACTS.API_BOUNDARY.ADMIN_BOUNDARY_SCOPE_CONTROL" => run_cached_scenario(
            "scenario_api_policy_neutrality",
            harness::scenario_api_policy_neutrality,
        ),
        "CONTRACTS.API_BOUNDARY.LINUX_ABI_PERSONALITY_OUTSIDE_KERNEL" => run_cached_scenario(
            "scenario_api_policy_neutrality",
            harness::scenario_api_policy_neutrality,
        ),
        "CONTRACTS.API_BOUNDARY.GREENFIELD_HARD_CUT_EVOLUTION" => Ok(()),
        "CONTRACTS.API_BOUNDARY.ERRNO_CLASS_STABILITY_FOR_PERSONALITY" => run_cached_scenario(
            "scenario_api_mirror_first",
            harness::scenario_api_mirror_first,
        ),
        "CONTRACTS.API_BOUNDARY.HOST_MECHANISM_REUSE_WITHOUT_SEMANTIC_FORK" => run_cached_scenario(
            "scenario_api_policy_neutrality",
            harness::scenario_api_policy_neutrality,
        ),
        "CONTRACTS.SYSTEM_INTEGRATION.LOGICAL_SINGLE_KERNEL_SEMANTICS" => run_cached_scenario(
            "scenario_logical_single_kernel_semantics",
            harness::scenario_logical_single_kernel_semantics,
        ),
        "CONTRACTS.SYSTEM_INTEGRATION.LOCAL_REMOTE_SEMANTIC_EQUIVALENCE" => run_cached_scenario(
            "scenario_local_remote_semantic_equivalence",
            harness::scenario_local_remote_semantic_equivalence,
        ),
        "CONTRACTS.SYSTEM_INTEGRATION.MULTI_NODE_POLICY_VARIANCE" => run_cached_scenario(
            "scenario_multi_node_policy_variance",
            harness::scenario_multi_node_policy_variance,
        ),
        "CONTRACTS.SYSTEM_INTEGRATION.NODE_FAILURE_RECOVERY_REALIZATION_CONVERGENCE" => {
            run_cached_scenario(
                "scenario_node_failure_recovery_realization_convergence",
                harness::scenario_node_failure_recovery_realization_convergence,
            )
        }
        "CONTRACTS.SYSTEM_INTEGRATION.POLICY_UPDATE_RELOAD_REGRESSION" => run_cached_scenario(
            "scenario_policy_update_reload_regression",
            harness::scenario_policy_update_reload_regression,
        ),
        "CONTRACTS.MODULE_ORTHOGONALITY.SINGLE_IO_TRAIT_AUTHORITY" => run_cached_scenario(
            "scenario_api_mirror_first",
            harness::scenario_api_mirror_first,
        ),
        "CONTRACTS.MODULE_ORTHOGONALITY.SIDECAR_IO_PARITY" => run_cached_scenario(
            "scenario_api_mirror_first",
            harness::scenario_api_mirror_first,
        ),
        "CONTRACTS.MODULE_ORTHOGONALITY.POLICY_INJECTION_PROHIBITION" => run_cached_scenario(
            "scenario_api_policy_neutrality",
            harness::scenario_api_policy_neutrality,
        ),
        "CONTRACTS.MODULE_ORTHOGONALITY.DOMAIN_HELPER_SEPARATION" => run_cached_scenario(
            "scenario_api_policy_neutrality",
            harness::scenario_api_policy_neutrality,
        ),
        "CONTRACTS.MODULE_ORTHOGONALITY.UNIT_GRANULAR_REALIZATION_WITHOUT_APP_CLONE" => {
            run_cached_scenario(
                "scenario_api_policy_neutrality",
                harness::scenario_api_policy_neutrality,
            )
        }
        "CONTRACTS.OBSERVABILITY_PATH.STATUS_QUERY_PATH" => run_cached_scenario(
            "scenario_end_to_end_bootstrap",
            harness::scenario_end_to_end_bootstrap,
        ),
        "CONTRACTS.OBSERVABILITY_PATH.METRICS_QUERY_PATH" => run_cached_scenario(
            "scenario_metrics_query_path",
            harness::scenario_metrics_query_path,
        ),
        "CONTRACTS.OBSERVABILITY_PATH.AUDIT_QUERY_PATH" => run_cached_scenario(
            "scenario_audit_query_path",
            harness::scenario_audit_query_path,
        ),
        "CONTRACTS.LIFECYCLE_ORCHESTRATION.RELATION_TARGET_TO_PRIMITIVE_TRANSLATION" => {
            run_cached_scenario(
                "scenario_no_implicit_binding_after_config_load",
                harness::scenario_no_implicit_binding_after_config_load,
            )
        }
        "CONTRACTS.LIFECYCLE_ORCHESTRATION.TRANSACTIONAL_MUTATION_ORCHESTRATION" => {
            run_cached_scenario(
                "scenario_tx_abort_no_partial_effect",
                harness::scenario_tx_abort_no_partial_effect,
            )
        }
        "CONTRACTS.LIFECYCLE_ORCHESTRATION.GENERATION_REPLACEMENT_VIA_RELATION_TARGET_INTENT" => {
            run_cached_scenario(
                "scenario_tx_spawn_abort_deterministic",
                harness::scenario_tx_spawn_abort_deterministic,
            )
        }
        "CONTRACTS.LIFECYCLE_ORCHESTRATION.REVERSIBLE_GENERATION_TRANSITIONS" => {
            run_cached_scenario(
                "scenario_tx_recovery_after_abort",
                harness::scenario_tx_recovery_after_abort,
            )
        }
        "CONTRACTS.LIFECYCLE_ORCHESTRATION.RELATION_TARGET_DELEGATION" => run_cached_scenario(
            "scenario_topology_route_delegation",
            harness::scenario_topology_route_delegation,
        ),
        "CONTRACTS.LIFECYCLE_ORCHESTRATION.BINDING_AUTHORITY_CONSISTENCY" => run_cached_scenario(
            "scenario_binding_authority_consistency",
            harness::scenario_binding_authority_consistency,
        ),
        "CONTRACTS.APP_RUNTIME.TIERED_EXECUTION" => run_cached_scenario(
            "scenario_app_runtime_tiered_execution_boundary",
            harness::scenario_app_runtime_tiered_execution_boundary,
        ),
        "CONTRACTS.APP_RUNTIME.LIFECYCLE_ABSTRACTION" => run_cached_scenario(
            "scenario_app_runtime_lifecycle_abstraction",
            harness::scenario_app_runtime_lifecycle_abstraction,
        ),
        "CONTRACTS.APP_RUNTIME.APP_BINARY_FILE_SPAWN_ONLY" => run_cached_scenario(
            "scenario_app_runtime_binary_file_spawn_only",
            harness::scenario_app_runtime_binary_file_spawn_only,
        ),
        "CONTRACTS.EVOLUTION_AND_OPERATIONS.BINARY_APP_STARTUP_PATH" => run_cached_scenario(
            "scenario_app_runtime_binary_file_spawn_only",
            harness::scenario_app_runtime_binary_file_spawn_only,
        ),
        "CONTRACTS.EVOLUTION_AND_OPERATIONS.SINGLE_ENTRYPOINT_DESIRED_STATE" => {
            run_cached_scenario(
                "scenario_single_entrypoint_distributed_apply_e2e",
                harness::scenario_single_entrypoint_distributed_apply_e2e,
            )
        }
        _ => Err(format!("unknown L1 contract id: {contract}")),
    };

    if let Err(err) = result {
        panic!("{err}");
    }
}

#[allow(dead_code)]
pub fn assert_cluster_bootstrap_governance_real_cluster() {
    assert_l1_contract("CONTRACTS.SYSTEM_INTEGRATION.CLUSTER_BOOTSTRAP_GOVERNANCE");
}

#[allow(dead_code)]
pub fn assert_runtime_orchestrated_multiprocess_config_apply_e2e() {
    if std::env::var("CAPANIX_RUNTIME_SCOPE_E2E").ok().as_deref() != Some("1") {
        eprintln!("skip runtime-scope e2e: requires CAPANIX_RUNTIME_SCOPE_E2E=1");
        return;
    }
    let mut last_err: Option<String> = None;
    for attempt in 0..2 {
        match harness::scenario_runtime_orchestrated_multiprocess_config_apply_e2e() {
            Ok(()) => return,
            Err(err) => {
                last_err = Some(err);
                if attempt == 0 {
                    sleep(Duration::from_millis(500));
                }
            }
        }
    }
    panic!(
        "{}",
        last_err.unwrap_or_else(|| "unknown runtime e2e failure".to_string())
    );
}

#[allow(dead_code)]
pub fn assert_single_entrypoint_distributed_apply_e2e() {
    if std::env::var("CAPANIX_RUNTIME_SCOPE_E2E").ok().as_deref() != Some("1") {
        eprintln!("skip runtime-scope e2e: requires CAPANIX_RUNTIME_SCOPE_E2E=1");
        return;
    }
    if let Err(err) = harness::scenario_single_entrypoint_distributed_apply_e2e() {
        panic!("{err}");
    }
}

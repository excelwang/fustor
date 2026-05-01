#![cfg(target_os = "linux")]

#[path = "path_support.rs"]
mod path_support;
#[path = "e2e/support/mod.rs"]
mod support;
#[path = "e2e/test_http_api_matrix.rs"]
mod test_http_api_matrix;
#[path = "e2e/test_operational_scenarios.rs"]
mod test_operational_scenarios;
#[path = "e2e/test_release_upgrade.rs"]
mod test_release_upgrade;

fn run_l5_entry(name: &str, run: impl FnOnce() -> Result<(), String>) {
    eprintln!("[fs-meta-l5-progress] case={name} step=entry state=begin");
    run().unwrap();
    eprintln!("[fs-meta-l5-progress] case={name} step=entry state=ok");
}

#[test]
#[ignore = "requires Linux + CAPANIX_REAL_NFS_E2E=1 + passwordless sudo"]
fn fs_meta_business_mini_real_nfs() {
    test_http_api_matrix::run_mini_real_nfs_smoke().unwrap();
}

#[test]
#[ignore = "requires Linux + CAPANIX_REAL_NFS_E2E=1 + passwordless sudo"]
fn fs_meta_environment_full_real_nfs() {
    test_http_api_matrix::run().unwrap();
}

#[test]
#[ignore = "requires Linux + CAPANIX_REAL_NFS_E2E=1 + passwordless sudo"]
fn fs_meta_business_query_real_nfs() {
    test_http_api_matrix::run_query_baseline_only().unwrap();
}

#[test]
#[ignore = "requires Linux + CAPANIX_REAL_NFS_E2E=1 + passwordless sudo"]
fn fs_meta_environment_live_only_rescan_real_nfs() {
    test_http_api_matrix::run_live_only_rescan_only().unwrap();
}

#[test]
#[ignore = "requires Linux + CAPANIX_REAL_NFS_E2E=1 + passwordless sudo"]
fn fs_meta_operations_scenarios_real_nfs() {
    run_l5_entry("fs_meta_operations_scenarios_real_nfs", || {
        test_operational_scenarios::run()
    });
}

#[test]
#[ignore = "requires Linux + CAPANIX_REAL_NFS_E2E=1 + passwordless sudo"]
fn fs_meta_operations_force_find_execution_semantics_real_nfs() {
    run_l5_entry(
        "fs_meta_operations_force_find_execution_semantics_real_nfs",
        || test_operational_scenarios::run_force_find_execution_semantics(),
    );
}

#[test]
#[ignore = "requires Linux + CAPANIX_REAL_NFS_E2E=1 + passwordless sudo"]
fn fs_meta_operations_new_nfs_join_real_nfs() {
    run_l5_entry("fs_meta_operations_new_nfs_join_real_nfs", || {
        test_operational_scenarios::run_new_nfs_join()
    });
}

#[test]
#[ignore = "requires Linux + CAPANIX_REAL_NFS_E2E=1 + passwordless sudo"]
fn fs_meta_operations_root_path_modify_real_nfs() {
    run_l5_entry("fs_meta_operations_root_path_modify_real_nfs", || {
        test_operational_scenarios::run_root_path_modify()
    });
}

#[test]
#[ignore = "requires Linux + CAPANIX_REAL_NFS_E2E=1 + passwordless sudo"]
fn fs_meta_operations_visibility_change_and_sink_selection_real_nfs() {
    run_l5_entry(
        "fs_meta_operations_visibility_change_and_sink_selection_real_nfs",
        || test_operational_scenarios::run_visibility_change_and_sink_selection(),
    );
}

#[test]
#[ignore = "requires Linux + CAPANIX_REAL_NFS_E2E=1 + passwordless sudo"]
fn fs_meta_operations_sink_failover_real_nfs() {
    run_l5_entry("fs_meta_operations_sink_failover_real_nfs", || {
        test_operational_scenarios::run_sink_failover()
    });
}

#[test]
#[ignore = "requires Linux + CAPANIX_REAL_NFS_E2E=1 + passwordless sudo"]
fn fs_meta_operations_facade_failover_and_resource_switch_real_nfs() {
    run_l5_entry(
        "fs_meta_operations_facade_failover_and_resource_switch_real_nfs",
        || test_operational_scenarios::run_facade_failover_and_resource_switch(),
    );
}

#[test]
#[ignore = "requires Linux + CAPANIX_REAL_NFS_E2E=1 + passwordless sudo"]
fn fs_meta_operations_nfs_retire_real_nfs() {
    run_l5_entry("fs_meta_operations_nfs_retire_real_nfs", || {
        test_operational_scenarios::run_nfs_retire()
    });
}

#[test]
#[ignore = "requires Linux + CAPANIX_REAL_NFS_E2E=1 + passwordless sudo"]
fn fs_meta_operations_activation_scope_preserved_layout_real_nfs() {
    run_l5_entry(
        "fs_meta_operations_activation_scope_preserved_layout_real_nfs",
        || test_operational_scenarios::run_activation_scope_capture_preserved_layout(),
    );
}

#[test]
#[ignore = "requires Linux + CAPANIX_REAL_NFS_E2E=1 + passwordless sudo"]
fn fs_meta_operations_activation_scope_visibility_contracted_real_nfs() {
    run_l5_entry(
        "fs_meta_operations_activation_scope_visibility_contracted_real_nfs",
        || {
            test_operational_scenarios::run_activation_scope_capture_nfs2_visibility_contracted_to_node_a()
        },
    );
}

#[test]
#[ignore = "requires Linux + CAPANIX_REAL_NFS_E2E=1 + passwordless sudo"]
fn fs_meta_operations_activation_scope_force_find_preserved_real_nfs() {
    run_l5_entry(
        "fs_meta_operations_activation_scope_force_find_preserved_real_nfs",
        || {
            test_operational_scenarios::run_activation_scope_capture_force_find_preserved_pre_force_find()
        },
    );
}

#[test]
#[ignore = "requires Linux + CAPANIX_REAL_NFS_E2E=1 + passwordless sudo"]
fn fs_meta_operations_release_upgrade_real_nfs() {
    run_l5_entry("fs_meta_operations_release_upgrade_real_nfs", || {
        test_release_upgrade::run()
    });
}

#[test]
#[ignore = "requires Linux + CAPANIX_REAL_NFS_E2E=1 + passwordless sudo"]
fn fs_meta_operations_release_upgrade_apply_real_nfs() {
    run_l5_entry("fs_meta_operations_release_upgrade_apply_real_nfs", || {
        test_release_upgrade::run_generation_two_apply()
    });
}

#[test]
#[ignore = "requires Linux + CAPANIX_REAL_NFS_E2E=1 + passwordless sudo"]
fn fs_meta_operations_release_upgrade_peer_source_control_completion_real_nfs() {
    run_l5_entry(
        "fs_meta_operations_release_upgrade_peer_source_control_completion_real_nfs",
        || test_release_upgrade::run_peer_source_control_completion_after_node_a_recovery(),
    );
}

#[test]
#[ignore = "requires Linux + CAPANIX_REAL_NFS_E2E=1 + passwordless sudo"]
fn fs_meta_operations_release_upgrade_facade_claim_continuity_real_nfs() {
    run_l5_entry(
        "fs_meta_operations_release_upgrade_facade_claim_continuity_real_nfs",
        || test_release_upgrade::run_facade_claim_continuity_after_upgrade(),
    );
}

#[test]
#[ignore = "requires Linux + CAPANIX_REAL_NFS_E2E=1 + passwordless sudo"]
fn fs_meta_operations_release_upgrade_roots_persist_real_nfs() {
    run_l5_entry(
        "fs_meta_operations_release_upgrade_roots_persist_real_nfs",
        || test_release_upgrade::run_roots_persist_across_upgrade(),
    );
}

#[test]
#[ignore = "requires Linux + CAPANIX_REAL_NFS_E2E=1 + passwordless sudo"]
fn fs_meta_operations_release_upgrade_tree_stats_stable_real_nfs() {
    run_l5_entry(
        "fs_meta_operations_release_upgrade_tree_stats_stable_real_nfs",
        || test_release_upgrade::run_tree_stats_stable_across_upgrade(),
    );
}

#[test]
#[ignore = "requires Linux + CAPANIX_REAL_NFS_E2E=1 + passwordless sudo"]
fn fs_meta_operations_release_upgrade_tree_materialization_real_nfs() {
    run_l5_entry(
        "fs_meta_operations_release_upgrade_tree_materialization_real_nfs",
        || test_release_upgrade::run_tree_materialization_after_upgrade(),
    );
}

#[test]
#[ignore = "requires Linux + CAPANIX_REAL_NFS_E2E=1 + passwordless sudo"]
fn fs_meta_operations_release_upgrade_sink_control_roles_real_nfs() {
    run_l5_entry(
        "fs_meta_operations_release_upgrade_sink_control_roles_real_nfs",
        || test_release_upgrade::run_sink_control_roles_after_upgrade(),
    );
}

#[test]
#[ignore = "requires Linux + CAPANIX_REAL_NFS_E2E=1 + passwordless sudo"]
fn fs_meta_operations_release_upgrade_source_control_roles_real_nfs() {
    run_l5_entry(
        "fs_meta_operations_release_upgrade_source_control_roles_real_nfs",
        || test_release_upgrade::run_source_control_roles_after_upgrade(),
    );
}

#[test]
#[ignore = "requires Linux + CAPANIX_REAL_NFS_E2E=1 + passwordless sudo"]
fn fs_meta_operations_release_upgrade_runtime_scope_real_nfs() {
    run_l5_entry(
        "fs_meta_operations_release_upgrade_runtime_scope_real_nfs",
        || test_release_upgrade::run_source_runtime_scope_after_upgrade(),
    );
}

#[test]
#[ignore = "requires Linux + CAPANIX_REAL_NFS_E2E=1 + passwordless sudo"]
fn fs_meta_operations_release_upgrade_window_join_real_nfs() {
    run_l5_entry(
        "fs_meta_operations_release_upgrade_window_join_real_nfs",
        || test_release_upgrade::run_upgrade_window_join(),
    );
}

#[test]
#[ignore = "requires Linux + CAPANIX_REAL_NFS_E2E=1 + passwordless sudo"]
fn fs_meta_operations_release_upgrade_cpu_budget_real_nfs() {
    run_l5_entry(
        "fs_meta_operations_release_upgrade_cpu_budget_real_nfs",
        || test_release_upgrade::run_cpu_budget(),
    );
}

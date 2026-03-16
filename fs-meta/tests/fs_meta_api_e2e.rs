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

#[test]
#[ignore = "requires Linux + CAPANIX_REAL_NFS_E2E=1 + passwordless sudo"]
fn fs_meta_http_api_matrix_real_nfs() {
    test_http_api_matrix::run().unwrap();
}

#[test]
#[ignore = "requires Linux + CAPANIX_REAL_NFS_E2E=1 + passwordless sudo"]
fn fs_meta_operational_scenarios_real_nfs() {
    test_operational_scenarios::run().unwrap();
}

#[test]
#[ignore = "requires Linux + CAPANIX_REAL_NFS_E2E=1 + passwordless sudo"]
fn fs_meta_release_upgrade_real_nfs() {
    test_release_upgrade::run().unwrap();
}

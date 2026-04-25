#!/usr/bin/env bash
set -euo pipefail

# Command templates for the fs-meta test matrix. Run from the fustor repository
# root. Real-NFS suites require an explicit environment and are intentionally
# not part of ordinary cargo test.

: "${CAPANIX_WORKER_HOST_BINARY:=/root/repo/capanix/target/debug/capanix_worker_host}"

contract_fast() {
  cargo test -p fustor-specs-root --test app_specs
  cargo test -p fustor-specs-root --test cli_specs
  cargo test -p fustor-specs-root --test specs_fs_meta_contract_fast
}

contract_data_boundary_slow() {
  cargo test -p fustor-specs-root --test specs_fs_meta_domain \
    data_boundary -- --nocapture --test-threads=1
}

core_query_fast() {
  CAPANIX_WORKER_HOST_BINARY="$CAPANIX_WORKER_HOST_BINARY" \
    cargo test -p fs-meta-runtime selected_group -- --nocapture --test-threads=1
  CAPANIX_WORKER_HOST_BINARY="$CAPANIX_WORKER_HOST_BINARY" \
    cargo test -p fs-meta-runtime force_find -- --nocapture --test-threads=1
  cargo test -p fs-meta-runtime status_stats -- --nocapture --test-threads=1
}

core_management_fast() {
  CAPANIX_WORKER_HOST_BINARY="$CAPANIX_WORKER_HOST_BINARY" \
    cargo test -p fs-meta-runtime roots_put -- --nocapture --test-threads=1
}

core_worker_fast() {
  CAPANIX_WORKER_HOST_BINARY="$CAPANIX_WORKER_HOST_BINARY" \
    cargo test -p fs-meta-runtime source -- --nocapture --test-threads=1
  CAPANIX_WORKER_HOST_BINARY="$CAPANIX_WORKER_HOST_BINARY" \
    cargo test -p fs-meta-runtime sink -- --nocapture --test-threads=1
}

core_unit_fast() {
  core_query_fast
  core_management_fast
  core_worker_fast
}

runtime_scope_gate() {
  CAPANIX_RUNTIME_SCOPE_E2E=1 \
    CAPANIX_WORKER_HOST_BINARY="$CAPANIX_WORKER_HOST_BINARY" \
    cargo test -p fustor-specs-root --test runtime_scope_e2e -- --nocapture --test-threads=1
}

mini_real_nfs_smoke() {
  if ! grep -q "fn fs_meta_http_api_matrix_mini_real_nfs" fs-meta/tests/fs_meta_api_e2e.rs; then
    echo "mini-real-nfs-smoke is only available in validation assets that define fs_meta_http_api_matrix_mini_real_nfs" >&2
    exit 3
  fi
  CAPANIX_REAL_NFS_E2E=1 \
    CAPANIX_WORKER_HOST_BINARY="$CAPANIX_WORKER_HOST_BINARY" \
    cargo test -p fustor-specs-root --test fs_meta_api_e2e \
      fs_meta_http_api_matrix_mini_real_nfs -- --ignored --nocapture --test-threads=1
}

real_nfs_api_core() {
  CAPANIX_REAL_NFS_E2E=1 \
    CAPANIX_WORKER_HOST_BINARY="$CAPANIX_WORKER_HOST_BINARY" \
    cargo test -p fustor-specs-root --test fs_meta_api_e2e \
      fs_meta_http_api_matrix_real_nfs -- --ignored --nocapture --test-threads=1
  CAPANIX_REAL_NFS_E2E=1 \
    CAPANIX_WORKER_HOST_BINARY="$CAPANIX_WORKER_HOST_BINARY" \
    cargo test -p fustor-specs-root --test fs_meta_api_e2e \
      fs_meta_http_api_matrix_query_baseline_real_nfs -- --ignored --nocapture --test-threads=1
  CAPANIX_REAL_NFS_E2E=1 \
    CAPANIX_WORKER_HOST_BINARY="$CAPANIX_WORKER_HOST_BINARY" \
    cargo test -p fustor-specs-root --test fs_meta_api_e2e \
      fs_meta_http_api_matrix_live_only_rescan_real_nfs -- --ignored --nocapture --test-threads=1
}

real_nfs_ops() {
  CAPANIX_REAL_NFS_E2E=1 \
    CAPANIX_WORKER_HOST_BINARY="$CAPANIX_WORKER_HOST_BINARY" \
    cargo test -p fustor-specs-root --test fs_meta_api_e2e \
      fs_meta_operational -- --ignored --nocapture --test-threads=1
}

real_nfs_component() {
  CAPANIX_REAL_NFS_E2E=1 \
    CAPANIX_WORKER_HOST_BINARY="$CAPANIX_WORKER_HOST_BINARY" \
    cargo test -p fs-meta-runtime external_runtime_app_selected_group_proxy \
      -- --ignored --nocapture --test-threads=1
  CAPANIX_REAL_NFS_E2E=1 \
    CAPANIX_WORKER_HOST_BINARY="$CAPANIX_WORKER_HOST_BINARY" \
    cargo test -p fs-meta-runtime external_source_worker_real_nfs_manual_rescan \
      -- --ignored --nocapture --test-threads=1
}

real_nfs_upgrade() {
  CAPANIX_REAL_NFS_E2E=1 \
    CAPANIX_WORKER_HOST_BINARY="$CAPANIX_WORKER_HOST_BINARY" \
    cargo test -p fustor-specs-root --test fs_meta_api_e2e \
      fs_meta_release_upgrade -- --ignored --nocapture --test-threads=1
}

real_nfs_all() {
  real_nfs_api_core
  real_nfs_ops
  real_nfs_component
  real_nfs_upgrade
}

usage() {
  cat <<'EOF'
Usage: fs-meta/docs/examples/test-matrix-commands.sh <suite>

Suites:
  contract-fast
  contract-data-boundary-slow
  core-query-fast
  core-management-fast
  core-worker-fast
  core-unit-fast
  runtime-scope-gate
  mini-real-nfs-smoke
  real-nfs-api-core
  real-nfs-ops
  real-nfs-component
  real-nfs-upgrade
  real-nfs-all
EOF
}

case "${1:-}" in
  contract-fast) contract_fast ;;
  contract-data-boundary-slow) contract_data_boundary_slow ;;
  core-query-fast) core_query_fast ;;
  core-management-fast) core_management_fast ;;
  core-worker-fast) core_worker_fast ;;
  core-unit-fast) core_unit_fast ;;
  runtime-scope-gate) runtime_scope_gate ;;
  mini-real-nfs-smoke) mini_real_nfs_smoke ;;
  real-nfs-api-core) real_nfs_api_core ;;
  real-nfs-ops) real_nfs_ops ;;
  real-nfs-component) real_nfs_component ;;
  real-nfs-upgrade) real_nfs_upgrade ;;
  real-nfs-all) real_nfs_all ;;
  *) usage; exit 2 ;;
esac

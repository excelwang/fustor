#!/usr/bin/env bash
set -euo pipefail

# Command templates for the fs-meta test matrix. Run from the fustor repository
# root. The public suite surface is organized by feature priority first, then
# environment: business -> environment -> operations.

: "${CAPANIX_WORKER_HOST_BINARY:=/root/repo/capanix/target/debug/capanix_worker_host}"
: "${FSMETA_FULL_NFS_ROOTS_FILE:=}"

announce_suite() {
  local suite="$1"
  local environment="$2"
  local full_real_nfs="$3"
  local operations="$4"
  cat <<EOF2
[fs-meta-test-matrix] suite=${suite}
[fs-meta-test-matrix] environment=${environment}
[fs-meta-test-matrix] full_real_nfs_allowed=${full_real_nfs}
[fs-meta-test-matrix] includes_operations=${operations}
EOF2
}

require_full_demo_assets() {
  local roots_file="$FSMETA_FULL_NFS_ROOTS_FILE"
  if [[ -z "$roots_file" ]]; then
    echo "full real-NFS suites require FSMETA_FULL_NFS_ROOTS_FILE to point at the local demo roots asset" >&2
    exit 3
  fi
  if [[ ! -f "$roots_file" ]]; then
    echo "full real-NFS suites require a local demo roots asset: missing $roots_file" >&2
    exit 3
  fi
  local host_count
  host_count="$(grep -E '\"host_ip\"[[:space:]]*:' "$roots_file" | wc -l | tr -d ' ')"
  if [[ "$host_count" -lt 5 ]]; then
    echo "full real-NFS suites require a 5-node demo roots asset; found host_ip count=$host_count in $roots_file" >&2
    exit 3
  fi
  echo "[fs-meta-test-matrix] demo_validation_assets=$roots_file"
}

contract_fast() {
  cargo test -p fustor-specs-root --test app_specs
  cargo test -p fustor-specs-root --test cli_specs
  cargo test -p fustor-specs-root --test specs_fs_meta_contract_fast
}

business_api_fast() {
  CAPANIX_WORKER_HOST_BINARY="$CAPANIX_WORKER_HOST_BINARY" \
    cargo test -p fs-meta-runtime selected_group -- --nocapture --test-threads=1
  CAPANIX_WORKER_HOST_BINARY="$CAPANIX_WORKER_HOST_BINARY" \
    cargo test -p fs-meta-runtime force_find -- --nocapture --test-threads=1
  cargo test -p fs-meta-runtime status_stats -- --nocapture --test-threads=1
  CAPANIX_WORKER_HOST_BINARY="$CAPANIX_WORKER_HOST_BINARY" \
    cargo test -p fs-meta-runtime roots_put -- --nocapture --test-threads=1
}

business_fast() {
  announce_suite "business-fast" "local/tmpfs/mock-worker" "no" "no"
  contract_fast
  business_api_fast
}

business_mini_nfs() {
  announce_suite "business-mini-nfs" "5-node-mini-real-nfs" "no" "no"
  CAPANIX_REAL_NFS_E2E=1 \
    CAPANIX_WORKER_HOST_BINARY="$CAPANIX_WORKER_HOST_BINARY" \
    cargo test -p fustor-specs-root --test fs_meta_api_e2e \
      fs_meta_business_mini_real_nfs -- --ignored --nocapture --test-threads=1
}

environment_full_nfs() {
  announce_suite "environment-full-nfs" "5-node-full-real-nfs-demo" "yes" "no"
  require_full_demo_assets
  CAPANIX_REAL_NFS_E2E=1 \
    CAPANIX_WORKER_HOST_BINARY="$CAPANIX_WORKER_HOST_BINARY" \
    cargo test -p fustor-specs-root --test fs_meta_api_e2e \
      fs_meta_business_query_real_nfs -- --ignored --nocapture --test-threads=1
  CAPANIX_REAL_NFS_E2E=1 \
    CAPANIX_WORKER_HOST_BINARY="$CAPANIX_WORKER_HOST_BINARY" \
    cargo test -p fustor-specs-root --test fs_meta_api_e2e \
      fs_meta_environment -- --ignored --nocapture --test-threads=1
}

operations_local() {
  announce_suite "operations-local" "local-worker-runtime" "no" "yes"
  CAPANIX_WORKER_HOST_BINARY="$CAPANIX_WORKER_HOST_BINARY" \
    cargo test -p fs-meta-runtime --lib workers::source::tests::source_control \
      -- --nocapture --test-threads=1
  CAPANIX_WORKER_HOST_BINARY="$CAPANIX_WORKER_HOST_BINARY" \
    cargo test -p fs-meta-runtime --lib workers::source::tests::external_source_worker_force_find \
      -- --nocapture --test-threads=1
  CAPANIX_WORKER_HOST_BINARY="$CAPANIX_WORKER_HOST_BINARY" \
    cargo test -p fs-meta-runtime --lib workers::sink::tests::sink_control_frame_machine \
      -- --nocapture --test-threads=1
  CAPANIX_WORKER_HOST_BINARY="$CAPANIX_WORKER_HOST_BINARY" \
    cargo test -p fs-meta-runtime --lib workers::sink::tests::status_snapshot_nonblocking \
      -- --nocapture --test-threads=1
  CAPANIX_RUNTIME_SCOPE_E2E=1 \
    CAPANIX_WORKER_HOST_BINARY="$CAPANIX_WORKER_HOST_BINARY" \
    cargo test -p fustor-specs-root --test runtime_scope_e2e -- --nocapture --test-threads=1
}

operations_real_nfs() {
  announce_suite "operations-real-nfs" "5-node-full-real-nfs-demo" "yes" "yes"
  require_full_demo_assets
  CAPANIX_REAL_NFS_E2E=1 \
    CAPANIX_WORKER_HOST_BINARY="$CAPANIX_WORKER_HOST_BINARY" \
    cargo test -p fustor-specs-root --test fs_meta_api_e2e \
      fs_meta_operations -- --ignored --nocapture --test-threads=1
  CAPANIX_REAL_NFS_E2E=1 \
    CAPANIX_WORKER_HOST_BINARY="$CAPANIX_WORKER_HOST_BINARY" \
    cargo test -p fs-meta-runtime external_runtime_app_selected_group_proxy \
      -- --ignored --nocapture --test-threads=1
  CAPANIX_REAL_NFS_E2E=1 \
    CAPANIX_WORKER_HOST_BINARY="$CAPANIX_WORKER_HOST_BINARY" \
    cargo test -p fs-meta-runtime external_source_worker_real_nfs_manual_rescan \
      -- --ignored --nocapture --test-threads=1
}

progressive_business() {
  business_fast
  business_mini_nfs
}

progressive_environment() {
  progressive_business
  environment_full_nfs
}

progressive_operations() {
  progressive_environment
  operations_local
  operations_real_nfs
}

usage() {
  cat <<'EOF2'
Usage: fs-meta/docs/examples/test-matrix-commands.sh <suite>

Suites:
  business-fast
  business-mini-nfs
  environment-full-nfs
  operations-local
  operations-real-nfs
  progressive-business
  progressive-environment
  progressive-operations
EOF2
}

case "${1:-}" in
  business-fast) business_fast ;;
  business-mini-nfs) business_mini_nfs ;;
  environment-full-nfs) environment_full_nfs ;;
  operations-local) operations_local ;;
  operations-real-nfs) operations_real_nfs ;;
  progressive-business) progressive_business ;;
  progressive-environment) progressive_environment ;;
  progressive-operations) progressive_operations ;;
  -h|--help|help|"") usage ;;
  *)
    echo "unknown suite: $1" >&2
    usage >&2
    exit 2
    ;;
esac

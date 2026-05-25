#!/usr/bin/env bash
set -euo pipefail

# Command templates for the fs-meta test matrix. Run from the fustor repository
# root. The public suite surface is organized by failure-localization depth:
# contracts -> single-process -> local runtime -> NFS environment -> real cluster.

: "${CAPANIX_WORKER_HOST_BINARY:=}"
: "${FSMETA_FULL_NFS_ROOTS_FILE:=}"

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../../.." && pwd)"

resolve_worker_host_binary() {
  local bin_name="capanix_worker_host"
  if [[ "${OS:-}" == "Windows_NT" ]]; then
    bin_name="capanix_worker_host.exe"
  fi

  if [[ -n "${CAPANIX_WORKER_HOST_BINARY}" ]]; then
    if [[ "${CAPANIX_WORKER_HOST_BINARY}" != /* ]]; then
      echo "CAPANIX_WORKER_HOST_BINARY must be an absolute path: ${CAPANIX_WORKER_HOST_BINARY}" >&2
      exit 2
    fi
    if [[ ! -x "${CAPANIX_WORKER_HOST_BINARY}" ]]; then
      echo "CAPANIX_WORKER_HOST_BINARY is not executable: ${CAPANIX_WORKER_HOST_BINARY}" >&2
      exit 2
    fi
    printf '%s\n' "${CAPANIX_WORKER_HOST_BINARY}"
    return 0
  fi

  local candidate
  for candidate in \
    "${repo_root}/../capanix/target/debug/${bin_name}" \
    "${repo_root}/../capanix/.target/debug/${bin_name}"
  do
    if [[ -x "${candidate}" ]]; then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done

  if command -v "${bin_name}" >/dev/null 2>&1; then
    command -v "${bin_name}"
    return 0
  fi

  cat >&2 <<EOF2
unable to resolve capanix worker host binary
- set CAPANIX_WORKER_HOST_BINARY to an absolute executable path, or
- keep a sibling capanix checkout at ../capanix with target/debug/${bin_name}
repo_root=${repo_root}
EOF2
  exit 2
}

ensure_worker_host_binary() {
  CAPANIX_WORKER_HOST_BINARY="$(resolve_worker_host_binary)"
  export CAPANIX_WORKER_HOST_BINARY
  echo "[fs-meta-test-matrix] worker_host_binary=${CAPANIX_WORKER_HOST_BINARY}"
}

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
  python3 - "$roots_file" <<'PY'
import json
import os
import sys

roots_file = sys.argv[1]
with open(roots_file, "r", encoding="utf-8") as handle:
    payload = json.load(handle)
roots = payload.get("roots", payload)
if not isinstance(roots, list) or len(roots) < 5:
    raise SystemExit(
        f"full real-NFS suites require at least 5 demo roots; found {len(roots) if isinstance(roots, list) else 'invalid'} in {roots_file}"
    )

mounted = set()
with open("/proc/self/mountinfo", "r", encoding="utf-8") as handle:
    for line in handle:
        fields = line.split(" - ", 1)[0].split()
        if len(fields) >= 5:
            mounted.add(bytes(fields[4], "utf-8").decode("unicode_escape"))

for root in roots:
    selector = root.get("selector") or {}
    root_id = root.get("id", "<missing>")
    host_ip = selector.get("host_ip")
    mount_point = selector.get("mount_point")
    subpath_scope = root.get("subpath_scope") or "/"
    if not host_ip:
        raise SystemExit(f"full demo root {root_id} must set selector.host_ip")
    if not mount_point:
        raise SystemExit(f"full demo root {root_id} must set selector.mount_point")
    if not isinstance(subpath_scope, str) or not subpath_scope.startswith("/"):
        raise SystemExit(f"full demo root {root_id} must set absolute subpath_scope")
    if not os.path.isdir(mount_point):
        raise SystemExit(f"full demo root {root_id} mount {mount_point} for host {host_ip} is not available")
    if mount_point not in mounted:
        raise SystemExit(f"full demo root {root_id} path {mount_point} for host {host_ip} is not an active mount")
    try:
        os.scandir(mount_point).close()
    except OSError as exc:
        raise SystemExit(f"full demo root {root_id} mount {mount_point} for host {host_ip} is not readable: {exc}") from exc
    scoped_path = mount_point if subpath_scope == "/" else os.path.join(mount_point, subpath_scope.lstrip("/"))
    if not os.path.isdir(scoped_path):
        raise SystemExit(f"full demo root {root_id} scoped path {scoped_path} for host {host_ip} is not available")
    try:
        os.scandir(scoped_path).close()
    except OSError as exc:
        raise SystemExit(f"full demo root {root_id} scoped path {scoped_path} for host {host_ip} is not readable: {exc}") from exc
PY
  echo "[fs-meta-test-matrix] demo_validation_assets=$roots_file"
}

run_real_nfs_api_filter() {
  local stage="$1"
  local filter="$2"
  echo "[fs-meta-test-matrix] stage=${stage}"
  echo "[fs-meta-test-matrix] filter=${filter}"
  CAPANIX_REAL_NFS_E2E=1 \
    CAPANIX_WORKER_HOST_BINARY="$CAPANIX_WORKER_HOST_BINARY" \
    cargo test -p fustor-specs-root --test fs_meta_api_e2e \
      "$filter" -- --ignored --nocapture --test-threads=1
}

run_real_nfs_runtime_filter() {
  local stage="$1"
  local filter="$2"
  echo "[fs-meta-test-matrix] stage=${stage}"
  echo "[fs-meta-test-matrix] filter=${filter}"
  CAPANIX_REAL_NFS_E2E=1 \
    CAPANIX_WORKER_HOST_BINARY="$CAPANIX_WORKER_HOST_BINARY" \
    cargo test -p fs-meta-runtime "$filter" \
      -- --ignored --nocapture --test-threads=1
}

L5_STAGE_TOTAL=12

run_l5_marker() {
  local index="$1"
  local phase="$2"
  local boundary="$3"
  echo "[fs-meta-test-matrix] l5_stage=${index}/${L5_STAGE_TOTAL}"
  echo "[fs-meta-test-matrix] l5_phase=${phase}"
  echo "[fs-meta-test-matrix] boundary=${boundary}"
}

run_l5_acceptance_marker() {
  local index="$1"
  local boundary="$2"
  run_l5_marker "$index" "acceptance" "$boundary"
}

run_l5_api_case() {
  local index="$1"
  local phase="$2"
  local boundary="$3"
  local stage="$4"
  local filter="$5"
  run_l5_marker "$index" "$phase" "$boundary"
  echo "[fs-meta-test-matrix] l5_case=${stage}"
  run_real_nfs_api_filter "l5.${stage}" "$filter"
}

run_l5_runtime_case() {
  local index="$1"
  local phase="$2"
  local boundary="$3"
  local stage="$4"
  local filter="$5"
  run_l5_marker "$index" "$phase" "$boundary"
  echo "[fs-meta-test-matrix] l5_case=${stage}"
  run_real_nfs_runtime_filter "l5.${stage}" "$filter"
}

contract_fast() {
  cargo test -p fustor-specs-root --test app_specs
  cargo test -p fustor-specs-root --test cli_specs
  cargo test -p fustor-specs-root --test specs_fs_meta_contract_fast
}

root_namespace_contracts() {
  contract_fast
  cargo test -p fs-meta-runtime subpath_scope_materializes_root_relative_paths \
    -- --nocapture --test-threads=1
  cargo test -p fs-meta-runtime update_logical_roots_resets_materialized_group_when_covered_scope_changes \
    -- --nocapture --test-threads=1
}

single_process_closed_loop_core() {
  cargo test -p fs-meta-runtime slow_watch_setup_does_not_block_initial_scan_stream \
    -- --nocapture --test-threads=1
  CAPANIX_WORKER_HOST_BINARY="$CAPANIX_WORKER_HOST_BINARY" \
    cargo test -p fs-meta-runtime selected_group -- --nocapture --test-threads=1
  CAPANIX_WORKER_HOST_BINARY="$CAPANIX_WORKER_HOST_BINARY" \
    cargo test -p fs-meta-runtime force_find -- --nocapture --test-threads=1
  cargo test -p fs-meta-runtime status_stats -- --nocapture --test-threads=1
  CAPANIX_WORKER_HOST_BINARY="$CAPANIX_WORKER_HOST_BINARY" \
    cargo test -p fs-meta-runtime roots_put -- --nocapture --test-threads=1
}

runtime_local_multinode_core() {
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

contracts_fast() {
  announce_suite "contracts-fast" "local/no-runtime/no-real-nfs" "no" "no"
  root_namespace_contracts
}

single_process_closed_loop() {
  announce_suite "single-process-closed-loop" "local/tmpfs/mock-worker" "no" "no"
  ensure_worker_host_binary
  single_process_closed_loop_core
}

runtime_local_multinode() {
  announce_suite "runtime-local-multinode" "local-capanix-runtime-worker" "no" "yes"
  ensure_worker_host_binary
  runtime_local_multinode_core
}

business_fast() {
  announce_suite "business-fast" "compat: contracts-fast + single-process-closed-loop" "no" "no"
  ensure_worker_host_binary
  root_namespace_contracts
  single_process_closed_loop_core
}

business_mini_nfs() {
  announce_suite "business-mini-nfs" "5-node-mini-real-nfs" "no" "no"
  ensure_worker_host_binary
  CAPANIX_REAL_NFS_E2E=1 \
    CAPANIX_WORKER_HOST_BINARY="$CAPANIX_WORKER_HOST_BINARY" \
    cargo test -p fustor-specs-root --test fs_meta_api_e2e \
      fs_meta_business_mini_real_nfs -- --ignored --nocapture --test-threads=1
}

environment_full_nfs() {
  announce_suite "environment-full-nfs" "5-node-full-real-nfs-demo" "yes" "no"
  ensure_worker_host_binary
  require_full_demo_assets
  CAPANIX_REAL_NFS_E2E=1 \
    CAPANIX_WORKER_HOST_BINARY="$CAPANIX_WORKER_HOST_BINARY" \
    cargo test -p fustor-specs-root --test fs_meta_api_e2e \
      fs_meta_business_query_real_nfs -- --ignored --nocapture --test-threads=1
  CAPANIX_REAL_NFS_E2E=1 \
    CAPANIX_WORKER_HOST_BINARY="$CAPANIX_WORKER_HOST_BINARY" \
    cargo test -p fustor-specs-root --test fs_meta_api_e2e \
      fs_meta_environment_full_real_nfs -- --ignored --nocapture --test-threads=1
  FSMETA_FULL_NFS_ROOTS_FILE= \
    CAPANIX_REAL_NFS_E2E=1 \
    CAPANIX_WORKER_HOST_BINARY="$CAPANIX_WORKER_HOST_BINARY" \
    cargo test -p fustor-specs-root --test fs_meta_api_e2e \
      fs_meta_environment_live_only_rescan_real_nfs -- --ignored --nocapture --test-threads=1
}

operations_local() {
  announce_suite "operations-local" "compat: runtime-local-multinode" "no" "yes"
  ensure_worker_host_binary
  runtime_local_multinode_core
}

nfs_environment_gate() {
  local stage="${1:-all}"
  case "$stage" in
    all)
      business_mini_nfs
      environment_full_nfs
      ;;
    mini)
      business_mini_nfs
      ;;
    full)
      environment_full_nfs
      ;;
    *)
      echo "unknown nfs-environment-gate stage: $stage" >&2
      usage >&2
      exit 2
      ;;
  esac
}

l5_acceptance_all() {
  l5_dispatch preflight
  l5_dispatch deploy-upgrade
  l5_dispatch management-api
  l5_dispatch source-audit
  l5_dispatch sink-materialization
  l5_dispatch query
  l5_dispatch resilience
}

l5_ops_all() {
  l5_dispatch foundation-real-runtime
  l5_dispatch upgrade-core
  l5_dispatch topology-change
  l5_dispatch recovery-switch
  l5_dispatch resource-budget
}

l5_dispatch() {
  local stage="${1:-all}"
  case "$stage" in
    all)
      l5_acceptance_all
      l5_ops_all
      ;;
    acceptance)
      l5_acceptance_all
      ;;
    ops)
      l5_ops_all
      ;;
    preflight)
      run_l5_acceptance_marker 1 "preflight"
      ;;
    deploy-upgrade)
      run_l5_acceptance_marker 2 "deploy-upgrade"
      run_real_nfs_api_filter "l5.acceptance.deploy-upgrade.apply" \
        fs_meta_operations_release_upgrade_apply_real_nfs
      run_real_nfs_api_filter "l5.acceptance.deploy-upgrade.http" \
        fs_meta_operations_release_upgrade_real_nfs
      ;;
    management-api)
      run_l5_acceptance_marker 3 "management-api"
      run_real_nfs_api_filter "l5.acceptance.management-api.roots-persist" \
        fs_meta_operations_release_upgrade_roots_persist_real_nfs
      ;;
    source-audit)
      run_l5_acceptance_marker 4 "source-audit"
      run_real_nfs_runtime_filter "l5.acceptance.source-audit.manual-rescan" \
        external_source_worker_real_nfs_manual_rescan
      run_real_nfs_api_filter "l5.acceptance.source-audit.delivery-ready" \
        fs_meta_operations_visibility_source_delivery_ready_real_nfs
      ;;
    sink-materialization)
      run_l5_acceptance_marker 5 "sink-materialization"
      run_real_nfs_api_filter "l5.acceptance.sink-materialization.tree" \
        fs_meta_operations_release_upgrade_tree_materialization_real_nfs
      ;;
    query)
      run_l5_acceptance_marker 6 "query"
      run_real_nfs_api_filter "l5.acceptance.query.tree-stats" \
        fs_meta_operations_release_upgrade_tree_stats_stable_real_nfs
      run_real_nfs_api_filter "l5.acceptance.query.force-find" \
        fs_meta_operations_scenarios_real_nfs
      ;;
    resilience)
      run_l5_acceptance_marker 7 "resilience"
      run_real_nfs_api_filter "l5.acceptance.resilience.facade-continuity" \
        fs_meta_operations_release_upgrade_facade_claim_continuity_real_nfs
      run_real_nfs_api_filter "l5.acceptance.resilience.sink-failover" \
        fs_meta_operations_sink_failover_real_nfs
      ;;
    foundation-real-runtime)
      l5_dispatch runtime-selected-group-proxy
      l5_dispatch ops-force-find-semantics
      l5_dispatch ops-visibility-facade-live
      ;;
    upgrade-core)
      l5_dispatch upgrade-peer-source-control
      l5_dispatch upgrade-sink-scope
      l5_dispatch upgrade-runtime-scope
      ;;
    topology-change)
      l5_dispatch ops-new-nfs-join
      l5_dispatch ops-root-path-modify
      l5_dispatch ops-nfs-retire
      l5_dispatch upgrade-window-join
      ;;
    recovery-switch)
      l5_dispatch ops-facade-resource-switch
      l5_dispatch ops-activation-preserved
      l5_dispatch ops-activation-visibility
      l5_dispatch ops-activation-force-find
      ;;
    resource-budget)
      l5_dispatch upgrade-cpu-budget
      ;;
    runtime-selected-group-proxy)
      run_l5_runtime_case 8 "ops" "foundation-real-runtime" \
        "ops.runtime.selected-group-proxy" \
        external_runtime_app_selected_group_proxy
      ;;
    ops-force-find-semantics)
      run_l5_api_case 8 "ops" "foundation-real-runtime" \
        "ops.force-find-semantics" \
        fs_meta_operations_force_find_execution_semantics_real_nfs
      ;;
    ops-visibility-facade-live)
      run_l5_api_case 8 "ops" "foundation-real-runtime" \
        "ops.visibility.facade-live" \
        fs_meta_operations_visibility_facade_live_real_nfs
      ;;
    upgrade-peer-source-control)
      run_l5_api_case 9 "ops" "upgrade-core" \
        "ops.upgrade.peer-source-control" \
        fs_meta_operations_release_upgrade_peer_source_control_completion_real_nfs
      ;;
    upgrade-sink-scope)
      run_l5_api_case 9 "ops" "upgrade-core" \
        "ops.upgrade.sink-scope" \
        fs_meta_operations_release_upgrade_sink_control_roles_real_nfs
      ;;
    upgrade-runtime-scope)
      run_l5_api_case 9 "ops" "upgrade-core" \
        "ops.upgrade.runtime-scope" \
        fs_meta_operations_release_upgrade_runtime_scope_real_nfs
      ;;
    ops-new-nfs-join)
      run_l5_api_case 10 "ops" "topology-change" \
        "ops.new-nfs-join" \
        fs_meta_operations_new_nfs_join_real_nfs
      ;;
    ops-root-path-modify)
      run_l5_api_case 10 "ops" "topology-change" \
        "ops.root-path-modify" \
        fs_meta_operations_root_path_modify_real_nfs
      ;;
    ops-nfs-retire)
      run_l5_api_case 10 "ops" "topology-change" \
        "ops.nfs-retire" \
        fs_meta_operations_nfs_retire_real_nfs
      ;;
    upgrade-window-join)
      run_l5_api_case 10 "ops" "topology-change" \
        "ops.upgrade.window-join" \
        fs_meta_operations_release_upgrade_window_join_real_nfs
      ;;
    ops-facade-resource-switch)
      run_l5_api_case 11 "ops" "recovery-switch" \
        "ops.facade-resource-switch" \
        fs_meta_operations_facade_failover_and_resource_switch_real_nfs
      ;;
    ops-activation-preserved)
      run_l5_api_case 11 "ops" "recovery-switch" \
        "ops.activation-preserved" \
        fs_meta_operations_activation_scope_preserved_layout_real_nfs
      ;;
    ops-activation-visibility)
      run_l5_api_case 11 "ops" "recovery-switch" \
        "ops.activation-visibility" \
        fs_meta_operations_activation_scope_visibility_contracted_real_nfs
      ;;
    ops-activation-force-find)
      run_l5_api_case 11 "ops" "recovery-switch" \
        "ops.activation-force-find" \
        fs_meta_operations_activation_scope_force_find_preserved_real_nfs
      ;;
    upgrade-cpu-budget)
      run_l5_api_case 12 "ops" "resource-budget" \
        "ops.upgrade.cpu-budget" \
        fs_meta_operations_release_upgrade_cpu_budget_real_nfs
      ;;
    *)
      echo "unknown l5 stage: $stage" >&2
      usage >&2
      exit 2
      ;;
  esac
}

l5_stage_known() {
  local stage="${1:-all}"
  case "$stage" in
    all|acceptance|ops|\
    preflight|deploy-upgrade|management-api|source-audit|sink-materialization|query|resilience|\
    foundation-real-runtime|upgrade-core|topology-change|recovery-switch|resource-budget|\
    runtime-selected-group-proxy|ops-force-find-semantics|ops-visibility-facade-live|\
    upgrade-peer-source-control|upgrade-sink-scope|upgrade-runtime-scope|ops-new-nfs-join|\
    ops-root-path-modify|ops-nfs-retire|upgrade-window-join|ops-facade-resource-switch|\
    ops-activation-preserved|ops-activation-visibility|ops-activation-force-find|upgrade-cpu-budget)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

l5() {
  local stage="${1:-all}"
  if ! l5_stage_known "$stage"; then
    echo "unknown l5 stage: $stage" >&2
    usage >&2
    exit 2
  fi
  announce_suite "l5" "real-cluster-full-nfs" "yes" "yes"
  ensure_worker_host_binary
  require_full_demo_assets
  l5_dispatch "$stage"
}

progressive_business() {
  contracts_fast
  single_process_closed_loop
}

progressive_environment() {
  progressive_business
  runtime_local_multinode
  nfs_environment_gate
}

progressive_operations() {
  progressive_environment
  l5
}

usage() {
  cat <<'EOF2'
Usage: fs-meta/docs/examples/test-matrix-commands.sh <suite>
       fs-meta/docs/examples/test-matrix-commands.sh nfs-environment-gate [mini|full|all]
       fs-meta/docs/examples/test-matrix-commands.sh l5 [all|acceptance|ops|stage]

Suites:
  contracts-fast
  single-process-closed-loop
  runtime-local-multinode
  nfs-environment-gate
  l5
  business-fast
  business-mini-nfs
  environment-full-nfs
  operations-local
  progressive-business
  progressive-environment
  progressive-operations

L5 suite selectors:
  all
  acceptance
  ops

L5 acceptance stages:
  preflight
  deploy-upgrade
  management-api
  source-audit
  sink-materialization
  query
  resilience

L5 ops groups:
  foundation-real-runtime
  upgrade-core
  topology-change
  recovery-switch
  resource-budget

L5 ops atomic stages:
  runtime-selected-group-proxy
  ops-force-find-semantics
  ops-visibility-facade-live
  upgrade-peer-source-control
  upgrade-sink-scope
  upgrade-runtime-scope
  ops-new-nfs-join
  ops-root-path-modify
  ops-nfs-retire
  upgrade-window-join
  ops-facade-resource-switch
  ops-activation-preserved
  ops-activation-visibility
  ops-activation-force-find
  upgrade-cpu-budget
EOF2
}

case "${1:-}" in
  contracts-fast) contracts_fast ;;
  single-process-closed-loop) single_process_closed_loop ;;
  runtime-local-multinode) runtime_local_multinode ;;
  nfs-environment-gate) nfs_environment_gate "${2:-all}" ;;
  l5) l5 "${2:-all}" ;;
  business-fast) business_fast ;;
  business-mini-nfs) business_mini_nfs ;;
  environment-full-nfs) environment_full_nfs ;;
  operations-local) operations_local ;;
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

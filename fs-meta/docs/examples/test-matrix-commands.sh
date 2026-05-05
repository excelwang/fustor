#!/usr/bin/env bash
set -euo pipefail

# Command templates for the fs-meta test matrix. Run from the fustor repository
# root. The public suite surface is organized by feature priority first, then
# environment: business -> environment -> operations.

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
    if not host_ip:
        raise SystemExit(f"full demo root {root_id} must set selector.host_ip")
    if not mount_point:
        raise SystemExit(f"full demo root {root_id} must set selector.mount_point")
    if not os.path.isdir(mount_point):
        raise SystemExit(f"full demo root {root_id} mount {mount_point} for host {host_ip} is not available")
    if mount_point not in mounted:
        raise SystemExit(f"full demo root {root_id} path {mount_point} for host {host_ip} is not an active mount")
    try:
        os.scandir(mount_point).close()
    except OSError as exc:
        raise SystemExit(f"full demo root {root_id} mount {mount_point} for host {host_ip} is not readable: {exc}") from exc
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

L5_STAGE_TOTAL=24
L5_VISIBILITY_SUBSTAGE_TOTAL=7

run_l5_api_stage() {
  local index="$1"
  local stage="$2"
  local filter="$3"
  echo "[fs-meta-test-matrix] l5_progress=${index}/${L5_STAGE_TOTAL}"
  run_real_nfs_api_filter "l5.${stage}" "$filter"
}

run_l5_api_substage() {
  local subindex="$1"
  local stage="$2"
  local filter="$3"
  local effective
  effective="$(python3 - "$subindex" "$L5_VISIBILITY_SUBSTAGE_TOTAL" <<'PY'
import sys

subindex = int(sys.argv[1])
total = int(sys.argv[2])
print(f"{4 + (subindex / total):.2f}")
PY
)"
  echo "[fs-meta-test-matrix] l5_stage=5/${L5_STAGE_TOTAL}"
  echo "[fs-meta-test-matrix] l5_subprogress=${subindex}/${L5_VISIBILITY_SUBSTAGE_TOTAL}"
  echo "[fs-meta-test-matrix] l5_progress=${effective}/${L5_STAGE_TOTAL}"
  run_real_nfs_api_filter "l5.${stage}" "$filter"
}

run_l5_runtime_stage() {
  local index="$1"
  local stage="$2"
  local filter="$3"
  echo "[fs-meta-test-matrix] l5_progress=${index}/${L5_STAGE_TOTAL}"
  run_real_nfs_runtime_filter "l5.${stage}" "$filter"
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
  ensure_worker_host_binary
  contract_fast
  business_api_fast
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
      fs_meta_environment -- --ignored --nocapture --test-threads=1
}

operations_local() {
  announce_suite "operations-local" "local-worker-runtime" "no" "yes"
  ensure_worker_host_binary
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
  local stage="${1:-all}"
  announce_suite "operations-real-nfs" "5-node-full-real-nfs-demo" "yes" "yes"
  ensure_worker_host_binary
  require_full_demo_assets
  case "$stage" in
    all)
      operations_real_nfs foundation-real-runtime
      operations_real_nfs upgrade-core
      operations_real_nfs topology-change
      operations_real_nfs recovery-switch
      operations_real_nfs resource-budget
      ;;
    foundation-real-runtime)
      operations_real_nfs runtime-selected-group-proxy
      operations_real_nfs runtime-source-worker-manual-rescan
      operations_real_nfs ops-force-find-smoke
      operations_real_nfs ops-force-find-semantics
      operations_real_nfs ops-visibility-sink-selection
      ;;
    upgrade-core)
      operations_real_nfs upgrade-apply-generation-two
      operations_real_nfs upgrade-generation-two-http
      operations_real_nfs upgrade-peer-source-control
      operations_real_nfs upgrade-sink-scope
      operations_real_nfs upgrade-runtime-scope
      operations_real_nfs upgrade-roots-persist
      operations_real_nfs upgrade-tree-stats
      operations_real_nfs upgrade-tree-materialization
      ;;
    topology-change)
      operations_real_nfs ops-new-nfs-join
      operations_real_nfs ops-root-path-modify
      operations_real_nfs ops-nfs-retire
      operations_real_nfs upgrade-window-join
      ;;
    recovery-switch)
      operations_real_nfs ops-sink-failover
      operations_real_nfs ops-facade-resource-switch
      operations_real_nfs upgrade-facade-continuity
      operations_real_nfs ops-activation-preserved
      operations_real_nfs ops-activation-visibility
      operations_real_nfs ops-activation-force-find
      ;;
    resource-budget)
      operations_real_nfs upgrade-cpu-budget
      ;;
    ops-force-find-smoke)
      run_l5_api_stage 3 "ops.force-find-smoke" \
        fs_meta_operations_scenarios_real_nfs
      ;;
    ops-force-find-semantics)
      run_l5_api_stage 4 "ops.force-find-semantics" \
        fs_meta_operations_force_find_execution_semantics_real_nfs
      ;;
    ops-new-nfs-join)
      run_l5_api_stage 14 "ops.new-nfs-join" \
        fs_meta_operations_new_nfs_join_real_nfs
      ;;
    ops-root-path-modify)
      run_l5_api_stage 15 "ops.root-path-modify" \
        fs_meta_operations_root_path_modify_real_nfs
      ;;
    ops-visibility-sink-selection)
      run_l5_api_stage 5 "ops.visibility-sink-selection" \
        fs_meta_operations_visibility_change_and_sink_selection_real_nfs
      ;;
    ops-visibility-roots-narrowed)
      run_l5_api_substage 1 "ops.visibility.5.1.roots-narrowed" \
        fs_meta_operations_visibility_roots_narrowed_real_nfs
      ;;
    ops-visibility-source-delivery-ready)
      run_l5_api_substage 2 "ops.visibility.5.2.source-delivery-ready" \
        fs_meta_operations_visibility_source_delivery_ready_real_nfs
      ;;
    ops-visibility-manual-rescan-accepted)
      run_l5_api_substage 3 "ops.visibility.5.3.manual-rescan-accepted" \
        fs_meta_operations_visibility_manual_rescan_accepted_real_nfs
      ;;
    ops-visibility-grants-visible)
      run_l5_api_substage 4 "ops.visibility.5.4.grants-visible" \
        fs_meta_operations_visibility_grants_visible_real_nfs
      ;;
    ops-visibility-withdraw-converged)
      run_l5_api_substage 5 "ops.visibility.5.5.withdraw-converged" \
        fs_meta_operations_visibility_withdraw_converged_real_nfs
      ;;
    ops-visibility-sink-holder-moved)
      run_l5_api_substage 6 "ops.visibility.5.6.sink-holder-moved" \
        fs_meta_operations_visibility_sink_holder_moved_real_nfs
      ;;
    ops-visibility-facade-live)
      run_l5_api_substage 7 "ops.visibility.5.7.facade-live" \
        fs_meta_operations_visibility_facade_live_real_nfs
      ;;
    ops-sink-failover)
      run_l5_api_stage 18 "ops.sink-failover" \
        fs_meta_operations_sink_failover_real_nfs
      ;;
    ops-facade-resource-switch)
      run_l5_api_stage 19 "ops.facade-resource-switch" \
        fs_meta_operations_facade_failover_and_resource_switch_real_nfs
      ;;
    ops-nfs-retire)
      run_l5_api_stage 16 "ops.nfs-retire" \
        fs_meta_operations_nfs_retire_real_nfs
      ;;
    ops-activation-preserved)
      run_l5_api_stage 21 "ops.activation-preserved" \
        fs_meta_operations_activation_scope_preserved_layout_real_nfs
      ;;
    ops-activation-visibility)
      run_l5_api_stage 22 "ops.activation-visibility" \
        fs_meta_operations_activation_scope_visibility_contracted_real_nfs
      ;;
    ops-activation-force-find)
      run_l5_api_stage 23 "ops.activation-force-find" \
        fs_meta_operations_activation_scope_force_find_preserved_real_nfs
      ;;
    upgrade-apply-generation-two)
      run_l5_api_stage 6 "upgrade.apply-generation-two" \
        fs_meta_operations_release_upgrade_apply_real_nfs
      ;;
    upgrade-peer-source-control)
      run_l5_api_stage 8 "upgrade.peer-source-control" \
        fs_meta_operations_release_upgrade_peer_source_control_completion_real_nfs
      ;;
    upgrade-generation-two-http)
      run_l5_api_stage 7 "upgrade.generation-two-http" \
        fs_meta_operations_release_upgrade_real_nfs
      ;;
    upgrade-facade-continuity)
      run_l5_api_stage 20 "upgrade.facade-continuity" \
        fs_meta_operations_release_upgrade_facade_claim_continuity_real_nfs
      ;;
    upgrade-sink-scope)
      run_l5_api_stage 9 "upgrade.sink-scope" \
        fs_meta_operations_release_upgrade_sink_control_roles_real_nfs
      ;;
    upgrade-runtime-scope)
      run_l5_api_stage 10 "upgrade.runtime-scope" \
        fs_meta_operations_release_upgrade_runtime_scope_real_nfs
      ;;
    upgrade-roots-persist)
      run_l5_api_stage 11 "upgrade.roots-persist" \
        fs_meta_operations_release_upgrade_roots_persist_real_nfs
      ;;
    upgrade-tree-stats)
      run_l5_api_stage 12 "upgrade.tree-stats" \
        fs_meta_operations_release_upgrade_tree_stats_stable_real_nfs
      ;;
    upgrade-tree-materialization)
      run_l5_api_stage 13 "upgrade.tree-materialization" \
        fs_meta_operations_release_upgrade_tree_materialization_real_nfs
      ;;
    upgrade-window-join)
      run_l5_api_stage 17 "upgrade.window-join" \
        fs_meta_operations_release_upgrade_window_join_real_nfs
      ;;
    upgrade-cpu-budget)
      run_l5_api_stage 24 "upgrade.cpu-budget" \
        fs_meta_operations_release_upgrade_cpu_budget_real_nfs
      ;;
    runtime-selected-group-proxy)
      run_l5_runtime_stage 1 "runtime.selected-group-proxy" \
        external_runtime_app_selected_group_proxy
      ;;
    runtime-source-worker-manual-rescan)
      run_l5_runtime_stage 2 "runtime.source-worker-manual-rescan" \
        external_source_worker_real_nfs_manual_rescan
      ;;
    *)
      echo "unknown operations-real-nfs stage: $stage" >&2
      usage >&2
      exit 2
      ;;
  esac
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
       fs-meta/docs/examples/test-matrix-commands.sh operations-real-nfs [stage]

Suites:
  business-fast
  business-mini-nfs
  environment-full-nfs
  operations-local
  operations-real-nfs
  progressive-business
  progressive-environment
  progressive-operations

operations-real-nfs stages:
  all

ordered L5 groups:
  foundation-real-runtime
  upgrade-core
  topology-change
  recovery-switch
  resource-budget

atomic L5 stages:
  runtime-selected-group-proxy
  runtime-source-worker-manual-rescan
  ops-force-find-smoke
  ops-force-find-semantics
  ops-visibility-sink-selection
  ops-visibility-roots-narrowed
  ops-visibility-source-delivery-ready
  ops-visibility-manual-rescan-accepted
  ops-visibility-grants-visible
  ops-visibility-withdraw-converged
  ops-visibility-sink-holder-moved
  ops-visibility-facade-live
  upgrade-apply-generation-two
  upgrade-generation-two-http
  upgrade-peer-source-control
  upgrade-sink-scope
  upgrade-runtime-scope
  upgrade-roots-persist
  upgrade-tree-stats
  upgrade-tree-materialization
  ops-new-nfs-join
  ops-root-path-modify
  ops-nfs-retire
  upgrade-window-join
  ops-sink-failover
  ops-facade-resource-switch
  upgrade-facade-continuity
  ops-activation-preserved
  ops-activation-visibility
  ops-activation-force-find
  upgrade-cpu-budget
EOF2
}

case "${1:-}" in
  business-fast) business_fast ;;
  business-mini-nfs) business_mini_nfs ;;
  environment-full-nfs) environment_full_nfs ;;
  operations-local) operations_local ;;
  operations-real-nfs) operations_real_nfs "${2:-all}" ;;
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

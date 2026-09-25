#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GUARD="${SCRIPT_DIR}/resource-guard.sh"

available_cpus="$(nproc)"
expected_cpus=2
if ((available_cpus < expected_cpus)); then
    expected_cpus="${available_cpus}"
fi

result="$(
    FUSTOR_CPU_LIMIT=2 \
    FUSTOR_CARGO_BUILD_JOBS=2 \
    FUSTOR_TEST_THREADS=1 \
    FUSTOR_TOKIO_WORKER_THREADS=1 \
    FUSTOR_RAYON_NUM_THREADS=1 \
    FUSTOR_SOURCE_SCAN_WORKERS=2 \
    FUSTOR_SOURCE_HOST_FS_OP_WORKERS=2 \
    FUSTOR_RUST_MIN_STACK=3145728 \
    FUSTOR_RESOURCE_GUARD_QUIET=1 \
        "${GUARD}" bash -c \
        'printf "%s %s %s %s %s %s %s %s %s %s %s\n" "$CARGO_BUILD_JOBS" "$RUST_TEST_THREADS" "$TOKIO_WORKER_THREADS" "$RAYON_NUM_THREADS" "$FS_META_SOURCE_SCAN_WORKERS" "$FS_META_SOURCE_HOST_FS_OP_WORKERS" "$CAPANIX_WORKER_RUNTIME_THREADS" "$CAPANIX_WORKER_MAX_BLOCKING_THREADS" "$CAPANIX_BOUND_ROUTE_DISPATCHER_THREADS" "$RUST_MIN_STACK" "$(nproc)"'
)"

read -r build_jobs test_threads tokio_threads rayon_threads source_scan_workers source_host_fs_op_workers capanix_worker_runtime_threads capanix_worker_max_blocking_threads capanix_bound_route_dispatcher_threads rust_min_stack visible_cpus <<<"${result}"
[[ "${build_jobs}" == "2" ]]
[[ "${test_threads}" == "1" ]]
[[ "${tokio_threads}" == "1" ]]
[[ "${rayon_threads}" == "1" ]]
[[ "${source_scan_workers}" == "2" ]]
[[ "${source_host_fs_op_workers}" == "2" ]]
[[ "${capanix_worker_runtime_threads}" == "2" ]]
[[ "${capanix_worker_max_blocking_threads}" == "4" ]]
[[ "${capanix_bound_route_dispatcher_threads}" == "4" ]]
[[ "${rust_min_stack}" == "3145728" ]]
[[ "${visible_cpus}" == "${expected_cpus}" ]]

set +e
FUSTOR_CPU_LIMIT=3 FUSTOR_RESOURCE_GUARD_QUIET=1 \
    "${GUARD}" true >/dev/null 2>&1
unsafe_cpu_status=$?
FUSTOR_TEST_THREADS=2 FUSTOR_RESOURCE_GUARD_QUIET=1 \
    "${GUARD}" true >/dev/null 2>&1
unsafe_test_threads_status=$?
CAPANIX_WORKER_RUNTIME_THREADS=1 FUSTOR_RESOURCE_GUARD_QUIET=1 \
    "${GUARD}" true >/dev/null 2>&1
unsafe_capanix_runtime_threads_status=$?
CAPANIX_WORKER_MAX_BLOCKING_THREADS=5 FUSTOR_RESOURCE_GUARD_QUIET=1 \
    "${GUARD}" true >/dev/null 2>&1
unsafe_capanix_blocking_threads_status=$?
CAPANIX_BOUND_ROUTE_DISPATCHER_THREADS=5 FUSTOR_RESOURCE_GUARD_QUIET=1 \
    "${GUARD}" true >/dev/null 2>&1
unsafe_capanix_bound_route_dispatcher_threads_status=$?
set -e
[[ "${unsafe_cpu_status}" == "2" ]]
[[ "${unsafe_test_threads_status}" == "2" ]]
[[ "${unsafe_capanix_runtime_threads_status}" == "2" ]]
[[ "${unsafe_capanix_blocking_threads_status}" == "2" ]]
[[ "${unsafe_capanix_bound_route_dispatcher_threads_status}" == "2" ]]

echo "FUSTOR_RESOURCE_GUARD_TEST_PASS visible_cpus=${visible_cpus}"

#!/usr/bin/env bash
set -euo pipefail

fail() {
    echo "FUSTOR_RESOURCE_GUARD_FAIL $*" >&2
    exit 2
}

is_positive_integer() {
    [[ "$1" =~ ^[1-9][0-9]*$ ]]
}

[[ $# -gt 0 ]] || fail "usage: scripts/resource-guard.sh COMMAND [ARG ...]"
taskset_bin="$(command -v taskset || true)"
nice_bin="$(command -v nice || true)"
[[ -n "${taskset_bin}" ]] || fail "taskset is required"
[[ -n "${nice_bin}" ]] || fail "nice is required"

cpu_limit="${FUSTOR_CPU_LIMIT:-2}"
is_positive_integer "${cpu_limit}" || fail "FUSTOR_CPU_LIMIT must be a positive integer"
((cpu_limit <= 2)) || fail "FUSTOR_CPU_LIMIT must be at most 2"

allowed_cpu_list="$(awk '$1 == "Cpus_allowed_list:" { print $2; exit }' /proc/self/status)"
[[ -n "${allowed_cpu_list}" ]] || fail "unable to read Cpus_allowed_list"

selected_cpus=()
IFS=',' read -r -a allowed_ranges <<<"${allowed_cpu_list}"
for allowed_range in "${allowed_ranges[@]}"; do
    if [[ "${allowed_range}" =~ ^([0-9]+)-([0-9]+)$ ]]; then
        range_start="${BASH_REMATCH[1]}"
        range_end="${BASH_REMATCH[2]}"
    elif [[ "${allowed_range}" =~ ^[0-9]+$ ]]; then
        range_start="${allowed_range}"
        range_end="${allowed_range}"
    else
        fail "unsupported Cpus_allowed_list entry: ${allowed_range}"
    fi

    for ((cpu = range_start; cpu <= range_end; cpu++)); do
        selected_cpus+=("${cpu}")
        if ((${#selected_cpus[@]} >= cpu_limit)); then
            break 2
        fi
    done
done

((${#selected_cpus[@]} > 0)) || fail "no allowed CPU was selected"
cpu_set="$(IFS=,; echo "${selected_cpus[*]}")"
selected_cpu_count="${#selected_cpus[@]}"

build_jobs="${FUSTOR_CARGO_BUILD_JOBS:-${selected_cpu_count}}"
test_threads="${FUSTOR_TEST_THREADS:-1}"
default_runtime_threads=2
if ((selected_cpu_count < default_runtime_threads)); then
    default_runtime_threads="${selected_cpu_count}"
fi
tokio_threads="${FUSTOR_TOKIO_WORKER_THREADS:-${default_runtime_threads}}"
rayon_threads="${FUSTOR_RAYON_NUM_THREADS:-${default_runtime_threads}}"
rust_min_stack="${FUSTOR_RUST_MIN_STACK:-4194304}"
nice_level="${FUSTOR_NICE_LEVEL:-10}"
source_scan_workers="${FUSTOR_SOURCE_SCAN_WORKERS:-${FS_META_SOURCE_SCAN_WORKERS:-2}}"
source_host_fs_op_workers="${FUSTOR_SOURCE_HOST_FS_OP_WORKERS:-${FS_META_SOURCE_HOST_FS_OP_WORKERS:-2}}"
capanix_worker_runtime_threads="${CAPANIX_WORKER_RUNTIME_THREADS:-2}"
capanix_worker_max_blocking_threads="${CAPANIX_WORKER_MAX_BLOCKING_THREADS:-4}"
capanix_bound_route_dispatcher_threads="${CAPANIX_BOUND_ROUTE_DISPATCHER_THREADS:-4}"

is_positive_integer "${build_jobs}" || fail "FUSTOR_CARGO_BUILD_JOBS must be a positive integer"
is_positive_integer "${test_threads}" || fail "FUSTOR_TEST_THREADS must be a positive integer"
is_positive_integer "${tokio_threads}" || fail "FUSTOR_TOKIO_WORKER_THREADS must be a positive integer"
is_positive_integer "${rayon_threads}" || fail "FUSTOR_RAYON_NUM_THREADS must be a positive integer"
is_positive_integer "${rust_min_stack}" || fail "FUSTOR_RUST_MIN_STACK must be a positive integer"
is_positive_integer "${source_scan_workers}" || fail "FUSTOR_SOURCE_SCAN_WORKERS must be a positive integer"
is_positive_integer "${source_host_fs_op_workers}" || fail "FUSTOR_SOURCE_HOST_FS_OP_WORKERS must be a positive integer"
is_positive_integer "${capanix_worker_runtime_threads}" || fail "CAPANIX_WORKER_RUNTIME_THREADS must be a positive integer"
is_positive_integer "${capanix_worker_max_blocking_threads}" || fail "CAPANIX_WORKER_MAX_BLOCKING_THREADS must be a positive integer"
is_positive_integer "${capanix_bound_route_dispatcher_threads}" || fail "CAPANIX_BOUND_ROUTE_DISPATCHER_THREADS must be a positive integer"
((build_jobs <= selected_cpu_count)) || fail "FUSTOR_CARGO_BUILD_JOBS must not exceed selected CPUs (${selected_cpu_count})"
((test_threads <= 1)) || fail "FUSTOR_TEST_THREADS must be at most 1"
((tokio_threads <= 2)) || fail "FUSTOR_TOKIO_WORKER_THREADS must be at most 2"
((rayon_threads <= 2)) || fail "FUSTOR_RAYON_NUM_THREADS must be at most 2"
((source_scan_workers <= 2)) || fail "FUSTOR_SOURCE_SCAN_WORKERS must be at most 2"
((source_host_fs_op_workers <= 2)) || fail "FUSTOR_SOURCE_HOST_FS_OP_WORKERS must be at most 2"
((capanix_worker_runtime_threads >= 2)) || fail "CAPANIX_WORKER_RUNTIME_THREADS must be at least 2 for Fustor external workers"
((capanix_worker_runtime_threads <= 2)) || fail "CAPANIX_WORKER_RUNTIME_THREADS must be at most 2"
((capanix_worker_max_blocking_threads >= 4)) || fail "CAPANIX_WORKER_MAX_BLOCKING_THREADS must be at least 4 for Fustor worker I/O lanes"
((capanix_worker_max_blocking_threads <= 4)) || fail "CAPANIX_WORKER_MAX_BLOCKING_THREADS must be at most 4"
((capanix_bound_route_dispatcher_threads >= 4)) || fail "CAPANIX_BOUND_ROUTE_DISPATCHER_THREADS must be at least 4 for concurrent Fustor route replies"
((capanix_bound_route_dispatcher_threads <= 4)) || fail "CAPANIX_BOUND_ROUTE_DISPATCHER_THREADS must be at most 4"
[[ "${nice_level}" =~ ^([0-9]|1[0-9])$ ]] || fail "FUSTOR_NICE_LEVEL must be between 0 and 19"

if [[ "${FUSTOR_RESOURCE_GUARD_QUIET:-0}" != "1" ]]; then
    printf 'FUSTOR_RESOURCE_GUARD cpu_set=%s cargo_jobs=%s test_threads=%s tokio_threads=%s rayon_threads=%s source_scan_workers=%s source_host_fs_op_workers=%s capanix_worker_runtime_threads=%s capanix_worker_max_blocking_threads=%s capanix_bound_route_dispatcher_threads=%s rust_min_stack=%s nice=%s command=' \
        "${cpu_set}" "${build_jobs}" "${test_threads}" "${tokio_threads}" "${rayon_threads}" "${source_scan_workers}" "${source_host_fs_op_workers}" "${capanix_worker_runtime_threads}" "${capanix_worker_max_blocking_threads}" "${capanix_bound_route_dispatcher_threads}" "${rust_min_stack}" "${nice_level}" >&2
    printf ' %q' "$@" >&2
    printf '\n' >&2
fi

guarded_command=("${taskset_bin}" -c "${cpu_set}" "${nice_bin}" -n "${nice_level}")
ionice_bin="$(command -v ionice || true)"
if [[ -n "${ionice_bin}" ]]; then
    guarded_command+=("${ionice_bin}" -c 2 -n 7)
fi
guarded_command+=(
    /usr/bin/env
    "CARGO_BUILD_JOBS=${build_jobs}"
    "RUST_TEST_THREADS=${test_threads}"
    "TOKIO_WORKER_THREADS=${tokio_threads}"
    "RAYON_NUM_THREADS=${rayon_threads}"
    "FS_META_SOURCE_SCAN_WORKERS=${source_scan_workers}"
    "FS_META_SOURCE_HOST_FS_OP_WORKERS=${source_host_fs_op_workers}"
    "CAPANIX_WORKER_RUNTIME_THREADS=${capanix_worker_runtime_threads}"
    "CAPANIX_WORKER_MAX_BLOCKING_THREADS=${capanix_worker_max_blocking_threads}"
    "CAPANIX_BOUND_ROUTE_DISPATCHER_THREADS=${capanix_bound_route_dispatcher_threads}"
    "RUST_MIN_STACK=${rust_min_stack}"
    "$@"
)

exec "${guarded_command[@]}"

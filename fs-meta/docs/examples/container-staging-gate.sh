#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../../.." && pwd)"
resource_guard="${FUSTOR_RESOURCE_GUARD:-${repo_root}/scripts/resource-guard.sh}"
process_tree_watchdog="${FUSTOR_PROCESS_TREE_WATCHDOG:-${repo_root}/scripts/process-tree-watchdog.sh}"
nfs_cleanup="${FSMETA_CONTAINER_STAGING_NFS_CLEANUP:-${repo_root}/fs-meta/docs/examples/container-staging-nfs-cleanup.sh}"
nfs_cleanup_test="${FSMETA_CONTAINER_STAGING_NFS_CLEANUP_TEST:-${repo_root}/scripts/test-container-staging-nfs-cleanup.sh}"
default_capanix_root="$(cd "${repo_root}/../capanix" 2>/dev/null && pwd || true)"

image="${FSMETA_CONTAINER_STAGING_IMAGE:-fsmeta-container-staging-gate:latest}"
engine="${FSMETA_CONTAINER_ENGINE:-docker}"
dockerfile="${script_dir}/container-staging-gate.Dockerfile"
capanix_root="${CAPANIX_REPO_ROOT:-${default_capanix_root}}"
staging_tmp_root="${DATANIX_E2E_TMP_ROOT:-/tmp/fsmeta-container-staging}"
short_cluster_artifact_root="${FSMETA_CONTAINER_SHORT_CLUSTER_ARTIFACT_ROOT:-/tmp/fsm-e2e}"
short_cluster_container_root="/tf"
quic_socket_buffer_bytes="${FSMETA_CONTAINER_QUIC_SOCKET_BUFFER_BYTES:-16777216}"
inotify_max_user_instances="${FSMETA_CONTAINER_INOTIFY_MAX_USER_INSTANCES:-256}"
host_rmem_max_before=""
host_wmem_max_before=""
host_inotify_max_user_instances_before=""
host_sysctl_restore_armed=0
active_container_name=""
staging_container_label="fsmeta.container-staging-gate=1"

usage() {
  cat <<'EOF2'
Usage:
  fs-meta/docs/examples/container-staging-gate.sh build-image
  fs-meta/docs/examples/container-staging-gate.sh cleanup-stale
  fs-meta/docs/examples/container-staging-gate.sh self-test-resource-limits
  fs-meta/docs/examples/container-staging-gate.sh run [active-three|mini|all|l5:<stage>]
  fs-meta/docs/examples/container-staging-gate.sh shell
  fs-meta/docs/examples/container-staging-gate.sh inside [mini|active-three|all|l5:<stage>]

Environment:
  FSMETA_CONTAINER_ENGINE              docker or podman, default docker
  FSMETA_CONTAINER_STAGING_IMAGE       image tag, default fsmeta-container-staging-gate:latest
  CAPANIX_REPO_ROOT                    sibling capanix checkout, default ../capanix
  DATANIX_E2E_TMP_ROOT                 absolute host temp root; default is a per-run /tmp/fsmeta-container-staging/* path
  FSMETA_CONTAINER_SHORT_CLUSTER_ARTIFACT_ROOT
                                      absolute host path for short Unix-socket-safe cluster logs; default /tmp/fsm-e2e
  FSMETA_ACTIVE_THREE_FILES_PER_ROOT   active-three seed size, default 9
  FSMETA_ACTIVE_THREE_DIRS_PER_ROOT    active-three directory fanout, default 1
  FSMETA_CONTAINER_STAGING_REQUIRE_MINI
                                      set to 0 to make mini diagnostic-only during run all; default 1
  FSMETA_CONTAINER_CPUS                hard CPU quota for the disposable container; default/max 2
  FSMETA_CONTAINER_BUILD_TIMEOUT_SECONDS
                                      timeout for each inner Cargo build; default 1800
  FSMETA_CONTAINER_STAGE_TIMEOUT_SECONDS
                                      timeout for each inner acceptance stage; default 1800
  FSMETA_CONTAINER_RUN_TIMEOUT_SECONDS timeout for one disposable container; default 2700
  FSMETA_CONTAINER_QUIC_SOCKET_BUFFER_BYTES
                                      required temporary host rmem/wmem maximum; default 16777216
  FSMETA_CONTAINER_INOTIFY_MAX_USER_INSTANCES
                                      required temporary host inotify instance ceiling; default 256
  FSMETA_CONTAINER_STAGING_TUNE_HOST_SYSCTLS
                                      set to 0 to fail instead of temporarily raising low host limits; default 1
  FUSTOR_CPU_LIMIT                     CPU affinity limit for Cargo/test commands; default 2

This gate is a disposable local container staging gate. It must not SSH to or
deploy against the official 198.51.100.144-146 fsmeta-stable cluster.
EOF2
}

is_positive_integer() {
  [[ "$1" =~ ^[1-9][0-9]*$ ]]
}

require_bounded_positive_integer() {
  local name="$1"
  local value="$2"
  local maximum="$3"
  if ! is_positive_integer "${value}" || ((value > maximum)); then
    echo "${name} must be a positive integer at most ${maximum}: ${value}" >&2
    return 2
  fi
}

require_bounded_positive_number() {
  local name="$1"
  local value="$2"
  local maximum="$3"
  if ! awk -v value="${value}" -v maximum="${maximum}" \
    'BEGIN { exit !(value ~ /^[0-9]+([.][0-9]+)?$/ && value > 0 && value <= maximum) }'; then
    echo "${name} must be a positive number at most ${maximum}: ${value}" >&2
    return 2
  fi
}

build_timeout_seconds() {
  echo "${FSMETA_CONTAINER_BUILD_TIMEOUT_SECONDS:-1800}"
}

stage_timeout_seconds() {
  echo "${FSMETA_CONTAINER_STAGE_TIMEOUT_SECONDS:-1800}"
}

container_run_timeout_seconds() {
  echo "${FSMETA_CONTAINER_RUN_TIMEOUT_SECONDS:-2700}"
}

require_resource_limit_configuration() {
  local name value
  for name in \
    FSMETA_CONTAINER_BUILD_TIMEOUT_SECONDS \
    FSMETA_CONTAINER_STAGE_TIMEOUT_SECONDS \
    FSMETA_CONTAINER_RUN_TIMEOUT_SECONDS
  do
    case "${name}" in
      FSMETA_CONTAINER_BUILD_TIMEOUT_SECONDS) value="$(build_timeout_seconds)" ;;
      FSMETA_CONTAINER_STAGE_TIMEOUT_SECONDS) value="$(stage_timeout_seconds)" ;;
      FSMETA_CONTAINER_RUN_TIMEOUT_SECONDS) value="$(container_run_timeout_seconds)" ;;
    esac
    if ! is_positive_integer "${value}"; then
      echo "${name} must be a positive integer: ${value}" >&2
      return 2
    fi
  done
  require_bounded_positive_number \
    FSMETA_CONTAINER_CPUS "${FSMETA_CONTAINER_CPUS:-2}" 2 || return $?
  require_bounded_positive_integer \
    FUSTOR_CPU_LIMIT "${FUSTOR_CPU_LIMIT:-2}" 2 || return $?
  require_bounded_positive_integer \
    FUSTOR_CARGO_BUILD_JOBS "${FUSTOR_CARGO_BUILD_JOBS:-2}" 2 || return $?
  require_bounded_positive_integer \
    FUSTOR_TEST_THREADS "${FUSTOR_TEST_THREADS:-1}" 1 || return $?
  require_bounded_positive_integer \
    FUSTOR_TOKIO_WORKER_THREADS "${FUSTOR_TOKIO_WORKER_THREADS:-1}" 2 || return $?
  require_bounded_positive_integer \
    FUSTOR_RAYON_NUM_THREADS "${FUSTOR_RAYON_NUM_THREADS:-1}" 2 || return $?
  require_bounded_positive_integer \
    FUSTOR_SOURCE_SCAN_WORKERS "${FUSTOR_SOURCE_SCAN_WORKERS:-1}" 2 || return $?
  require_bounded_positive_integer \
    FUSTOR_SOURCE_HOST_FS_OP_WORKERS "${FUSTOR_SOURCE_HOST_FS_OP_WORKERS:-2}" 2 || return $?
  require_bounded_positive_integer \
    CAPANIX_WORKER_RUNTIME_THREADS "${CAPANIX_WORKER_RUNTIME_THREADS:-2}" 2 || return $?
  if (( ${CAPANIX_WORKER_RUNTIME_THREADS:-2} < 2 )); then
    echo "CAPANIX_WORKER_RUNTIME_THREADS must be at least 2 for Fustor external workers" >&2
    return 2
  fi
  require_bounded_positive_integer \
    CAPANIX_WORKER_MAX_BLOCKING_THREADS "${CAPANIX_WORKER_MAX_BLOCKING_THREADS:-4}" 4 || return $?
  if (( ${CAPANIX_WORKER_MAX_BLOCKING_THREADS:-4} < 4 )); then
    echo "CAPANIX_WORKER_MAX_BLOCKING_THREADS must be at least 4 for Fustor worker I/O lanes" >&2
    return 2
  fi
  require_bounded_positive_integer \
    CAPANIX_BOUND_ROUTE_DISPATCHER_THREADS "${CAPANIX_BOUND_ROUTE_DISPATCHER_THREADS:-4}" 4 || return $?
  if (( ${CAPANIX_BOUND_ROUTE_DISPATCHER_THREADS:-4} < 4 )); then
    echo "CAPANIX_BOUND_ROUTE_DISPATCHER_THREADS must be at least 4 for concurrent Fustor route replies" >&2
    return 2
  fi
  require_bounded_positive_integer \
    FUSTOR_PROCESS_TREE_MAX_THREADS "${FUSTOR_PROCESS_TREE_MAX_THREADS:-512}" 512 || return $?
  test -x "${resource_guard}" || {
    echo "resource guard is not executable: ${resource_guard}" >&2
    return 2
  }
  test -x "${process_tree_watchdog}" || {
    echo "process-tree watchdog is not executable: ${process_tree_watchdog}" >&2
    return 2
  }
  test -x "${nfs_cleanup}" || {
    echo "container staging NFS cleanup is not executable: ${nfs_cleanup}" >&2
    return 2
  }
  test -x "${nfs_cleanup_test}" || {
    echo "container staging NFS cleanup test is not executable: ${nfs_cleanup_test}" >&2
    return 2
  }
  command -v timeout >/dev/null 2>&1 || {
    echo "timeout is required for bounded container staging" >&2
    return 2
  }
}

run_bounded() {
  local label="$1"
  local timeout_seconds="$2"
  shift 2
  echo "CONTAINER_STAGING_BOUNDED_BEGIN label=${label} timeout_seconds=${timeout_seconds}"
  local status=0
  timeout --foreground --signal=TERM --kill-after=30s "${timeout_seconds}s" "$@" || status=$?
  if [[ "${status}" -eq 124 || "${status}" -eq 137 ]]; then
    echo "CONTAINER_STAGING_BOUNDED_TIMEOUT label=${label} timeout_seconds=${timeout_seconds} status=${status}" >&2
    return "${status}"
  fi
  if [[ "${status}" -ne 0 ]]; then
    echo "CONTAINER_STAGING_BOUNDED_FAIL label=${label} status=${status}" >&2
    return "${status}"
  fi
  echo "CONTAINER_STAGING_BOUNDED_PASS label=${label}"
}

run_guarded() {
  local label="$1"
  local timeout_seconds="$2"
  shift 2
  run_bounded \
    "${label}" \
    "${timeout_seconds}" \
    "${process_tree_watchdog}" \
    "${resource_guard}" \
    "$@"
}

self_test_resource_limits() {
  require_resource_limit_configuration

  local affinity_output
  affinity_output="$(
    FUSTOR_CPU_LIMIT=2 \
      run_guarded resource-affinity-self-test 10 bash -c 'nproc'
  )"
  local visible_cpus
  visible_cpus="$(tail -n 2 <<<"${affinity_output}" | head -n 1)"
  if [[ ! "${visible_cpus}" =~ ^[0-9]+$ || "${visible_cpus}" -gt 2 ]]; then
    echo "CONTAINER_STAGING_RESOURCE_SELF_TEST_FAIL visible_cpus=${visible_cpus:-unknown}" >&2
    return 1
  fi

  local timeout_log timeout_status=0
  timeout_log="$(mktemp /tmp/fsmeta-container-staging-timeout-self-test.XXXXXX)"
  if run_bounded resource-timeout-self-test 1 bash -c 'sleep 30' >"${timeout_log}" 2>&1; then
    timeout_status=0
  else
    timeout_status=$?
  fi
  if [[ "${timeout_status}" -ne 124 ]]; then
    cat "${timeout_log}" >&2
    rm -f "${timeout_log}"
    echo "CONTAINER_STAGING_RESOURCE_SELF_TEST_FAIL timeout_status=${timeout_status}" >&2
    return 1
  fi
  if ! grep -q 'CONTAINER_STAGING_BOUNDED_TIMEOUT label=resource-timeout-self-test' "${timeout_log}"; then
    cat "${timeout_log}" >&2
    rm -f "${timeout_log}"
    echo "CONTAINER_STAGING_RESOURCE_SELF_TEST_FAIL timeout_sentinel=missing" >&2
    return 1
  fi
  rm -f "${timeout_log}"

  local unsafe_status=0
  FSMETA_CONTAINER_CPUS=3 require_resource_limit_configuration >/dev/null 2>&1 || unsafe_status=$?
  if [[ "${unsafe_status}" -ne 2 ]]; then
    echo "CONTAINER_STAGING_RESOURCE_SELF_TEST_FAIL unsafe_container_cpu_status=${unsafe_status}" >&2
    return 1
  fi
  unsafe_status=0
  FUSTOR_PROCESS_TREE_MAX_THREADS=513 require_resource_limit_configuration >/dev/null 2>&1 || unsafe_status=$?
  if [[ "${unsafe_status}" -ne 2 ]]; then
    echo "CONTAINER_STAGING_RESOURCE_SELF_TEST_FAIL unsafe_thread_limit_status=${unsafe_status}" >&2
    return 1
  fi
  unsafe_status=0
  CAPANIX_BOUND_ROUTE_DISPATCHER_THREADS=5 require_resource_limit_configuration >/dev/null 2>&1 || unsafe_status=$?
  if [[ "${unsafe_status}" -ne 2 ]]; then
    echo "CONTAINER_STAGING_RESOURCE_SELF_TEST_FAIL unsafe_bound_route_dispatcher_threads_status=${unsafe_status}" >&2
    return 1
  fi
  unsafe_status=0
  CAPANIX_WORKER_MAX_BLOCKING_THREADS=5 require_resource_limit_configuration >/dev/null 2>&1 || unsafe_status=$?
  if [[ "${unsafe_status}" -ne 2 ]]; then
    echo "CONTAINER_STAGING_RESOURCE_SELF_TEST_FAIL unsafe_worker_max_blocking_threads_status=${unsafe_status}" >&2
    return 1
  fi
  unsafe_status=0
  "${nfs_cleanup}" self-test-outside-container >/dev/null 2>&1 || unsafe_status=$?
  if [[ "${unsafe_status}" -ne 2 ]]; then
    echo "CONTAINER_STAGING_RESOURCE_SELF_TEST_FAIL nfs_cleanup_outside_refusal_status=${unsafe_status}" >&2
    return 1
  fi
  "${nfs_cleanup_test}"

  echo "CONTAINER_STAGING_RESOURCE_SELF_TEST_PASS visible_cpus=${visible_cpus} hard_cpu_max=2 max_threads=512 worker_max_blocking_threads=4 bound_route_dispatcher_threads=4 timeout_status=${timeout_status}"
}

restore_host_kernel_limits() {
  if [[ "${host_sysctl_restore_armed}" != "1" ]]; then
    return 0
  fi
  local status=0
  sysctl -q -w "net.core.rmem_max=${host_rmem_max_before}" || status=1
  sysctl -q -w "net.core.wmem_max=${host_wmem_max_before}" || status=1
  sysctl -q -w "fs.inotify.max_user_instances=${host_inotify_max_user_instances_before}" || status=1
  if [[ "${status}" -eq 0 ]]; then
    echo "CONTAINER_STAGING_SYSCTL_RESTORED rmem_max=${host_rmem_max_before} wmem_max=${host_wmem_max_before} inotify_max_user_instances=${host_inotify_max_user_instances_before}"
    host_sysctl_restore_armed=0
  else
    echo "CONTAINER_STAGING_SYSCTL_RESTORE_FAIL rmem_max=${host_rmem_max_before} wmem_max=${host_wmem_max_before}" >&2
  fi
  return "${status}"
}

cleanup_active_container() {
  if [[ -z "${active_container_name}" ]]; then
    return 0
  fi
  local name="${active_container_name}"
  active_container_name=""
  if ! "${engine}" inspect "${name}" >/dev/null 2>&1; then
    return 0
  fi
  if timeout 30s "${engine}" rm -f "${name}" >/dev/null 2>&1; then
    echo "CONTAINER_STAGING_RUN_CONTAINER_REMOVED name=${name}"
    return 0
  fi
  echo "CONTAINER_STAGING_RUN_CONTAINER_CLEANUP_FAIL name=${name}" >&2
  return 1
}

cleanup_outer_run_state() {
  local original_status=$?
  trap - EXIT
  cleanup_active_container || true
  restore_host_kernel_limits || true
  exit "${original_status}"
}

prepare_host_kernel_limits() {
  is_positive_integer "${quic_socket_buffer_bytes}" || {
    echo "FSMETA_CONTAINER_QUIC_SOCKET_BUFFER_BYTES must be a positive integer" >&2
    return 2
  }
  is_positive_integer "${inotify_max_user_instances}" || {
    echo "FSMETA_CONTAINER_INOTIFY_MAX_USER_INSTANCES must be a positive integer" >&2
    return 2
  }
  host_rmem_max_before="$(cat /proc/sys/net/core/rmem_max)"
  host_wmem_max_before="$(cat /proc/sys/net/core/wmem_max)"
  host_inotify_max_user_instances_before="$(cat /proc/sys/fs/inotify/max_user_instances)"
  if ((host_rmem_max_before >= quic_socket_buffer_bytes && host_wmem_max_before >= quic_socket_buffer_bytes && host_inotify_max_user_instances_before >= inotify_max_user_instances)); then
    echo "CONTAINER_STAGING_SYSCTL_READY rmem_max=${host_rmem_max_before} wmem_max=${host_wmem_max_before} quic_required=${quic_socket_buffer_bytes} inotify_max_user_instances=${host_inotify_max_user_instances_before} inotify_required=${inotify_max_user_instances} changed=0"
    return 0
  fi
  if [[ "${FSMETA_CONTAINER_STAGING_TUNE_HOST_SYSCTLS:-1}" != "1" ]]; then
    echo "CONTAINER_STAGING_SYSCTL_INSUFFICIENT rmem_max=${host_rmem_max_before} wmem_max=${host_wmem_max_before} quic_required=${quic_socket_buffer_bytes} inotify_max_user_instances=${host_inotify_max_user_instances_before} inotify_required=${inotify_max_user_instances} tuning=disabled" >&2
    return 1
  fi
  command -v sysctl >/dev/null 2>&1 || {
    echo "sysctl is required to prepare disposable QUIC socket limits" >&2
    return 2
  }
  host_sysctl_restore_armed=1
  trap restore_host_kernel_limits EXIT
  if ((host_rmem_max_before < quic_socket_buffer_bytes)); then
    sysctl -q -w "net.core.rmem_max=${quic_socket_buffer_bytes}"
  fi
  if ((host_wmem_max_before < quic_socket_buffer_bytes)); then
    sysctl -q -w "net.core.wmem_max=${quic_socket_buffer_bytes}"
  fi
  if ((host_inotify_max_user_instances_before < inotify_max_user_instances)); then
    sysctl -q -w "fs.inotify.max_user_instances=${inotify_max_user_instances}"
  fi
  local current_rmem current_wmem current_inotify
  current_rmem="$(cat /proc/sys/net/core/rmem_max)"
  current_wmem="$(cat /proc/sys/net/core/wmem_max)"
  current_inotify="$(cat /proc/sys/fs/inotify/max_user_instances)"
  if ((current_rmem < quic_socket_buffer_bytes || current_wmem < quic_socket_buffer_bytes || current_inotify < inotify_max_user_instances)); then
    echo "CONTAINER_STAGING_SYSCTL_INSUFFICIENT rmem_max=${current_rmem} wmem_max=${current_wmem} quic_required=${quic_socket_buffer_bytes} inotify_max_user_instances=${current_inotify} inotify_required=${inotify_max_user_instances} tuning=failed" >&2
    return 1
  fi
  echo "CONTAINER_STAGING_SYSCTL_READY rmem_max=${current_rmem} wmem_max=${current_wmem} quic_required=${quic_socket_buffer_bytes} inotify_max_user_instances=${current_inotify} inotify_required=${inotify_max_user_instances} changed=1"
}

require_capanix_root() {
  if [[ -z "${capanix_root}" || ! -d "${capanix_root}" ]]; then
    echo "unable to find capanix checkout; set CAPANIX_REPO_ROOT" >&2
    exit 2
  fi
}

require_staging_tmp_root() {
  if [[ "${staging_tmp_root}" != /* ]]; then
    echo "DATANIX_E2E_TMP_ROOT must be an absolute host path: ${staging_tmp_root}" >&2
    exit 2
  fi
  mkdir -p "${staging_tmp_root}"
  if [[ "${short_cluster_artifact_root}" != /* ]]; then
    echo "FSMETA_CONTAINER_SHORT_CLUSTER_ARTIFACT_ROOT must be an absolute host path: ${short_cluster_artifact_root}" >&2
    exit 2
  fi
  mkdir -p "${short_cluster_artifact_root}"
}

build_image() {
  "${engine}" build \
    -f "${dockerfile}" \
    -t "${image}" \
    "${script_dir}"
}

container_args() {
  local mode="${1:-batch}"
  local container_name="${2:-}"
  require_capanix_root
  require_staging_tmp_root
  printf '%s\0' \
    run --rm
  if [[ "${mode}" == "interactive" ]]; then
    printf '%s\0' -it
  fi
  if [[ -n "${container_name}" ]]; then
    printf '%s\0' --name "${container_name}"
  fi
  printf '%s\0' \
    --privileged \
    --cpus "${FSMETA_CONTAINER_CPUS:-2}" \
    --security-opt seccomp=unconfined \
    --label "${staging_container_label}" \
    --tmpfs /run \
    -e "DATANIX_E2E_TMP_ROOT=${staging_tmp_root}" \
    -e "TMPDIR=${short_cluster_container_root}" \
    -e "FSMETA_NFS_LAB_V4_PSEUDOROOT=1" \
    -e "FSMETA_CONTAINER_STAGING_INSIDE=1" \
    -e "FSMETA_ACTIVE_THREE_FILES_PER_ROOT=${FSMETA_ACTIVE_THREE_FILES_PER_ROOT:-9}" \
    -e "FSMETA_ACTIVE_THREE_DIRS_PER_ROOT=${FSMETA_ACTIVE_THREE_DIRS_PER_ROOT:-1}" \
    -e "FSMETA_CONTAINER_STAGING_REQUIRE_MINI=${FSMETA_CONTAINER_STAGING_REQUIRE_MINI:-1}" \
    -e "FUSTOR_CPU_LIMIT=${FUSTOR_CPU_LIMIT:-2}" \
    -e "FUSTOR_CARGO_BUILD_JOBS=${FUSTOR_CARGO_BUILD_JOBS:-2}" \
    -e "FUSTOR_TEST_THREADS=${FUSTOR_TEST_THREADS:-1}" \
    -e "FUSTOR_TOKIO_WORKER_THREADS=${FUSTOR_TOKIO_WORKER_THREADS:-1}" \
    -e "FUSTOR_RAYON_NUM_THREADS=${FUSTOR_RAYON_NUM_THREADS:-1}" \
    -e "FUSTOR_SOURCE_SCAN_WORKERS=${FUSTOR_SOURCE_SCAN_WORKERS:-1}" \
    -e "FUSTOR_SOURCE_HOST_FS_OP_WORKERS=${FUSTOR_SOURCE_HOST_FS_OP_WORKERS:-2}" \
    -e "CAPANIX_WORKER_RUNTIME_THREADS=${CAPANIX_WORKER_RUNTIME_THREADS:-2}" \
    -e "CAPANIX_WORKER_MAX_BLOCKING_THREADS=${CAPANIX_WORKER_MAX_BLOCKING_THREADS:-4}" \
    -e "CAPANIX_BOUND_ROUTE_DISPATCHER_THREADS=${CAPANIX_BOUND_ROUTE_DISPATCHER_THREADS:-4}" \
    -e "FUSTOR_PROCESS_TREE_MAX_THREADS=${FUSTOR_PROCESS_TREE_MAX_THREADS:-512}" \
    -e "FUSTOR_PROCESS_TREE_PRE_TERMINATE_HOOK=/work/fustor/fs-meta/docs/examples/container-staging-nfs-cleanup.sh" \
    -e "FSMETA_CONTAINER_BUILD_TIMEOUT_SECONDS=$(build_timeout_seconds)" \
    -e "FSMETA_CONTAINER_STAGE_TIMEOUT_SECONDS=$(stage_timeout_seconds)" \
    -e "DATANIX_TEST_PRIVILEGED_COMMAND_TIMEOUT_SECS=${DATANIX_TEST_PRIVILEGED_COMMAND_TIMEOUT_SECS:-30}"
  local debug_env debug_value
  for debug_env in \
    CAPANIX_DEBUG_BIND_ADMISSION \
    CAPANIX_DEBUG_BOUND_ROUTE \
    CAPANIX_DEBUG_STREAM_ROUTE \
    CAPANIX_DEBUG_WORKER_CONTROL \
    DATANIX_KEEP_E2E_ARTIFACTS \
    FSMETA_DEBUG_CONTROL_SCOPE_CAPTURE \
    FSMETA_DEBUG_ENDPOINT_REQUEST_LIFECYCLE \
    FSMETA_DEBUG_EVENTS_GATE \
    FSMETA_DEBUG_MATERIALIZED_ROUTE_CAPTURE \
    FSMETA_DEBUG_SINK_APPLY_EVENTS \
    FSMETA_DEBUG_SOURCE_BATCH_FLOW \
    FSMETA_DEBUG_SOURCE_ROOT_TASK \
    FSMETA_DEBUG_SOURCE_STATUS_LIFECYCLE \
    FSMETA_DEBUG_SINK_QUERY_ROUTE_TRACE \
    FSMETA_DEBUG_SINK_WORKER_PRE_DISPATCH \
    FSMETA_DEBUG_STATUS_ROUTE_FANIN \
    FSMETA_DEBUG_STREAM_DELIVERY
  do
    debug_value="${!debug_env-}"
    if [[ -n "${debug_value}" ]]; then
      printf '%s\0' -e "${debug_env}=${debug_value}"
    fi
  done
  printf '%s\0' \
    -v "${repo_root}:/work/fustor" \
    -v "${capanix_root}:/work/capanix" \
    -v "${staging_tmp_root}:${staging_tmp_root}" \
    -v "${short_cluster_artifact_root}:${short_cluster_container_root}" \
    -w /work/fustor \
    "${image}"
}

run_container() {
  local suite="${1:-active-three}"
  if [[ "${suite}" == "all" ]]; then
    run_all_container
    return
  fi
  if [[ -z "${DATANIX_E2E_TMP_ROOT:-}" ]]; then
    staging_tmp_root="/tmp/fsmeta-container-staging/${suite//[^A-Za-z0-9_.-]/_}-$(date +%Y%m%d%H%M%S)-$$"
  fi
  require_resource_limit_configuration
  assert_staging_slot_available
  build_image
  prepare_host_kernel_limits
  local log="${FSMETA_CONTAINER_STAGING_LOG:-/tmp/fsmeta-container-staging-gate-$(date +%Y%m%d%H%M%S).log}"
  active_container_name="fsmeta-staging-${suite//[^A-Za-z0-9_.-]/-}-$(date +%Y%m%d%H%M%S)-$$"
  trap cleanup_outer_run_state EXIT
  local -a args
  mapfile -d '' -t args < <(container_args batch "${active_container_name}")
  set -o pipefail
  set +e
  timeout --foreground --signal=TERM --kill-after=30s \
    "$(container_run_timeout_seconds)s" \
    "${engine}" "${args[@]}" \
      /work/fustor/fs-meta/docs/examples/container-staging-gate.sh inside "${suite}" \
      2>&1 | tee "${log}"
  local status=${PIPESTATUS[0]}
  set -e
  if [[ "${status}" -eq 124 || "${status}" -eq 137 ]]; then
    echo "CONTAINER_STAGING_RUN_TIMEOUT suite=${suite} timeout_seconds=$(container_run_timeout_seconds) status=${status}" >&2
  fi
  if ! cleanup_active_container; then
    status=1
  fi
  if ! restore_host_kernel_limits; then
    status=1
  fi
  trap - EXIT
  if [[ "${status}" -eq 0 ]]; then
    echo "CONTAINER_STAGING_GATE_PASS suite=${suite} log=${log}"
  else
    echo "CONTAINER_STAGING_GATE_FAIL suite=${suite} status=${status} log=${log}"
  fi
  return "${status}"
}

run_all_container() {
  local log="${FSMETA_CONTAINER_STAGING_LOG:-/tmp/fsmeta-container-staging-gate-all-$(date +%Y%m%d%H%M%S).log}"
  local require_mini="${FSMETA_CONTAINER_STAGING_REQUIRE_MINI:-1}"
  set -o pipefail
  set +e
  (
    FSMETA_CONTAINER_STAGING_LOG="${log%.log}-active-three.log" \
      "$0" run active-three
    active_three_status=$?
    if [[ "${active_three_status}" -ne 0 ]]; then
      exit "${active_three_status}"
    fi

    FSMETA_CONTAINER_STAGING_LOG="${log%.log}-mini.log" \
      "$0" run mini
    mini_status=$?
    if [[ "${mini_status}" -ne 0 ]]; then
      if [[ "${require_mini}" == "1" ]]; then
        echo "CONTAINER_STAGING_GATE_BLOCKING_FAIL suite=mini status=${mini_status} require_mini=${require_mini}" >&2
        exit "${mini_status}"
      fi
      echo "CONTAINER_STAGING_GATE_OPTIONAL_FAIL suite=mini status=${mini_status} require_mini=${require_mini}" >&2
    fi
    exit 0
  ) 2>&1 | tee "${log}"
  local status=${PIPESTATUS[0]}
  set -e
  if [[ "${status}" -eq 0 ]]; then
    echo "CONTAINER_STAGING_GATE_PASS suite=all log=${log}"
  else
    echo "CONTAINER_STAGING_GATE_FAIL suite=all status=${status} log=${log}"
  fi
  return "${status}"
}

open_shell() {
  build_image
  local -a args
  mapfile -d '' -t args < <(container_args interactive)
  "${engine}" "${args[@]}" bash
}

collect_descendants() {
  local parent="$1"
  local child
  for child in $(pgrep -P "${parent}" 2>/dev/null || true); do
    printf ' %s' "${child}"
    collect_descendants "${child}"
  done
}

staging_container_rows() {
  "${engine}" ps -a \
    --filter "label=${staging_container_label}" \
    --format '{{.ID}} {{.Names}} {{.Status}}' 2>/dev/null || true
}

container_process_tree_rows() {
  local root_pid="$1"
  local tree_pids
  tree_pids="${root_pid}$(collect_descendants "${root_pid}")"
  tree_pids="$(tr ' ' ',' <<<"${tree_pids}" | sed 's/^,*//;s/,*$//;s/,,*/,/g')"
  [[ -n "${tree_pids}" ]] || return 0
  ps -p "${tree_pids}" -o pid=,ppid=,stat=,wchan:32=,comm=,args= 2>/dev/null || true
}

container_d_state_rows() {
  local root_pid="$1"
  container_process_tree_rows "${root_pid}" | awk '$3 ~ /^D/ { print }'
}

print_container_process_tree() {
  local root_pid="$1"
  echo "CONTAINER_STAGING_PROCESS_TREE pid ppid stat wchan command args" >&2
  container_process_tree_rows "${root_pid}" >&2
  pstree -ap "${root_pid}" >&2 || true
}

assert_staging_slot_available() {
  local rows=()
  mapfile -t rows < <(staging_container_rows)
  if [[ "${#rows[@]}" -eq 0 ]]; then
    echo "CONTAINER_STAGING_PREFLIGHT_PASS existing_containers=0"
    return 0
  fi

  local row id name state pid proc_state d_state_rows reason
  for row in "${rows[@]}"; do
    id="$(awk '{print $1}' <<<"${row}")"
    name="$(awk '{print $2}' <<<"${row}")"
    state="$("${engine}" inspect "${id}" --format '{{.State.Status}}' 2>/dev/null || echo unknown)"
    pid="$("${engine}" inspect "${id}" --format '{{.State.Pid}}' 2>/dev/null || echo 0)"
    proc_state=""
    d_state_rows=""
    if [[ "${pid}" =~ ^[0-9]+$ && "${pid}" -gt 0 && -e "/proc/${pid}/status" ]]; then
      proc_state="$(awk '/^State:/ {print $2}' "/proc/${pid}/status" 2>/dev/null || true)"
      d_state_rows="$(container_d_state_rows "${pid}")"
    fi
    reason="existing-container"
    [[ -z "${d_state_rows}" ]] || reason="d-state-process"
    echo "CONTAINER_STAGING_PREFLIGHT_BLOCKED id=${id} name=${name} state=${state} pid=${pid} proc_state=${proc_state:-unknown} reason=${reason}" >&2
    if [[ -n "${d_state_rows}" ]]; then
      print_container_process_tree "${pid}"
    fi
  done
  echo "CONTAINER_STAGING_PREFLIGHT_BLOCKED maintenance_required=1 containers=${#rows[@]} action=run-cleanup-stale-before-retry" >&2
  return 1
}

cleanup_stale() {
  local status=0
  local rows=()
  if ! command -v "${engine}" >/dev/null 2>&1; then
    echo "container engine not found: ${engine}" >&2
    return 2
  fi
  mapfile -t rows < <(staging_container_rows)
  if [[ "${#rows[@]}" -eq 0 ]]; then
    echo "CONTAINER_STAGING_CLEANUP_PASS containers=0"
    return 0
  fi

  local row id name state pid proc_state d_state_rows
  for row in "${rows[@]}"; do
    id="$(awk '{print $1}' <<<"${row}")"
    name="$(awk '{print $2}' <<<"${row}")"
    state="$("${engine}" inspect "${id}" --format '{{.State.Status}}' 2>/dev/null || echo unknown)"
    pid="$("${engine}" inspect "${id}" --format '{{.State.Pid}}' 2>/dev/null || echo 0)"
    proc_state=""
    if [[ "${pid}" =~ ^[0-9]+$ && "${pid}" -gt 0 && -e "/proc/${pid}/status" ]]; then
      proc_state="$(awk '/^State:/ {print $2}' "/proc/${pid}/status" 2>/dev/null || true)"
    fi
    echo "CONTAINER_STAGING_CLEANUP_CONTAINER id=${id} name=${name} state=${state} pid=${pid} proc_state=${proc_state:-unknown}"
    d_state_rows=""
    if [[ "${pid}" =~ ^[0-9]+$ && "${pid}" -gt 0 ]]; then
      d_state_rows="$(container_d_state_rows "${pid}")"
    fi
    if [[ -n "${d_state_rows}" ]]; then
      status=1
      echo "CONTAINER_STAGING_CLEANUP_BLOCKED id=${id} state=${state} pid=${pid} proc_state=${proc_state:-unknown} reason=d-state-process removal_attempted=0" >&2
      print_container_process_tree "${pid}"
      continue
    fi
    if timeout 20s "${engine}" rm -f "${id}" >/dev/null 2>&1; then
      echo "CONTAINER_STAGING_CLEANUP_REMOVED id=${id}"
    else
      status=1
      echo "CONTAINER_STAGING_CLEANUP_BLOCKED id=${id} state=${state} pid=${pid} proc_state=${proc_state:-unknown}" >&2
      if [[ "${pid}" =~ ^[0-9]+$ && "${pid}" -gt 0 ]]; then
        print_container_process_tree "${pid}"
      fi
    fi
  done
  if [[ "${status}" -eq 0 ]]; then
    echo "CONTAINER_STAGING_CLEANUP_PASS containers=${#rows[@]}"
  else
    echo "CONTAINER_STAGING_CLEANUP_BLOCKED maintenance_required=1 containers=${#rows[@]}" >&2
  fi
  return "${status}"
}

build_inside() {
  require_resource_limit_configuration
  cd /work/capanix
  rm -f target/debug/capanixd target/debug/cnxctl target/debug/capanix_worker_host
  run_guarded \
    build-capanix \
    "$(build_timeout_seconds)" \
    cargo build -p capanix-daemon -p capanix-cli -p capanix-worker-host

  cd /work/fustor
  rm -f target/release/libfs_meta_runtime.so target/release/fsmeta
  run_guarded \
    build-fs-meta \
    "$(build_timeout_seconds)" \
    cargo build --release -p fs-meta-runtime -p fs-meta-tooling

  export CAPANIXD_BIN=/work/capanix/target/debug/capanixd
  export CNXCTL_BIN=/work/capanix/target/debug/cnxctl
  export CAPANIX_WORKER_HOST_BINARY=/work/capanix/target/debug/capanix_worker_host
  export CAPANIX_FS_META_APP_BINARY=/work/fustor/target/release/libfs_meta_runtime.so
  export DATANIX_E2E_TMP_ROOT="${DATANIX_E2E_TMP_ROOT:-/tmp/fsmeta-container-staging}"
  export FSMETA_NFS_LAB_V4_PSEUDOROOT=1
  export FSMETA_ACTIVE_THREE_FILES_PER_ROOT="${FSMETA_ACTIVE_THREE_FILES_PER_ROOT:-9}"
  export FSMETA_ACTIVE_THREE_DIRS_PER_ROOT="${FSMETA_ACTIVE_THREE_DIRS_PER_ROOT:-1}"

  test -x "${CAPANIXD_BIN}"
  test -x "${CNXCTL_BIN}"
  test -x "${CAPANIX_WORKER_HOST_BINARY}"
  test -f "${CAPANIX_FS_META_APP_BINARY}"
  sha256sum \
    "${CAPANIXD_BIN}" \
    "${CNXCTL_BIN}" \
    "${CAPANIX_WORKER_HOST_BINARY}" \
    "${CAPANIX_FS_META_APP_BINARY}" \
    target/release/fsmeta
}

run_active_three_inside() {
  run_guarded \
    active-three \
    "$(stage_timeout_seconds)" \
    /usr/bin/env CAPANIX_REAL_NFS_E2E=1 \
      cargo test -p fustor-specs-root --test fs_meta_api_e2e \
      fs_meta_active_three_external_worker_replay_real_nfs \
      -- --ignored --nocapture --test-threads=1
}

run_matrix_stage_inside() {
  local label="$1"
  shift
  run_bounded \
    "${label}" \
    "$(stage_timeout_seconds)" \
    "${process_tree_watchdog}" \
    fs-meta/docs/examples/test-matrix-commands.sh \
    "$@"
}

cleanup_inside_run_state() {
  local original_status=$?
  trap - EXIT
  local cleanup_status=0
  "${nfs_cleanup}" normal-exit || cleanup_status=$?
  if [[ "${original_status}" -eq 0 && "${cleanup_status}" -ne 0 ]]; then
    original_status="${cleanup_status}"
  fi
  exit "${original_status}"
}

run_inside() {
  local suite="${1:-active-three}"
  if [[ "${FSMETA_CONTAINER_STAGING_INSIDE:-0}" != "1" ]]; then
    echo "CONTAINER_STAGING_INSIDE_REFUSED reason=missing-disposable-container-marker" >&2
    return 2
  fi
  trap cleanup_inside_run_state EXIT
  build_inside
  case "${suite}" in
    mini)
      run_matrix_stage_inside mini business-mini-nfs
      ;;
    active-three)
      run_active_three_inside
      ;;
    all)
      run_active_three_inside
      local mini_status=0
      run_matrix_stage_inside mini business-mini-nfs || mini_status=$?
      if [[ "${mini_status}" -ne 0 ]]; then
        if [[ "${FSMETA_CONTAINER_STAGING_REQUIRE_MINI:-1}" == "1" ]]; then
          echo "CONTAINER_STAGING_GATE_BLOCKING_FAIL suite=mini status=${mini_status} require_mini=${FSMETA_CONTAINER_STAGING_REQUIRE_MINI:-1}" >&2
          return "${mini_status}"
        fi
        echo "CONTAINER_STAGING_GATE_OPTIONAL_FAIL suite=mini status=${mini_status} require_mini=${FSMETA_CONTAINER_STAGING_REQUIRE_MINI:-1}" >&2
      fi
      ;;
    l5:*)
      run_matrix_stage_inside "${suite}" l5 "${suite#l5:}"
      ;;
    *)
      echo "unknown container staging suite: ${suite}" >&2
      usage >&2
      exit 2
      ;;
  esac
  echo "CONTAINER_STAGING_GATE_INSIDE_PASS suite=${suite}"
}

case "${1:-}" in
  build-image)
    build_image
    ;;
  cleanup-stale)
    cleanup_stale
    ;;
  self-test-resource-limits)
    self_test_resource_limits
    ;;
  run)
    shift
    run_container "${1:-active-three}"
    ;;
  shell)
    open_shell
    ;;
  inside)
    shift
    run_inside "${1:-active-three}"
    ;;
  -h|--help|help|"")
    usage
    ;;
  *)
    echo "unknown command: $1" >&2
    usage >&2
    exit 2
    ;;
esac

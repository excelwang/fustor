#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
RESOURCE_GUARD="${FUSTOR_RESOURCE_GUARD:-${SCRIPT_DIR}/resource-guard.sh}"
PROCESS_TREE_WATCHDOG="${FUSTOR_PROCESS_TREE_WATCHDOG:-${SCRIPT_DIR}/process-tree-watchdog.sh}"
LOG_PATH="${FUSTOR_WORKSPACE_TEST_LOG:-/tmp/fustor-workspace-test-$(date +%Y%m%d%H%M%S).log}"
TIMEOUT_SECONDS="${FUSTOR_WORKSPACE_TEST_TIMEOUT_SECONDS:-14400}"
MAX_THREADS="${FUSTOR_WORKSPACE_TEST_MAX_THREADS:-512}"
WATCHDOG_POLL_SECONDS="${FUSTOR_WORKSPACE_TEST_WATCHDOG_POLL_SECONDS:-1}"
WATCHDOG_TERM_GRACE_SECONDS="${FUSTOR_WORKSPACE_TEST_WATCHDOG_TERM_GRACE_SECONDS:-5}"
QUIC_SOCKET_BUFFER_BYTES=16777216

host_rmem_max_before=""
host_wmem_max_before=""
host_sysctl_restore_armed=0

restore_host_kernel_limits() {
    if [[ "${host_sysctl_restore_armed}" != "1" ]]; then
        return 0
    fi
    local status=0
    sysctl -q -w "net.core.rmem_max=${host_rmem_max_before}" || status=1
    sysctl -q -w "net.core.wmem_max=${host_wmem_max_before}" || status=1
    if [[ "${status}" -eq 0 ]]; then
        echo "FUSTOR_WORKSPACE_TEST_SYSCTL_RESTORED rmem_max=${host_rmem_max_before} wmem_max=${host_wmem_max_before}"
        host_sysctl_restore_armed=0
    else
        echo "FUSTOR_WORKSPACE_TEST_SYSCTL_RESTORE_FAIL rmem_max=${host_rmem_max_before} wmem_max=${host_wmem_max_before}" >&2
    fi
    return "${status}"
}

cleanup_workspace_gate() {
    local status=$?
    trap - EXIT
    if ! restore_host_kernel_limits; then
        status=1
    fi
    exit "${status}"
}

prepare_host_kernel_limits() {
    host_rmem_max_before="$(cat /proc/sys/net/core/rmem_max)"
    host_wmem_max_before="$(cat /proc/sys/net/core/wmem_max)"
    if ((host_rmem_max_before >= QUIC_SOCKET_BUFFER_BYTES && host_wmem_max_before >= QUIC_SOCKET_BUFFER_BYTES)); then
        echo "FUSTOR_WORKSPACE_TEST_SYSCTL_READY rmem_max=${host_rmem_max_before} wmem_max=${host_wmem_max_before} required=${QUIC_SOCKET_BUFFER_BYTES} changed=0"
        return 0
    fi
    if [[ "${FUSTOR_WORKSPACE_TEST_TUNE_HOST_SYSCTLS:-1}" != "1" ]]; then
        echo "FUSTOR_WORKSPACE_TEST_SYSCTL_INSUFFICIENT rmem_max=${host_rmem_max_before} wmem_max=${host_wmem_max_before} required=${QUIC_SOCKET_BUFFER_BYTES} tuning=disabled" >&2
        return 1
    fi
    command -v sysctl >/dev/null 2>&1 || {
        echo "sysctl is required to prepare the workspace QUIC socket limits" >&2
        return 2
    }

    host_sysctl_restore_armed=1
    trap cleanup_workspace_gate EXIT
    if ((host_rmem_max_before < QUIC_SOCKET_BUFFER_BYTES)); then
        sysctl -q -w "net.core.rmem_max=${QUIC_SOCKET_BUFFER_BYTES}"
    fi
    if ((host_wmem_max_before < QUIC_SOCKET_BUFFER_BYTES)); then
        sysctl -q -w "net.core.wmem_max=${QUIC_SOCKET_BUFFER_BYTES}"
    fi

    local current_rmem current_wmem
    current_rmem="$(cat /proc/sys/net/core/rmem_max)"
    current_wmem="$(cat /proc/sys/net/core/wmem_max)"
    if ((current_rmem < QUIC_SOCKET_BUFFER_BYTES || current_wmem < QUIC_SOCKET_BUFFER_BYTES)); then
        echo "FUSTOR_WORKSPACE_TEST_SYSCTL_INSUFFICIENT rmem_max=${current_rmem} wmem_max=${current_wmem} required=${QUIC_SOCKET_BUFFER_BYTES} tuning=failed" >&2
        return 1
    fi
    echo "FUSTOR_WORKSPACE_TEST_SYSCTL_READY rmem_max=${current_rmem} wmem_max=${current_wmem} required=${QUIC_SOCKET_BUFFER_BYTES} changed=1"
}

cd "${ROOT_DIR}"
mkdir -p "$(dirname "${LOG_PATH}")"

prepare_host_kernel_limits
"${SCRIPT_DIR}/test-resource-guard.sh"
"${SCRIPT_DIR}/test-process-tree-watchdog.sh"

status=0
set +e
FUSTOR_PROCESS_TREE_MAX_THREADS="${MAX_THREADS}" \
    FUSTOR_PROCESS_TREE_POLL_SECONDS="${WATCHDOG_POLL_SECONDS}" \
    FUSTOR_PROCESS_TREE_TERM_GRACE_SECONDS="${WATCHDOG_TERM_GRACE_SECONDS}" \
    "${PROCESS_TREE_WATCHDOG}" \
    timeout --foreground "${TIMEOUT_SECONDS}" \
    "${RESOURCE_GUARD}" cargo test --workspace --all-targets -- --test-threads=1 \
    2>&1 | tee "${LOG_PATH}"
status="${PIPESTATUS[0]}"
set -e

if ! restore_host_kernel_limits; then
    status=1
fi
trap - EXIT

if [[ "${status}" -eq 0 ]]; then
    echo "FUSTOR_WORKSPACE_TEST_PASS log=${LOG_PATH}" | tee -a "${LOG_PATH}"
else
    echo "FUSTOR_WORKSPACE_TEST_FAIL status=${status} log=${LOG_PATH}" | tee -a "${LOG_PATH}" >&2
    exit "${status}"
fi

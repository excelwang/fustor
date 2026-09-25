#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
AGENTIC_MAP_REPO_ROOT="${AGENTIC_MAP_REPO_ROOT:-/opt/fusiongraph/src/agentic-map}"
RESOURCE_GUARD="${FUSTOR_RESOURCE_GUARD:-${SCRIPT_DIR}/resource-guard.sh}"
LOG_PATH="${DATA_ACTION_MAP_CROSS_REPO_GATE_LOG:-/tmp/data-action-map-cross-repo-gate-$(date +%Y%m%d%H%M%S).log}"

run() {
    printf '\n+'
    for arg in "$@"; do
        printf ' %q' "$arg"
    done
    printf '\n'
    "$@"
}

run_data_action_map_local_gate() {
    local child_log="/tmp/data-action-map-local-gate-cross-repo-child.log"
    printf '\n+ DATA_ACTION_MAP_LOCAL_GATE_LOG=%q %q\n' \
        "${child_log}" "${ROOT_DIR}/scripts/data-action-map-local-gate.sh"
    DATA_ACTION_MAP_LOCAL_GATE_LOG="${child_log}" \
        "${ROOT_DIR}/scripts/data-action-map-local-gate.sh"
}

run_agentic_map_local_gate() {
    local child_log="/tmp/agentic-map-local-gate-cross-repo-child.log"
    printf '\n+ AGENTIC_MAP_LOCAL_GATE_LOG=%q %q\n' \
        "${child_log}" "${AGENTIC_MAP_REPO_ROOT}/scripts/agentic-map-local-gate.sh"
    AGENTIC_MAP_LOCAL_GATE_LOG="${child_log}" \
        "${RESOURCE_GUARD}" "${AGENTIC_MAP_REPO_ROOT}/scripts/agentic-map-local-gate.sh"
}

fail() {
    echo "$*" >&2
    return 1
}

check_agentic_map_repo() {
    [[ -f "${AGENTIC_MAP_REPO_ROOT}/Cargo.toml" ]] || \
        fail "agentic-map Cargo.toml not found at ${AGENTIC_MAP_REPO_ROOT}"
    [[ -x "${AGENTIC_MAP_REPO_ROOT}/scripts/agentic-map-local-gate.sh" ]] || \
        fail "agentic-map local gate is not executable"
}

check_fustor_boundary() {
    if rg -n "agentic-map" \
        "${ROOT_DIR}/Cargo.toml" \
        "${ROOT_DIR}/private-discovery" \
        "${ROOT_DIR}/source-kit" \
        "${ROOT_DIR}/union-graph"; then
        fail "Fustor reusable crates must not depend on or name agentic-map"
    fi

    if rg -n "ResearchPlanSession|AgenticMapRequest|AgenticMapPayload|agentic-map\\.plan|agentic-map-cli" \
        "${ROOT_DIR}/private-discovery/src" \
        "${ROOT_DIR}/specs/L2-ARCHITECTURE.md" \
        "${ROOT_DIR}/specs/L3-RUNTIME"; then
        fail "Fustor private-discovery spec/src still owns agentic-map runtime terms"
    fi

    if rg -n "agentic-map|Agentic Map|agent-facing|agent-visible|human-agent|agent-owned|research-plan" \
        "${ROOT_DIR}/specs/L0-VISION.md" \
        "${ROOT_DIR}/specs/L1-CONTRACTS.md" \
        "${ROOT_DIR}/specs/L2-ARCHITECTURE.md" \
        "${ROOT_DIR}/specs/L3-RUNTIME" \
        "${ROOT_DIR}/specs/readme.md"; then
        fail "Fustor active specs must stay caller-agnostic and must not name an upper-layer agent/app runtime"
    fi

    if rg -n "PermissionSubgraph|BoundedActionContract|BudgetLedger|FrontierState|AgentProbeClass|ActionExecutionResult|agent_message|permission_subgraph|budget_ledger" \
        "${ROOT_DIR}/private-discovery/src" \
        "${ROOT_DIR}/specs/L2-ARCHITECTURE.md" \
        "${ROOT_DIR}/specs/L3-RUNTIME"; then
        fail "Fustor private-discovery spec/src still contains stale primitive names"
    fi

    if rg -n "Bio Pipeline Demonstration|bio pipeline|\"frontier_action\"" \
        "${ROOT_DIR}/specs"; then
        fail "Fustor specs still contain stale demo residue or old frontier example shape"
    fi
}

check_agentic_map_boundary() {
    if find "${AGENTIC_MAP_REPO_ROOT}" -maxdepth 2 -type d \
        \( -name "pg-source" -o -name "mysql-source" -o -name "s3-source" \
        -o -name "source-kit" -o -name "union-graph" \) | rg .; then
        fail "agentic-map must not copy Fustor source connector or graph crates"
    fi

    rg -n 'private-discovery = \{[^}]*path = "../fustor/private-discovery"' \
        "${AGENTIC_MAP_REPO_ROOT}/Cargo.toml" >/dev/null || \
        fail "agentic-map must depend on Fustor private-discovery through the local path during this gate"
    rg -n 'source-kit = \{[^}]*path = "../fustor/source-kit"' \
        "${AGENTIC_MAP_REPO_ROOT}/Cargo.toml" >/dev/null || \
        fail "agentic-map must depend on Fustor source-kit through the local path during this gate"
    rg -n 'union-graph = \{[^}]*path = "../fustor/union-graph/lib"' \
        "${AGENTIC_MAP_REPO_ROOT}/Cargo.toml" >/dev/null || \
        fail "agentic-map must depend on Fustor union-graph through the local path during this gate"
    rg -n 'capanix-runtime-entry-sdk = \{[^}]*path = "../capanix/crates/runtime-entry-sdk"' \
        "${AGENTIC_MAP_REPO_ROOT}/Cargo.toml" >/dev/null || \
        fail "agentic-map must depend on Capanix runtime-entry-sdk through the local path during this gate"

    if rg -n '^## PRIVATE_DISCOVERY\.' "${AGENTIC_MAP_REPO_ROOT}/specs"; then
        fail "agentic-map specs must use AGENTIC_MAP.* sections, not PRIVATE_DISCOVERY.*"
    fi
    if rg -n 'PrivateDataDiscoveryRequest \{|PrivateDataDiscoveryResponse \{|BoundedSourceAction \{|DiscoveryBudget \{|DiscoveryFrontier \{' \
        "${AGENTIC_MAP_REPO_ROOT}/specs"; then
        fail "agentic-map specs must import Fustor primitives instead of copying their model blocks"
    fi
    if rg -n '"frontier_action"' "${AGENTIC_MAP_REPO_ROOT}/specs"; then
        fail "agentic-map specs still contain the old frontier_action example shape"
    fi
}

main() {
    cd "${ROOT_DIR}"
    echo "DATA_ACTION_MAP_CROSS_REPO_GATE_BEGIN fustor=${ROOT_DIR} agentic_map=${AGENTIC_MAP_REPO_ROOT}"
    echo "official_stable=not_touched hosts=198.51.100.144,198.51.100.145,198.51.100.146"

    check_agentic_map_repo || return $?
    run_data_action_map_local_gate || return $?
    run_agentic_map_local_gate || return $?
    check_fustor_boundary || return $?
    check_agentic_map_boundary || return $?
}

mkdir -p "$(dirname "${LOG_PATH}")"
if main 2>&1 | tee "${LOG_PATH}"; then
    echo "AIBOX_GATE_PASS data-action-map-cross-repo-gate log=${LOG_PATH}" | tee -a "${LOG_PATH}"
else
    status=${PIPESTATUS[0]}
    echo "AIBOX_GATE_FAIL data-action-map-cross-repo-gate status=${status} log=${LOG_PATH}" | tee -a "${LOG_PATH}" >&2
    exit "${status}"
fi

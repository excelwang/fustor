#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
RESOURCE_GUARD="${FUSTOR_RESOURCE_GUARD:-${SCRIPT_DIR}/resource-guard.sh}"
PROCESS_TREE_WATCHDOG="${FUSTOR_PROCESS_TREE_WATCHDOG:-${SCRIPT_DIR}/process-tree-watchdog.sh}"
MAX_THREADS="${FUSTOR_DATA_ACTION_MAP_LOCAL_GATE_MAX_THREADS:-512}"
LOG_PATH="${DATA_ACTION_MAP_LOCAL_GATE_LOG:-/tmp/data-action-map-local-gate-$(date +%Y%m%d%H%M%S).log}"

run() {
    printf '\n+'
    for arg in "$@"; do
        printf ' %q' "$arg"
    done
    printf '\n'
    "$@"
}

run_guarded_cargo() {
    FUSTOR_PROCESS_TREE_MAX_THREADS="${MAX_THREADS}" \
        "${PROCESS_TREE_WATCHDOG}" "${RESOURCE_GUARD}" cargo "$@"
}

check_no_removed_mock_residue() {
    if rg -n "bio_pipeline_mock|BioPipelineMock|bio_mock|bio-mock|ingest synthetic skeleton|runtime control frames are not applied" \
        "${ROOT_DIR}/TODO.md" \
        "${ROOT_DIR}/private-discovery" \
        "${ROOT_DIR}/union-graph" \
        "${ROOT_DIR}/source-kit" \
        "${ROOT_DIR}/scripts/local-capanix-5node-deploy.sh" \
        "${ROOT_DIR}/csv-source" \
        "${ROOT_DIR}/es-source" \
        "${ROOT_DIR}/jsonl-source" \
        "${ROOT_DIR}/local-fs-source" \
        "${ROOT_DIR}/mysql-source" \
        "${ROOT_DIR}/pg-source" \
        "${ROOT_DIR}/s3-source"; then
        echo "removed mock/control-stub residue found" >&2
        return 1
    fi
}

main() {
    cd "${ROOT_DIR}"
    echo "DATA_ACTION_MAP_LOCAL_GATE_BEGIN root=${ROOT_DIR}"
    echo "official_stable=not_touched hosts=198.51.100.144,198.51.100.145,198.51.100.146"

    run "${SCRIPT_DIR}/test-resource-guard.sh" || return $?
    run "${SCRIPT_DIR}/test-process-tree-watchdog.sh" || return $?
    run "${ROOT_DIR}/fs-meta/docs/examples/container-staging-gate.sh" self-test-resource-limits || return $?
    run "${SCRIPT_DIR}/test-source-connector-acceptance-resource-limits.sh" || return $?
    run run_guarded_cargo fmt --check --manifest-path Cargo.toml || return $?
    check_no_removed_mock_residue || return $?
    run run_guarded_cargo test --manifest-path Cargo.toml --test specs_fustor_architecture -- --nocapture || return $?
    run run_guarded_cargo test --manifest-path Cargo.toml \
        -p data-view \
        -p data-view-normalizers \
        -p source-kit \
        -p private-discovery \
        -p union-graph \
        -p union-graph-runtime \
        -p pg-source-runtime \
        --all-targets -- --nocapture || return $?
    run run_guarded_cargo test --manifest-path Cargo.toml \
        -p csv-source-runtime \
        -p jsonl-source-runtime \
        -p local-fs-source-runtime \
        -p mysql-source-runtime \
        -p s3-source-runtime \
        -p es-source-runtime \
        --all-targets -- --nocapture || return $?
}

mkdir -p "$(dirname "${LOG_PATH}")"
if main 2>&1 | tee "${LOG_PATH}"; then
    echo "AIBOX_GATE_PASS data-action-map-local-gate log=${LOG_PATH}" | tee -a "${LOG_PATH}"
else
    status=${PIPESTATUS[0]}
    echo "AIBOX_GATE_FAIL data-action-map-local-gate status=${status} log=${LOG_PATH}" | tee -a "${LOG_PATH}" >&2
    exit "${status}"
fi

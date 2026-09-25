#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WATCHDOG="${SCRIPT_DIR}/process-tree-watchdog.sh"
TMP_DIR="$(mktemp -d /tmp/fustor-process-tree-watchdog-test.XXXXXX)"
trap 'rm -rf "${TMP_DIR}"' EXIT

result="$(
    FUSTOR_PROCESS_TREE_MAX_THREADS=4 \
    FUSTOR_PROCESS_TREE_WATCHDOG_QUIET=1 \
        "${WATCHDOG}" bash -c 'sleep 0.2; printf "watchdog-pass"'
)"
[[ "${result}" == "watchdog-pass" ]]

set +e
FUSTOR_PROCESS_TREE_MAX_THREADS=513 \
FUSTOR_PROCESS_TREE_WATCHDOG_QUIET=1 \
    "${WATCHDOG}" true >/dev/null 2>&1
unsafe_limit_status=$?
set -e
[[ "${unsafe_limit_status}" == "2" ]]

pid_file="${TMP_DIR}/children"
hook_file="${TMP_DIR}/pre-terminate-hook"
hook_script="${TMP_DIR}/pre-terminate-hook.sh"
cat >"${hook_script}" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$1" >"$FUSTOR_WATCHDOG_TEST_HOOK_FILE"
EOF
chmod +x "${hook_script}"
log_file="${TMP_DIR}/limit.log"
set +e
FUSTOR_PROCESS_TREE_MAX_THREADS=4 \
FUSTOR_PROCESS_TREE_POLL_SECONDS=0.1 \
FUSTOR_PROCESS_TREE_TERM_GRACE_SECONDS=0.1 \
FUSTOR_PROCESS_TREE_PRE_TERMINATE_HOOK="${hook_script}" \
FUSTOR_WATCHDOG_TEST_HOOK_FILE="${hook_file}" \
FUSTOR_PROCESS_TREE_WATCHDOG_QUIET=1 \
    "${WATCHDOG}" bash -c '
        pid_file="$1"
        for _ in 1 2 3 4 5 6 7 8; do
            sleep 30 &
            printf "%s\n" "$!" >>"${pid_file}"
        done
        wait
    ' _ "${pid_file}" >"${log_file}" 2>&1
status=$?
set -e

[[ "${status}" == "125" ]]
rg -q "FUSTOR_PROCESS_TREE_WATCHDOG_LIMIT .*threads=.*max_threads=4" "${log_file}"
rg -q "FUSTOR_PROCESS_TREE_WATCHDOG_DIAGNOSTICS" "${log_file}"
rg -q "FUSTOR_PROCESS_TREE_WATCHDOG_PRE_TERMINATE_DONE reason=thread-limit" "${log_file}"
[[ "$(<"${hook_file}")" == "thread-limit" ]]

while read -r child_pid; do
    if kill -0 "${child_pid}" 2>/dev/null; then
        echo "watchdog left child process running: ${child_pid}" >&2
        exit 1
    fi
done <"${pid_file}"

echo "FUSTOR_PROCESS_TREE_WATCHDOG_TEST_PASS"

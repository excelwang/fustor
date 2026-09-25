#!/usr/bin/env bash
set -euo pipefail

fail() {
    echo "FUSTOR_PROCESS_TREE_WATCHDOG_FAIL $*" >&2
    exit 2
}

is_positive_integer() {
    [[ "$1" =~ ^[1-9][0-9]*$ ]]
}

is_positive_duration() {
    [[ "$1" =~ ^([1-9][0-9]*|0\.[0-9]*[1-9][0-9]*|[1-9][0-9]*\.[0-9]+)$ ]]
}

[[ $# -gt 0 ]] || fail "usage: scripts/process-tree-watchdog.sh COMMAND [ARG ...]"

setsid_bin="$(command -v setsid || true)"
ps_bin="$(command -v ps || true)"
[[ -n "${setsid_bin}" ]] || fail "setsid is required"
[[ -n "${ps_bin}" ]] || fail "ps is required"

max_threads="${FUSTOR_PROCESS_TREE_MAX_THREADS:-512}"
poll_seconds="${FUSTOR_PROCESS_TREE_POLL_SECONDS:-1}"
term_grace_seconds="${FUSTOR_PROCESS_TREE_TERM_GRACE_SECONDS:-5}"
limit_exit_status="${FUSTOR_PROCESS_TREE_LIMIT_EXIT_STATUS:-125}"
pre_terminate_hook="${FUSTOR_PROCESS_TREE_PRE_TERMINATE_HOOK:-}"
pre_terminate_hook_timeout_seconds="${FUSTOR_PROCESS_TREE_PRE_TERMINATE_HOOK_TIMEOUT_SECONDS:-30}"

is_positive_integer "${max_threads}" || fail "FUSTOR_PROCESS_TREE_MAX_THREADS must be a positive integer"
((max_threads <= 512)) || fail "FUSTOR_PROCESS_TREE_MAX_THREADS must be at most 512"
is_positive_duration "${poll_seconds}" || fail "FUSTOR_PROCESS_TREE_POLL_SECONDS must be a positive duration"
is_positive_duration "${term_grace_seconds}" || fail "FUSTOR_PROCESS_TREE_TERM_GRACE_SECONDS must be a positive duration"
is_positive_integer "${limit_exit_status}" || fail "FUSTOR_PROCESS_TREE_LIMIT_EXIT_STATUS must be a positive integer"
is_positive_integer "${pre_terminate_hook_timeout_seconds}" || fail "FUSTOR_PROCESS_TREE_PRE_TERMINATE_HOOK_TIMEOUT_SECONDS must be a positive integer"
((limit_exit_status <= 255)) || fail "FUSTOR_PROCESS_TREE_LIMIT_EXIT_STATUS must be at most 255"
if [[ -n "${pre_terminate_hook}" && ! -x "${pre_terminate_hook}" ]]; then
    fail "FUSTOR_PROCESS_TREE_PRE_TERMINATE_HOOK must be executable: ${pre_terminate_hook}"
fi

root_pid=""
peak_threads=0
peak_rss_kb=0
peak_processes=0
pre_terminate_hook_ran=0

process_tree_rows() {
    "${ps_bin}" -e -o pid=,ppid=,pgid=,nlwp=,rss=,stat=,comm= | awk -v root="${root_pid}" '
        {
            pid[NR] = $1
            ppid[NR] = $2
            row[NR] = $0
            if ($1 == root) {
                selected[$1] = 1
            }
        }
        END {
            changed = 1
            while (changed) {
                changed = 0
                for (i = 1; i <= NR; i++) {
                    if (!selected[pid[i]] && selected[ppid[i]]) {
                        selected[pid[i]] = 1
                        changed = 1
                    }
                }
            }
            for (i = 1; i <= NR; i++) {
                if (selected[pid[i]]) {
                    print row[i]
                }
            }
        }
    '
}

terminate_process_tree() {
    local signal="$1"
    local rows pids

    rows="$(process_tree_rows || true)"
    pids="$(awk '{ print $1 }' <<<"${rows}")"
    if [[ -n "${pids}" ]]; then
        # shellcheck disable=SC2086
        kill "-${signal}" ${pids} 2>/dev/null || true
    fi
    kill "-${signal}" -- "-${root_pid}" 2>/dev/null || true
}

run_pre_terminate_hook() {
    local reason="$1"
    if [[ -z "${pre_terminate_hook}" || "${pre_terminate_hook_ran}" == "1" ]]; then
        return 0
    fi
    pre_terminate_hook_ran=1
    echo "FUSTOR_PROCESS_TREE_WATCHDOG_PRE_TERMINATE_BEGIN reason=${reason} hook=${pre_terminate_hook} timeout_seconds=${pre_terminate_hook_timeout_seconds}" >&2
    local status=0
    timeout --foreground --signal=TERM --kill-after=5s \
        "${pre_terminate_hook_timeout_seconds}s" \
        "${pre_terminate_hook}" "${reason}" || status=$?
    if [[ "${status}" -eq 0 ]]; then
        echo "FUSTOR_PROCESS_TREE_WATCHDOG_PRE_TERMINATE_DONE reason=${reason}" >&2
    else
        echo "FUSTOR_PROCESS_TREE_WATCHDOG_PRE_TERMINATE_FAIL reason=${reason} status=${status}" >&2
    fi
    return 0
}

print_thread_name_diagnostics() {
    local rows="$1"
    local pid task name

    echo "FUSTOR_PROCESS_TREE_WATCHDOG_THREAD_NAMES count pid name" >&2
    while read -r pid _; do
        [[ -n "${pid}" ]] || continue
        for task in "/proc/${pid}/task/"*; do
            [[ -r "${task}/comm" ]] || continue
            IFS= read -r name <"${task}/comm" || true
            printf '%s\t%s\n' "${pid}" "${name:-unknown}"
        done
    done <<<"${rows}" | awk -F '\t' '
        { key = $1 "\t" $2; count[key] += 1 }
        END { for (key in count) print count[key] "\t" key }
    ' | sort -k1,1nr | head -n 60 >&2 || true
}

handle_signal() {
    local signal="$1"
    local status="$2"

    if [[ -n "${root_pid}" ]]; then
        run_pre_terminate_hook "signal-${signal,,}"
        terminate_process_tree "${signal}"
    fi
    exit "${status}"
}

trap 'handle_signal TERM 143' TERM
trap 'handle_signal INT 130' INT
trap 'handle_signal HUP 129' HUP

if [[ "${FUSTOR_PROCESS_TREE_WATCHDOG_QUIET:-0}" != "1" ]]; then
    printf 'FUSTOR_PROCESS_TREE_WATCHDOG max_threads=%s poll_seconds=%s term_grace_seconds=%s command=' \
        "${max_threads}" "${poll_seconds}" "${term_grace_seconds}" >&2
    printf ' %q' "$@" >&2
    printf '\n' >&2
fi

# Always make setsid keep a waiting parent. Without --fork/--wait, util-linux
# setsid may return immediately when it has to fork a session leader, leaving
# the real command detached from this watchdog.
"${setsid_bin}" --fork --wait -- "$@" &
root_pid=$!

while kill -0 "${root_pid}" 2>/dev/null; do
    rows="$(process_tree_rows || true)"
    read -r total_threads total_rss_kb process_count <<<"$(
        awk '{ threads += $4; rss += $5; processes += 1 } END { print threads + 0, rss + 0, processes + 0 }' <<<"${rows}"
    )"

    ((total_threads > peak_threads)) && peak_threads="${total_threads}"
    ((total_rss_kb > peak_rss_kb)) && peak_rss_kb="${total_rss_kb}"
    ((process_count > peak_processes)) && peak_processes="${process_count}"

    if ((total_threads > max_threads)); then
        echo "FUSTOR_PROCESS_TREE_WATCHDOG_LIMIT root_pid=${root_pid} threads=${total_threads} max_threads=${max_threads} processes=${process_count} rss_kb=${total_rss_kb}" >&2
        echo "FUSTOR_PROCESS_TREE_WATCHDOG_DIAGNOSTICS pid ppid pgid threads rss_kb stat command" >&2
        { sort -k4,4nr -k5,5nr <<<"${rows}" | head -n 40; } >&2 || true
        print_thread_name_diagnostics "${rows}"

        run_pre_terminate_hook "thread-limit"
        terminate_process_tree TERM
        sleep "${term_grace_seconds}"
        terminate_process_tree KILL
        wait "${root_pid}" 2>/dev/null || true
        exit "${limit_exit_status}"
    fi

    sleep "${poll_seconds}"
done

set +e
wait "${root_pid}"
status=$?
set -e
if [[ "${FUSTOR_PROCESS_TREE_WATCHDOG_QUIET:-0}" != "1" ]]; then
    echo "FUSTOR_PROCESS_TREE_WATCHDOG_DONE status=${status} peak_threads=${peak_threads} peak_processes=${peak_processes} peak_rss_kb=${peak_rss_kb}" >&2
fi
exit "${status}"

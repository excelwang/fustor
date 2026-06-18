#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  collect_official_node_logs.sh [options]

Collect one bounded, read-only daemon-log sample from each official fsmeta-stable
node. The script performs one SSH command per node and never polls.

Options:
  --focus <name>        roots-control | status-fanin | source-scan | endpoints | all
                        Default: roots-control
  --pattern <regex>     Extra extended grep pattern ORed with the focus pattern.
  --lines <n>           Tail lines per node after filtering. Default: 180, max: 600.
  --out-dir <path>      Optional local output directory for per-node excerpts.
  --run-root <path>     Remote run root. Default: /home/wanghuajin/fsmeta-stable/run
  --ssh-user <user>     SSH user. Default: wanghuajin
  --connect-timeout <s> SSH connect timeout seconds. Default: 10.
  --nodes <csv>         host_ref:ip pairs. Default:
                        panda144:10.0.82.144,panda145:10.0.82.145,panda146:10.0.82.146
  --dry-run             Print the resolved plan without SSH.
  -h, --help            Show this help.

Examples:
  collect_official_node_logs.sh --focus roots-control --lines 220
  collect_official_node_logs.sh --focus status-fanin --out-dir /tmp/fsmeta-node-logs
  collect_official_node_logs.sh --pattern 'invalid or revoked grant attachment|nfs-144'
USAGE
}

focus=roots-control
extra_pattern=
lines=180
out_dir=
run_root=/home/wanghuajin/fsmeta-stable/run
ssh_user=wanghuajin
connect_timeout=10
nodes_csv=panda144:10.0.82.144,panda145:10.0.82.145,panda146:10.0.82.146
dry_run=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --focus)
      focus=${2:?missing value for --focus}
      shift 2
      ;;
    --pattern)
      extra_pattern=${2:?missing value for --pattern}
      shift 2
      ;;
    --lines)
      lines=${2:?missing value for --lines}
      shift 2
      ;;
    --out-dir)
      out_dir=${2:?missing value for --out-dir}
      shift 2
      ;;
    --run-root)
      run_root=${2:?missing value for --run-root}
      shift 2
      ;;
    --ssh-user)
      ssh_user=${2:?missing value for --ssh-user}
      shift 2
      ;;
    --connect-timeout)
      connect_timeout=${2:?missing value for --connect-timeout}
      shift 2
      ;;
    --nodes)
      nodes_csv=${2:?missing value for --nodes}
      shift 2
      ;;
    --dry-run)
      dry_run=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if ! [[ "$lines" =~ ^[0-9]+$ ]]; then
  echo "--lines must be a positive integer" >&2
  exit 2
fi
if (( lines < 1 )); then
  lines=1
fi
if (( lines > 600 )); then
  lines=600
fi
if ! [[ "$connect_timeout" =~ ^[0-9]+$ ]] || (( connect_timeout < 1 )); then
  echo "--connect-timeout must be a positive integer" >&2
  exit 2
fi

case "$focus" in
  roots-control)
    focus_pattern='source-logical-roots-control|sink-logical-roots-control|roots_put control send|roots_control|runtime\.exec\.source|runtime\.exec\.scan|scheduled_source|scheduled_scan|invalid or revoked grant attachment|drained/fenced|grant attachments|AccessDenied'
    ;;
  status-fanin)
    focus_pattern='fs_meta_api_status|status authoritative fan-in|source_coverage_gaps|scan_coverage_gaps|sink_owner_partition_gaps|source-status|sink-status|repair_lanes'
    ;;
  source-scan)
    focus_pattern='runtime\.exec\.source|runtime\.exec\.scan|source-scan-audit|scheduled_source|scheduled_scan|scan_coverage|source_coverage|source_observability|source-status'
    ;;
  endpoints)
    focus_pattern='spawning .*endpoint|retiring inactive runtime endpoint|pruning finished runtime endpoint|stale grant-attachment recv gap|route_plan route=|grant attachment'
    ;;
  all)
    focus_pattern='.'
    ;;
  *)
    echo "unknown --focus: $focus" >&2
    usage >&2
    exit 2
    ;;
esac

pattern=$focus_pattern
if [[ -n "$extra_pattern" ]]; then
  pattern="($focus_pattern)|($extra_pattern)"
fi

set +e
grep -E "$pattern" /dev/null >/dev/null 2>&1
grep_rc=$?
set -e
if (( grep_rc == 2 )); then
  echo "invalid extended grep pattern for --focus/--pattern: $pattern" >&2
  exit 2
fi

if [[ -n "$out_dir" ]]; then
  mkdir -p "$out_dir"
  {
    printf 'script=%s\n' "$0"
    printf 'started_at_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'collector_host=%s\n' "$(hostname 2>/dev/null || printf unknown)"
    printf 'focus=%s\n' "$focus"
    printf 'lines=%s\n' "$lines"
    printf 'run_root=%s\n' "$run_root"
    printf 'ssh_user=%s\n' "$ssh_user"
    printf 'connect_timeout=%s\n' "$connect_timeout"
    printf 'nodes=%s\n' "$nodes_csv"
    printf 'pattern=%s\n' "$pattern"
    printf 'mode=single_bounded_read_only_sample\n'
    printf 'secret_inputs=none\n'
  } >"$out_dir/collection-plan.txt"
  summary_file="$out_dir/collection-summary.txt"
  : >"$summary_file"
else
  summary_file=
fi

IFS=',' read -r -a node_pairs <<<"$nodes_csv"

printf 'fsmeta_node_log_collection focus=%s lines=%s run_root=%s\n' "$focus" "$lines" "$run_root"

tmp_out=
cleanup_tmp() {
  if [[ -n "${tmp_out:-}" ]]; then
    rm -f "$tmp_out"
  fi
}
trap cleanup_tmp EXIT

record_summary() {
  if [[ -n "$summary_file" ]]; then
    printf '%s\n' "$*" >>"$summary_file"
  fi
}

failures=0

for pair in "${node_pairs[@]}"; do
  host_ref=${pair%%:*}
  host_ip=${pair#*:}
  if [[ -z "$host_ref" || -z "$host_ip" || "$host_ref" == "$host_ip" ]]; then
    echo "invalid node pair: $pair" >&2
    exit 2
  fi
  printf -v remote_cmd 'bash -s -- %q %q %q %q' \
    "$run_root" "$host_ref" "$pattern" "$lines"
  printf '\n===== %s %s =====\n' "$host_ref" "$host_ip"
  if (( dry_run )); then
    printf 'ssh -o BatchMode=yes -o ConnectTimeout=%s %s@%s %q\n' \
      "$connect_timeout" "$ssh_user" "$host_ip" "$remote_cmd"
    printf 'remote_log=%s\n' "$run_root/nodes/$host_ref/daemon.log"
    record_summary "node=$host_ref ip=$host_ip status=dry_run output=stdout"
    continue
  fi
  tmp_out=$(mktemp)
  node_file=
  if [[ -n "$out_dir" ]]; then
    node_file="$out_dir/${host_ref}-daemon-${focus}.log"
  fi
  if ssh -o BatchMode=yes -o ConnectTimeout="$connect_timeout" "${ssh_user}@${host_ip}" \
    "$remote_cmd" >"$tmp_out" 2>&1; then
    cat "$tmp_out"
    match_count=$(grep -Ev '^(log|missing_log)=' "$tmp_out" | wc -l | awk '{print $1}')
    if [[ -n "$out_dir" ]]; then
      cp "$tmp_out" "$node_file"
    fi
    if grep -q '^missing_log=' "$tmp_out"; then
      failures=$((failures + 1))
      record_summary "node=$host_ref ip=$host_ip status=missing_log matched_lines=0 output=${node_file:-stdout}"
    else
      record_summary "node=$host_ref ip=$host_ip status=ok matched_lines=$match_count output=${node_file:-stdout}"
    fi
  else
    rc=$?
    cat "$tmp_out" || true
    match_count=$(grep -Ev '^(log|missing_log)=' "$tmp_out" | wc -l | awk '{print $1}')
    if [[ -n "$out_dir" ]]; then
      cp "$tmp_out" "$node_file"
    fi
    failures=$((failures + 1))
    record_summary "node=$host_ref ip=$host_ip status=ssh_error rc=$rc matched_lines=$match_count output=${node_file:-stdout}"
  fi <<'REMOTE'
set -euo pipefail
run_root=$1
host_ref=$2
pattern=$3
lines=$4
log_path="$run_root/nodes/$host_ref/daemon.log"
if [[ ! -f "$log_path" ]]; then
  printf 'missing_log=%s\n' "$log_path"
  exit 0
fi
printf 'log=%s\n' "$log_path"
grep -E "$pattern" "$log_path" 2>/dev/null | tail -n "$lines" || true
REMOTE
  rm -f "$tmp_out"
  tmp_out=
done

if [[ -n "$summary_file" ]]; then
  printf 'completed_at_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >>"$summary_file"
  printf '\nsummary=%s\n' "$summary_file"
fi

if (( failures > 0 )); then
  echo "fsmeta_node_log_collection completed with $failures node failure(s)" >&2
  exit 1
fi

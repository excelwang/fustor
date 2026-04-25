#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "usage: $0 <api_base_url> <query_api_key> [path]" >&2
  exit 1
fi

API_BASE=${1%/}
QUERY_API_KEY=$2
QUERY_PATH=${3:-/}

RESP_FILE="$(mktemp)"
trap 'rm -f "$RESP_FILE"' EXIT

curl -sS "$API_BASE/api/fs-meta/v1/stats?path=$QUERY_PATH&read_class=materialized" \
  -H "authorization: Bearer $QUERY_API_KEY" \
  >"$RESP_FILE"

python3 - "$RESP_FILE" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as f:
    data = json.load(f)

groups = data.get("groups", {})
total = 0

print("demo_result_scope=stats_display_only")
print("demo_acceptance_note=this script never reports pass/fail; full acceptance requires /status artifact, full metadata coverage, and materialization readiness evidence")
print("group breakdown:")
for group_id in sorted(groups):
    env = groups[group_id]
    if env.get("status") != "ok":
        print(f"  {group_id}: status={env.get('status')} message={env.get('message', '')}")
        continue
    stats = env.get("data", {})
    files = int(stats.get("total_files", 0))
    dirs = int(stats.get("total_dirs", 0))
    nodes = int(stats.get("total_nodes", 0))
    total += files
    print(f"  {group_id}: total_files={files:,} total_dirs={dirs:,} total_nodes={nodes:,}")

print(f"aggregate_total_files={total:,}")
PY

#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ACCEPTANCE="$ROOT/scripts/source-connector-container-acceptance.sh"
TMP_DIR="$(mktemp -d /tmp/fustor-source-acceptance-resource-test.XXXXXX)"
trap 'rm -rf "$TMP_DIR"' EXIT

rg -q 'run_source "\$source" /usr/bin/env' "$ACCEPTANCE"
rg -q 'run_guarded_source source-to-data-view cargo test -p source-system-tests --test native_source_to_data_view_e2e' "$ACCEPTANCE"

SOURCE_ACCEPTANCE_PREFLIGHT_ONLY=1 \
SOURCE_ACCEPTANCE_ARTIFACT_DIR="$TMP_DIR/pass" \
  bash "$ACCEPTANCE" >/dev/null
rg -q 'SOURCE_ACCEPTANCE final_status=PASS exit_code=0' "$TMP_DIR/pass/acceptance.log"

if SOURCE_ACCEPTANCE_PREFLIGHT_ONLY=1 \
  SOURCE_ACCEPTANCE_ELASTICSEARCH_CPUS=8 \
  SOURCE_ACCEPTANCE_ARTIFACT_DIR="$TMP_DIR/fail" \
  bash "$ACCEPTANCE" >/dev/null 2>&1; then
  echo "source acceptance accepted an excessive container CPU override" >&2
  exit 1
fi

rg -q 'reason=invalid-container-cpu-limits' "$TMP_DIR/fail/acceptance.log"
rg -q 'SOURCE_ACCEPTANCE final_status=FAIL exit_code=1' "$TMP_DIR/fail/acceptance.log"

if SOURCE_ACCEPTANCE_PREFLIGHT_ONLY=1 \
  SOURCE_ACCEPTANCE_MAX_CONTAINER_CPUS=4 \
  SOURCE_ACCEPTANCE_ARTIFACT_DIR="$TMP_DIR/unsafe-max" \
  bash "$ACCEPTANCE" >/dev/null 2>&1; then
  echo "source acceptance accepted an unsafe aggregate CPU ceiling" >&2
  exit 1
fi

rg -q 'reason=unsafe-max-container-cpus' "$TMP_DIR/unsafe-max/acceptance.log"
rg -q 'SOURCE_ACCEPTANCE final_status=FAIL exit_code=1' "$TMP_DIR/unsafe-max/acceptance.log"
printf 'SOURCE_ACCEPTANCE_RESOURCE_LIMIT_TEST_PASS\n'

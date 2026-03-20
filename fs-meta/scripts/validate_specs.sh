#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOMAIN_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

mkdir -p "${TMP_DIR}/specs"
rsync -a "${DOMAIN_DIR}/specs/" "${TMP_DIR}/specs/"

VALIDATOR="${HOME}/.codex/skills/vibespec/scripts/validate.py"
PRECHECK="${DOMAIN_DIR}/scripts/specs_precheck.py"

# Canonical contract-coverage audit source for the single fs-meta formal specs
# tree. Merge all fs-meta contract-style tests so moved app/cli clauses remain
# traceable under one authority root.
mkdir -p "${TMP_DIR}/tests_merged"
rsync -a "${DOMAIN_DIR}/tests/specs/" "${TMP_DIR}/tests_merged/"
rsync -a "${DOMAIN_DIR}/tests/app_specs/" "${TMP_DIR}/tests_merged/app_specs/"
rsync -a "${DOMAIN_DIR}/tests/cli_specs/" "${TMP_DIR}/tests_merged/cli_specs/"

echo "== repo-local specs precheck =="
python3 "${PRECHECK}" "${DOMAIN_DIR}/specs"

echo "== validate specs against repo-level specs tests =="
python3 "${VALIDATOR}" "${TMP_DIR}/specs" --tests-dir "${TMP_DIR}/tests_merged"

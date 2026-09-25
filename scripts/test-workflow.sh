#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 || "$1" != "contracts" ]]; then
    echo "usage: ./scripts/test-workflow.sh contracts" >&2
    exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
RESOURCE_GUARD="${FUSTOR_RESOURCE_GUARD:-${SCRIPT_DIR}/resource-guard.sh}"

cd "${ROOT_DIR}"

"${RESOURCE_GUARD}" cargo test -q --test specs_fs_meta_domain -- --skip common::runtime_admin::tests::runtime_admin_request_roundtrip_preserves_worker_intent_payload_shape
"${RESOURCE_GUARD}" cargo test -q -p fs-meta --test app_specs
"${RESOURCE_GUARD}" cargo test -q -p fs-meta --test cli_specs

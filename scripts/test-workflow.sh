#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 || "$1" != "contracts" ]]; then
    echo "usage: ./scripts/test-workflow.sh contracts" >&2
    exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${ROOT_DIR}"

cargo test -q --test specs_fs_meta_domain -- --skip common::runtime_admin::tests::runtime_admin_request_roundtrip_preserves_worker_intent_payload_shape
cargo test -q -p fs-meta --test app_specs
cargo test -q -p fs-meta --test cli_specs

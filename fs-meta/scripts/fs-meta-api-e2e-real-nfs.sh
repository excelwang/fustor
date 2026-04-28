#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." >/dev/null 2>&1 && pwd)"
cd "$ROOT_DIR"

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "[fs-meta-api-e2e-real-nfs] skip: linux-only"
  exit 0
fi

REAL_NFS_FLAG="${CAPANIX_REAL_NFS_E2E:-${DATANIX_REAL_NFS_E2E:-0}}"
if [[ "$REAL_NFS_FLAG" != "1" ]]; then
  echo "[fs-meta-api-e2e-real-nfs] skip: CAPANIX_REAL_NFS_E2E!=1"
  exit 0
fi

if [[ ! -e /proc/fs/nfsd ]]; then
  echo "[fs-meta-api-e2e-real-nfs] skip: /proc/fs/nfsd is unavailable"
  exit 0
fi

if ! sudo -n true >/dev/null 2>&1; then
  echo "[fs-meta-api-e2e-real-nfs] skip: requires passwordless sudo"
  exit 0
fi

for bin in rpcbind rpc.nfsd rpc.mountd exportfs mount umount pgrep pkill; do
  if ! command -v "$bin" >/dev/null 2>&1; then
    echo "[fs-meta-api-e2e-real-nfs] skip: missing $bin"
    exit 0
  fi
done

SUITE="${FSMETA_REAL_NFS_SUITE:-progressive-operations}"
echo "[fs-meta-api-e2e-real-nfs] running matrix suite: $SUITE"
fs-meta/docs/examples/test-matrix-commands.sh "$SUITE"

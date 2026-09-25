#!/usr/bin/env bash
set -uo pipefail

reason="${1:-normal-exit}"
command_timeout_seconds="${DATANIX_TEST_PRIVILEGED_COMMAND_TIMEOUT_SECS:-30}"
nfsd_threads_file="${FSMETA_CONTAINER_STAGING_NFSD_THREADS_FILE:-/proc/fs/nfsd/threads}"
status=0

if [[ "${FSMETA_CONTAINER_STAGING_INSIDE:-0}" != "1" ]]; then
  echo "CONTAINER_STAGING_NFS_CLEANUP_REFUSED reason=outside-disposable-container" >&2
  exit 2
fi
if ! [[ "${command_timeout_seconds}" =~ ^[1-9][0-9]*$ ]]; then
  echo "CONTAINER_STAGING_NFS_CLEANUP_FAIL reason=invalid-timeout value=${command_timeout_seconds}" >&2
  exit 2
fi

case "${reason}" in
  normal-exit)
    stop_server=1
    ;;
  thread-limit|signal-*)
    stop_server=0
    ;;
  *)
    echo "CONTAINER_STAGING_NFS_CLEANUP_FAIL reason=unsupported-cleanup-phase value=${reason}" >&2
    exit 2
    ;;
esac

run_cleanup_command() {
  local label="$1"
  shift
  local command_status=0
  timeout --foreground --signal=TERM --kill-after=5s \
    "${command_timeout_seconds}s" "$@" >/dev/null 2>&1 || command_status=$?
  if [[ "${command_status}" -ne 0 ]]; then
    echo "CONTAINER_STAGING_NFS_CLEANUP_STEP_FAIL label=${label} status=${command_status}" >&2
    status=1
  fi
}

echo "CONTAINER_STAGING_NFS_CLEANUP_BEGIN reason=${reason}"

if command -v findmnt >/dev/null 2>&1; then
  mapfile -t nfs_mounts < <(
    findmnt -rn -t nfs,nfs4 -o TARGET 2>/dev/null \
      | awk 'length($0) > 0 { print length($0), $0 }' \
      | sort -rn \
      | cut -d' ' -f2-
  )
  for mount_path in "${nfs_mounts[@]}"; do
    run_cleanup_command "umount:${mount_path}" sudo -n umount -f -l -- "${mount_path}"
  done
fi

run_cleanup_command unexport-all sudo -n exportfs -au

# A watchdog hook runs while workload processes can still hold detached NFS
# references. Keep the local server alive until those processes have exited so
# NFSv4 delegation return can finish instead of entering uninterruptible sleep.
if [[ "${stop_server}" != "1" ]]; then
  if [[ "${status}" -eq 0 ]]; then
    echo "CONTAINER_STAGING_NFS_CLEANUP_DEFERRED_SERVER_STOP reason=${reason}"
  else
    echo "CONTAINER_STAGING_NFS_CLEANUP_FAIL reason=${reason}" >&2
  fi
  exit "${status}"
fi

if [[ -r "${nfsd_threads_file}" ]] && read -r nfsd_threads <"${nfsd_threads_file}" \
  && [[ "${nfsd_threads}" =~ ^[0-9]+$ ]] && ((nfsd_threads > 0)); then
  run_cleanup_command stop-nfsd sudo -n rpc.nfsd 0
fi
if pgrep -x rpc.mountd >/dev/null 2>&1; then
  run_cleanup_command stop-mountd sudo -n pkill -TERM -x rpc.mountd
fi
if mountpoint -q /proc/fs/nfsd 2>/dev/null; then
  run_cleanup_command unmount-nfsd sudo -n umount /proc/fs/nfsd
fi
if pgrep -x rpcbind >/dev/null 2>&1; then
  run_cleanup_command stop-rpcbind sudo -n pkill -TERM -x rpcbind
fi

if [[ "${status}" -eq 0 ]]; then
  echo "CONTAINER_STAGING_NFS_CLEANUP_PASS reason=${reason}"
else
  echo "CONTAINER_STAGING_NFS_CLEANUP_FAIL reason=${reason}" >&2
fi
exit "${status}"

#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
CLEANUP="${ROOT_DIR}/fs-meta/docs/examples/container-staging-nfs-cleanup.sh"
TMP_DIR="$(mktemp -d /tmp/fsmeta-staging-nfs-cleanup-test.XXXXXX)"
trap 'rm -rf "${TMP_DIR}"' EXIT

FAKE_BIN="${TMP_DIR}/bin"
COMMAND_LOG="${TMP_DIR}/commands.log"
NFSD_THREADS="${TMP_DIR}/nfsd-threads"
TEST_MOUNT="${TMP_DIR}/nfs-client"
mkdir -p "${FAKE_BIN}" "${TEST_MOUNT}"
printf '8\n' >"${NFSD_THREADS}"

write_stub() {
  local name="$1"
  shift
  {
    printf '#!/usr/bin/env bash\n'
    printf '%s\n' "$@"
  } >"${FAKE_BIN}/${name}"
  chmod +x "${FAKE_BIN}/${name}"
}

write_stub findmnt 'printf "%s\n" "${FSMETA_CLEANUP_TEST_MOUNT}"'
write_stub sudo 'printf "%s\n" "$*" >>"${FSMETA_CLEANUP_TEST_COMMAND_LOG}"'
write_stub pgrep 'exit 0'
write_stub mountpoint 'exit 0'

run_cleanup() {
  local reason="$1"
  PATH="${FAKE_BIN}:${PATH}" \
    FSMETA_CONTAINER_STAGING_INSIDE=1 \
    FSMETA_CONTAINER_STAGING_NFSD_THREADS_FILE="${NFSD_THREADS}" \
    FSMETA_CLEANUP_TEST_COMMAND_LOG="${COMMAND_LOG}" \
    FSMETA_CLEANUP_TEST_MOUNT="${TEST_MOUNT}" \
    DATANIX_TEST_PRIVILEGED_COMMAND_TIMEOUT_SECS=2 \
    "${CLEANUP}" "${reason}"
}

: >"${COMMAND_LOG}"
pre_terminate_output="$(run_cleanup thread-limit)"
grep -q 'CONTAINER_STAGING_NFS_CLEANUP_DEFERRED_SERVER_STOP reason=thread-limit' \
  <<<"${pre_terminate_output}"
grep -q -- "-n umount -f -l -- ${TEST_MOUNT}" "${COMMAND_LOG}"
grep -q -- '-n exportfs -au' "${COMMAND_LOG}"
if grep -Eq 'rpc\.nfsd|pkill|umount /proc/fs/nfsd' "${COMMAND_LOG}"; then
  echo "CONTAINER_STAGING_NFS_CLEANUP_TEST_FAIL reason=pre-terminate-stopped-server" >&2
  cat "${COMMAND_LOG}" >&2
  exit 1
fi

: >"${COMMAND_LOG}"
normal_output="$(run_cleanup normal-exit)"
grep -q 'CONTAINER_STAGING_NFS_CLEANUP_PASS reason=normal-exit' <<<"${normal_output}"
for expected in \
  '-n rpc.nfsd 0' \
  '-n pkill -TERM -x rpc.mountd' \
  '-n umount /proc/fs/nfsd' \
  '-n pkill -TERM -x rpcbind'
do
  grep -q -- "${expected}" "${COMMAND_LOG}"
done

: >"${COMMAND_LOG}"
unsupported_status=0
run_cleanup unsupported-phase >/dev/null 2>&1 || unsupported_status=$?
if [[ "${unsupported_status}" -ne 2 || -s "${COMMAND_LOG}" ]]; then
  echo "CONTAINER_STAGING_NFS_CLEANUP_TEST_FAIL reason=unsupported-phase-not-fail-closed status=${unsupported_status}" >&2
  exit 1
fi

echo "CONTAINER_STAGING_NFS_CLEANUP_TEST_PASS pre_terminate_server_stop=deferred normal_exit_server_stop=complete"

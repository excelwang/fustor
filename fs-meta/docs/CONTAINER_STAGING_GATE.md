# Fs-Meta Container Staging Gate

This runbook covers the disposable local fs-meta container staging gate. It is
peer-level with the official stable acceptance workflow, but it is not official
production acceptance.

## Safety Boundary

- Do not SSH to `198.51.100.144`, `198.51.100.145`, or `198.51.100.146`.
- Do not read or write `/home/operator/fsmeta-stable/run`.
- Do not deploy, restart, clear logs, apply roots, or rescan the official
  `fsmeta-stable` cluster from this workflow.
- Do not use host networking or mount production data paths into the container.
- Treat failures as local staging or source blockers, not official cluster
  evidence.

## Prerequisites

- Run from `/opt/fusiongraph/src/fustor` or another fustor checkout with a sibling
  Capanix checkout.
- Use rootful Docker or Podman. The harness needs a privileged container for
  `rpcbind`, `rpc.nfsd`, `rpc.mountd`, `exportfs`, `mount`, and `umount`.
- The host kernel must expose `/proc/fs/nfsd` to the privileged container.
- If `DATANIX_E2E_TMP_ROOT` is set, it must be an absolute host path. If unset,
  the gate creates a per-run path below `/tmp/fsmeta-container-staging/`.
- Capanix QUIC requires effective 16 MiB UDP receive/send socket buffers. The
  outer gate temporarily raises low local `net.core.rmem_max` and
  `net.core.wmem_max` maxima before creating each disposable container and
  restores the exact prior values on every exit path. The real-NFS helper also
  requires one `rpc.mountd` inotify instance; the gate temporarily raises a low
  `fs.inotify.max_user_instances` ceiling to 256 and restores its exact prior
  value. These changes raise only allocation ceilings and do not preallocate
  socket memory or inotify instances. Set
  `FSMETA_CONTAINER_STAGING_TUNE_HOST_SYSCTLS=0` to fail read-only when the host
  limits are insufficient.
- The disposable container has a hard 2-CPU quota. Upward overrides are
  rejected. Every inner Cargo build and acceptance stage additionally runs
  through the repository's two-CPU affinity guard and 512-thread process-tree
  watchdog. The lightweight profile uses one Source scan worker, two host-FS
  operation workers, one default Tokio/Rayon worker per runtime, and a
  four-thread Capanix worker blocking pool and four-thread process-level
  Capanix bound-route dispatcher. The two Capanix overrides lower app-neutral
  defaults of eight and are rejected if raised above four in this gate; these
  bounds do not alter production defaults.
- Real-NFS privileged helper commands have a 30-second default timeout. The
  disposable mounts use finite mount retry and soft I/O timeout semantics so a
  failed local NFS server cannot hold the workstation indefinitely.
- NFS cleanup is container-only, bounded, and two-phase. Before watchdog
  termination it detaches client mounts and unexports roots but deliberately
  keeps `rpc.nfsd`, `rpc.mountd`, and `rpcbind` alive while workload processes
  release NFSv4 delegations. The normal-exit finalizer repeats those idempotent
  steps after workload termination, then stops `rpc.nfsd`, stops `rpc.mountd`,
  unmounts `/proc/fs/nfsd`, and stops `rpcbind`. The helper refuses to run
  without the disposable-container marker and rejects unknown cleanup phases.
- Before image build or host sysctl tuning, `run` requires an empty staging
  slot. Any existing container carrying the staging label causes
  `CONTAINER_STAGING_PREFLIGHT_BLOCKED`; if its process tree contains a D-state
  task, the gate prints bounded process diagnostics and refuses to create a
  second privileged NFS environment.

## Standard Commands

Build the image:

```bash
fs-meta/scripts/fs-meta-container-staging-gate.sh build-image
```

Run the default lightweight production-path staging gate:

```bash
fs-meta/scripts/fs-meta-container-staging-gate.sh run
```

Run active-three external-worker replay:

```bash
fs-meta/scripts/fs-meta-container-staging-gate.sh run active-three
```

Run active-three plus blocking `business-mini-nfs` smoke:

```bash
fs-meta/scripts/fs-meta-container-staging-gate.sh run all
```

Run only the quick mini smoke:

```bash
fs-meta/scripts/fs-meta-container-staging-gate.sh run mini
```

Clean up old disposable containers:

```bash
fs-meta/scripts/fs-meta-container-staging-gate.sh cleanup-stale
```

Verify resource limits without starting a container:

```bash
fs-meta/scripts/fs-meta-container-staging-gate.sh self-test-resource-limits
```

## Gate Semantics

- `active-three` proves local container staging coverage for release fs-meta
  artifacts, matching sibling Capanix binaries, temporary real NFS exports,
  active-three source/sink/scan ownership, external-worker replay, and
  materialization evidence.
- `all` runs blocking `active-three` and then blocking `mini` in separate
  disposable containers.
- `FSMETA_CONTAINER_STAGING_REQUIRE_MINI=0` is diagnostic-only. It allows
  `mini` to report `CONTAINER_STAGING_GATE_OPTIONAL_FAIL suite=mini` without
  failing the final `all` sentinel.
- Default `all` mode treats mini failure as blocking and reports
  `CONTAINER_STAGING_GATE_BLOCKING_FAIL suite=mini`.
- Each inner build is bounded by
  `FSMETA_CONTAINER_BUILD_TIMEOUT_SECONDS=1800`, each acceptance stage by
  `FSMETA_CONTAINER_STAGE_TIMEOUT_SECONDS=1800`, and each disposable container
  by `FSMETA_CONTAINER_RUN_TIMEOUT_SECONDS=2700`. Timeout exits are blocking and
  emit `CONTAINER_STAGING_BOUNDED_TIMEOUT` or
  `CONTAINER_STAGING_RUN_TIMEOUT` before the failing gate sentinel.

## Required Evidence

Capture the terminal sentinel and log path:

- `CONTAINER_STAGING_GATE_PASS suite=<suite> log=<path>`
- `CONTAINER_STAGING_GATE_FAIL suite=<suite> status=<status> log=<path>`
- `CONTAINER_STAGING_RESOURCE_SELF_TEST_PASS visible_cpus=<n> hard_cpu_max=2 max_threads=512 worker_max_blocking_threads=4 bound_route_dispatcher_threads=4 timeout_status=124`
- `CONTAINER_STAGING_NFS_CLEANUP_TEST_PASS pre_terminate_server_stop=deferred normal_exit_server_stop=complete`
- `CONTAINER_STAGING_NFS_CLEANUP_DEFERRED_SERVER_STOP reason=<watchdog-reason>`
- `CONTAINER_STAGING_BOUNDED_TIMEOUT label=<stage> ...`
- `CONTAINER_STAGING_RUN_TIMEOUT suite=<suite> ...`
- `CONTAINER_STAGING_PREFLIGHT_PASS existing_containers=0`
- `CONTAINER_STAGING_PREFLIGHT_BLOCKED ...`

For cleanup:

- `CONTAINER_STAGING_CLEANUP_PASS ...`
- `CONTAINER_STAGING_CLEANUP_REMOVED ...`
- `CONTAINER_STAGING_CLEANUP_BLOCKED ...`

## Cleanup Blockers

`CONTAINER_STAGING_CLEANUP_BLOCKED` means a container runtime or kernel process
still holds the container. A process stuck in kernel D-state may not be
removable by the cleanup command.

`cleanup-stale` inspects the complete host-side container process tree first.
When any descendant is in D-state it reports `removal_attempted=0` and does not
call `docker rm -f`/`podman rm -f`; a later `run` remains fail-closed until the
stale container has genuinely disappeared.

Do not restart Docker or the host during ordinary validation. Record the
blocked sentinel, keep official stable dormant, and schedule a Docker or host
maintenance window only if the process does not unwind on its own.

## Non-Coverage

This local gate does not prove:

- official `198.51.100.144-146` health;
- official three-host network behavior;
- CentOS 7 or `GLIBC_2.17` ABI compatibility;
- high-cardinality production acceptance where status-returned live-node count
  exceeds `200000000`.

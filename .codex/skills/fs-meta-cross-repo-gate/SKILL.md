---
name: fs-meta-cross-repo-gate
description: Coordinate fs-meta cross-repo blocker work between fustor and capanix with shared blocker-state tracking, direct bounded iterations, production-parity aibox predeploy validation, and safe clean-log binary build/deploy gates for fs-meta clusters. Use when the user says "fs-meta gate", "fustor cross-repo gate", "fustor blocker gate", asks how to build or deploy capanix binaries for fs-meta, wants logs cleared before redeploy validation, wants the current blocker handled through a shared blocker-state document instead of ad hoc coordination, or needs the official fs-meta deploy gate workflow.
---

# Fs-meta Cross-Repo Gate

Use this skill for `fs-meta` cross-repo blocker work that spans `fustor` and `capanix`.

Do not depend on a separate coordination helper skill. Keep the coordination rules here and work the blocker directly in the current session.

## Trigger

One-click activation phrases:

- `fs-meta gate`
- `fustor cross-repo gate`
- `fustor blocker gate`
- `capanix binary deploy`
- `fs-meta binary deploy`
- `clear logs before redeploy`

## Shared State

- Use one blocker-state document shared with the upstream repo.
- In the current local environment, the default shared blocker-state document is `/root/repo/capanix/todo.md`.
- Re-read it before every new bounded iteration.
- The side named by the current first raw failing boundary is the active owner.
- While the blocker remains on `fustor`, the active owner is the only side allowed to update `/root/repo/capanix/todo.md`.
- Treat the blocker-state document as a rolling state document, not an append-only history log.
- Keep only the active blocker, current first raw boundary, current exact seam, latest evidence that still changes the plan, and remaining validation order.
- Compress closed seams to short "closed / do not revisit without fresh raw evidence" bullets.
- Remove stale artifact lists, superseded reruns, and completed sub-iterations once they no longer affect the current blocker.

## Iteration Model

- Keep exactly one active owner at a time.
- Treat the inactive side as dormant.
- Re-read `/root/repo/capanix/todo.md` before every new bounded iteration.
- Run one bounded iteration directly in the current session for the active owner.
- Use long blocking waits instead of polling when waiting for external validation or cluster state.
- Process fresh evidence before updating the blocker-state document or summarizing status.

## Quick Start

1. Read `/root/repo/capanix/todo.md`.
2. Read the governing `fs-meta` specs for the current blocker line.
3. Identify the active owner from the current first raw failing boundary.
4. Run one bounded iteration for that owner directly in the current session.
5. Re-read `/root/repo/capanix/todo.md` before starting the next bounded iteration.
6. Process fresh evidence before giving any status summary.

## Cluster Access

- For the `wanghuajin@10.0.82.144~148` fs-meta cluster, the management shadow secret is on `10.0.82.145` at `/home/wanghuajin/fsmeta-stable/run/.fsmeta-state/fs-meta.shadow`.
- Read that file over SSH only when API authentication is required for diagnostics or deployment validation.
- Never write the secret value into repository files, logs, artifacts, shell history, or user-facing responses; report only safe authentication success/failure evidence.

## Management API Usage

- For `fsmeta roots apply`, pass only the facade origin as `--api-base`, for example `http://10.0.82.145:18080`.
- Do not include `/api/fs-meta/v1` in `--api-base`; the CLI appends `/api/fs-meta/v1/session/login` and other API paths internally.
- If `roots apply` fails with `Error: login failed:` after using `--api-base .../api/fs-meta/v1`, treat that as an operator invocation error, not a runtime blocker.
- Manual HTTP clients use the full product API path, for example `POST http://10.0.82.145:18080/api/fs-meta/v1/session/login` and `GET http://10.0.82.145:18080/api/fs-meta/v1/status`.
- Prefer `--password-file /home/wanghuajin/fsmeta-stable/run/.fsmeta-state/fs-meta.shadow` for CLI authentication on the real cluster. Never print the password or bearer token; store only redacted HTTP status/evidence.
- See `references/binary-deploy.md` for the full active-three `roots apply` command pattern.

## Fustor Iteration Rules

When `fustor` owns the blocker:

- Work only on `/root/repo/fustor`.
- Respect existing uncommitted changes; do not revert them.
- Use red-test-first discipline at the owning layer before any fix.
- Prefer the narrowest preserved reproducer named by the blocker-state document before broader validation.
- Update `/root/repo/capanix/todo.md` only when new first-boundary evidence, repo-local closure, or execution order changes are proven.
- When updating `/root/repo/capanix/todo.md`, rewrite the affected blocker section into its new minimal state instead of appending another long historical tranche.
- Do not broaden to high-NFS, compatible-builder, or deploy work until the current local boundary is closed.

## Coordination Rules

- Keep one active owner and one dormant side.
- When the blocker remains on `fustor`, keep `capanix` dormant and work the `fustor` seam directly.
- When the blocker crosses back to `capanix`, keep `fustor` dormant and work the `capanix` seam directly.
- Never let a dormant side edit the blocker-state document or speculate about ownership.
- Do not require a separate handoff protocol to progress the blocker.

## Capanix Binary Deploy Gate

Use this gate before changing `capanixd` on an existing `fs-meta` cluster.

- Treat remote cluster ABI as the deployment target. The `wanghuajin@10.0.82.144~148` cluster is CentOS 7 / glibc 2.17; do not deploy an aibox-built `capanixd` unless its GLIBC requirements are proven compatible.
- Before any `panda14x` build/deploy attempt, run the full aibox predeploy gate from `references/binary-deploy.md` as a hard blocker. It must use complete source snapshots unpacked into a fresh builder-like long path, preserve sibling repo layout, delete stale target artifacts, build release/runtime artifacts, point tests at an explicit external `capanix_worker_host`, exercise external-worker IPC, and replay the closest practical production-equivalent active-three config/API/resource/app/root flow under clean logs. Match the target topology and operational sequence enough to expose statecell, sidecar bind, worker lifecycle, route apply, status fan-in, timeout, and stale-binary failures before touching `panda14x`.
- Do not treat a narrow unit test, direct checkout build, or simplified smoke as permission to build/deploy on `panda14x`. If aibox cannot exactly reproduce a target detail, record the gap and compensate with the nearest stricter local check; unexplained parity gaps block remote build/deploy.
- A passing aibox gate only permits the next `panda145` compatible-builder stage. It does not prove CentOS 7 ABI compatibility and does not allow deploying an aibox-built ELF unless GLIBC compatibility is separately proven.
- Build on a target-compatible host, such as `panda145`, or in a toolchain/container that targets the remote glibc. For CentOS 7, every deployed ELF must require `GLIBC_2.17` or lower.
- When building on `panda145`, update the builder source by uploading a complete local source snapshot archive from the current repo; do not update the builder by applying ad hoc patch files to an old source tree.
- Before publishing, run the narrow tests for the changed layer on the compatible builder, then verify the exact artifact with `sha256sum`, `ldd`, and `objdump -T`.
- Stage artifacts on every node first. Never overwrite a live file until the staged file hash matches across all target nodes.
- Replace by stopping affected daemons/processes, deleting the old live `bin/<artifact>` first, moving the staged artifact into place, then restarting. Do not create `*.prev-*` backups unless the user explicitly asks for backup retention.
- Clean-log redeploy is mandatory: after stopping affected processes and before restarting daemons or re-applying app declarations, archive or truncate all live `*.log`, `*.out`, and `*.err` files under `nodes/`. If you cannot prove logs were cleared for the current redeploy window, clear them and restart validation.
- After restart, verify binary hash, process health, `cluster status`, `peer_count`, duplicate peers, and app/process registration. If startup fails because of linker errors or missing GLIBC symbols, stop immediately and rebuild or reinstall a known-good staged artifact; do not rely on `.prev-*` rollback files.
- Do not modify the `fs-meta` app to compensate for a `capanixd` deploy issue. Restore apps only by re-applying existing declarations or deployed operational scripts after the daemon layer is healthy.
- For exact artifact names, build commands, staging commands, replacement commands, and source/app restore commands, load `references/binary-deploy.md`.

## References

Load exactly what you need:

- `references/workflow.md` for blocker-state usage, validation order, governing spec families, and acceptance discipline
- `references/binary-deploy.md` when building, staging, deploying, replacing, or validating `capanixd`, `cnxctl`, `capanix_worker_host`, source runtime `.so` files, union-graph runtime `.so`, `fsmeta`, or `libfs_meta_runtime.so`

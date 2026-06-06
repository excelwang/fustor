---
name: fs-meta-cross-repo-gate
description: Coordinate fs-meta cross-repo blocker work between fustor and capanix with a coordinator-plus-one-worker baton loop, including safe clean-log binary build/deploy gates for fs-meta clusters. Use when the user says "fs-meta gate", "fustor cross-repo gate", "fustor blocker gate", asks how to build or deploy capanix binaries for fs-meta, wants logs cleared before redeploy validation, wants one-click activation of the capanix-fustor repair workflow, or wants the current blocker handled through a shared blocker-state document instead of ad hoc coordination.
---

# Fs-meta Cross-Repo Gate

Use this skill for `fs-meta` cross-repo blocker work that spans `fustor` and `capanix`.

Treat the installed `subagent-baton` skill as the coordination authority.
This skill is the repo-specific adapter for the `fustor` side workflow.

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
- While the blocker remains on `fustor`, the worker is the only side allowed to update `/root/repo/capanix/todo.md`.
- Treat the blocker-state document as a rolling state document, not an append-only history log.
- Keep only the active blocker, current first raw boundary, current exact seam, latest evidence that still changes the plan, and remaining validation order.
- Compress closed seams to short "closed / do not revisit without fresh raw evidence" bullets.
- Remove stale artifact lists, superseded reruns, and completed sub-iterations once they no longer affect the current blocker.

## Quick Start

1. Read `/root/repo/capanix/todo.md`.
2. Read the governing `fs-meta` specs for the current blocker line.
3. If the blocker is localized to `fustor`, activate exactly one worker for one bounded iteration.
4. If the blocker is localized back to `capanix`, keep the worker dormant and continue locally as coordinator.
5. Process worker control markers before giving any status summary.

## Cluster Access

- For the `wanghuajin@10.0.82.144~148` fs-meta cluster, the management shadow secret is on `10.0.82.145` at `/home/wanghuajin/fsmeta-stable/run/.fsmeta-state/fs-meta.shadow`.
- Read that file over SSH only when API authentication is required for diagnostics or deployment validation.
- Never write the secret value into repository files, logs, artifacts, shell history, or user-facing responses; report only safe authentication success/failure evidence.

## Fustor Iteration Rules

When `fustor` owns the blocker:

- Work only on `/root/repo/fustor`.
- Respect existing uncommitted changes; do not revert them.
- Use red-test-first discipline at the owning layer before any fix.
- Prefer the narrowest preserved reproducer named by the blocker-state document before broader validation.
- Update `/root/repo/capanix/todo.md` only when new first-boundary evidence, repo-local closure, or execution order changes are proven.
- When updating `/root/repo/capanix/todo.md`, rewrite the affected blocker section into its new minimal state instead of appending another long historical tranche.

Required worker end markers:

- `ITERATION_DONE_WORKER_CONTINUE`
- `BATON_READY_FOR_COORDINATOR`
- `BLOCKED`

After the marker, require:

- concise evidence
- commands and tests run
- whether `/root/repo/capanix/todo.md` changed
- whether the first raw boundary stayed on `fustor` or crossed back to `capanix`
- which spec files or sections were used as authority

## Coordinator Rules

When the worker is active:

- stay in orchestration mode
- use long blocking waits instead of frequent polling
- if the worker returns `ITERATION_DONE_WORKER_CONTINUE`, immediately dispatch the next bounded iteration
- if the worker returns `BATON_READY_FOR_COORDINATOR`, verify `/root/repo/capanix/todo.md`, park the worker, and continue locally
- if the worker returns `BLOCKED`, inspect the precise first blocking boundary before changing the plan

When the coordinator is active:

- keep the worker dormant
- handle the `capanix` side directly
- only wake the worker again after `/root/repo/capanix/todo.md` clearly localizes the first raw failing boundary back to `fustor`

## Capanix Binary Deploy Gate

Use this gate before changing `capanixd` on an existing `fs-meta` cluster.

- Treat remote cluster ABI as the deployment target. The `wanghuajin@10.0.82.144~148` cluster is CentOS 7 / glibc 2.17; do not deploy an aibox-built `capanixd` unless its GLIBC requirements are proven compatible.
- Build on a target-compatible host, such as `panda145`, or in a toolchain/container that targets the remote glibc. For CentOS 7, every deployed ELF must require `GLIBC_2.17` or lower.
- Before publishing, run the narrow tests for the changed layer on the compatible builder, then verify the exact artifact with `sha256sum`, `ldd`, and `objdump -T`.
- Stage artifacts on every node first. Never overwrite a live file until the staged file hash matches across all target nodes.
- Replace with an explicit rollback point: stop affected daemons/processes, copy the live file to `*.prev-<timestamp>-<hash-prefix>`, move the staged file into place, then restart.
- Clean-log redeploy is mandatory: after stopping affected processes and before restarting daemons or re-applying app declarations, archive or truncate all live `*.log`, `*.out`, and `*.err` files under `nodes/`. If you cannot prove logs were cleared for the current redeploy window, clear them and restart validation.
- After restart, verify binary hash, process health, `cluster status`, `peer_count`, duplicate peers, and app/process registration. If startup fails because of linker errors or missing GLIBC symbols, restore the `.prev-*` binary immediately.
- Do not modify the `fs-meta` app to compensate for a `capanixd` deploy issue. Restore apps only by re-applying existing declarations or deployed operational scripts after the daemon layer is healthy.
- For exact artifact names, build commands, staging commands, rollback commands, and source/app restore commands, load `references/binary-deploy.md`.

## References

Load exactly what you need:

- `references/workflow.md` for blocker-state usage, validation order, governing spec families, and acceptance discipline
- `references/binary-deploy.md` when building, staging, deploying, rolling back, or validating `capanixd`, `cnxctl`, `capanix_worker_host`, source runtime `.so` files, union-graph runtime `.so`, `fsmeta`, or `libfs_meta_runtime.so`

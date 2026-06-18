---
name: fs-meta-cross-repo-gate
description: Coordinate fs-meta cross-repo blocker work between fustor and capanix with the single fustor goal trace, direct bounded iterations, production-parity aibox predeploy validation, exec_command plus 300s empty write_stdin long waits, bounded official per-node log evidence, and safe clean-log binary build/deploy gates for fs-meta clusters. Use when the user says "fs-meta gate", "fustor cross-repo gate", "fustor blocker gate", asks how to build, start, restart, or deploy capanix binaries for fs-meta, wants logs cleared before redeploy validation, wants official node logs collected, wants the current blocker handled through the canonical goal trace instead of ad hoc coordination, asks how to wait for fs-meta validation/deploy convergence, asks for the correct official startup method, or needs the official fs-meta deploy gate workflow.
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
- `collect official node logs`
- `correct official startup method`
- `fsmeta-stable restart`

## Goal Trace

- Use exactly one goal-trace document for this work.
- In the current local environment, the only goal trace is `/root/repo/fustor/TODO.md`.
- Re-read it before every new bounded iteration.
- The side named by the current first raw failing boundary is the active owner.
- Do not create, update, or rely on `/root/repo/capanix/todo.md`.
- Treat the goal trace as a rolling state document, not an append-only history log.
- Treat `Final Target` and `Final Acceptance` as invariant acceptance criteria, not iteration history. Preserve them unless the user explicitly changes the goal.
- In particular, never remove or weaken the final authenticated `/api/fs-meta/v1/status` requirement that the status-returned live-node index entry count is stable/increasing and exceeds `200,000,000` (`200000000`), using an explicit global alias if present or aggregate `sink.live_nodes` otherwise.
- Before compressing `TODO.md`, classify each bullet as invariant acceptance, current blocker/evidence, or superseded evidence; only prune superseded evidence.
- Keep only the active blocker, current first raw boundary, current exact seam, latest evidence that still changes the plan, and remaining validation order.
- Compress closed seams to short "closed / do not revisit without fresh raw evidence" bullets.
- Remove stale artifact lists, superseded reruns, and completed sub-iterations once they no longer affect the current blocker.

## Iteration Model

- Keep exactly one active owner at a time.
- Treat the inactive side as dormant.
- Re-read `/root/repo/fustor/TODO.md` before every new bounded iteration.
- Run one bounded iteration directly in the current session for the active owner.
- Implement long waits with the execution session and `write_stdin`, not with clock probes, log tails, process checks, or status-only replies.
- Treat `exec_command` plus empty `write_stdin` as the only allowed long-wait implementation. Plans, `TODO.md`, and user-facing status must name that path directly and only; do not propose, compare against, name, or document other wait mechanisms.
- Do not introduce any out-of-band wait facility, scheduler, CLI wrapper, helper process, or replacement tool for fs-meta gate waits; if the normal CLI path is unavailable, keep using the active `exec_command` session and empty `write_stdin`.
- Launch long-running build/test/deploy/convergence commands as one foreground `exec_command` that writes its own log and prints one terminal sentinel line, such as `AIBOX_GATE_PASS ...` or `AIBOX_GATE_FAIL ...`, when it exits. Set `yield_time_ms` as high as the exec tool allows.
- If `exec_command` yields a session id before the sentinel appears, record a wait contract in `TODO.md` or the response: reason, command shape, session id, log path, expected terminal sentinel, and a `write_stdin` wait budget. Use count-based wait budget, not wall-clock probing.
- Continue that command only with empty `write_stdin` calls on the same session. For long waits, `yield_time_ms=300000` (300s) is mandatory on every empty `write_stdin` call. If `300000` is rejected by the tool, stop and classify it as a tooling wait-boundary; do not silently lower the value, use 30s chunks, or send characters unless the running program explicitly requires input.
- Treat each 300s empty `write_stdin` call as the wait operation itself. A shorter empty wait is a protocol violation unless the previous call already returned the terminal sentinel or a clear first raw failure. It is valid to issue the next 300s empty `write_stdin` when the previous one returns without a terminal sentinel and the command is still running. Do not insert `date`, `ps`, `pgrep`, SSH checks, log tails, file probes, or status summaries between wait calls.
- Do not interpret intermediate output as progress unless it contains the planned terminal sentinel or a clear first raw failure. If intermediate output is noisy but non-terminal, keep waiting with the next max-duration `write_stdin` within the recorded budget.
- For cluster convergence, put the wait inside the launched local command before the single remote sample, then use `write_stdin` to wait for the command result. Example shape: `sleep 900; ssh ...one bounded sample...`. The `sleep` must appear before `ssh`; do not put long `sleep` inside an SSH heredoc or remote script.
- Forbidden wait shapes: `ssh host 'sleep 900; ...'`, `ssh host 'bash -s' <<'REMOTE'` with a long remote `sleep`, `while sleep 30; do ssh ...; done`, repeated local `date`, repeated `ps`/`pgrep`, repeated log tails, repeated SSH status samples, status-only replies, or ordinary goal continuations that only restate the wait contract.
- If a wait spans turns or context resumes, continue with the next planned 300s empty `write_stdin` on the recorded session. Do not use `date`, logs, process lists, SSH, or file probes to reconstruct state.
- If `write_stdin` returns the terminal sentinel, classify the result once and then inspect only the evidence paths named by the sentinel or wait contract. If it returns a clear first raw failure, classify that boundary and stop the wait.
- If the recorded `write_stdin` budget is exhausted without a terminal sentinel or clear failure, classify this as a tooling/runtime wait-boundary and decide the next bounded action from that evidence. Do not switch to short-interval polling.
- If the user asks for status while a wait is active, either continue the planned `write_stdin` wait or answer only from the recorded wait contract. Do not run side probes.
- If the user supersedes the task while a wait is active, leave the session untouched unless the user explicitly asks to cancel it or cancellation is needed to prevent harm. If cancellation is required, cancel once by known session/process handle without prior status probing, then proceed with the new request.
- Process fresh evidence before updating the goal trace or summarizing status.

## Quick Start

1. Read `/root/repo/fustor/TODO.md`.
2. Read the governing `fs-meta` specs for the current blocker line.
3. Identify the active owner from the current first raw failing boundary.
4. Run one bounded iteration for that owner directly in the current session.
5. Re-read `/root/repo/fustor/TODO.md` before starting the next bounded iteration.
6. Process fresh evidence before giving any status summary.

## Cluster Access

- For the `wanghuajin@10.0.82.144-146` fs-meta cluster, the management shadow secret is on `10.0.82.145` at `/home/wanghuajin/fsmeta-stable/run/.fsmeta-state/fs-meta.shadow`.
- Read that file over SSH only when API authentication is required for diagnostics or deployment validation.
- Never write the secret value into repository files, logs, artifacts, shell history, or user-facing responses; report only safe authentication success/failure evidence.

## Official Stable Startup

- For official `fsmeta-stable` daemon start/restart, use the node-owned scripts under `/home/wanghuajin/fsmeta-stable/run/nodes/<host_ref>/`.
- Do not hand-write `nohup bin/capanixd -c nodes/<host_ref>/config.yaml -b <ip>:19401`; that omits the node environment loaded by `start.sh`.
- Load `references/binary-deploy.md` for the exact stop/start commands, process verification, management account format, and CLI authentication pattern.
- For official per-node daemon-log evidence, call the bundled read-only helper instead of hand-writing repeated SSH/grep snippets:

```bash
.codex/skills/fs-meta-cross-repo-gate/scripts/collect_official_node_logs.sh \
  --focus status-fanin \
  --pattern 'nfs-146|source_coverage_gaps|scan_coverage_gaps|missing_source_owner' \
  --lines 260 \
  --out-dir /tmp/fsmeta-node-logs-$(date +%Y%m%d%H%M%S)
```

- Use the helper only as a bounded evidence collection after a wait/deploy/sample boundary. It performs one SSH command per node, reads daemon logs only, validates complex regexes before SSH, and writes `collection-plan.txt`, `collection-summary.txt`, and persisted per-node excerpts when `--out-dir` is set. If one node fails, it still collects the remaining nodes and exits nonzero at the end. Do not wrap it in loops, replace it with hand-written SSH/grep snippets, or use it to poll during a long wait.

## Management API Usage

- The stable-run management shadow account is a three-field enabled account line:

```text
admin:plain$<password>:0
```

- The `plain$` prefix is part of the password encoding, and the trailing `:0` keeps the account enabled.
- Store the line at `/home/wanghuajin/fsmeta-stable/run/.fsmeta-state/fs-meta.shadow` with the real password in place of `<password>`.
- Do not write `admin:<password>`, omit `plain$`, omit the third field, or print the password/token in repository files, logs, TODO, or user-facing output.
- For `fsmeta roots apply`, pass only the facade origin as `--api-base`, for example `http://10.0.82.145:18080`.
- Do not include `/api/fs-meta/v1` in `--api-base`; the CLI appends `/api/fs-meta/v1/session/login` and other API paths internally.
- If `roots apply` fails with `Error: login failed:` after using `--api-base .../api/fs-meta/v1`, treat that as an operator invocation error, not a runtime blocker.
- Manual HTTP clients use the full product API path, for example `POST http://10.0.82.145:18080/api/fs-meta/v1/session/login` and `GET http://10.0.82.145:18080/api/fs-meta/v1/status`.
- Prefer `--password-file /home/wanghuajin/fsmeta-stable/run/.fsmeta-state/fs-meta.shadow` for CLI authentication on the real cluster. Never print the password or bearer token; store only redacted HTTP status/evidence.
- See `references/binary-deploy.md` for the full active-three `roots apply` command pattern.

## Manual Rescan Safety

- Treat `POST /api/fs-meta/v1/index/rescan` (`/index/rescan`) as a mutating repair API, not a diagnostic, wait accelerator, or routine deploy-validation step.
- On official high-cardinality NFS roots with `subpath_scope="/"`, a manual rescan can enqueue or prove repair work across the full current root set and consume source, scan, sink, and background repair budgets. It can add substantial load while normal materialization catch-up is already running.
- `pending-materialization`, `selected-pending`, `initial_audit_completed=false`, `trusted_observation_readiness=false`, or an absent final index count in an otherwise healthy topology are not by themselves a raw failure and must not trigger `/index/rescan`.
- Prefer patient read-only convergence windows: one local long wait followed by one authenticated `/status` sample, then record and report the status-returned live-node index entry count. If `/status` does not expose an explicitly named global field, treat aggregate `sink.live_nodes` as the current status-returned live-node index count and `sink.groups[].live_nodes` as the per-root breakdown; report absence only when neither an explicit global field nor `sink.live_nodes` is present.
- Call `/index/rescan` only when the user explicitly asks for that repair or when fresh first-boundary evidence proves stale or incorrect accepted-scope/source/scan repair state and the goal trace names rescan as the next authorized repair. Record the reason, target, expected bounded follow-up sample, and index-count reporting plan before calling it.

## Fustor Iteration Rules

When `fustor` owns the blocker:

- Work only on `/root/repo/fustor`.
- Respect existing uncommitted changes; do not revert them.
- Use red-test-first discipline at the owning layer before any fix.
- Prefer the narrowest preserved reproducer named by `/root/repo/fustor/TODO.md` before broader validation.
- Update `/root/repo/fustor/TODO.md` only when new first-boundary evidence, repo-local closure, or execution order changes are proven.
- When updating `/root/repo/fustor/TODO.md`, rewrite the affected blocker section into its new minimal state instead of appending another long historical tranche.
- Do not broaden to high-NFS, compatible-builder, or deploy work until the current local boundary is closed.

## Breadth-First Static Review Before Heavy Gates

When a heavy gate such as active-three real-NFS e2e, compatible-builder, or official deploy repeatedly exposes adjacent defects, do not continue the narrow "one failing line, one heavy rerun" loop by default. First run a bounded breadth-first static review around the failing subsystem, batch related fixes, then run the heavy gate once after local closure.

Use this method when the failure is in or near manual rescan, source-status fan-in, roots-control replay, source/sink route readiness, facade/API liveness, worker lifecycle, background repair lanes, or retryable management errors.

1. Re-read `TODO.md` and name the failing subsystem, not only the last log line.
2. Map the one-hop neighborhood of that subsystem: shared control plane, request budget, timeout constants, retry classifier, route collect helper, cache lifecycle, accepted-scope proof path, background repair lane, and client/e2e retry wrapper.
3. Search for the same defect class across that neighborhood before editing. Typical classes include unbounded waits inside management requests, cache entries without failure cleanup, retryable errors missing from client retry classifiers, concurrent route probes that can cause `TxBusy`, status/route loops that do not early-release on retryable evidence, stale owner evidence accepted as fresh proof, and off-facade helpers that still block shared API liveness.
4. Batch only narrow, related fixes that share the current first-boundary subsystem. Do not mix unrelated refactors, unrelated product behavior changes, or speculative cleanup into the batch.
5. Add or update focused tests for every defect class closed in the batch. Include liveness assertions when the bug can starve `/session/login`, `/status`, request tracker drain, manual-rescan completion, or background repair progress.
6. Run the focused local validation for the batch first. Only after local closure should you run the next fresh aibox, compatible-builder, or official deploy gate.
7. Record in `TODO.md` that the iteration used breadth-first static review, list the adjacent defect classes closed, keep stale gate history compressed, and keep the next heavy validation order explicit.

## Coordination Rules

- Keep one active owner and one dormant side.
- When the blocker remains on `fustor`, keep `capanix` dormant and work the `fustor` seam directly.
- When the blocker crosses back to `capanix`, keep `fustor` dormant and work the `capanix` seam directly.
- Never let a dormant side edit `/root/repo/fustor/TODO.md` or speculate about ownership.
- Do not require a separate handoff protocol to progress the blocker.

## Capanix Binary Deploy Gate

Use this gate before changing `capanixd` on an existing `fs-meta` cluster.

- Treat remote cluster ABI as the deployment target. The `wanghuajin@10.0.82.144-146` cluster is CentOS 7 / glibc 2.17; do not deploy an aibox-built `capanixd` unless its GLIBC requirements are proven compatible.
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
- `scripts/collect_official_node_logs.sh` for one-shot bounded daemon-log evidence from `panda144`, `panda145`, and `panda146`; use `--dry-run` to validate focus and regex without SSH

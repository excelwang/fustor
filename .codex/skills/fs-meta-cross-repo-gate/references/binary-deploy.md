# Fs-meta Binary Build and Deploy Runbook

Use this runbook when changing binaries on an existing fs-meta Capanix cluster.

## Deployment Boundary

Default production-like fs-meta cluster:

- SSH user: `wanghuajin`
- Nodes: `10.0.82.144`, `10.0.82.145`, `10.0.82.146`
- Active fs-meta runtime nodes for the current gate: `10.0.82.144`, `10.0.82.145`, `10.0.82.146`.
- Run root: `/home/wanghuajin/fsmeta-stable/run`
- Control socket example: `nodes/panda145/home/core.sock`
- Remote ABI: CentOS 7 / glibc 2.17

Hard gates:

- Before any `panda14x` build/deploy attempt, run the full aibox predeploy gate below. It must use complete source snapshots unpacked into a fresh builder-like long path, the same sibling repo layout expected on `panda145`, freshly built release/runtime artifacts, explicit external-worker binaries, external-worker IPC, and the closest practical production-equivalent active-three config/API/resource/app/root replay plus focused seam tests. If this fails, fix locally first. If aibox cannot reproduce a target detail exactly, document the gap and add the nearest stricter local check instead of skipping it. If it passes, still run the `panda145` compatible builder gate because aibox cannot prove CentOS 7 ABI or all remote IPC behavior.
- Build on a target-compatible host, normally `panda145`, or in a toolchain/container that targets the remote glibc.
- For `panda145` builds, refresh builder source by uploading a complete local source snapshot archive for the repo being built. Do not apply patch files onto an existing builder tree as the normal update mechanism.
- Do not deploy an aibox-built ELF unless `objdump -T` proves the maximum required GLIBC symbol is `GLIBC_2.17` or lower.
- Record source commit, build command, artifact path, SHA-256, `ldd` result, and max GLIBC symbol before staging.
- Stage to every node and verify matching hashes before replacing any live file.
- Install staged artifacts by deleting the old live `bin/<artifact>` first, then moving the staged artifact into place. Do not create `*.prev-*` backups unless explicitly requested for a specific operation.
- Every redeploy needs a clean-log checkpoint: stop affected processes, clear live runtime logs including external worker stdout/stderr, then start daemons or re-apply app declarations. Post-deploy diagnosis must use only logs produced by the current deploy attempt.
- If you cannot prove logs were cleared after the last stop and before the current start/apply, stop again, clear logs, restart/re-apply, and discard the previous evidence window.
- Use `bash -s` or `bash -lc` for remote commands because node login shells may differ.
- Do not rely on running `stop-all.sh`, `start-all.sh`, or all node scripts once from `panda145` for cross-node state. Run each node-owned stop/start script on that node's own host through SSH, then verify no stale `capanixd` or `capanix_worker_host` remains for the run root.
- Correct official startup method is the node-owned script path under the stable run root:

```bash
ssh wanghuajin@10.0.82.144 'cd /home/wanghuajin/fsmeta-stable/run && nodes/panda144/stop.sh || true; cd /home/wanghuajin/fsmeta-stable/run && nodes/panda144/start.sh'
ssh wanghuajin@10.0.82.145 'cd /home/wanghuajin/fsmeta-stable/run && nodes/panda145/stop.sh || true; cd /home/wanghuajin/fsmeta-stable/run && nodes/panda145/start.sh'
ssh wanghuajin@10.0.82.146 'cd /home/wanghuajin/fsmeta-stable/run && nodes/panda146/stop.sh || true; cd /home/wanghuajin/fsmeta-stable/run && nodes/panda146/start.sh'
```

- Do not run `nodes/panda144/start.sh` or `nodes/panda146/start.sh` from `panda145`: each script binds its own `10.0.82.x:19401`, so the wrong host fails with `failed to bind transport`. Do not replace this with a hand-written `nohup bin/capanixd -c nodes/<host_ref>/config.yaml -b <ip>:19401`. The `start.sh` scripts source `admin.env` and `nodes/<host_ref>/node.env`, then set `CAPANIX_HOME`, `CAPANIX_NODE_SK_B64`, `CAPANIX_CONFIG`, `CAPANIX_BIND_ADDR`, `CAPANIX_LOG`, and `CAPANIX_PID`. After startup, verify each `nodes/<host_ref>/daemon.pid` on its own host points to a live process from `/home/wanghuajin/fsmeta-stable/run/bin/capanixd` with that node's config and bind address.
- The fs-meta management account file under the stable run root must use the enabled three-field format:

```text
admin:plain$<password>:0
```

Store it at `/home/wanghuajin/fsmeta-stable/run/.fsmeta-state/fs-meta.shadow` with the real password in place of `<password>`. The `plain$` prefix is part of the password encoding, and the trailing `:0` keeps the account enabled. Do not write only `admin:<password>`, omit `plain$`, or omit the third field.

- Use that account through file-based authentication. For `fsmeta` CLI commands, pass only the facade origin as `--api-base` and pass the password file:

```bash
bin/fsmeta roots apply \
  --config config.yaml \
  --socket nodes/panda145/home/core.sock \
  --domain-id fsmeta-stable \
  --api-base http://10.0.82.145:18080 \
  --password-file /home/wanghuajin/fsmeta-stable/run/.fsmeta-state/fs-meta.shadow \
  roots.yaml
```

Do not append `/api/fs-meta/v1` to the CLI `--api-base`; the CLI appends product paths itself. Manual HTTP clients use the full API path, for example `http://10.0.82.145:18080/api/fs-meta/v1/session/login`. Never echo the password, bearer token, or shadow-file contents into logs, TODO, or user-facing output; record only redacted login/status evidence.
- If the branch says not to modify `fs-meta app`, do not build or deploy `libfs_meta_runtime.so`; only restore existing fs-meta declarations after daemon health is proven.

## Long Wait Discipline

- Implement long waits with the execution session and `write_stdin`, not with clock probes, log tails, process checks, or status-only replies.
- Treat `exec_command` plus empty `write_stdin` as the only allowed long-wait implementation. Plans, `TODO.md`, and user-facing status must name that path directly and only; do not propose, compare against, name, or document other wait mechanisms.
- Do not introduce any out-of-band wait facility, scheduler, CLI wrapper, helper process, or replacement tool for build, deploy, or convergence waits; if the normal CLI path is unavailable, keep using the active foreground command and empty `write_stdin`.
- Launch build/test/deploy/convergence work as one foreground command that writes its own log and prints one terminal sentinel line when it exits, for example `AIBOX_GATE_PASS ...` or `AIBOX_GATE_FAIL ...`. Set the initial execution-tool wait window as high as practical.
- If the command yields a session id before the terminal sentinel appears, record the wait contract: reason, command shape, session id, log path, expected terminal sentinel, and a count-based `write_stdin` wait budget. Do not use wall-clock probes to decide when to collect.
- Continue the running command only with empty `write_stdin` calls on that session. For long waits, `yield_time_ms=300000` (300s) is mandatory on every empty `write_stdin` call. If `300000` is rejected by the tool, stop and classify it as a tooling wait-boundary; do not silently lower the value, use 30s chunks, or send characters unless the program explicitly requires input.
- Treat each 300s empty `write_stdin` call as the wait operation itself. A shorter empty wait is a protocol violation unless the previous call already returned the terminal sentinel or a clear first raw failure. When it returns without a terminal sentinel and the command is still running, the next allowed action is another 300s empty `write_stdin` within the recorded budget.
- Do not insert `date`, `ps`, `pgrep`, SSH checks, log tails, file probes, or status summaries between wait calls.
- Do not interpret intermediate output as progress unless it contains the planned terminal sentinel or a clear first raw failure. If intermediate output is noisy but non-terminal, keep waiting with the next max-duration `write_stdin` within the recorded budget.
- For cluster convergence, put the wait inside the launched local command before the single remote sample, then use `write_stdin` to wait for the command result. The `sleep` must appear before `ssh`; do not put long `sleep` inside an SSH heredoc or remote script.
- Use this shape for convergence waits:

```bash
set -euo pipefail
sleep 900
ssh -o BatchMode=yes -o ConnectTimeout=10 wanghuajin@10.0.82.145 'bash -s' <<'REMOTE'
set -euo pipefail
# Collect exactly one bounded authenticated /status sample or one bounded diagnostic.
REMOTE
```

- Do not use these shapes for waits: `ssh host 'sleep 900; ...'`, a remote heredoc that starts with a long `sleep`, `while sleep 30; do ssh ...; done`, repeated `date`, repeated `ps`/`pgrep`, repeated log tails, repeated SSH status samples, or status-only replies.
- If a wait spans turns or context resumes, continue with the next planned 300s empty `write_stdin` on the recorded session. Do not use `date`, logs, process lists, SSH, or file probes to reconstruct state.
- If `write_stdin` returns the terminal sentinel, classify the result once and inspect only the evidence paths named by the sentinel or wait contract. If it returns a clear first raw failure, classify that boundary and stop the wait.
- If the recorded `write_stdin` budget is exhausted without a terminal sentinel or clear failure, classify this as a tooling/runtime wait-boundary and choose the next bounded action from that evidence. Do not switch to short-interval polling.
- If asked for status during the wait, either continue the planned `write_stdin` wait or answer only from the recorded wait contract. Do not run side probes.

## Clean Redeploy Order

Use this order for every binary redeploy or app/source re-apply:

1. Build and verify compatible artifacts.
2. Stage artifacts to all nodes and verify matching hashes.
3. Stop affected daemons, worker hosts, and app processes on every node.
4. Clear or archive all live `*.log`, `*.out`, and `*.err` files under `nodes/` and `.fsmeta-state/worker-runtime/` on every node, and clear stale `/tmp/capanix-*worker*.{stdout,stderr}.log` files that predate the validation window.
5. Replace staged artifacts by deleting old live binaries first and moving staged files into `bin/`; do not create rollback copies unless explicitly requested.
6. Start daemons or re-apply app/source declarations.
7. Validate only with process state and logs generated after step 6.

Never mix logs from before step 4 into acceptance evidence. If a command fails after logs are cleared but before validation finishes, repeat the stop-clear-start/apply sequence before collecting new evidence.

## Per-Node Daemon Log Evidence

For official `panda144` through `panda146` daemon-log diagnosis, use the gate skill's bundled helper instead of hand-writing repeated SSH/grep snippets:

```bash
.codex/skills/fs-meta-cross-repo-gate/scripts/collect_official_node_logs.sh \
  --focus status-fanin \
  --pattern 'nfs-146|source_coverage_gaps|scan_coverage_gaps|missing_source_owner' \
  --lines 260 \
  --out-dir /tmp/fsmeta-node-logs-$(date +%Y%m%d%H%M%S)
```

The helper is read-only, performs one SSH command per node, filters `nodes/<host_ref>/daemon.log` remotely, and writes `collection-plan.txt`, `collection-summary.txt`, and per-node excerpts when `--out-dir` is set. Treat each invocation as one bounded evidence collection after a deploy, wait, or status-sample boundary. Do not loop it, use it before a planned wait deadline, or replace it with ad hoc multi-node SSH log checks.

## Artifact Matrix

Build from a `capanix` checkout. In the aibox dev environment this is usually `/root/repo/capanix`; on a compatible builder, first locate or clone the equivalent checkout.

- `cargo build --release -p capanix-daemon` -> `target/release/capanixd`
- `cargo build --release -p capanix-cli` -> `target/release/cnxctl`
- `cargo build --release -p capanix-worker-host` -> `target/release/capanix_worker_host`

Build from a `fustor-apps` checkout. In the aibox dev environment this is usually `/root/repo/fustor-apps`; on a compatible builder, first locate or clone the equivalent checkout.

- `cargo build --release -p es-source-runtime` -> `target/release/libes_source_runtime.so`
- `cargo build --release -p mysql-source-runtime` -> `target/release/libmysql_source_runtime.so`
- `cargo build --release -p s3-source-runtime` -> `target/release/libs3_source_runtime.so`
- `cargo build --release -p union-graph-runtime` -> `target/release/libunion_graph_runtime.so`

Build from a `fustor` checkout only when explicitly allowed. In the aibox dev environment this is usually `/root/repo/fustor`; on a compatible builder, first locate or clone the equivalent checkout.

- `cargo build --release -p fs-meta-tooling` -> `target/release/fsmeta`
- `cargo build --release -p fs-meta-runtime` -> `target/release/libfs_meta_runtime.so`

## Aibox Predeploy Gate

Run this before uploading sources to `panda145`. This is a hard predeploy gate, not a quick smoke. The goal is to catch stale source trees, stale target artifacts, path-length failures, external worker host mismatches, release/runtime mismatches, API invocation mistakes, statecell/sidecar bind timeouts, worker lifecycle churn, route apply regressions, partial status fan-in, and production-config regressions on aibox before spending a `panda14x` build/deploy cycle.

Required shape:

- Build and test from complete local source snapshot archives unpacked into a fresh aibox builder directory, not directly from an old checkout or old target directory.
- Preserve the `panda145` sibling layout for path dependencies, for example `fullsrc-<timestamp>/fustor` beside `fullsrc-<timestamp>/capanix`.
- Use a builder path that is at least as demanding as the remote path shape. For path-sensitive code, choose a long base comparable to `/home/wanghuajin/fsmeta-build/current/fullsrc-<timestamp>/...` so Unix socket and log path limits can fail locally.
- Delete the exact target artifacts before building so a failed build cannot reuse a stale binary.
- Build the app/runtime artifact in release mode where the remote deploy uses release mode.
- Build or select an explicit `capanix_worker_host` from the matching source snapshot and set `CAPANIX_WORKER_HOST_BINARY`; do not rely on PATH or an installed worker host.
- Set app/runtime binary environment variables to the freshly built snapshot artifact and verify the exact paths and hashes before tests.
- Exercise external-worker IPC in tests. A test that bypasses the worker host is not sufficient for deployment approval.
- Reproduce the target active-three topology as closely as possible before remote build: active nodes/resources/apps/roots equivalent to `panda144-146`, one owner per source/scan/sink logical root, and the same source app declaration, `fsmeta deploy`, `roots apply`, `/status`, materialization, and process/log inspection order used on the real cluster.
- Run the production-equivalent replay under a clean validation window on aibox: clear old daemon/app/worker logs, start from no stale processes or stale app roots, apply resources, apply app declarations, deploy fs-meta, apply roots through the facade-origin `--api-base`, then inspect fresh worker stdout/stderr, daemon logs, process list, duplicate runtime/worker rows, and `/status` completeness.
- Run focused tests for the changed seam plus the production-equivalent smoke for the affected config/API/resource path. For fs-meta management paths, use the same API-base semantics as the real cluster: CLI `--api-base` gets only the facade origin, while manual HTTP uses the full `/api/fs-meta/v1/...` path.
- Treat `operation timed out`, missing worker stdout/stderr, worker sidecar bind failures, statecell read/write timeouts, duplicate or zombie workers, unexpected partial/degraded status, stale binary hashes, source snapshot drift, or config differences from the target deployment as blockers. Do not upload to `panda145` until they are explained or fixed.

For `fs-meta-runtime` changes from `/root/repo/fustor`:

```bash
set -euo pipefail
ts=$(date +%Y%m%d%H%M%S)
PREFLIGHT_ROOT=/tmp/aibox-fsmeta-build/panda145/current/fullsrc-$ts
mkdir -p "$PREFLIGHT_ROOT/fustor" "$PREFLIGHT_ROOT/capanix"

python3 - <<PY
from pathlib import Path
root = Path("$PREFLIGHT_ROOT")
for repo in ("fustor", "capanix"):
    path = str(root / repo)
    print(f"{repo} preflight path length: {len(path)} {path}")
    if len(path) < 67:
        raise SystemExit(f"{repo} preflight path is shorter than the known panda145 builder path")
PY

for repo in fustor capanix; do
  src="/root/repo/$repo"
  archive="/tmp/aibox-$repo-$ts.tar.gz"
  (
    cd "$src"
    git status --short
    git rev-parse HEAD || true
    git diff --check
    tar \
      --exclude='./target' \
      --exclude='./.git' \
      --exclude='./.tmp' \
      --exclude='./tmp' \
      -czf "$archive" .
  )
  tar -xzf "$archive" -C "$PREFLIGHT_ROOT/$repo"
done

cd "$PREFLIGHT_ROOT/capanix"
cargo build -p capanix-worker-host

cd "$PREFLIGHT_ROOT/fustor"
rm -f target/release/libfs_meta_runtime.so
cargo build --release -p fs-meta-runtime

export CAPANIX_FS_META_APP_BINARY=$PREFLIGHT_ROOT/fustor/target/release/libfs_meta_runtime.so
export CAPANIX_WORKER_HOST_BINARY=$PREFLIGHT_ROOT/capanix/target/debug/capanix_worker_host
test -x "$CAPANIX_WORKER_HOST_BINARY"
test -f "$CAPANIX_FS_META_APP_BINARY"
sha256sum "$CAPANIX_WORKER_HOST_BINARY" "$CAPANIX_FS_META_APP_BINARY"

cargo test -p fs-meta-runtime source_repair_retained_replay_cancellation_preserves_replay_required --lib -- --nocapture
cargo test -p fs-meta-runtime rearm_source_rescan_endpoints_replays_retained_control_before_ready_proof --lib
cargo test -p fs-meta-runtime management_write_recovery_clears_retained_replay_before_write_ready_wait --lib
cargo test -p fs-meta-runtime source_repair_recovery_ --lib
cargo test -p fs-meta-runtime source_replay_retained_control_state_ --lib
cargo test -p fs-meta-runtime source_worker_client --lib
cargo fmt --check -p fs-meta-runtime
```

For `capanixd` or `capanix_worker_host` changes, use the same complete-snapshot aibox builder shape for `capanix`, run the local focused tests for the changed Capanix crates, then run at least one real external-worker smoke from the dependent app layer when available. Do not proceed to `panda145` if the aibox predeploy gate fails. Do not treat aibox success as deployable proof; `panda145` must still build and run focused tests with `GLIBC_2.17` or lower.

## Upload Complete Source To Panda145

Use this before every `panda145` build. Package the current local source tree for the repo being built and unpack it into a new builder directory. This is the standard builder update path; do not apply patch files to old builder directories.

From the local repo:

```bash
set -euo pipefail
REPO=/root/repo/fustor
NAME=fustor-src-$(date +%Y%m%d%H%M%S)
REMOTE_ROOT=/home/wanghuajin/fsmeta-build/current
ARCHIVE=/tmp/$NAME.tar.gz

cd "$REPO"
git status --short
git rev-parse HEAD || true
tar \
  --exclude='./target' \
  --exclude='./.git' \
  --exclude='./.tmp' \
  --exclude='./tmp' \
  -czf "$ARCHIVE" .
scp "$ARCHIVE" "wanghuajin@10.0.82.145:/tmp/$NAME.tar.gz"
ssh wanghuajin@10.0.82.145 'bash -s' -- "$REMOTE_ROOT" "$NAME" <<'REMOTE'
set -euo pipefail
remote_root=$1
name=$2
mkdir -p "$remote_root/$name"
tar -xzf "/tmp/$name.tar.gz" -C "$remote_root/$name"
printf 'BUILDER_REPO=%s\n' "$remote_root/$name"
REMOTE
```

For `capanix` or `fustor-apps`, change `REPO` and `NAME` accordingly. If the uploaded repo uses workspace path dependencies such as `../capanix`, also upload that sibling repo as a complete snapshot so the relative paths on `panda145` match the local workspace layout.

Before a release build, remove the target artifact that will be produced so a failed build cannot accidentally reuse a stale binary:

```bash
cd "$BUILDER_REPO"
rm -f target/release/libfs_meta_runtime.so
cargo build --release -p fs-meta-runtime
```

## Compatible Build

On the compatible builder, record the source state first. Replace `CAPANIX_REPO`, `FUSTOR_APPS_REPO`, and `FUSTOR_REPO` with paths on that builder.

```bash
cd "${CAPANIX_REPO:-/root/repo/capanix}"
git status --short
git rev-parse HEAD
cargo test -p capanix-kernel gossip::state
cargo test -p capanix-kernel gossip::transport
cargo test -p capanix-kernel gossip::peer_matcher
cargo build --release -p capanix-daemon -p capanix-cli -p capanix-worker-host
```

For `fustor-apps` source and union-graph runtimes:

```bash
cd "${FUSTOR_APPS_REPO:-/root/repo/fustor-apps}"
git status --short
git rev-parse HEAD
cargo test -p es-source-runtime -p mysql-source-runtime -p s3-source-runtime -p union-graph-runtime
cargo build --release -p es-source-runtime -p mysql-source-runtime -p s3-source-runtime -p union-graph-runtime
```

For explicitly approved fs-meta runtime/tooling builds:

```bash
cd "${FUSTOR_REPO:-/root/repo/fustor}"
git status --short 2>/dev/null || true
git rev-parse HEAD 2>/dev/null || true
rm -f target/release/libfs_meta_runtime.so target/release/fsmeta
cargo test -p fs-meta-runtime -p fs-meta-tooling
cargo build --release -p fs-meta-runtime -p fs-meta-tooling
```

Verify each exact ELF before staging:

```bash
artifact=target/release/capanixd
sha256sum "$artifact"
ldd "$artifact"
objdump -T "$artifact" | grep -o 'GLIBC_[0-9.]*' | sort -Vu | tail -1
```

For CentOS 7, the last command must print `GLIBC_2.17` or lower.

## Stage To All Nodes

Run from the compatible builder or from a host that can reach all nodes. Replace `artifact`, `name`, and `sha` for each file.

```bash
set -euo pipefail
RUN_ROOT=/home/wanghuajin/fsmeta-stable/run
NODES=(10.0.82.144 10.0.82.145 10.0.82.146)
artifact=target/release/capanixd
name=capanixd
sha=$(sha256sum "$artifact" | awk '{print $1}')

for host in "${NODES[@]}"; do
  ssh "wanghuajin@$host" 'bash -s' -- "$RUN_ROOT" <<'REMOTE'
set -euo pipefail
run_root=$1
mkdir -p "$run_root/bin.staging"
REMOTE
  scp "$artifact" "wanghuajin@$host:$RUN_ROOT/bin.staging/$name.$sha"
  ssh "wanghuajin@$host" 'bash -s' -- "$RUN_ROOT" "$name" "$sha" <<'REMOTE'
set -euo pipefail
run_root=$1
name=$2
sha=$3
cd "$run_root"
test "$(sha256sum "bin.staging/$name.$sha" | awk '{print $1}')" = "$sha"
cp "bin.staging/$name.$sha" "bin/$name.fixed"
test "$(sha256sum "bin/$name.fixed" | awk '{print $1}')" = "$sha"
REMOTE
done
```

Never continue to replacement unless every node has the same staged hash.

## Stop Affected Processes

Stop through each node's local control path. Do not run `stop-all.sh` once on `panda145` and assume the full cluster stopped.

```bash
set -euo pipefail
RUN_ROOT=/home/wanghuajin/fsmeta-stable/run
NODES=(10.0.82.144 10.0.82.145 10.0.82.146)

for host in "${NODES[@]}"; do
  ssh "wanghuajin@$host" 'bash -s' -- "$RUN_ROOT" <<'REMOTE'
set -euo pipefail
run_root=$1
cd "$run_root"
node_dir="nodes/$(hostname -s)"

if [ -x "$node_dir/stop.sh" ]; then
  "$node_dir/stop.sh" || true
else
  echo "missing official node stop script: $node_dir/stop.sh" >&2
  exit 1
fi

pkill -TERM -f "$run_root/bin/capanix_worker_host" || true
pkill -TERM -f "$run_root/bin/capanixd" || true
sleep 2
pkill -KILL -f "$run_root/bin/capanix_worker_host" || true
pkill -KILL -f "$run_root/bin/capanixd" || true

if pgrep -af 'capanixd|capanix_worker_host' | grep -F "$run_root"; then
  echo "stale capanix process remains under $run_root" >&2
  exit 1
fi
REMOTE
done
```

For app-runtime-only redeploys, stopping `capanix_worker_host` is usually sufficient, but using the full stop path is safer when process ownership is unclear.

## Clear Live Logs

Clear logs on every redeploy after stopping affected processes and before starting daemons or re-applying app declarations. This prevents historical hot-loop evidence from contaminating the new validation window.

```bash
set -euo pipefail
RUN_ROOT=/home/wanghuajin/fsmeta-stable/run
NODES=(10.0.82.144 10.0.82.145 10.0.82.146)
ts=$(date +%Y%m%d%H%M%S)

for host in "${NODES[@]}"; do
  ssh "wanghuajin@$host" 'bash -s' -- "$RUN_ROOT" "$ts" <<'REMOTE'
set -euo pipefail
run_root=$1
ts=$2
cd "$run_root"
mkdir -p ".fsmeta-state/log-archives/$ts"
find nodes .fsmeta-state/worker-runtime -type f \( -name '*.log' -o -name '*.out' -o -name '*.err' \) -print0 2>/dev/null |
  while IFS= read -r -d '' f; do
    archive=".fsmeta-state/log-archives/$ts/${f//\//__}"
    cp "$f" "$archive" 2>/dev/null || true
    : > "$f"
  done
find /tmp /var/tmp -maxdepth 2 -type f \
  \( -name 'capanix-*worker*.stdout.log' -o -name 'capanix-*worker*.stderr.log' \) \
  -delete 2>/dev/null || true
REMOTE
done
```

If preserving old logs is unnecessary or disk pressure is high, the archive copy may be skipped, but the live log files must still be truncated before restart. Do not accept validation evidence from `/tmp/capanix-*worker*` logs unless they were created after this cleanup window; current `fsmeta` deploys should place worker logs under `.fsmeta-state/worker-runtime/<role>/`.

## Replace Capanix Daemon

Verify the official node scripts exist first:

```bash
ssh wanghuajin@10.0.82.145 'bash -s' <<'REMOTE'
set -euo pipefail
cd /home/wanghuajin/fsmeta-stable/run
ls -l nodes/panda144/start.sh nodes/panda144/stop.sh \
  nodes/panda145/start.sh nodes/panda145/stop.sh \
  nodes/panda146/start.sh nodes/panda146/stop.sh
REMOTE
```

Use the "Stop Affected Processes" and "Clear Live Logs" steps before replacement, then delete the old `bin/capanixd` and install the staged file on every node. Do not create `*.prev-*` backups unless explicitly requested:

```bash
set -euo pipefail
RUN_ROOT=/home/wanghuajin/fsmeta-stable/run
NODES=(10.0.82.144 10.0.82.145 10.0.82.146)
sha=<expected-sha256>

for host in "${NODES[@]}"; do
  ssh "wanghuajin@$host" 'bash -s' -- "$RUN_ROOT" "$sha" <<'REMOTE'
set -euo pipefail
run_root=$1
sha=$2
cd "$run_root"
test "$(sha256sum bin/capanixd.fixed | awk '{print $1}')" = "$sha"
rm -f bin/capanixd
mv bin/capanixd.fixed bin/capanixd
chmod +x bin/capanixd
test "$(sha256sum bin/capanixd | awk '{print $1}')" = "$sha"
REMOTE
done
```

Start the local node on every host only after replacement and log clearing are complete:

```bash
set -euo pipefail
RUN_ROOT=/home/wanghuajin/fsmeta-stable/run
NODES=(10.0.82.144 10.0.82.145 10.0.82.146)

for host in "${NODES[@]}"; do
  ssh "wanghuajin@$host" 'bash -s' -- "$RUN_ROOT" <<'REMOTE'
set -euo pipefail
run_root=$1
cd "$run_root"
node_dir="nodes/$(hostname -s)"

if [ -x "$node_dir/start.sh" ]; then
  "$node_dir/start.sh"
else
  echo "missing official node start script: $node_dir/start.sh" >&2
  exit 1
fi
REMOTE
done
```

Do not fall back to `start-all.sh`, `stop-all.sh`, or a hand-written `nohup bin/capanixd ...` path on the official stable cluster. The node-local scripts are the startup contract because they load the required admin and node identity environment.

If any node fails to start because of a linker or GLIBC error, stop affected processes on every node, clear logs, rebuild or reinstall a known-good staged artifact, and start again.

## Replace Runtime Or Tooling Artifacts

Use this for `cnxctl`, `capanix_worker_host`, source runtime `.so` files, union-graph runtime `.so`, and explicitly approved `fsmeta` or `libfs_meta_runtime.so` replacements.

Before replacing files that may already be loaded by a worker process, run "Stop Affected Processes" and "Clear Live Logs". For a `cnxctl`-only replacement, stopping the daemon is not required, but logs must still be cleared immediately before any validation app/source re-apply.

```bash
set -euo pipefail
RUN_ROOT=/home/wanghuajin/fsmeta-stable/run
NODES=(10.0.82.144 10.0.82.145 10.0.82.146)
name=libes_source_runtime.so
sha=<expected-sha256>

for host in "${NODES[@]}"; do
  ssh "wanghuajin@$host" 'bash -s' -- "$RUN_ROOT" "$name" "$sha" <<'REMOTE'
set -euo pipefail
run_root=$1
name=$2
sha=$3
cd "$run_root"
test "$(sha256sum "bin/$name.fixed" | awk '{print $1}')" = "$sha"
rm -f "bin/$name"
mv "bin/$name.fixed" "bin/$name"
chmod +x "bin/$name" || true
test "$(sha256sum "bin/$name" | awk '{print $1}')" = "$sha"
REMOTE
done
```

After replacing app runtimes, re-apply the app declarations. Do not assume process registration survives daemon or runtime replacement.
Clear live logs immediately before the app declaration re-apply if the daemon was not restarted. If logs were not cleared after the last process stop, stop the affected workers again and clear logs before applying.

## Fs-meta Management API Usage

Use the facade origin and API path consistently during validation.

- `fsmeta roots apply --api-base` expects only the facade origin, for example `http://10.0.82.145:18080`. The CLI appends `/api/fs-meta/v1/session/login` and other API paths internally.
- Do not pass `--api-base http://10.0.82.145:18080/api/fs-meta/v1` to `fsmeta roots apply`. That double-prefixes the login URL and can surface as `Error: login failed:`; treat that as an operator invocation error, not a runtime blocker.
- Manual HTTP clients use the full product API path: login at `POST http://10.0.82.145:18080/api/fs-meta/v1/session/login`, then call `GET http://10.0.82.145:18080/api/fs-meta/v1/status` with `Authorization: Bearer <token>`.
- For the real cluster, pass `--password-file /home/wanghuajin/fsmeta-stable/run/.fsmeta-state/fs-meta.shadow` when the CLI can authenticate for you. Never print the file contents, recovered password, or bearer token.
- When saving login evidence, store only HTTP status and a redacted response; do not write secrets to validation artifacts.

Example active-three roots apply:

```bash
./bin/fsmeta --output json roots apply \
  --api-base http://10.0.82.145:18080 \
  --username admin \
  --password-file /home/wanghuajin/fsmeta-stable/run/.fsmeta-state/fs-meta.shadow \
  --socket ./nodes/panda145/home/core.sock \
  --domain-id fsmeta-stable \
  --actor-id local-admin \
  --key-id "$CAPANIX_CTL_KEY_ID" \
  --admin-sk-b64 "$CAPANIX_CTL_SK_B64" \
  --config fs-meta-external.yaml \
  --file monitoring-roots-3group.local.json
```

## Restore Existing Apps And Sources

From `panda145`, use the run root's binaries and environment:

If this restore is part of a redeploy validation window, run "Stop Affected Processes" and "Clear Live Logs" first, then execute the restore. Applying declarations against old logs invalidates the evidence window.

```bash
ssh wanghuajin@10.0.82.145 'bash -s' <<'REMOTE'
set -euo pipefail
cd /home/wanghuajin/fsmeta-stable/run
source ./admin.env
export PATH="/home/wanghuajin/fsmeta-stable/run/bin:$PATH"

COMMON=(
  -s ./nodes/panda145/home/core.sock
  --domain-id fsmeta-stable
  --actor-id local-admin
  --key-id "$CAPANIX_CTL_KEY_ID"
  --admin-sk-b64 "$CAPANIX_CTL_SK_B64"
  --output json
)

./bin/cnxctl "${COMMON[@]}" config resource-announce fs-meta-all-resources.yaml

if [ -f ./source-engines-8555/source-env.sh ]; then
  source ./source-engines-8555/source-env.sh
fi

for declaration in \
  source-engines-8555/declarations/es-source.yaml \
  source-engines-8555/declarations/mysql-source.yaml \
  source-engines-8555/declarations/s3-source.yaml
do
  [ -f "$declaration" ] && ./bin/cnxctl "${COMMON[@]}" app apply "$declaration"
done

if [ -f fs-meta-embedded.yaml ]; then
  ./bin/fsmeta --output json deploy \
    --socket ./nodes/panda145/home/core.sock \
    --domain-id fsmeta-stable \
    --actor-id local-admin \
    --key-id "$CAPANIX_CTL_KEY_ID" \
    --admin-sk-b64 "$CAPANIX_CTL_SK_B64" \
    --config fs-meta-embedded.yaml
fi
REMOTE
```

The `PATH` export is required because `fsmeta deploy` may invoke `cnxctl` internally.

## Acceptance Evidence

Collect these before claiming success:

```bash
ssh wanghuajin@10.0.82.145 'bash -s' <<'REMOTE'
set -euo pipefail
cd /home/wanghuajin/fsmeta-stable/run
source ./admin.env
COMMON=(
  -s ./nodes/panda145/home/core.sock
  --domain-id fsmeta-stable
  --actor-id local-admin
  --key-id "$CAPANIX_CTL_KEY_ID"
  --admin-sk-b64 "$CAPANIX_CTL_SK_B64"
  --output json
)

sha256sum bin/capanixd bin/cnxctl bin/capanix_worker_host 2>/dev/null || true
ps -C capanixd -o pid,pcpu,pmem,rss,args
./bin/cnxctl "${COMMON[@]}" --target-scope cluster status
./bin/cnxctl "${COMMON[@]}" process list
REMOTE
```

Required interpretation:

- `peer_count` must match the intended node count; for the current active-three gate it must be `3`.
- Cluster status must not show duplicate self peers.
- `process list` must show the expected apps, including fs-meta and any source or union-graph apps that should be deployed.
- CPU is diagnostic only; final acceptance is stable complete/non-partial index growth beyond the target entry count, not low CPU overhead.
- Dashboard source counts should be diagnosed from `process list` and declaration/apply state before changing app code.

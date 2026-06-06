# Fs-meta Binary Build and Deploy Runbook

Use this runbook when changing binaries on an existing fs-meta Capanix cluster.

## Deployment Boundary

Default production-like fs-meta cluster:

- SSH user: `wanghuajin`
- Nodes: `10.0.82.144`, `10.0.82.145`, `10.0.82.146`, `10.0.82.147`, `10.0.82.148`
- Run root: `/home/wanghuajin/fsmeta-stable/run`
- Control socket example: `nodes/panda145/home/core.sock`
- Remote ABI: CentOS 7 / glibc 2.17

Hard gates:

- Build on a target-compatible host, normally `panda145`, or in a toolchain/container that targets the remote glibc.
- Do not deploy an aibox-built ELF unless `objdump -T` proves the maximum required GLIBC symbol is `GLIBC_2.17` or lower.
- Record source commit, build command, artifact path, SHA-256, `ldd` result, and max GLIBC symbol before staging.
- Stage to every node and verify matching hashes before replacing any live file.
- Always create `*.prev-<timestamp>-<hash-prefix>` before replacement.
- Every redeploy needs a clean-log checkpoint: stop affected processes, clear live runtime logs, then start daemons or re-apply app declarations. Post-deploy diagnosis must use only logs produced by the current deploy attempt.
- If you cannot prove logs were cleared after the last stop and before the current start/apply, stop again, clear logs, restart/re-apply, and discard the previous evidence window.
- Use `bash -s` or `bash -lc` for remote commands because node login shells may differ.
- Do not rely on running `stop-all.sh` or `start-all.sh` once from `panda145` for cross-node state. Run the local stop/start path through SSH on every node, then verify no stale `capanixd` or `capanix_worker_host` remains for the run root.
- If the branch says not to modify `fs-meta app`, do not build or deploy `libfs_meta_runtime.so`; only restore existing fs-meta declarations after daemon health is proven.

## Clean Redeploy Order

Use this order for every binary redeploy or app/source re-apply:

1. Build and verify compatible artifacts.
2. Stage artifacts to all nodes and verify matching hashes.
3. Stop affected daemons, worker hosts, and app processes on every node.
4. Clear or archive all live `*.log`, `*.out`, and `*.err` files under `nodes/` on every node.
5. Replace staged artifacts with rollback copies if this is a binary redeploy.
6. Start daemons or re-apply app/source declarations.
7. Validate only with process state and logs generated after step 6.

Never mix logs from before step 4 into acceptance evidence. If a command fails after logs are cleared but before validation finishes, repeat the stop-clear-start/apply sequence before collecting new evidence.

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
git status --short
git rev-parse HEAD
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
NODES=(10.0.82.144 10.0.82.145 10.0.82.146 10.0.82.147 10.0.82.148)
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
NODES=(10.0.82.144 10.0.82.145 10.0.82.146 10.0.82.147 10.0.82.148)

for host in "${NODES[@]}"; do
  ssh "wanghuajin@$host" 'bash -s' -- "$RUN_ROOT" <<'REMOTE'
set -euo pipefail
run_root=$1
cd "$run_root"
node_dir="nodes/$(hostname -s)"

if [ -x "$node_dir/stop.sh" ]; then
  "$node_dir/stop.sh" || true
elif [ -x ./stop-all.sh ]; then
  ./stop-all.sh || true
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
NODES=(10.0.82.144 10.0.82.145 10.0.82.146 10.0.82.147 10.0.82.148)
ts=$(date +%Y%m%d%H%M%S)

for host in "${NODES[@]}"; do
  ssh "wanghuajin@$host" 'bash -s' -- "$RUN_ROOT" "$ts" <<'REMOTE'
set -euo pipefail
run_root=$1
ts=$2
cd "$run_root"
mkdir -p ".fsmeta-state/log-archives/$ts"
find nodes -type f \( -name '*.log' -o -name '*.out' -o -name '*.err' \) -print0 |
  while IFS= read -r -d '' f; do
    archive=".fsmeta-state/log-archives/$ts/${f//\//__}"
    cp "$f" "$archive" 2>/dev/null || true
    : > "$f"
  done
REMOTE
done
```

If preserving old logs is unnecessary or disk pressure is high, the archive copy may be skipped, but the live log files must still be truncated before restart.

## Replace Capanix Daemon

Discover node scripts first:

```bash
ssh wanghuajin@10.0.82.145 'bash -s' <<'REMOTE'
set -euo pipefail
cd /home/wanghuajin/fsmeta-stable/run
ls -l start-all.sh stop-all.sh nodes/*/start.sh nodes/*/stop.sh 2>/dev/null || true
REMOTE
```

Use the "Stop Affected Processes" and "Clear Live Logs" steps before replacement, then replace `bin/capanixd` on every node with rollback files:

```bash
set -euo pipefail
RUN_ROOT=/home/wanghuajin/fsmeta-stable/run
NODES=(10.0.82.144 10.0.82.145 10.0.82.146 10.0.82.147 10.0.82.148)
sha=<expected-sha256>
ts=$(date +%Y%m%d%H%M%S)

for host in "${NODES[@]}"; do
  ssh "wanghuajin@$host" 'bash -s' -- "$RUN_ROOT" "$sha" "$ts" <<'REMOTE'
set -euo pipefail
run_root=$1
sha=$2
ts=$3
cd "$run_root"
test "$(sha256sum bin/capanixd.fixed | awk '{print $1}')" = "$sha"
old_sha=$(sha256sum bin/capanixd | awk '{print $1}')
cp bin/capanixd "bin/capanixd.prev-$ts-${old_sha:0:12}"
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
NODES=(10.0.82.144 10.0.82.145 10.0.82.146 10.0.82.147 10.0.82.148)

for host in "${NODES[@]}"; do
  ssh "wanghuajin@$host" 'bash -s' -- "$RUN_ROOT" <<'REMOTE'
set -euo pipefail
run_root=$1
cd "$run_root"
node_dir="nodes/$(hostname -s)"

if [ -x "$node_dir/start.sh" ]; then
  "$node_dir/start.sh"
elif [ -x ./start-all.sh ]; then
  ./start-all.sh
else
  echo "no local start script found for $(hostname -s)" >&2
  exit 1
fi
REMOTE
done
```

If any node fails to start because of a linker or GLIBC error, stop affected processes on every node, clear logs, restore the matching `bin/capanixd.prev-*` file on every node, and start again.

## Replace Runtime Or Tooling Artifacts

Use this for `cnxctl`, `capanix_worker_host`, source runtime `.so` files, union-graph runtime `.so`, and explicitly approved `fsmeta` or `libfs_meta_runtime.so` replacements.

Before replacing files that may already be loaded by a worker process, run "Stop Affected Processes" and "Clear Live Logs". For a `cnxctl`-only replacement, stopping the daemon is not required, but logs must still be cleared immediately before any validation app/source re-apply.

```bash
set -euo pipefail
RUN_ROOT=/home/wanghuajin/fsmeta-stable/run
NODES=(10.0.82.144 10.0.82.145 10.0.82.146 10.0.82.147 10.0.82.148)
name=libes_source_runtime.so
sha=<expected-sha256>
ts=$(date +%Y%m%d%H%M%S)

for host in "${NODES[@]}"; do
  ssh "wanghuajin@$host" 'bash -s' -- "$RUN_ROOT" "$name" "$sha" "$ts" <<'REMOTE'
set -euo pipefail
run_root=$1
name=$2
sha=$3
ts=$4
cd "$run_root"
test "$(sha256sum "bin/$name.fixed" | awk '{print $1}')" = "$sha"
if [ -f "bin/$name" ]; then
  old_sha=$(sha256sum "bin/$name" | awk '{print $1}')
  cp "bin/$name" "bin/$name.prev-$ts-${old_sha:0:12}"
fi
mv "bin/$name.fixed" "bin/$name"
chmod +x "bin/$name" || true
test "$(sha256sum "bin/$name" | awk '{print $1}')" = "$sha"
REMOTE
done
```

After replacing app runtimes, re-apply the app declarations. Do not assume process registration survives daemon or runtime replacement.
Clear live logs immediately before the app declaration re-apply if the daemon was not restarted. If logs were not cleared after the last process stop, stop the affected workers again and clear logs before applying.

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

- `peer_count` must match the intended node count, normally `5`.
- Cluster status must not show duplicate self peers.
- `process list` must show the expected apps, including fs-meta and any source or union-graph apps that should be deployed.
- CPU must remain within the current acceptance target while workload is active.
- Dashboard source counts should be diagnosed from `process list` and declaration/apply state before changing app code.

#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CAPANIX_ROOT="${CAPANIX_ROOT:-$(cd "$ROOT/../capanix" && pwd)}"
WORKDIR="${WORKDIR:-/tmp/capanix-5node}"
KEEP="${KEEP:-0}"
DEFAULT_LISTEN_IP="$(
  (command -v ip >/dev/null 2>&1 && ip -4 -o addr show scope global || true) \
    | awk '{ split($4, addr, "/"); if (addr[1] ~ /^10\./) { print addr[1]; exit } }'
)"
LISTEN_IP="${LISTEN_IP:-${DEFAULT_LISTEN_IP:-127.0.0.1}}"
CLUSTER_BIND_IP="${CLUSTER_BIND_IP:-$LISTEN_IP}"
BACKEND_BIND_IP="${BACKEND_BIND_IP:-$LISTEN_IP}"

CNXCTL_BIN="$CAPANIX_ROOT/target/debug/cnxctl"
CAPANIXD_BIN="$CAPANIX_ROOT/target/debug/capanixd"
FSMETA_BIN="$ROOT/target/debug/fsmeta"
FSMETA_LIB="$ROOT/target/debug/libfs_meta_runtime.so"
ES_SOURCE_LIB="$ROOT/target/debug/libes_source_runtime.so"
MYSQL_SOURCE_LIB="$ROOT/target/debug/libmysql_source_runtime.so"
S3_SOURCE_LIB="$ROOT/target/debug/libs3_source_runtime.so"
UNION_GRAPH_LIB="$ROOT/target/debug/libunion_graph_runtime.so"

COMPOSE_FILE="$WORKDIR/docker-compose.yaml"
CLUSTER_DIR="$WORKDIR/cluster"
MANIFEST_DIR="$WORKDIR/manifests"
DECL_DIR="$WORKDIR/declarations"
SEED_DIR="$WORKDIR/seed"
EXPORT_DIR="$WORKDIR/fs-exports"
LOG_DIR="$WORKDIR/logs"

MYSQL_LOCAL_USERNAME="${MYSQL_LOCAL_USERNAME:-testuser}"
MYSQL_LOCAL_PASSWORD="${MYSQL_LOCAL_PASSWORD:-testpass}"
S3_LOCAL_ACCESS_KEY_ID="${S3_LOCAL_ACCESS_KEY_ID:-minioadmin}"
S3_LOCAL_SECRET_ACCESS_KEY="${S3_LOCAL_SECRET_ACCESS_KEY:-minioadmin}"

ES_BASE_URL="http://${BACKEND_BIND_IP}:19200"
MYSQL_ENDPOINT_URI="mysql://${BACKEND_BIND_IP}:13306/fustor_demo"
S3_BASE_URL="http://${BACKEND_BIND_IP}:19000"

log() {
  printf '[local-capanix-5node] %s\n' "$*"
}

die() {
  printf '[local-capanix-5node] ERROR: %s\n' "$*" >&2
  exit 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

docker_compose() {
  docker compose -f "$COMPOSE_FILE" "$@"
}

cnxctl() {
  "$CNXCTL_BIN" \
    --output json \
    --socket "$SOCKET" \
    --actor-id local-admin \
    --domain-id local \
    --key-id "${CAPANIX_CTL_KEY_ID:-local-admin-ed25519-1}" \
    --admin-sk-b64 "$CAPANIX_CTL_SK_B64" \
    "$@"
}

cnxctl_cluster() {
  "$CNXCTL_BIN" \
    --output json \
    --socket "$SOCKET" \
    --actor-id local-admin \
    --domain-id local \
    --key-id "${CAPANIX_CTL_KEY_ID:-local-admin-ed25519-1}" \
    --admin-sk-b64 "$CAPANIX_CTL_SK_B64" \
    --target-scope cluster \
    "$@"
}

wait_for_file() {
  local path="$1"
  local label="$2"
  local deadline="${3:-60}"
  local start
  start="$(date +%s)"
  while [ ! -S "$path" ] && [ ! -e "$path" ]; do
    if [ "$(( $(date +%s) - start ))" -ge "$deadline" ]; then
      die "timed out waiting for $label at $path"
    fi
    sleep 1
  done
}

pid_is_running() {
  local pid_file="$1"
  [ -s "$pid_file" ] && kill -0 "$(cat "$pid_file")" >/dev/null 2>&1
}

tail_node_log() {
  local log_file="$1"
  if [ -s "$log_file" ]; then
    tail -n 120 "$log_file" >&2 || true
  else
    printf '[local-capanix-5node] node log is empty: %s\n' "$log_file" >&2
  fi
}

start_node_direct() {
  local node_dir="$1"
  # shellcheck disable=SC1091
  source "$node_dir/node.env"
  mkdir -p "$CAPANIX_HOME"
  rm -f "$CAPANIX_HOME/core.sock"
  if pid_is_running "$CAPANIX_PID"; then
    log "node already running: $(cat "$CAPANIX_PID")"
    return 0
  fi
  : >"$CAPANIX_LOG"
  CAPANIX_HOME="$CAPANIX_HOME" \
    CAPANIX_NODE_SK_B64="$CAPANIX_NODE_SK_B64" \
    CAPANIX_ADMIN_SK_B64="$CAPANIX_ADMIN_SK_B64" \
    /usr/bin/setsid "$CAPANIXD_BIN" \
    -c "$CAPANIX_CONFIG" \
    -b "$CAPANIX_BIND_ADDR" \
    >>"$CAPANIX_LOG" 2>&1 </dev/null &
  echo $! >"$CAPANIX_PID"
  log "started $CAPANIX_NODE_ID pid=$(cat "$CAPANIX_PID")"
}

wait_for_socket() {
  local socket_path="$1"
  local label="$2"
  local pid_file="$3"
  local log_file="$4"
  local deadline="${5:-60}"
  local start
  start="$(date +%s)"
  while [ ! -S "$socket_path" ]; do
    if [ -s "$pid_file" ] && ! pid_is_running "$pid_file"; then
      tail_node_log "$log_file"
      die "$label exited before creating socket $socket_path"
    fi
    if [ "$(( $(date +%s) - start ))" -ge "$deadline" ]; then
      tail_node_log "$log_file"
      die "timed out waiting for $label socket at $socket_path"
    fi
    sleep 1
  done
}

wait_for_http() {
  local url="$1"
  local label="$2"
  local deadline="${3:-120}"
  local start
  start="$(date +%s)"
  until curl -fsS "$url" >/dev/null 2>&1; do
    if [ "$(( $(date +%s) - start ))" -ge "$deadline" ]; then
      die "timed out waiting for $label at $url"
    fi
    sleep 2
  done
}

wait_for_mysql() {
  local deadline="${1:-120}"
  local start
  start="$(date +%s)"
  until docker_compose exec -T mysql mysqladmin ping -h 127.0.0.1 -uroot -prootpass --silent >/dev/null 2>&1; do
    if [ "$(( $(date +%s) - start ))" -ge "$deadline" ]; then
      die "timed out waiting for MySQL"
    fi
    sleep 2
  done
}

cleanup_workdir() {
  if [ "$KEEP" != "1" ]; then
    if [ -f "$COMPOSE_FILE" ]; then
      docker_compose down -v --remove-orphans >/dev/null 2>&1 || true
    fi
    if [ -x "$CLUSTER_DIR/stop-all.sh" ]; then
      "$CLUSTER_DIR/stop-all.sh" >/dev/null 2>&1 || true
    fi
    rm -rf "$WORKDIR"
  fi
}

write_compose_file() {
  mkdir -p "$WORKDIR"
  cat >"$COMPOSE_FILE" <<YAML
services:
  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.15.3
    environment:
      discovery.type: single-node
      xpack.security.enabled: "false"
      ES_JAVA_OPTS: -Xms512m -Xmx512m
    ports:
      - "${BACKEND_BIND_IP}:19200:9200"
    healthcheck:
      test: ["CMD-SHELL", "curl -fsS http://127.0.0.1:9200/_cluster/health >/dev/null"]
      interval: 5s
      timeout: 3s
      retries: 40

  mysql:
    image: mysql:8.4
    environment:
      MYSQL_ROOT_PASSWORD: rootpass
      MYSQL_DATABASE: fustor_demo
      MYSQL_USER: ${MYSQL_LOCAL_USERNAME}
      MYSQL_PASSWORD: ${MYSQL_LOCAL_PASSWORD}
    ports:
      - "${BACKEND_BIND_IP}:13306:3306"
    healthcheck:
      test: ["CMD-SHELL", "mysqladmin ping -h 127.0.0.1 -uroot -prootpass --silent"]
      interval: 5s
      timeout: 3s
      retries: 40

  minio:
    image: minio/minio:RELEASE.2025-04-22T22-12-26Z
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: ${S3_LOCAL_ACCESS_KEY_ID}
      MINIO_ROOT_PASSWORD: ${S3_LOCAL_SECRET_ACCESS_KEY}
    ports:
      - "${BACKEND_BIND_IP}:19000:9000"
      - "${BACKEND_BIND_IP}:19001:9001"
    healthcheck:
      test: ["CMD-SHELL", "curl -fsS http://127.0.0.1:9000/minio/health/live >/dev/null"]
      interval: 5s
      timeout: 3s
      retries: 40

  minio-mc:
    image: minio/mc:RELEASE.2025-04-16T18-13-26Z
    depends_on:
      minio:
        condition: service_healthy
    entrypoint: ["/bin/sh", "-c"]
    command: ["mc alias set local http://minio:9000 ${S3_LOCAL_ACCESS_KEY_ID} ${S3_LOCAL_SECRET_ACCESS_KEY} && mc mb --ignore-existing local/fustor-demo && mc cp /seed/s3/logs/*.json local/fustor-demo/logs/"]
    volumes:
      - "${SEED_DIR}:/seed:ro"
YAML
}

write_seed_files() {
  mkdir -p "$SEED_DIR/s3/logs"
  cat >"$SEED_DIR/es-bulk.ndjson" <<'JSON'
{"index":{"_index":"logs-local","_id":"1"}}
{"@timestamp":"2026-06-05T00:00:00Z","message":"local capanix es seed one","service":"fustor-demo","level":"info"}
{"index":{"_index":"logs-local","_id":"2"}}
{"@timestamp":"2026-06-05T00:01:00Z","message":"local capanix es seed two","service":"fustor-demo","level":"warn"}
JSON
  cat >"$SEED_DIR/s3/logs/event-1.json" <<'JSON'
{"id":1,"message":"local capanix s3 seed one","ts":"2026-06-05T00:00:00Z"}
JSON
  cat >"$SEED_DIR/s3/logs/event-2.json" <<'JSON'
{"id":2,"message":"local capanix s3 seed two","ts":"2026-06-05T00:01:00Z"}
JSON
  cat >"$SEED_DIR/mysql-seed.sql" <<SQL
CREATE DATABASE IF NOT EXISTS fustor_demo;
CREATE USER IF NOT EXISTS '${MYSQL_LOCAL_USERNAME}'@'%' IDENTIFIED BY '${MYSQL_LOCAL_PASSWORD}';
GRANT ALL PRIVILEGES ON fustor_demo.* TO '${MYSQL_LOCAL_USERNAME}'@'%';
USE fustor_demo;
CREATE TABLE IF NOT EXISTS events (
  id BIGINT PRIMARY KEY,
  created_at TIMESTAMP NOT NULL,
  message VARCHAR(255) NOT NULL,
  severity VARCHAR(32) NOT NULL
);
INSERT INTO events (id, created_at, message, severity) VALUES
  (1, '2026-06-05 00:00:00', 'local capanix mysql seed one', 'info'),
  (2, '2026-06-05 00:01:00', 'local capanix mysql seed two', 'warn')
ON DUPLICATE KEY UPDATE
  created_at = VALUES(created_at),
  message = VALUES(message),
  severity = VALUES(severity);
SQL
}

start_backends() {
  log "starting local ES/MySQL/MinIO backends"
  docker_compose up -d elasticsearch mysql minio
  wait_for_http "$ES_BASE_URL/_cluster/health" "Elasticsearch"
  wait_for_mysql
  wait_for_http "$S3_BASE_URL/minio/health/live" "MinIO"

  log "seeding backends"
  curl -fsS -H 'Content-Type: application/x-ndjson' \
    --data-binary "@$SEED_DIR/es-bulk.ndjson" \
    "$ES_BASE_URL/_bulk?refresh=true" >"$LOG_DIR/es-bulk.json"
  if ! grep -q '"errors":false' "$LOG_DIR/es-bulk.json"; then
    cat "$LOG_DIR/es-bulk.json" >&2
    die "Elasticsearch seed bulk request reported errors"
  fi
  docker_compose exec -T mysql mysql -h 127.0.0.1 -uroot -prootpass <"$SEED_DIR/mysql-seed.sql"
  docker_compose run --rm minio-mc >/dev/null
}

write_cluster_spec() {
  cat >"$WORKDIR/capanix-cluster.yaml" <<YAML
domain_id: local
nodes:
  - node_id: capanix-node-1
    addr: ${CLUSTER_BIND_IP}:19401
  - node_id: capanix-node-2
    addr: ${CLUSTER_BIND_IP}:19402
  - node_id: capanix-node-3
    addr: ${CLUSTER_BIND_IP}:19403
  - node_id: capanix-node-4
    addr: ${CLUSTER_BIND_IP}:19404
  - node_id: capanix-node-5
    addr: ${CLUSTER_BIND_IP}:19405
YAML
}

write_source_manifest() {
  local app="$1"
  local route_token="$2"
  local host_ref="$3"
  local suffix
  suffix="$(printf '%s' "$host_ref" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9]/_/g')"
  cat >"$MANIFEST_DIR/${app}.${host_ref}.yaml" <<YAML
ports:
  - route_token: ${route_token}
    use_port: fetch
    dispatch_tag: fetch
    pattern: request_reply
    route_key: ${app}.fetch.${suffix}:v1
    visibility: internal
    roles:
      - serve
      - use
  - route_token: ${route_token}
    use_port: status
    dispatch_tag: status
    pattern: request_reply
    route_key: ${app}.status.${suffix}:v1
    visibility: internal
    roles:
      - serve
      - use
YAML
}

write_union_graph_manifest() {
  local host_ref="$1"
  local suffix
  suffix="$(printf '%s' "$host_ref" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9]/_/g')"
  cat >"$MANIFEST_DIR/union-graph.${host_ref}.yaml" <<YAML
ports:
  - route_token: union-graph.internal
    use_port: query
    dispatch_tag: query
    pattern: request_reply
    route_key: union-graph.query.${suffix}:v1
    visibility: internal
    roles:
      - serve
      - use
  - route_token: union-graph.internal
    use_port: ingest
    dispatch_tag: ingest
    pattern: request_reply
    route_key: union-graph.ingest.${suffix}:v1
    visibility: internal
    roles:
      - serve
      - use
YAML
}

write_source_declarations() {
  local host_ref="capanix-node-1"
  mkdir -p "$MANIFEST_DIR" "$DECL_DIR"
  write_source_manifest "es-source" "es-source.internal" "$host_ref"
  write_source_manifest "mysql-source" "mysql-source.internal" "$host_ref"
  write_source_manifest "s3-source" "s3-source.internal" "$host_ref"
  write_union_graph_manifest "$host_ref"

  cat >"$DECL_DIR/es-source.yaml" <<YAML
schema_version: scope-worker-declaration-v1
target_id: es-source
target_generation: 1
workers:
  - worker_role: main
    worker_id: es-source-${host_ref}
    mode: embedded
    startup:
      path: ${ES_SOURCE_LIB}
      manifest: ${MANIFEST_DIR}/es-source.${host_ref}.yaml
    config:
      default_page_size: 500
      max_page_size: 5000
      endpoints:
        - object_ref: es-local
          endpoint_uri: ${ES_BASE_URL}
          index_scopes:
            - logs-*
          timestamp_field: "@timestamp"
          tie_breaker_field: _id
      credentials: []
    runtime:
      local_host_ref: ${host_ref}
      resource_grants:
        - resource_kind: elasticsearch
          object_ref: es-local
          endpoint_ref: local-es
          grant_state: active
          grant_epoch: 1
          interfaces:
            - read
          index_scopes:
            - logs-*
    policy:
      replicas: 1
route_plans: []
YAML

  cat >"$DECL_DIR/mysql-source.yaml" <<YAML
schema_version: scope-worker-declaration-v1
target_id: mysql-source
target_generation: 1
workers:
  - worker_role: main
    worker_id: mysql-source-${host_ref}
    mode: embedded
    startup:
      path: ${MYSQL_SOURCE_LIB}
      manifest: ${MANIFEST_DIR}/mysql-source.${host_ref}.yaml
    config:
      default_page_size: 500
      max_page_size: 5000
      snapshot_ttl_ms: 300000
      endpoints:
        - object_ref: mysql-local
          endpoint_uri: ${MYSQL_ENDPOINT_URI}
          credential_ref: mysql-local-reader
          schema_scopes:
            - fustor_demo
          table_scopes:
            - events
          primary_key: id
      credentials:
        - credential_ref: mysql-local-reader
          auth_type: basic_env
          username_env: MYSQL_LOCAL_USERNAME
          password_env: MYSQL_LOCAL_PASSWORD
    runtime:
      local_host_ref: ${host_ref}
      resource_grants:
        - resource_kind: mysql
          object_ref: mysql-local
          endpoint_ref: local-mysql
          grant_state: active
          grant_epoch: 1
          interfaces:
            - read
          schema_scopes:
            - fustor_demo
          table_scopes:
            - events
    policy:
      replicas: 1
route_plans: []
YAML

  cat >"$DECL_DIR/s3-source.yaml" <<YAML
schema_version: scope-worker-declaration-v1
target_id: s3-source
target_generation: 1
workers:
  - worker_role: main
    worker_id: s3-source-${host_ref}
    mode: embedded
    startup:
      path: ${S3_SOURCE_LIB}
      manifest: ${MANIFEST_DIR}/s3-source.${host_ref}.yaml
    config:
      default_page_size: 1000
      max_page_size: 10000
      endpoints:
        - object_ref: s3-local
          endpoint_uri: ${S3_BASE_URL}
          bucket: fustor-demo
          region: us-east-1
          prefix: logs/
          credential_ref: s3-local-reader
          bucket_scopes:
            - fustor-demo
          prefix_scopes:
            - logs/*
      credentials:
        - credential_ref: s3-local-reader
          auth_type: basic_env
          username_env: S3_LOCAL_ACCESS_KEY_ID
          password_env: S3_LOCAL_SECRET_ACCESS_KEY
    runtime:
      local_host_ref: ${host_ref}
      resource_grants:
        - resource_kind: s3
          object_ref: s3-local
          endpoint_ref: local-s3
          grant_state: active
          grant_epoch: 1
          interfaces:
            - read
          bucket_scopes:
            - fustor-demo
          prefix_scopes:
            - logs/*
    policy:
      replicas: 1
route_plans: []
YAML

  cat >"$DECL_DIR/union-graph.yaml" <<YAML
schema_version: scope-worker-declaration-v1
target_id: union-graph
target_generation: 1
workers:
  - worker_role: main
    worker_id: union-graph-${host_ref}
    mode: embedded
    startup:
      path: ${UNION_GRAPH_LIB}
      manifest: ${MANIFEST_DIR}/union-graph.${host_ref}.yaml
    config:
      bio_pipeline_mock:
        enabled: true
        schema_path: ${ROOT}/union-graph/fixtures/bio-mock/PPG_Schema.json
        union_schema_path: ${ROOT}/union-graph/fixtures/bio-mock/PPG_Union_Schema.json
        ppg_status_json_path: ${ROOT}/union-graph/fixtures/bio-mock/ppg-status.json
        observed_at: 1779984000000000
    runtime:
      local_host_ref: ${host_ref}
    policy:
      replicas: 1
route_plans: []
YAML
}

write_fsmeta_config() {
  cat >"$WORKDIR/fs-meta.local.yaml" <<YAML
api:
  facade_resource_id: fs-meta-tcp-listener
auth:
  bootstrap_management:
    username: admin
workers:
  source:
    mode: embedded
  sink:
    mode: embedded
YAML
}

build_binaries() {
  log "building Capanix CLI/daemon"
  cargo build --manifest-path "$CAPANIX_ROOT/Cargo.toml" -p capanix-cli -p capanix-daemon
  log "building fustor runtime libraries and tooling"
  cargo build --manifest-path "$ROOT/Cargo.toml" \
    -p fs-meta-runtime \
    -p es-source-runtime \
    -p mysql-source-runtime \
    -p s3-source-runtime \
    -p union-graph-runtime \
    --lib
  cargo build --manifest-path "$ROOT/Cargo.toml" -p fs-meta-tooling --bin fsmeta
  [ -x "$CNXCTL_BIN" ] || die "cnxctl binary not found at $CNXCTL_BIN"
  [ -x "$CAPANIXD_BIN" ] || die "capanixd binary not found at $CAPANIXD_BIN"
  [ -x "$FSMETA_BIN" ] || die "fsmeta binary not found at $FSMETA_BIN"
  [ -f "$FSMETA_LIB" ] || die "fs-meta runtime library not found at $FSMETA_LIB"
  [ -f "$ES_SOURCE_LIB" ] || die "es-source runtime library not found at $ES_SOURCE_LIB"
  [ -f "$MYSQL_SOURCE_LIB" ] || die "mysql-source runtime library not found at $MYSQL_SOURCE_LIB"
  [ -f "$S3_SOURCE_LIB" ] || die "s3-source runtime library not found at $S3_SOURCE_LIB"
  [ -f "$UNION_GRAPH_LIB" ] || die "union-graph runtime library not found at $UNION_GRAPH_LIB"
}

start_cluster() {
  log "scaffolding 5-node Capanix cluster under $CLUSTER_DIR"
  if [ -x "$CLUSTER_DIR/stop-all.sh" ]; then
    "$CLUSTER_DIR/stop-all.sh" >/dev/null 2>&1 || true
  fi
  "$CNXCTL_BIN" cluster scaffold --spec "$WORKDIR/capanix-cluster.yaml" --out "$CLUSTER_DIR" >/dev/null
  # shellcheck disable=SC1091
  source "$CLUSTER_DIR/admin.env"
  export CAPANIX_CTL_SK_B64 CAPANIX_ADMIN_SK_B64 CAPANIX_CTL_KEY_ID
  export CAPANIXD_BIN
  export MYSQL_LOCAL_USERNAME MYSQL_LOCAL_PASSWORD
  export S3_LOCAL_ACCESS_KEY_ID S3_LOCAL_SECRET_ACCESS_KEY

  log "starting 5 local Capanix nodes"
  for n in 1 2 3 4 5; do
    local node_dir="$CLUSTER_DIR/nodes/capanix-node-$n"
    start_node_direct "$node_dir"
    wait_for_socket \
      "$node_dir/home/core.sock" \
      "capanix-node-$n" \
      "$node_dir/daemon.pid" \
      "$node_dir/daemon.log" \
      60
  done
  SOCKET="$CLUSTER_DIR/nodes/capanix-node-1/home/core.sock"
  export SOCKET
  sleep 2
}

announce_demo_exports() {
  mkdir -p "$EXPORT_DIR"
  for n in 1 2 3 4 5; do
    mkdir -p "$EXPORT_DIR/node-$n"
    printf 'local capanix node %s export\n' "$n" >"$EXPORT_DIR/node-$n/README.txt"
    cnxctl resource announce \
      --node-id "capanix-node-$n" \
      --source "$EXPORT_DIR/node-$n" >/dev/null
  done
}

deploy_apps() {
  log "deploying fs-meta with embedded local workers"
  export CNXCTL_BIN CAPANIX_FS_META_APP_BINARY="$FSMETA_LIB"
  "$FSMETA_BIN" deploy \
    --socket "$SOCKET" \
    --actor-id local-admin \
    --domain-id local \
    --key-id "${CAPANIX_CTL_KEY_ID:-local-admin-ed25519-1}" \
    --admin-sk-b64 "$CAPANIX_CTL_SK_B64" \
    --config "$WORKDIR/fs-meta.local.yaml" >"$LOG_DIR/fsmeta-deploy.json"

  log "deploying es-source/mysql-source/s3-source/union-graph"
  cnxctl app apply "$DECL_DIR/es-source.yaml" >"$LOG_DIR/es-source-apply.json"
  cnxctl app apply "$DECL_DIR/mysql-source.yaml" >"$LOG_DIR/mysql-source-apply.json"
  cnxctl app apply "$DECL_DIR/s3-source.yaml" >"$LOG_DIR/s3-source-apply.json"
  cnxctl app apply "$DECL_DIR/union-graph.yaml" >"$LOG_DIR/union-graph-apply.json"
}

smoke_checks() {
  log "running smoke checks"
  cnxctl_cluster cluster status >"$LOG_DIR/cluster-status.json"
  cnxctl process list >"$LOG_DIR/process-list.json"
  curl -fsS "$ES_BASE_URL/logs-local/_count" >"$LOG_DIR/es-count.json"
  docker_compose exec -T mysql mysql -h 127.0.0.1 -u"$MYSQL_LOCAL_USERNAME" -p"$MYSQL_LOCAL_PASSWORD" \
    -D fustor_demo -e 'SELECT COUNT(*) AS events_count FROM events;' >"$LOG_DIR/mysql-count.txt"
  docker_compose run --rm minio-mc \
    "mc alias set local http://minio:9000 $S3_LOCAL_ACCESS_KEY_ID $S3_LOCAL_SECRET_ACCESS_KEY >/dev/null && mc ls local/fustor-demo/logs/" \
    >"$LOG_DIR/s3-ls.txt"
}

main() {
  need_cmd cargo
  need_cmd curl
  need_cmd docker

  if [ "$KEEP" != "1" ]; then
    cleanup_workdir
  fi
  mkdir -p "$WORKDIR" "$LOG_DIR"

  write_seed_files
  write_compose_file
  write_cluster_spec
  write_source_declarations
  write_fsmeta_config

  build_binaries
  start_backends
  start_cluster
  announce_demo_exports
  deploy_apps
  smoke_checks

  log "deployment complete"
  log "listen ip: $LISTEN_IP"
  log "cluster bind ip: $CLUSTER_BIND_IP"
  log "backend bind ip: $BACKEND_BIND_IP"
  log "workdir: $WORKDIR"
  log "control socket: $SOCKET"
  log "logs: $LOG_DIR"
  log "stop cluster: $CLUSTER_DIR/stop-all.sh"
  log "stop backends: docker compose -f $COMPOSE_FILE down -v --remove-orphans"
}

main "$@"

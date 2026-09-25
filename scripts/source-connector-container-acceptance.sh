#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESOURCE_GUARD="${FUSTOR_RESOURCE_GUARD:-$ROOT/scripts/resource-guard.sh}"
PROCESS_TREE_WATCHDOG="${FUSTOR_PROCESS_TREE_WATCHDOG:-$ROOT/scripts/process-tree-watchdog.sh}"
MAX_THREADS="${FUSTOR_SOURCE_ACCEPTANCE_MAX_THREADS:-512}"
MAX_CONTAINER_CPUS="${SOURCE_ACCEPTANCE_MAX_CONTAINER_CPUS:-3.50}"
HARD_MAX_CONTAINER_CPUS="3.50"
PREFLIGHT_ONLY="${SOURCE_ACCEPTANCE_PREFLIGHT_ONLY:-0}"
COMPOSE_FILE="$ROOT/tests/source-acceptance/docker-compose.yaml"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
PROJECT="fustor-source-${RUN_ID,,}"
PROJECT="${PROJECT//[^a-z0-9_-]/-}"
ARTIFACT_DIR="${SOURCE_ACCEPTANCE_ARTIFACT_DIR:-$ROOT/target/source-acceptance/$RUN_ID}"
KEEP="${SOURCE_ACCEPTANCE_KEEP:-0}"
STATUS=FAIL

mkdir -p "$ARTIFACT_DIR"
exec 3>&1
exec > >(tee -a "$ARTIFACT_DIR/acceptance.log") 2>&1

compose() {
  docker compose --project-name "$PROJECT" --file "$COMPOSE_FILE" "$@"
}

collect_logs() {
  compose logs --no-color --tail 200 postgres mysql elasticsearch minio \
    >"$ARTIFACT_DIR/container.log" 2>&1 || true
}

cleanup() {
  local exit_code=$?
  local final_line official_line
  collect_logs
  if [ "$KEEP" != "1" ]; then
    compose down --volumes --remove-orphans --timeout 15 >/dev/null 2>&1 || true
  fi
  final_line="SOURCE_ACCEPTANCE final_status=$STATUS exit_code=$exit_code artifacts=$ARTIFACT_DIR"
  official_line="SOURCE_ACCEPTANCE official_stable=not_touched hosts=198.51.100.144,198.51.100.145,198.51.100.146"
  printf '%s\n%s\n' "$final_line" "$official_line" >&3
  printf '%s\n%s\n' "$final_line" "$official_line" >>"$ARTIFACT_DIR/acceptance.log"
}
trap cleanup EXIT

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    printf 'SOURCE_ACCEPTANCE status=FAIL reason=missing-command command=%s\n' "$1" >&2
    exit 1
  }
}

host_port() {
  compose port "$1" "$2" | awk -F: 'NR == 1 { print $NF }'
}

run_source() {
  local source="$1"
  shift
  printf 'SOURCE_ACCEPTANCE source=%s status=RUNNING\n' "$source"
  if timeout 900 "$@"; then
    printf 'SOURCE_ACCEPTANCE source=%s status=PASS\n' "$source"
  else
    printf 'SOURCE_ACCEPTANCE source=%s status=FAIL\n' "$source" >&2
    return 1
  fi
}

run_guarded_source() {
  local source="$1"
  shift
  run_source "$source" /usr/bin/env \
    FUSTOR_PROCESS_TREE_MAX_THREADS="$MAX_THREADS" \
    "$PROCESS_TREE_WATCHDOG" "$RESOURCE_GUARD" "$@"
}

validate_container_cpu_limits() {
  local config_file="$ARTIFACT_DIR/effective-compose.json"
  if ! awk -v value="$MAX_CONTAINER_CPUS" -v maximum="$HARD_MAX_CONTAINER_CPUS" \
    'BEGIN { exit !(value ~ /^[0-9]+([.][0-9]+)?$/ && value > 0 && value <= maximum) }'; then
    printf 'SOURCE_ACCEPTANCE status=FAIL reason=unsafe-max-container-cpus requested=%s hard_max=%s\n' \
      "$MAX_CONTAINER_CPUS" "$HARD_MAX_CONTAINER_CPUS" >&2
    return 1
  fi
  compose config --format json >"$config_file"
  jq -e --argjson max_cpus "$MAX_CONTAINER_CPUS" '
    .services as $services
    | ["postgres", "mysql", "elasticsearch", "minio", "minio-mc"] as $required
    | ($required | all(.[]; . as $name | (($services[$name].cpus // 0) > 0)))
      and (($required | map($services[.].cpus) | add) <= $max_cpus)
  ' "$config_file" >/dev/null || {
    printf 'SOURCE_ACCEPTANCE status=FAIL reason=invalid-container-cpu-limits max_total_cpus=%s\n' \
      "$MAX_CONTAINER_CPUS" >&2
    return 1
  }
  printf 'SOURCE_ACCEPTANCE container_cpu_limit_total=%s max_total_cpus=%s\n' \
    "$(jq -r '[.services.postgres.cpus, .services.mysql.cpus, .services.elasticsearch.cpus, .services.minio.cpus, .services["minio-mc"].cpus] | add' "$config_file")" \
    "$MAX_CONTAINER_CPUS"
}

verify_running_container_cpu_limits() {
  local service container_id nano_cpus
  for service in postgres mysql elasticsearch minio; do
    container_id="$(compose ps --quiet "$service")"
    test -n "$container_id"
    nano_cpus="$(docker inspect --format '{{.HostConfig.NanoCpus}}' "$container_id")"
    if ! [[ "$nano_cpus" =~ ^[1-9][0-9]*$ ]]; then
      printf 'SOURCE_ACCEPTANCE status=FAIL reason=container-cpu-limit-not-enforced service=%s nano_cpus=%s\n' \
        "$service" "$nano_cpus" >&2
      return 1
    fi
    printf 'SOURCE_ACCEPTANCE service=%s nano_cpus=%s\n' "$service" "$nano_cpus"
  done
}

start_containers() {
  local attempt
  for attempt in 1 2 3; do
    if compose up --detach --wait --wait-timeout 240 postgres mysql elasticsearch minio; then
      return 0
    fi
    printf 'SOURCE_ACCEPTANCE infrastructure=docker-compose attempt=%s status=RETRY\n' "$attempt" >&2
    compose down --volumes --remove-orphans --timeout 15 >/dev/null 2>&1 || true
    sleep "$((attempt * 5))"
  done
  return 1
}

need_cmd cargo
need_cmd docker
need_cmd jq
need_cmd taskset
need_cmd timeout
test -x "$RESOURCE_GUARD"
test -x "$PROCESS_TREE_WATCHDOG"

printf 'SOURCE_ACCEPTANCE run_id=%s status=STARTING\n' "$RUN_ID"
printf 'SOURCE_ACCEPTANCE official_stable=not_touched hosts=198.51.100.144,198.51.100.145,198.51.100.146\n'

validate_container_cpu_limits
if [ "$PREFLIGHT_ONLY" = "1" ]; then
  STATUS=PASS
  printf 'SOURCE_ACCEPTANCE preflight_only=1 status=PASS\n'
  exit 0
fi
start_containers
verify_running_container_cpu_limits
compose run --rm minio-mc -c \
  'mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null && mc mb --ignore-existing local/fustor-source-acceptance >/dev/null'

PG_PORT="$(host_port postgres 5432)"
MYSQL_PORT="$(host_port mysql 3306)"
ES_PORT="$(host_port elasticsearch 9200)"
S3_PORT="$(host_port minio 9000)"
for value in "$PG_PORT" "$MYSQL_PORT" "$ES_PORT" "$S3_PORT"; do
  test -n "$value"
done

export FUSTOR_PG_SOURCE_ENDPOINT_URI="postgresql://127.0.0.1:${PG_PORT}/fustor_demo?sslmode=disable"
export FUSTOR_PG_SOURCE_USERNAME=fustor
export FUSTOR_PG_SOURCE_PASSWORD=fustorpass
export FUSTOR_PG_SOURCE_SCHEMA=public

export FUSTOR_MYSQL_SOURCE_ENDPOINT_URI="mysql://127.0.0.1:${MYSQL_PORT}/fustor_demo"
export FUSTOR_MYSQL_SOURCE_USERNAME=root
export FUSTOR_MYSQL_SOURCE_PASSWORD=rootpass
export FUSTOR_MYSQL_SOURCE_SCHEMA=fustor_demo

export FUSTOR_ES_SOURCE_ENDPOINT_URI="http://127.0.0.1:${ES_PORT}"

export FUSTOR_S3_SOURCE_ENDPOINT_URI="http://127.0.0.1:${S3_PORT}"
export FUSTOR_S3_SOURCE_ACCESS_KEY=minioadmin
export FUSTOR_S3_SOURCE_SECRET_KEY=minioadmin
export FUSTOR_S3_SOURCE_BUCKET=fustor-source-acceptance
export FUSTOR_S3_SOURCE_REGION=us-east-1

cd "$ROOT"
run_guarded_source local-fs cargo test -p local-fs-source-runtime --test native_bound_route -- --nocapture --test-threads=1
run_guarded_source csv cargo test -p csv-source-runtime --test native_bound_route -- --nocapture --test-threads=1
run_guarded_source jsonl cargo test -p jsonl-source-runtime --test native_bound_route -- --nocapture --test-threads=1
run_guarded_source postgresql cargo test -p pg-source-runtime --test native_pg_integration -- --ignored --nocapture --test-threads=1
run_guarded_source mysql cargo test -p mysql-source-runtime --test native_mysql_integration -- --ignored --nocapture --test-threads=1
run_guarded_source elasticsearch cargo test -p es-source-runtime --test native_es_integration -- --ignored --nocapture --test-threads=1
run_guarded_source s3 cargo test -p s3-source-runtime --test native_s3_integration -- --ignored --nocapture --test-threads=1
run_guarded_source source-to-data-view cargo test -p source-system-tests --test native_source_to_data_view_e2e -- --ignored --nocapture --test-threads=1

STATUS=PASS

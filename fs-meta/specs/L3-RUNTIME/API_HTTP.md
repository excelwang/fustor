---
version: 3.0.0
---

# L3 Runtime: fs-meta HTTP API Interfaces

Product-facing state names referenced in this file are owned by [STATE_MODEL.md](./STATE_MODEL.md). Query observation state, facade service state, group service state, rollout generation state, and node participation state MUST use that authority rather than implementation-local vocabulary.

## [interface] ApiNamespace

**Rationale**

保持 fs-meta 产品 API 命名空间稳定，确保独立 console、独立 `fsmeta` CLI 与自动化运维使用同一组产品化接口。

**Type Signature**

```text
ApiNamespace = "/api/fs-meta/v1"
```

1. Base path is `/api/fs-meta/v1`.
2. All endpoints except session login require bearer token.
3. Management endpoints accept management-session bearer tokens.
4. Query endpoints accept query-api-key bearer tokens.
5. API namespace is mandatory for fs-meta app startup; auth init failure or bind failure MUST fail startup explicitly.
6. Legacy product-management paths (`/auth/login`, `/config/roots`, `/ops/rescan`, `/release/render`, `/exports`, `/fanout`) are not part of the current public product API.

## [interface] SessionEndpoints

**Rationale**

保持登录入口极小且稳定，只承载产品访问令牌签发，不混入运行时策略语义。

**Type Signature**

```text
POST /session/login -> { token: string, expires_in_secs: u64, user: SessionUser }
```

1. `POST /session/login`
   1. request: `{ "username": string, "password": string }`
   2. response: `{ "token": string, "expires_in_secs": u64, "user": SessionUser }`

## [interface] StatusEndpoints

**Rationale**

保持 status 作为 resource-scoped facade 上的聚合读接口，统一暴露运行健康与授权根可见性。

**Type Signature**

```text
GET /status -> StatusResponse
```

1. `GET /status`
   1. response: source degraded roots, host-object-grant version, source coverage details, sink health/capacity stats, a `rollout` object whose `state` uses `RolloutGenerationState`, and a `facade` object whose `state` uses `FacadeServiceState` (`unavailable|pending|serving|degraded`) plus optional facade-pending diagnostics. These status/health surfaces are the current fs-meta observation-evidence boundary for materialized-query readiness, `GroupServiceState`, `FacadeServiceState`, `RolloutGenerationState`, and `NodeParticipationState`; they do not rely on legacy revision-pair status fields.
   2. source payload MUST expose per logical-root `service_state` plus coverage summary (`matched_grants`, `active_members`, `coverage_mode`) and per concrete-root `participation_state` plus operational evidence (`root_key`, `object_ref`, `watch_lru_capacity`, `audit_interval_ms`, `overflow_count`, `overflow_pending`, `rescan_pending`, `last_rescan_reason`, `last_error`, `last_audit_started_at_us`, `last_audit_completed_at_us`, `last_audit_duration_ms`). Source lifecycle text MAY remain in `source.debug.lifecycle_state` as diagnostic evidence only.
   3. sink payload MUST expose aggregate counts plus per-group capacity and materialization-evidence details (`estimated_heap_bytes`, `groups[].estimated_heap_bytes`, `groups[].shadow_lag_us`, `groups[].overflow_pending_materialization`, `groups[].initial_audit_completed`, `groups[].materialization_readiness`) so large-NFS deployments can distinguish liveness from coverage pressure, first-audit completion, and materialized catch-up readiness. `groups[].materialization_readiness` is evidence beneath `GroupServiceState`, not a competing service-state vocabulary.
   4. when facade activation is pending or retrying, status MAY expose `facade.pending` with `route_key`, `generation`, `resource_ids`, `runtime_exposure_confirmed`, `reason`, `retry_attempts`, `pending_since_us`, and optional retry diagnostics such as `last_error`, `last_attempt_at_us`, `last_error_at_us`, `retry_backoff_ms`, and `next_retry_at_us`.
   5. empty-roots deployment is valid; status must still report service health instead of configuration failure.

## [interface] RuntimeGrantEndpoints

**Rationale**

把 runtime grants 暴露为产品可读真值，确保在线 scope 选择基于当前授权资源而不是历史导出清单。

**Type Signature**

```text
GET /runtime/grants -> { grants: GrantedMountRoot[] }
```

1. `GET /runtime/grants`
   1. response: current runtime-injected `host_object_grants` visible to fs-meta.
   2. each grant carries `object_ref`, `host_ref`, `host_ip`, `mount_point`, `fs_source`, and `fs_type`.

## [interface] MonitoringRootEndpoints

**Rationale**

把业务 scope 选择明确放到 monitoring roots 管理接口，避免部署 bootstrap 配置重新承载在线业务策略。

**Type Signature**

```text
GET  /monitoring/roots         -> { roots: RootEntry[] }
POST /monitoring/roots/preview -> { preview: RootPreviewRow[], unmatched_roots: string[] }
PUT  /monitoring/roots         -> { roots_count: usize }
```

1. `GET /monitoring/roots`
   1. response: current logical roots.
2. `POST /monitoring/roots/preview`
   1. request: `{ "roots": RootEntry[] }`
   2. response: per-root matched grants + resolved monitor paths.
   3. empty `roots` is valid and returns an empty preview.
3. `PUT /monitoring/roots`
   1. request: `{ "roots": RootEntry[] }`
   2. response: `{ "roots_count": usize }`
   3. validation: reject duplicated/empty `id`; reject legacy `path` and `source_locator`; allow empty `roots`; each non-empty root must provide at least one selector field and an absolute `subpath_scope`.
   4. write guard: server MUST revalidate each submitted root against the current runtime grants and reject the whole write when any submitted root has no current grant match; the error response MUST keep the unmatched root ids explicit.

## [interface] IndexEndpoints

**Rationale**

把重扫保持为显式管理写接口，避免 query/read 路径隐式触发重型生命周期动作。

**Type Signature**

```text
POST /index/rescan -> { accepted: true }
```

1. `POST /index/rescan`
   1. response: `{ "accepted": true }`
   2. write access requires `admin`.

## [interface] QueryApiKeyManagementEndpoints

**Rationale**

保持 query api key 生命周期属于管理面接口，但它服务的数据面凭据与管理 session 明确分离。

**Type Signature**

```text
GET    /query-api-keys         -> { keys: QueryApiKeySummary[] }
POST   /query-api-keys         -> { api_key: string, key: QueryApiKeySummary }
DELETE /query-api-keys/:key_id -> { revoked: bool }
```

1. `GET /query-api-keys`
   1. response: `{ "keys": QueryApiKeySummary[] }`
   2. each summary uses `{ key_id, label, created_at_us }`.
2. `POST /query-api-keys`
   1. request: `{ "label": string }`
   2. response: `{ "api_key": string, "key": QueryApiKeySummary }`
   3. `api_key` is only returned on create; list/read surfaces do not re-expose the secret.
3. `DELETE /query-api-keys/:key_id`
   1. response: `{ "revoked": bool }`
4. all query-api-key management endpoints require management-session `admin` access and are not accepted with query-api-key bearer tokens.

## [interface] ProjectionQueryEndpoints

**Rationale**

保持 query/find HTTP 外观稳定，确保产品管理面与数据面分离。

**Type Signature**

```text
GET /tree                 -> QueryHttpResponse
GET /on-demand-force-find -> QueryHttpResponse
GET /stats                -> StatsHttpResponse
GET /bound-route-metrics  -> BoundRouteMetricsResponse
```

1. `GET /tree`
   1. query: `path` (UTF-8 convenience path, default `/`) or `path_b64` (authoritative base64url raw-path bytes, mutually exclusive with `path`), `recursive` (default `true`), `pit_id` (optional on first page, required on continuation), `group_order` (`group-key|file-count|file-age`, default `group-key`), `group_page_size` (default `64`, range `1..=1000`), `group_after` (opaque PIT cursor, optional), `entry_page_size` (default `1000`, range `1..=10000`), `entry_after` (opaque per-group PIT cursor bundle, optional), `read_class` (`fresh|materialized|trusted-materialized`, default `trusted-materialized`).
   2. response: `{ "path": string, "status": "ok", "read_class": ReadClass, "observation_status": ObservationStatus, "group_order": GroupOrder, "pit": PitHandle, "groups": TreeGroupEnvelope[], "group_page": GroupPage }`, plus optional `path_b64` when the authoritative raw path is not valid UTF-8.
2. `GET /on-demand-force-find`
   1. query: `path` (UTF-8 convenience path, default `/`) or `path_b64` (authoritative base64url raw-path bytes, mutually exclusive with `path`), `recursive` (default `true`), `pit_id` (optional on first page, required on continuation), `group_order` (`group-key|file-count|file-age`, default `group-key`), `group_page_size` (default `64`, range `1..=1000`), `group_after` (opaque PIT cursor, optional), `entry_page_size` (default `1000`, range `1..=10000`), `entry_after` (opaque per-group PIT cursor bundle, optional).
   2. response: `{ "path": string, "status": "ok", "read_class": "fresh", "observation_status": ObservationStatus, "group_order": GroupOrder, "pit": PitHandle, "groups": ForceFindGroupEnvelope[], "group_page": GroupPage }`, plus optional `path_b64` when the authoritative raw path is not valid UTF-8.
3. `GET /stats`
   1. query: `path` (UTF-8 convenience path, default `/`) or `path_b64` (authoritative base64url raw-path bytes, mutually exclusive with `path`), `recursive` (default `true`), `group` (optional), `read_class` (`fresh|materialized|trusted-materialized`, default `trusted-materialized`).
   2. response: `{ "path": string, "read_class": ReadClass, "observation_status": ObservationStatus, "groups": { "<group>": GroupEnvelopeStats } }`, plus optional `path_b64` when the authoritative raw path is not valid UTF-8.
4. `GET /bound-route-metrics`
   1. response: `{ "call_timeout_total": u64, "correlation_mismatch_total": u64, "uncorrelated_reply_total": u64, "recv_loop_iterations": u64, "pending_calls": usize }`.
5. `GET /tree` payload details
   1. `observation_status` keys: `state`, `reasons`.
   2. `observation_status.state` domain for `/tree` is the `QueryObservationState` subset `fresh-only|materialized-untrusted|trusted-materialized` from `STATE_MODEL.md`.
   3. `stability` keys remain `mode`, `state`, `quiet_window_ms`, `observed_quiet_for_ms`, `remaining_ms`, `blocked_reasons`.
   4. `stability.state` domain for tree groups remains `not-evaluated|stable|unstable|unknown|degraded`.
   5. `pit` uses `{ id, expires_at_ms }`.
   6. `group_page` uses `{ returned_groups, has_more_groups, next_cursor, next_entry_after }`.
   7. `meta` MUST include `read_class` and `metadata_available`; when metadata is withheld it MUST include `withheld_reason`.
   8. `root` is present only when a group's `meta.metadata_available=true`; it uses `{ path, size, modified_time_us, is_dir, exists, has_children }`, plus optional `path_b64` when the authoritative raw bytes are not valid UTF-8.
   9. `entries` is present only when a group's `meta.metadata_available=true`; each entry uses `{ path, depth, size, modified_time_us, is_dir, has_children }`, plus optional `path_b64` when the authoritative raw bytes are not valid UTF-8.
   10. `entry_page` is present only when a group's `meta.metadata_available=true`; it uses `{ order:"path-lex", page_size, returned_entries, has_more_entries, next_cursor }`.
   11. `group_after` and `entry_after` cursors MUST be treated as opaque by clients; any continuation using them MUST also send `pit_id`. Expired PIT returns explicit `PIT_EXPIRED`; the server does not expose materialized-revision stale cursors on the public contract.
   12. `path_b64` is the authoritative bytes-safe path field and appears only when the underlying raw bytes are not valid UTF-8. `path` remains the default display-only UTF-8/lossy rendering for convenience.
   13. `read_class=materialized` MAY return while `observation_status.state=materialized-untrusted`; `read_class=trusted-materialized` returns explicit `NOT_READY` until the same package-local observation evidence reaches trusted state.
6. `GET /on-demand-force-find` payload details
   1. `observation_status.state` MUST stay `fresh-only`, which is the live/fresh member of `QueryObservationState` in `STATE_MODEL.md`.
   2. `stability` keys use the same object shape as `/tree`, but `state` MUST stay `not-evaluated` and `mode` MUST stay `none`.
   3. `meta.read_class` is always `fresh`; `meta.metadata_available` is always `true`.
   4. each returned group item owns its own `root`, `entries`, and `entry_page`, using the same field shapes as `/tree`, including optional `path_b64` when display strings would be lossy.
   5. `group_after` and `entry_after` cursors MUST be treated as opaque PIT cursors by clients; after the first page, force-find continuation reuses frozen PIT results instead of rerunning the live query.
   6. `/on-demand-force-find` remains a freshness path and is not blocked by the trusted-materialized observation gate that protects `/tree` and `/stats`.
7. `PitHandle`
   1. required keys: `id`, `expires_at_ms`.
   2. `id` is an opaque server-issued query session id; clients do not derive meaning from it.
## [decision] QueryAvailabilityWindowOrdering

1. `/on-demand-force-find` is the widest query-side availability window in the public API. It is a freshness path and MUST NOT be blocked solely because trusted-materialized observation is not yet available.
2. `/tree` and `/stats` with `read_class=materialized` expose a middle availability window: they MAY answer while `observation_status.state=materialized-untrusted`, but they MUST NOT answer when `observation_status.state=unavailable`.
3. `/tree` and `/stats` with `read_class=trusted-materialized` expose the narrowest availability window: they MUST fail closed with explicit `NOT_READY` whenever `observation_status.state` is not `trusted-materialized`.
4. Default `/tree` and `/stats` behavior inherits the narrowest window because their default `read_class` is `trusted-materialized`.
5. The public availability ordering is contractual: `/on-demand-force-find` availability ⊇ materialized `/tree|/stats` availability ⊇ trusted-materialized `/tree|/stats` availability.
6. Status, degraded evidence, or diagnostics MAY explain why a narrower window is currently closed, but they MUST NOT collapse the wider `/on-demand-force-find` window into the trusted-materialized gate unless the facade itself or fresh source execution is unavailable.

8. `GroupEnvelopeStats` (used by `/stats`)
   1. required keys: `status`, `errors`; `members` key MUST be absent.
   2. when `status=ok`: envelope MUST include `data` with `total_nodes/total_files/total_dirs/total_size/latest_file_mtime_us/attested_count/blind_spot_count`, and `partial_failure`.
   3. when `status=error`: envelope MUST include `message`; `data` key MUST be absent.
   4. `read_class=materialized` MAY return while `observation_status.state=materialized-untrusted`; `read_class=trusted-materialized` returns explicit `NOT_READY` until the same package-local observation evidence reaches trusted state.

## [decision] AuthFileFormat

1. passwd file line format: `username:uid:gid:groups_csv:home:shell:locked_flag`.
2. shadow file line format: `username:password_hash:disabled_flag`.
3. comment/blank lines are ignored.
4. any format violation returns explicit bad-request auth configuration error.

## [decision] ManagementIngressSeparation

1. product-management API lives on `/session`, `/status`, `/runtime/grants`, `/monitoring/roots`, `/index/rescan`, and `/query-api-keys`, all under the resource-scoped one-cardinality domain facade owned by fs-meta app package boundary.
2. file-tree query semantics remain on projection router endpoints (`/tree`, `/stats`, `/on-demand-force-find`, `/bound-route-metrics`) and are accessed with query api keys rather than management sessions.
3. product docs and UI must treat runtime grants as the source for root selection, not legacy exports/fanout diagnostics.

## [decision] ProductConfigSurfaceSplit

1. deploy-time product config is thin bootstrap config (`api/auth`) only.
2. fs-meta consumes shared upstream config-loading, daemon/bootstrap, and runtime intent boundaries for config loading, manifest discovery, and relation-target compilation; the product API does not redefine those platform-owned semantics.
3. runtime grants plus monitoring-roots APIs own online business monitoring scope.
4. query-shaping knobs stay on query-path parameters (`path/pit_id/group_order/group_page_size/group_after/entry_page_size/entry_after/recursive/read_class`), not in deploy bootstrap config.
5. internal desired-state/runtime policy fields are generated deployment details, not operator business parameters.

## [decision] ProductAccessBoundary

1. management session can access `/status`, `/runtime/grants`, `/monitoring/roots`, `/monitoring/roots/preview`, `PUT /monitoring/roots`, `POST /index/rescan`, and query-api-key management endpoints.
2. management session bearer tokens are rejected on `/tree`, `/stats`, `/on-demand-force-find`, and `/bound-route-metrics`.
3. query api keys are rejected on management endpoints.

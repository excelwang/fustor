---
version: 3.0.0
---

# L1: fs-meta Domain Contracts

> Subject: `fs-meta System` at domain boundaries.
> Pattern: `[fs-meta System] MUST ...`
> Traceability: this file defines fs-meta-owned contracts while consuming
> upstream owner constraints from root `capanix/specs` and the directly used
> Capanix module specs.

---

## CONTRACTS.DOMAIN_IDENTITY

1. **DOMAIN_CONSTITUTION_SCOPE_DISCIPLINE**: **fs-meta System** MUST keep domain-level specs focused on externally observable contracts and cross-component ownership.
   > Covers L0: VISION.DOMAIN_IDENTITY.FS_META_DOMAIN_CONSTITUTION

2. **ROLE_BOUNDARY_ENFORCEMENT**: **fs-meta System** MUST expose one app package boundary to runtime/deployment while encapsulating source/sink partitioning as implementation detail.
   > Covers L0: VISION.DOMAIN_IDENTITY.MODULE_ROLE_BOUNDARY
   > Verification: fs-meta is treated as one downstream app product container, not as a generic reusable shared library package.

3. **META_INDEX_DOMAIN_OWNERSHIP**: **fs-meta System** MUST keep `meta-index` lifecycle and state ownership inside fs-meta domain services, not kernel business state.
   > Covers L0: VISION.DOMAIN_IDENTITY.META_INDEX_DOMAIN_OWNERSHIP

---

## CONTRACTS.KERNEL_RELATION

1. **KERNEL_DOMAIN_NEUTRAL_CONSUMPTION**: **fs-meta System** MUST consume only kernel thin runtime ABI plus generic route/channel-attach mechanisms, without requiring fs-meta-specific kernel primitives or kernel-owned fs-meta protocol verb interfaces.
   > Covers L0: VISION.KERNEL_RELATION.GENERIC_KERNEL_MECHANISM_CONSUMPTION
   > Verification: fs-meta does not treat `kernel-api` as the canonical ordinary app-facing runtime boundary; any remaining low-level primitive usage is explicit and secondary to the SDK-family path `managed-state-sdk` declarations/evaluator -> `service-sdk` authoring -> `runtime-entry-sdk` -> `app-sdk` -> `runtime-api`.

2. **HOST_DESCRIPTOR_GROUPING_IS_DOMAIN_POLICY**: **fs-meta System** MUST implement grouping from app-owned host descriptor selectors and object descriptors and MUST NOT require kernel/runtime hardcoded grouping semantics.
   > Covers L0: VISION.KERNEL_RELATION.POLICY_OUTSIDE_KERNEL_FOR_GROUPING

3. **HOST_ADAPTER_SDK_TRANSLATION_BOUNDARY**: **fs-meta System** MUST delegate host-local facade/ABI adaptation to generic host-adapter SDK boundaries, including directory watch/listen primitives.
   > Covers L0: VISION.KERNEL_RELATION.HOST_ADAPTER_SDK_TRANSLATION_OWNERSHIP
   > Responsibility: keep host-local adaptation semantics and distributed host-operation-contract avoidance out of domain code paths.
   > Verification: source watcher uses host-adapter watch/session boundaries instead of directly importing host watch libraries (for example inotify API types).
   > Verification: fs-meta domain crate dependency boundary MUST NOT directly depend on host watch library crates (for example `inotify`); host-specific watch backends remain in host-adapter-sdk.
   > Verification: source root-runtime assembly resolves one public `HostFsFacade` per granted host object via host-adapter-sdk and does not branch on local-vs-remote backend construction inline or model host calls as a distributed host-operation enumeration interface.
4. **RESOURCE_BOUND_SOURCE_LOCAL_HOST_PROGRAMMING**: **fs-meta System** MUST keep resource-bound source logic programming to public host-fs facade on the bound host and MUST NOT model host operations as distributed host-operation forwarding contract.
   > Covers L0: VISION.KERNEL_RELATION.RESOURCE_BOUND_LOCAL_HOST_PROGRAMMING
   > Responsibility: preserve local resource execution while keeping cross-node coordination in runtime/kernel control relations only.
   > Verification: startup and adapter-boundary workflows describe source-side local execution consuming granted mount-root objects through public host-fs facade on the bound host; fs-meta specs do not require route-transparent remote host-operation interfaces.
5. **THIN_RUNTIME_ABI_CONSUMPTION**: **fs-meta System** MUST consume only thin runtime ABI state (`host_object_grants`, run context, generation/lease, control events, channel hooks) at its app/runtime boundary.
   > Covers L0: VISION.KERNEL_RELATION.THIN_RUNTIME_ABI_CONSUMPTION
   > Responsibility: keep fs-meta below runtime orchestration and above domain protocol ownership.
   > Verification: fs-meta consumes bounded app-facing runtime, service, and stateful-observation surfaces above the low-level kernel mirror; exact helper-layer topology and dependency allowlists remain engineering-governance material.
6. **APP_OWNS_OPAQUE_PORT_MEANING**: **fs-meta System** MUST keep query/find/source/sink rendezvous naming and protocol meaning app-owned over opaque channels.
   > Covers L0: VISION.KERNEL_RELATION.APP_OWNS_OPAQUE_PORT_MEANING
   > Responsibility: prevent kernel/runtime/adapter layers from becoming owners of fs-meta protocol semantics.
   > Verification: fs-meta runs use and serve app-owned opaque ports for query/find/source/sink coordination, while kernel/runtime only resolve route/channel attachment and do not enumerate fs-meta protocol verbs.
7. **APP_DEFINED_GROUPS_RUNTIME_BIND_RUN**: **fs-meta System** MUST define groups in app/domain config and allow runtime to realize unit bind/run on those groups without interpreting group formation semantics.
   > Covers L0: VISION.KERNEL_RELATION.POLICY_OUTSIDE_KERNEL_FOR_GROUPING
   > Responsibility: keep grouping policy in fs-meta while allowing runtime to realize per-group unit bind/run realization.
   > Verification: group membership is computed from app config plus runtime-injected host object descriptors; runtime receives opaque groups and realizes per-group source/sink execution against those groups without upgrading any internal desired-state wiring into product vocabulary.
   > Verification: runtime host routing is resolved from `object_ref` and fs-meta app code does not parse node/member identifiers for host-fs calls.
   > Verification: single mount-root init/watch failure is isolated to that object's local execution partition and does not block other group pipelines.
8. **SOURCE_PRIMARY_EXECUTOR_APP_OWNED**: **fs-meta System** MUST keep the source-primary selection rule inside fs-meta app logic while leaving bind/run admission, execution gating, and generation/fencing semantics under runtime ownership.
   > Covers L0: VISION.KERNEL_RELATION.POLICY_OUTSIDE_KERNEL_FOR_GROUPING
   > Responsibility: keep per-group audit/sentinel authority in fs-meta domain without promoting fs-meta app code into runtime-owned execution policy.
   > Verification: fs-meta selects one bound source run per group using an app-owned deterministic rule.
   > Verification: runtime still owns whether a selected source run is admitted, activated, deactivated, fenced, or rebound; fs-meta only consumes the admitted runtime unit context and bound group realization.
   > Verification: all source members in the same group keep realtime watch/listen pipelines enabled when `watch=true`; periodic audit/sentinel loop authority is enabled only on that group's source-primary executor.
   > Verification: metadata from all members in the group is emitted to sink-side tree construction path; non-primary source instances are not excluded from data channel.

---

## CONTRACTS.QUERY_OUTCOME

1. **DUAL_QUERY_PATH_AVAILABILITY**: **fs-meta System** MUST expose both `query` (materialized) and `find` (fresh/live) user paths.
   > Covers L0: VISION.QUERY_OUTCOME.QUERY_PATH_AVAILABILITY, VISION.QUERY_OUTCOME.FIND_PATH_AVAILABILITY

2. **QUERY_PATH_OUTPUT_DISCIPLINE**: **fs-meta System** MUST keep query-path observation semantics consistent while allowing `/tree` and `/on-demand-force-find` to use path-specific response envelopes.
   > Responsibility: keep externally visible observations stable without confusing them with authoritative truth or forcing materially different paths into one fake uniform payload shape.
   > Verification: `/tree` returns a grouped envelope with top-level `path/status/read_class/observation_status/group_order/groups/group_page`; each group item carries its own `group/status/reliable/unreliable_reason/stability/meta` and optional `root/entries/entry_page`.
   > Verification: `/on-demand-force-find` remains a freshness path and is semantically equivalent to `/tree?read_class=fresh`; its per-group `stability` shape stays explicit while top-level `observation_status.state` remains `fresh-only`.
   > Verification: grouped response keys remain location-agnostic logical ids (for example `nfs1`) and MUST NOT expose absolute host mount paths.
   > Verification: returned envelopes are observation/projection outputs that may lag current authoritative truth; they do not redefine authoritative journal state by being readable.
   > Covers L0: VISION.QUERY_OUTCOME.UNIFIED_FOREST_RESPONSE, VISION.OBSERVATION_CONVERGENCE.OBSERVATION_IS_NOT_TRUTH

3. **EXPLICIT_QUERY_STATUS_AND_METADATA_AVAILABILITY**: **fs-meta System** MUST represent observation availability explicitly instead of silently dropping failed or withheld results.
   > Covers L0: VISION.QUERY_OUTCOME.PARTIAL_FAILURE_ISOLATION
   > Verification: `/tree` and `/stats` always expose explicit top-level `observation_status`; `trusted-materialized` refusal is reported as structured `NOT_READY` instead of silently downgrading to a weaker read mode.
   > Verification: grouped tree responses keep explicit `meta.metadata_available`; withheld metadata uses explicit `withheld_reason` instead of silently dropping a returned group bucket from the response.
   > Verification: grouped query paths keep explicit per-group `ok`/`error` envelopes and `partial_failure/errors` visibility instead of silently dropping failed groups.

4. **ROUTE_SAFE_MATERIALIZED_TREE_WINDOWS**: **fs-meta System** MUST keep internal materialized `/tree` route replies bounded by encoded payload bytes while preserving public PIT paging correctness.
   > Covers L0: VISION.QUERY_OUTCOME.ROUTE_SAFE_MATERIALIZED_TREE_WINDOWS
   > Responsibility: the app-owned query protocol slices large materialized tree reads into byte-budgeted same-PIT/same-group windows instead of relying on transport/kernel layers to split opaque fs-meta payloads.
   > Verification: a short positive internal tree reply caused by the encoded payload budget is not treated as end-of-group; the facade continues fetching bounded windows until the requested public page plus sentinel is available, a zero-entry continuation proves completion, or the caller deadline expires.
   > Verification: if one tree entry cannot fit the route byte budget, fs-meta MUST fail the internal materialized query closed instead of publishing an over-budget opaque route event or silently dropping that entry.

5. **CORRELATION_ID_CONTINUITY**: **fs-meta System** MUST preserve request-response correlation metadata on query paths.
   > Covers L0: VISION.QUERY_OUTCOME.QUERY_TRANSPORT_DIAGNOSTICS

6. **GROUP_ORDER_MULTI_GROUP_BUCKET_SELECTION**: **fs-meta System** MUST expose group bucket ordering as an explicit query axis instead of hiding it behind a single-group winner selection knob.
   > Covers L0: VISION.QUERY_OUTCOME.GROUP_ORDER_MULTI_GROUP_QUERY
   > Responsibility: keep group ranking deterministic while avoiding API-level “pick exactly one winner” semantics.
   > Verification: `/tree` and `/on-demand-force-find` accept `group_order=group-key|file-count|file-age`; default is `group-key`.
   > Verification: clients that want one group use `group_page_size=1`; the server does not require `group=<id>` or `best=true`.
   > Verification: when `group_order=file-count`, groups are ordered by highest file count; when `group_order=file-age`, groups are ordered by newest file activity (`latest_file_mtime_us`); ties break deterministically by group key.

7. **QUERY_PARAMETER_AXES_REMAIN_ORTHOGONAL**: **fs-meta System** MUST keep group ordering, PIT session ownership, bucket pagination, entry pagination, and named read selection orthogonal.
   > Covers L0: VISION.QUERY_OUTCOME.QUERY_HTTP_FACADE
   > Responsibility: let callers freely combine group selection (`group`), bounded traversal depth (`max_depth`), group ordering (`group_order`), PIT session continuation (`pit_id`), group bucket pagination (`group_page_size`, `group_after`), per-group entry pagination (`entry_page_size`, `entry_after`), and named read selection (`read_class`) without hidden semantic coupling.
   > Verification: `group` selects a group, `max_depth` bounds traversal depth, `group_order` affects only group ranking, `pit_id` only binds one frozen query session, `group_page_size/group_after` affect only bucket slicing, `entry_page_size/entry_after` affect only per-group entry slicing, and `read_class` affects only freshness/trust semantics.
   > Verification: a materialized `/tree` PIT freezes query scope, read class, observation status, group ranking, and the PIT-owned entry cursor stream; it MUST NOT require the facade to fetch or store a full subtree payload merely to prove that later entry pages exist, and the facade may assemble one public entry page from multiple smaller same-PIT/same-group sink windows.
   > Verification: `group_order=file-age` does not implicitly request `trusted-materialized`, and `read_class=trusted-materialized` does not redefine group ranking.

8. **FORCE_FIND_GROUP_LOCAL_EXCLUSIVITY**: **fs-meta System** MUST keep `force-find` execution exclusive per group while allowing any bound source run in that group to perform the scan.
   > Responsibility: avoid duplicate diagnostic scans without overloading runtime with transient lease semantics.
   > Verification: each group keeps one in-flight `force-find` at a time through app-owned mutex/state.
   > Verification: runner selection uses round-robin across bound source runs in the target group, and failed runner selection falls back to the next bound source run.
   > Verification: `file-count` and `file-age` ordering paths MAY use lightweight phase-1 stats probe; they MUST NOT require full fanout materialized tree payload just to rank groups.
   > Covers L0: VISION.QUERY_OUTCOME.GROUP_ORDER_MULTI_GROUP_QUERY

9. **FORCE_FIND_CURRENT_EPOCH_RUNNER_SELECTION**: **fs-meta System** MUST select `force-find` runners from Runner Binding Evidence that belongs to the current Authority Epoch.
   > Responsibility: keep freshness execution tied to the current business monitoring scope without waiting for materialized observation catch-up.
   > Verification: Runner Binding Evidence MAY be cached, but cached evidence is valid only while roots signature, grants signature, source stream generation, sink materialization generation, and facade/runtime generation still belong to the same Authority Epoch.
   > Verification: fs-meta discards runner binding evidence from older Authority Epochs before dispatching a fresh scan.
   > Covers L0: VISION.QUERY_OUTCOME.FIND_PATH_AVAILABILITY, VISION.OBSERVATION_CONVERGENCE.OBSERVATION_IS_NOT_TRUTH

10. **FORCE_FIND_MATERIALIZATION_INDEPENDENCE**: **fs-meta System** MUST NOT block an otherwise Force-Find Ready request on source initial audit completion, sink materialization readiness, or trusted materialized observation eligibility.
   > Responsibility: preserve `/on-demand-force-find` as the widest query-side availability window.
   > Verification: a group can be Force-Find Ready while `/tree` or `/stats` with `read_class=trusted-materialized` still returns explicit `NOT_READY`.
   > Verification: `force-find` results keep `read_class=fresh` and `observation_status.state=fresh-only`; they do not promote materialized observation to trusted state.
   > Covers L0: VISION.QUERY_OUTCOME.FIND_PATH_AVAILABILITY

11. **FORCE_FIND_GROUP_BOUNDARY**: **fs-meta System** MUST dispatch `force-find` only to runner candidates bound to the requested group in the current Authority Epoch.
   > Responsibility: keep group isolation, ownership visibility, and failure attribution intact.
   > Verification: if a requested group has no current runner candidate, fs-meta returns an explicit group-level unavailable/error result instead of borrowing a runner from another group.
   > Verification: multi-group `force-find` keeps successful groups independent from groups that are unbound or runner-unavailable.
   > Covers L0: VISION.QUERY_OUTCOME.UNIFIED_FOREST_RESPONSE, VISION.QUERY_OUTCOME.GROUP_ORDER_MULTI_GROUP_QUERY

12. **FORCE_FIND_SELECTED_RUNNER_SINGLE_REPLY**: **fs-meta System** MUST finish each selected group `force-find` from that group's selected runner outcome, not from a later collection of unrelated runners or groups.
   > Responsibility: keep selected-group fresh lookup as one group, one selected runner, one fresh outcome.
   > Verification: after fs-meta selects a runner for a Force-Find Ready group and receives selected-runner execution evidence, that group bucket completes on that selected runner's success, explicit failure, or execution timeout.
   > Verification: if the selected route produces no selected-runner execution evidence because the request/reply lane times out or reports a retryable delivery gap before the runner accepts the fresh scan, fs-meta MAY re-plan inside the same group and original caller budget; this is delivery recovery, not materialized fallback.
   > Verification: fs-meta MUST NOT keep a selected group open only to wait for replies from unrelated groups, and any same-group fallback collection must keep waiting until the requested group payload or explicit requested-group error is observed.
   > Verification: multi-group `force-find` MAY assemble several independent group buckets, but each bucket still follows its own Selected-Runner Fresh Execution outcome.
   > Verification: runtime/kernel transport carries the selected fresh execution exchange but does not decide group ownership or runner selection policy.
   > Covers L0: VISION.QUERY_OUTCOME.FIND_PATH_AVAILABILITY, VISION.QUERY_OUTCOME.PARTIAL_FAILURE_ISOLATION

13. **FORCE_FIND_EXPLICIT_UNAVAILABLE**: **fs-meta System** MUST represent unavailable `force-find` execution as explicit not-ready or group-level error evidence.
   > Responsibility: avoid silently dropping groups, returning empty success for unscanned groups, or substituting materialized results for failed fresh execution.
   > Verification: empty monitoring roots returns explicit `NOT_READY`; unavailable runtime grants or not-yet-built runner binding returns explicit `NOT_READY`; group-specific missing or unreachable runners return explicit per-group error.
   > Verification: when a bound remote mount-root is no longer a mounted root, fresh execution fails closed as `HOST_FS_UNAVAILABLE`; fs-meta MUST NOT scan the empty underlying host directory and report it as a fresh success.
   > Verification: when another current-epoch bound runner exists for the same logical group, fs-meta MUST route that same fresh group execution to the next bound runner before rendering the group as unavailable.
   > Covers L0: VISION.QUERY_OUTCOME.PARTIAL_FAILURE_ISOLATION

14. **NO_CROSS_GROUP_ENTRY_MERGE**: **fs-meta System** MUST keep multi-group query payloads as independent group buckets and MUST NOT merge metadata from multiple groups into one synthetic tree page.
   > Covers L0: VISION.QUERY_OUTCOME.UNIFIED_FOREST_RESPONSE
   > Responsibility: preserve per-group visibility, cursor correctness, and failure isolation semantics.
   > Verification: `/tree` and `/on-demand-force-find` return `groups[]`; each group item owns its own `root/entries/entry_page`.
   > Verification: `entry_after` continuation is per-group, and `group_after` continuation is top-level bucket paging inside one PIT; neither cursor family may be reinterpreted as a cross-group merged page.

15. **QUERY_HTTP_FACADE_DEFAULTS_AND_SHAPE**: **fs-meta System** MUST provide query HTTP facade with deterministic defaults and explicit grouped response shaping.
   > Responsibility: keep UI/CLI integration stable across `/tree` `/stats` `/on-demand-force-find`.
   > Verification: `/tree` default query params are `path=/`, `recursive=true`, no selected `group`, no `max_depth`, `group_order=group-key`, `group_page_size=64`, `entry_page_size=1000`, and `read_class=trusted-materialized`; `/stats` defaults `read_class=trusted-materialized`.
   > Verification: the first `/tree` request MAY omit `pit_id`; the response returns `pit.id` and `pit.expires_at_ms`. Any continuation using `group_after` or `entry_after` MUST supply that `pit_id`.
   > Verification: `/tree` accepts `group_after` only as an opaque PIT-owned group offset cursor, and accepts `entry_after` only as an opaque PIT-owned per-group entry cursor bundle; expired PIT returns explicit `PIT_EXPIRED` instead of stale materialized-revision errors.
   > Verification: materialized `/tree` entry continuation MAY extend a PIT-owned per-group entry window through bounded sink materialized reads for the same PIT scope and group; one public page MAY be assembled from multiple smaller internal windows under the caller deadline. This is entry pagination, not group re-ranking or live force-find rerun.
   > Verification: `/tree` responses expose top-level `read_class`, `observation_status`, `group_order`, `pit`, `groups`, and `group_page`; each group item exposes `stability`, and when metadata is available `root`, `entries`, and `entry_page`.
   > Verification: query paths accept either `path` (UTF-8 convenience input) or `path_b64` (authoritative raw-path bytes encoded as base64url), but never both in one request.
   > Verification: `/tree`, `/on-demand-force-find`, and `/stats` expose `path_b64` only when the authoritative raw path is not valid UTF-8; group metadata entries likewise expose optional `path_b64` in those lossy cases, while `path` remains the default display form.
   > Verification: `materialized` returns current materialized observation together with explicit `observation_status`, while `trusted-materialized` rejects the request until package-local observation evidence is trusted enough to be treated as current observation.
   > Verification: unscoped root `/tree` and `/stats` with `read_class=trusted-materialized` use the same app-authoritative active scan-enabled logical-root coverage as Trusted Observation Ready and fail closed instead of returning a partial trusted-materialized `ok` body. A partial routed source-status/status fan-in response MAY explain current convergence, but it MUST NOT shrink the root-readiness universe below the current configured scan-enabled roots while the response still returns all groups.
   > Verification: `trusted-materialized` query admission and public `trusted_observation_readiness` MUST use the same per-group sink/source service-readiness evidence. Pending materialization, overflow-pending materialization, or explicit source degradation keeps `QueryObservationState=materialized-untrusted`; per-node reliability evidence such as unattested, blind-spot, or suspect nodes remains visible through `groups[].reliable=false` and `unreliable_reason` and MUST NOT by itself close the service gate.
   > Verification: `/on-demand-force-find` defaults to `path=/`, `recursive=true`, `group_order=group-key`, `group_page_size=64`, and `entry_page_size=1000`, exposes the same grouped envelope family plus `pit`, and keeps every returned group's `stability.state` fixed to `not-evaluated`.
   > Verification: `/on-demand-force-find` uses the same PIT-only continuation model as `/tree`; live query executes once per PIT creation and later pages read frozen PIT results instead of rerunning the live scan.
   > Verification: `/stats` returns group envelopes with aggregate subtree stats and per-group `partial_failure/errors` visibility.
   > Covers L0: VISION.QUERY_OUTCOME.QUERY_HTTP_FACADE

16. **READ_CLASS_REPLACES_FREEFORM_STABILITY_SELECTION**: **fs-meta System** MUST replace free-form stability/mode selection with named read semantics so app developers do not hand-compose trust decisions.
   > Covers L0: VISION.QUERY_OUTCOME.UNIFIED_FOREST_RESPONSE
   > Responsibility: eliminate caller-owned “pick the right stable state” logic from public query surfaces.
   > Verification: `/tree` accepts `read_class=fresh|materialized|trusted-materialized` and `/stats` accepts only `read_class=materialized|trusted-materialized` instead of `stability_mode` / `quiet_window_ms` / `metadata_mode`.
   > Verification: `fresh` remains the live/freshness path for `/tree` and `/on-demand-force-find`; `/stats` does not expose a fresh read class unless a future contract defines live stats semantics.
   > Verification: `materialized` returns current materialized observation with explicit `observation_status`, and `trusted-materialized` is the only read mode that may be treated as stable current observation.
   > Verification: `/on-demand-force-find` remains a freshness path and does not expose a second free-form stability selector family.

17. **ORIGIN_TRACE_DRIVEN_GROUP_AGGREGATION**: **fs-meta System** MUST derive query aggregation groups from per-request RPC `origin_id` traces and policy mapping, without precomputed peer registry coupling.
   > Responsibility: keep projection membership resolution request-scoped and kernel-routing driven.
   > Verification: projection groups are built from returned events' `origin_id` plus policy mapping (`mount-path`/`source-locator`/`origin`) and do not require stored peer inventory.
   > Covers L0: VISION.QUERY_OUTCOME.STATELESS_QUERY_PROXY_AGGREGATION

18. **GROUPED_QUERY_PARTIAL_FAILURE_HTTP_SUCCESS_ENVELOPE**: **fs-meta System** MUST isolate per-group decode/peer failures on grouped query paths and preserve successful group payloads in the same HTTP response.
   > Responsibility: avoid cascading single-group observation failure into full-request failure.
   > Verification: `/stats` keeps decode failure on one group as explicit `status:error` envelope while other groups remain `status:ok`; when a grouped path has mixed decode success/failure it sets `partial_failure=true` and keeps `errors[]`.
   > Covers L0: VISION.QUERY_OUTCOME.PARTIAL_FAILURE_ISOLATION

19. **QUERY_TRANSPORT_TIMEOUT_AND_ERROR_MAPPING**: **fs-meta System** MUST bound projection RPC waits and report transport/protocol failures as explicit structured HTTP errors.
   > Covers L0: VISION.QUERY_OUTCOME.QUERY_TRANSPORT_DIAGNOSTICS
   > Responsibility: prevent indefinite query hangs and keep failure diagnostics machine-readable.
   > Verification: projection enforces bounded timeouts (`query_timeout_ms` default `30000`, `force_find_timeout_ms` default `60000`).
   > Verification: query error responses include `error/code/path`; timeout/protocol/transport/peer failures map to explicit HTTP status families.

20. **QUERY_BOUND_ROUTE_METRICS_DIAGNOSTICS_BOUNDARY**: **fs-meta System** MUST expose projection bound-route transport counters for runtime diagnostics.
   > Responsibility: provide lightweight observability for query transport anomalies.
   > Verification: `/bound-route-metrics` returns `call_timeout_total`, `correlation_mismatch_total`, `uncorrelated_reply_total`, `recv_loop_iterations`, and `pending_calls`.
   > Covers L0: VISION.QUERY_OUTCOME.QUERY_TRANSPORT_DIAGNOSTICS

21. **QUERY_CALLER_FLOW_LIFECYCLE_IMPLEMENTATION_DEFINED**: **fs-meta System** MUST keep query correctness independent from caller channel reuse policy; baseline implementation MAY use per-request caller open/close lifecycle.
   > Covers L0: VISION.QUERY_OUTCOME.STATELESS_QUERY_PROXY_AGGREGATION
   > Responsibility: avoid coupling correctness semantics to a specific connection-pooling model.
   > Verification: route call path can remain correct when caller channel is opened/closed per request (`caller.open -> ask -> close`) and without long-lived peer cache state.

---

## CONTRACTS.INDEX_LIFECYCLE

1. **REALTIME_PLUS_BASELINE_PLUS_AUDIT_PLUS_SENTINEL**: **fs-meta System** MUST keep index construction and repair on four mechanisms: 实时监听 + 初始全盘扫描 + audit 修正 + 哨兵反馈。
   > Covers L0: VISION.INDEX_LIFECYCLE.REALTIME_LISTENING, VISION.INDEX_LIFECYCLE.INITIAL_FULL_SCAN, VISION.INDEX_LIFECYCLE.AUDIT_REPAIR, VISION.INDEX_LIFECYCLE.SENTINEL_FEEDBACK
   > Responsibility: prevent reduced-mode operation that silently drops realtime guarantees.
   > Verification: unsupported host platforms fail startup explicitly instead of running without realtime watch support.
   > Verification: when a granted mount-root object is unavailable at stream bootstrap, source retries availability probe with exponential backoff (`1s`..`300s`) until the object recovers or `close()`/shutdown cancels the pipeline.
   > Verification: initial baseline scan is executed only on group-primary members; baseline scan on primary keeps watch registration unified (`watch-before-read`) and MUST NOT require separate prescan/snapshot dual-walk.
   > Verification: when a directory/route is not realtime-covered (`watch=false` or watch bind/run unavailable), source MUST keep coverage semantics explicit and rely on audit reconciliation instead of claiming realtime attestation.
   > Verification: scan/audit traversal uses configurable parallel workers (`scan_workers`) and bounded re-batch emission (`batch_size`).
   > Verification: sink/channel backpressure on scan/audit publication MUST propagate into source scan/audit read-ahead and concurrency/admission state; source MUST NOT hide that pressure in unbounded publication buffers or advance scan/audit progress as if downstream acceptance had already happened.
   > Verification: scan/audit backpressure MUST NOT be applied to realtime watch/listen ingestion. Realtime watch events use a separate bounded nonblocking lane; when that lane is full or unavailable, source emits explicit overflow/backpressure evidence and relies on audit/repair convergence instead of blocking the host watch loop or silently preserving trusted readiness.
   > Verification: scan/audit `EventMetadata.timestamp_us` MUST represent the metadata observation or scan-batch sealing time before any backpressured publication/result queue wait. It MUST NOT be minted only when an old scan record is later drained, because sink arbitration depends on this timestamp to reject delayed compensation metadata that predates accepted realtime evidence.
   > Verification: non-primary members keep realtime watch/listen hot set and metadata emission enabled, but do not execute epoch0 baseline/audit scan loops.
   > Verification: initial baseline includes root directory metadata; audit mtime-pruning uses cached directory mtime and emits `audit_skipped=true` heartbeat for silent directories.
   > Verification: scan traversal tracks visited directory identity `(dev, ino)` and MUST skip symlink loops with warning instead of recursive dead-loop.
   > Verification: watch `IN_IGNORED` invalidation MUST purge wd/path internal mapping; if invalidation is on root watch, source MUST terminate `pub_()` stream promptly (no silent hang).
   > Verification: watch queue overflow (`IN_Q_OVERFLOW`) MUST emit in-band overflow control marker and set sink group query response to `reliable=false` with `unreliable_reason=WatchOverflowPendingMaterialization`; overflow MUST NOT trigger immediate full rescan.
   > Verification: overflow-induced unreliability MUST clear only after next primary audit epoch boundary is processed.
   > Verification: source baseline/audit scan boundaries emit in-band `ControlEvent::EpochStart/EpochEnd` with per-group audit epoch progression, enabling sink epoch/MID lifecycle advancement without out-of-band coordination.
   > Verification: source data/control events emit `EventMetadata.timestamp_us` from drift-compensated observation time (`shadow_now_us`), decoupled from payload `modified_time_us`.
   > Verification: audit scan file payloads include `parent_path` and `parent_mtime_us` context so sink can perform parent-staleness rejection before first-seen Scan inserts.
   > Verification: source payload serialization follows shared schema MessagePack contract; path bytes preserve raw encoding fidelity and sync-track field (`source`) is always populated.
   > Verification: source in-root scan/watch emission normalizes `path` to leading-slash relative form (`/` for root path).
   > Verification: drift sample collection failure or empty sample window MUST NOT block `pub_()` startup; source keeps zero drift compensation baseline and continues emitting events with warning observability.
   > Verification: file-level transient races (`ENOENT`) or per-entry stat/read failures during scan/watch paths are isolated to that entry (`log+skip`) and MUST NOT abort the whole pipeline.
   > Verification: attribute-only watch events (`IN_ATTRIB`, e.g. `chmod`/`chown`) are emitted as `Update` with `is_atomic_write=false`, and MUST NOT be promoted to atomic-write authority.
   > Verification: force-find diagnostic error-marker events are excluded from this timestamp rule and keep lightweight diagnostic metadata semantics.

2. **GROUP_SELECTOR_CONFIGURATION**: **fs-meta System** MUST accept group definitions as primary input and derive group membership from app-owned descriptor selectors and optional subpath scope.
   > Covers L0: VISION.KERNEL_RELATION.POLICY_OUTSIDE_KERNEL_FOR_GROUPING
   > Responsibility: keep grouping semantics in fs-meta while making host/object descriptors available for policy selection.
   > Verification: invalid groups fail app config validation; startup returns explicit invalid-input errors; group config binds descriptor selectors plus optional `subpath_scope` and does not require runtime-visible export maps.
3. **DELETE_SEMANTICS_PRESERVATION**: **fs-meta System** MUST preserve delete semantics during aggregation and query projection.
   > Covers L0: VISION.INDEX_LIFECYCLE.DELETE_SEMANTICS_CONTINUITY
4. **DELETE_SEMANTICS_SPLIT**: **fs-meta System** MUST keep delete semantics split as: Realtime delete => tombstone, MID cleanup => hard remove.
   > Covers L0: VISION.INDEX_LIFECYCLE.DELETE_SEMANTICS_CONTINUITY
   > Responsibility: avoid zombie resurrection while keeping missing-item sweep deterministic.
   > Verification: realtime delete paths preserve tombstone protection window; MID only removes missing stale nodes directly.
   > Verification: tombstone reincarnation uses mtime tolerance window to reject stale-cache zombie events and accept genuine recreate events.
   > Verification: tombstone reincarnation also uses the incoming event's own `EventMetadata.timestamp_us`; an event at or before the retained realtime tombstone shadow-time MUST NOT clear or reincarnate that tombstone, including delayed Scan/audit compensation and delayed realtime atomic updates.
   > Verification: tombstone policy boundary is configurable (`sink_tombstone_ttl_ms`, `sink_tombstone_tolerance_us`), baseline defaults remain `90000ms` / `1000000us`.
   > Verification: `MID hard-remove` path may allow short-lived false-positive reappearance from delayed Scan only when no retained realtime/tombstone event-time fence exists for that path; this is an explicit accepted tradeoff and MUST converge by subsequent realtime/audit progression.
5. **GROUP_PARTITIONED_SINK_STATE**: **fs-meta System** MUST maintain sink materialization state partitioned by logical group.
   > Responsibility: prevent cross-group contamination and enable per-group lifecycle loops.
   > Verification: sink state organizes tree/clock/epoch by group only (single tree per group, no member sub-tree); same relative path from different members arbitrates into that group's single tree.
   > Verification: worker hosting identity is only a hosting boundary; query fanout, sink ownership, and materialized response assembly are defined per logical group rather than by "all groups currently hosted behind one worker host".
   > Verification: sink arbitration authority hierarchy is `Realtime atomic > Scan > Realtime non-atomic` for mtime dominance; equal/older compensation events do not rollback newer state.
   > Verification: sink applies compensation parent-staleness check (`parent_mtime_us`) before accepting first-seen Scan file insertions.
   > Verification: delayed Scan/audit compensation whose own `EventMetadata.timestamp_us` is equal to or older than accepted realtime evidence for the path MUST NOT mutate metadata, delete state, clear tombstones, or purge descendants; it may update audit/MID observability such as `last_seen_epoch`.
   > Verification: `audit_skipped=true` is audit/MID observability evidence only. It MUST NOT bypass metadata LWW or event-time arbitration to rollback newer realtime metadata.
   > Verification: when a path changes type (`dir<->file`), sink purges cached descendants of that path only after the incoming event has passed parent-staleness, event-time, tombstone, and LWW acceptance; stale rejected type-change events MUST NOT purge descendants.
   > Verification: sink suspect-age evaluation uses shadow clock domain (`shadow_time_high_us` vs payload `modified_time_us`) and MUST NOT use local host wall-clock as arbitration freshness source.
   > Verification: Scan with unchanged `mtime` preserves existing `monitoring_attested`; only mtime-changed Scan may downgrade attestation.
   > Verification: cold start (`shadow_time_high_us=0`) with empty materialized tree keeps query response reliability permissive (`reliable=true` unless independent overflow marker is pending).
   > Verification: extreme future payload `modified_time_us` MUST NOT advance sink shadow clock; source-side drift estimator (`P999` outlier filter + jump guard) protects emitted `EventMetadata.timestamp_us`.
   > Verification: sink keeps integrity flags (`monitoring_attested`, `suspect`, `blind_spot`) and derives group-global response reliability (`reliable/unreliable_reason`) from those flags rather than from service admission.
   > Verification: status service-state and materialized observation evidence use service-readiness blockers such as pending materialization, overflow-pending materialization, and explicit source degradation; query response reliability remains a separate per-result evidence channel so trusted-materialized responses can report `reliable=false` without hiding current materialized data.
   > Verification: sink shadow clock tracks `EventMetadata.timestamp_us` high-water mark for NFS-domain freshness calculations; `logical_ts` is transport-preserved ordering metadata and is not used as sink arbitration time axis.
   > Covers L0: VISION.INDEX_LIFECYCLE.SINK_SINGLE_TREE_ARBITRATION
6. **IN_MEMORY_MATERIALIZED_INDEX_BASELINE**: **fs-meta System** MUST keep sink materialized tree as in-memory observation/projection state in current baseline architecture.
   > Covers L0: VISION.OBSERVATION_CONVERGENCE.AUTHORITATIVE_TRUTH_LEDGER, VISION.OBSERVATION_CONVERGENCE.OBSERVATION_IS_NOT_TRUTH
   > Responsibility: preserve the current low-latency projection baseline while keeping authoritative truth distinct from derived observation state across both embedded and external-worker execution shapes.
   > Cross-ref: root `L1-CONTRACTS` `AUTHORITATIVE_TRUTH_OBSERVATION_SEPARATION`, `STATEFUL_APP_OBSERVATION_PLANE_OPT_IN`, `STATEFUL_APP_OBSERVATION_PLANE_MINIMUM_DECLARATIONS`, and `OPTIONAL_STATE_CARRIER_RUNTIME_HOSTING`.
   > Verification: sink materialized tree lifecycle is hosting-bound and rebuildable (no durable snapshot dependency required for startup).
   > Verification: any optional full-tree sink checkpoint to a runtime state carrier is bounded/best-effort and MUST NOT block scan/audit event application, initial materialization, or trusted-readiness convergence for giant roots.
   > Verification: sink state access passes through explicit in-memory carrier boundary (`SinkStateCell`) rather than scattering raw lock ownership across business handlers.
   > Verification: source mutable runtime state access passes through explicit in-memory carrier boundary (`SourceStateCell`) to keep source state hosting orthogonal to business handlers.
   > Verification: source/sink carrier boundaries keep a bounded authoritative mutation journal separate from projection state so future StateCell backend changes can replace hosting without rewriting business handlers.
   > Verification: app-side authority commits are expressed only through the runtime-owned state-carrier boundary (`statecell_read/write/watch`) with explicit `state_class`.
   > Clarification: runtime-only cleanup helpers such as `runtime-api::RuntimeStateBoundaryRetire::{statecell_retire_binding}` remain runtime-owned carrier lifecycle seams; fs-meta domain consumes read/write/watch semantics but does not assign domain meaning to retire cleanup unless a later contract promotes it explicitly.
   > Verification: authoritative journal cells use `state_class=authoritative`; projection or runtime scratch cells use `state_class=volatile`.
   > Verification: authoritative journal commit acceptance advances the current `authoritative_revision`; in-memory tree/view refresh advances only after projection rebuild applies that accepted truth.
   > Verification: derived materialized observation state MUST NOT outrun or replace `authoritative_revision` as the domain truth source.
   > Verification: source workers and facade/query workers MUST NOT construct or own materialized tree state; sink remains the only materialized-tree owner.
   > Verification: source/sink authoritative mutation recording passes through one shared commit-boundary abstraction, not duplicated runtime state-carrier write call paths.
   > Verification: fs-meta manifest config MUST reject removed authority carrier fields (`unit_authority_state_carrier`, `unit_authority_state_dir`) to prevent app-level carrier coupling.
   > Verification: if runtime state-carrier authority initialization fails, fs-meta startup MUST fail closed with explicit invalid-input error.
   > Verification: sink execution hosting is selected through worker-oriented deploy config and compiled runtime worker bindings; `external` hosts sink materialized tree in isolated sink-worker hosting separated from the embedded fs-meta app host.
   > Verification: under `workers.sink.mode=external`, sink worker restart/failover rebuilds materialized tree via baseline scan/audit path; in-memory tree remains projection cache, not authoritative durable state.
7. **AUTHORITATIVE_JOURNAL_TRUTH_LEDGER**: **fs-meta System** MUST treat the bounded authoritative mutation journal as the domain truth ledger for state/effect convergence.
   > Responsibility: keep “what fs-meta currently recognizes as truth” separate from query-ready materialized views and scratch runtime state.
   > Cross-ref: root `L1-CONTRACTS` `AUTHORITATIVE_TRUTH_OBSERVATION_SEPARATION` and `STATEFUL_APP_OBSERVATION_PLANE_MINIMUM_DECLARATIONS`.
   > Verification: authoritative mutation commits pass through one shared commit boundary and one journal abstraction, rather than being inferred from query results or duplicated across source/sink business handlers.
   > Verification: the journal is the only boundary that mints or advances `authoritative_revision`; query/materialized-tree readability and projection refresh do not become competing revision authorities.
   > Verification: restart, failover, and external-worker rebuild paths replay or rebuild from authoritative journal inputs plus scan/audit progression; successful query observation alone is never the authority source.
   > Covers L0: VISION.OBSERVATION_CONVERGENCE.AUTHORITATIVE_TRUTH_LEDGER

8. **AUTHORITY_EPOCH_BINDS_OBSERVATION_EVIDENCE**: **fs-meta System** MUST bind trusted materialized readiness evidence to the current Authority Epoch.
   > Responsibility: keep readiness caches simple and safe by making their validity depend on one explicit epoch key.
   > Verification: Authority Epoch is derived from monitoring roots signature, runtime grants signature, source stream generation, sink materialization generation, and facade/runtime generation.
   > Verification: trusted-materialized readiness cache, Materialized Readiness Evidence, and Runner Binding Evidence all carry or are evaluated against the current Authority Epoch.
   > Verification: when any Authority Epoch component changes, fs-meta invalidates old trusted-materialized readiness evidence and requires fresh evidence before serving trusted materialized results for the new epoch.
   > Covers L0: VISION.OBSERVATION_CONVERGENCE.OBSERVATION_IS_NOT_TRUTH, VISION.OBSERVATION_CONVERGENCE.OBSERVATION_ELIGIBILITY_BEFORE_EXPOSURE

9. **MATERIALIZED_READINESS_EVIDENCE_MONOTONIC_WITHIN_EPOCH**: **fs-meta System** MUST treat materialized readiness evidence as monotonic within one Authority Epoch and MUST invalidate that evidence on epoch change.
   > Responsibility: prevent transient source/sink status fan-in gaps from instantly downgrading groups whose same-epoch materialized readiness has already completed.
   > Verification: if a group has completed materialized readiness evidence for the current Authority Epoch, a short-lived fan-in miss or delayed status refresh does not by itself downgrade that group from ready to not-ready.
   > Verification: materialized readiness evidence may still move to degraded when same-epoch evidence explicitly reports overflow, audit invalidation, stale-writer fencing, sink loss, or another domain failure; monotonicity protects against missing status, not against contradictory failure evidence.
   > Verification: any Authority Epoch change, including roots signature, grants signature, source stream generation, sink materialization generation, or facade/runtime generation changes, invalidates the cached materialized readiness evidence and requires fresh evidence for the new epoch.
   > Verification: steady `/tree` and `/stats` reads MAY reuse cached same-epoch materialized readiness evidence instead of re-running full source/sink fan-in on every request, as long as the cache still matches the current Authority Epoch and no contradictory failure evidence has appeared.
   > Verification: cached materialized readiness evidence remains observation evidence only; it does not become authoritative truth and does not bypass trusted observation gates after an epoch change.
   > Covers L0: VISION.OBSERVATION_CONVERGENCE.OBSERVATION_IS_NOT_TRUTH, VISION.OBSERVATION_CONVERGENCE.OBSERVATION_ELIGIBILITY_BEFORE_EXPOSURE

10. **AUDIT_COVERAGE_MODE_IS_EXPLICIT**: **fs-meta System** MUST expose coarse audit coverage mode and coverage capabilities for group/root observation evidence.
   > Responsibility: make reduced metadata modes and NFS/demo coverage tradeoffs visible instead of pretending full metadata coverage exists.
   > Verification: status evidence for each covered group/root exposes one `AuditCoverageMode`: `realtime_hotset_plus_audit`, `audit_only`, `audit_with_metadata`, `audit_without_file_metadata`, or `watch_degraded`.
   > Verification: status or query metadata evidence exposes the capability bits `exists_coverage`, `file_count_coverage`, `file_metadata_coverage`, `mtime_size_coverage`, and `watch_freshness_coverage`.
   > Verification: when file metadata collection is disabled or unavailable, fs-meta may still report existence or file-count coverage, but it MUST mark file metadata and mtime/size coverage unavailable/degraded instead of fabricating metadata.
   > Verification: query responses, including fresh force-find responses, keep explicit `meta.metadata_available=false` with `withheld_reason` or degraded reason when required metadata capability is unavailable.
   > Covers L0: VISION.QUERY_OUTCOME.PARTIAL_FAILURE_ISOLATION, VISION.OBSERVATION_CONVERGENCE.OBSERVATION_IS_NOT_TRUTH

11. **HOST_FS_OPERATION_TIMEOUT_IS_OBSERVATION_EVIDENCE**: **fs-meta System** MUST treat host-fs operation timeout/backpressure as explicit observation evidence.
   > Responsibility: keep scan/stat/list/watch behavior bounded and diagnosable on large or slow NFS mounts.
   > Verification: host-fs facade scan, stat, list, and watch setup operations have a bounded latency policy or return explicit degraded evidence.
   > Verification: timeout or backpressure is not a silent skip; it is reported as root/group degraded or partial evidence.
   > Verification: repeated timeout exposes root/group degraded reason `HOST_FS_TIMEOUT`; repeated backpressure exposes root/group degraded reason `HOST_FS_BACKPRESSURE`; neither may disappear into logs only.
   > Verification: implementation must be bounded and cancellable; specs do not allow unbounded thread spawn as the routine backpressure policy.
   > Covers L0: VISION.QUERY_OUTCOME.PARTIAL_FAILURE_ISOLATION, VISION.INDEX_LIFECYCLE.AUDIT_REPAIR

12. **BUSINESS_MODULE_ORCHESTRATION_TOKEN_FREE**: **fs-meta System** MUST keep orchestration tokens and runtime-unit identifiers out of source/sink business logic modules.
   > Covers L0: VISION.APP_SCOPE.DOMAIN_CONTRACT_CONSUMPTION_ONLY
   > Responsibility: preserve local-style business coding model and keep orchestration concerns in dedicated ingress/gate adapters.
   > Verification: business modules (`sink/{arbitrator,clock,epoch,query,tree}` and `source/{scanner,watcher,drift,sentinel}`) do not contain `runtime.exec*`, orchestration-only `unit_id` carrier parsing, `unit_ids` activation summaries, or direct cluster-view coupling tokens.

---

## CONTRACTS.EVOLUTION_AND_OPERATIONS

1. **BINARY_APP_STARTUP_PATH**: **fs-meta System** MUST stay stable across runtime-managed worker startup and restart lifecycle.
   > Covers L0: VISION.DOMAIN_IDENTITY.BINARY_APP_RUNTIME

2. **INDEPENDENT_UPGRADE_CONTINUITY**: **fs-meta System** MUST allow higher-target-generation replacement of single-app binaries while keeping contracts stable.
   > Covers L0: VISION.EVOLUTION_AND_OPERATIONS.RELEASE_GENERATION_UPGRADE

3. **PRODUCT_CONFIGURATION_SPLIT**: **fs-meta System** MUST keep operator-visible product configuration split between thin deploy-time bootstrap config and online business-scope API configuration derived from runtime grants.
   > Covers L0: VISION.EVOLUTION_AND_OPERATIONS.PRODUCT_CONFIGURATION_SPLIT
   > Responsibility: prevent operators from editing internal desired-state/runtime policy details as business configuration.
   > Verification: product deployment docs keep `fs-meta.yaml` limited to bootstrap API/auth concerns; deploy defaults to `roots=[]`; operators configure monitoring roots through `/runtime/grants`, `/monitoring/roots/preview`, and `/monitoring/roots`.
   > Verification: deploy-time product config MUST reject unknown top-level fields, including business `roots`, instead of silently accepting and dropping them.
   > Verification: fs-meta consumes shared manifest/config loading and intent-compilation semantics through upstream deploy/runtime boundaries rather than redefining path precedence, manifest discovery, or relation-target meaning inside fs-meta specs.
4. **RELEASE_GENERATION_CUTOVER**: **fs-meta System** MUST realize upgrades as higher-target-generation replacement on one fs-meta app boundary while preserving the product API base and replaying current monitoring roots/runtime grants into the new generation.
   > Covers L0: VISION.EVOLUTION_AND_OPERATIONS.RELEASE_GENERATION_UPGRADE
   > Responsibility: make binary upgrades explicit at the app package boundary while keeping authoritative truth replay and externally visible observations separate.
   > Verification: upgrade workflows/docs describe submitting a higher target generation, replaying current monitoring roots/runtime grants as authoritative truth inputs, rebuilding in-memory observation/projection state through scan/audit/rescan on the candidate generation, reaching app-owned `observation_eligible` for materialized `/tree` and `/stats`, then cutting trusted external materialized observation exposure to the new generation while `/on-demand-force-find` remains a freshness path that may become available earlier, and issuing runtime-owned drain/deactivate/retire control intent to the old generation.

5. **RUNTIME_ARTIFACT_EVIDENCE_IS_VERIFIABLE**: **fs-meta System** MUST expose enough RuntimeArtifactEvidence to verify that each node loaded the expected runtime/app artifact.
   > Responsibility: prevent demo, deploy, or test runs from silently exercising a different binary/library than the one being validated.
   > Verification: runtime/app startup logs or status evidence include the loaded artifact path and content hash.
   > Verification: deploy validation can compare the expected artifact hash with the actual loaded hash on every participating node.
   > Verification: demo/test harness fails closed when the run/bin artifact and the actually loaded runtime artifact differ.
   > Verification: demo/test harness stale-artifact detection includes the fs-meta workspace and upstream capanix path dependencies that are linked into the runtime/app artifact, so cross-repo runtime, app SDK, or worker bridge changes cannot silently reuse an older runtime library artifact.
   > Verification: path/hash evidence is sufficient for this baseline; specs do not require SBOM, signing, or supply-chain attestation for the current demo contract.
   > Covers L0: VISION.EVOLUTION_AND_OPERATIONS.RELEASE_GENERATION_UPGRADE

6. **SINGLE_ENTRYPOINT_DESIRED_STATE**: **fs-meta System** MUST allow app desired-state submission from one management entrypoint, while peer nodes can start from baseline config and receive distributed apply from runtime/kernel privileged mutation path.
   > Covers L0: VISION.API_BOUNDARY.BOUNDED_PRODUCT_MANAGEMENT_NAMESPACE
7. **GLOBAL_HTTP_API_RESOURCE_SCOPED_APP_FACADE**: **fs-meta System** MUST keep one operator-facing HTTP API on a resource-scoped one-cardinality facade while internal source/sink execution evolves independently.
   > Covers L0: VISION.QUERY_OUTCOME.RESOURCE_SCOPED_DOMAIN_HTTP_FACADE
   > Responsibility: preserve one stable external URL through ingress-resource selection without introducing a separate roaming API boundary.
   > Verification: fs-meta generation-control document binds the HTTP facade through `api.facade_resource_id`, declares `runtime.exec.facade` as `resource_visible_nodes + one`, and does not define a standalone roaming HTTP execution role outside the app package boundary.
   > Verification: a normal fixed-bind facade handoff keeps the predecessor listener until the successor has runtime exposure proof; if the predecessor has already failed closed, the fixed-bind owner records that failure and the first pending successor for the same bind address MUST release the stale owner claim/listener instead of leaving a 503-only facade that blocks takeover.
   > Verification: if runtime has already deactivated the predecessor and the predecessor is only continuity-retaining the fixed listener, a successor that has accepted desired control state but then fail-closes before exposure MUST release the continuity-retained predecessor through the bounded handoff path and expose its own fail-closed app boundary; it must not leave the predecessor serving requests from a drained/fenced runtime PID.
   > Verification: after that bounded owner release, successor publication is an after-release fixed-bind handoff, not ordinary facade replacement. The successor MUST retry bind/promotion within the fixed handoff deadline and MAY expose a fail-closed HTTP boundary before runtime exposure confirmation; management writes and trusted materialized reads remain closed until the usual readiness evidence arrives.
   > Verification: a failed-owner fixed-bind release MUST run before uninitialized query/status cleanup work that can consume the recovery window; cleanup may withdraw stale dependent lanes only after the successor HTTP boundary has had its bounded after-release publication attempt. Cleanup-only facade follow-up frames that arrive while the failed owner is already uninitialized are part of this same release path, not a separate local cleanup-only return. A failed-owner release MUST NOT wait for stale predecessor facade read requests to drain, because those requests can be waiting on the same recovery handoff; the bounded successor publication owns operator API recovery and in-flight predecessor requests may fail/reconnect. After a fail-closed successor HTTP boundary is serving, cleanup-only facade deactivates MUST NOT clear the `facade-control` route or shut down the only operator API boundary.
   > Verification: after a bounded fixed-bind handoff releases a predecessor claim, facade/query publication decisions MUST refresh fixed-bind claim facts before suppressing dependent routes; a stale lifecycle snapshot MUST NOT keep treating the released predecessor as the current bind owner.
   > Verification: when a fixed-bind listener handle is retired, its process-local bind claim is cleared by bind address, not only by app instance id; successor retry MUST NOT be blocked by a stale claim whose recorded owner no longer matches the closed listener handle.
   > Verification: a fixed-bind successor with an unconfirmed local facade publication MUST keep facade-dependent query routes suppressed even when the bind blocker is an external listener and therefore no in-process predecessor claim exists; bind completion is part of facade publication.
   > Verification: once fixed-bind facade publication completes, retained facade-dependent sink-owned query/materialized route activates and retained core facade read lanes (`find`, materialized query proxy) that were suppressed by the publication barrier MUST replay even if the follow-up control wave is source-only. `sink-status` is sink-owner readiness evidence, not a facade-owned read lane, and MUST be published by the active sink runtime owners for their assigned groups.
8. **API_FAILOVER_RESCAN_REBUILD**: **fs-meta System** MUST rebuild materialized sink/query state on newly bound instances after unit failover via baseline scan/audit/rescan path.
   > Covers L0: VISION.OBSERVATION_CONVERGENCE.OBSERVATION_ELIGIBILITY_BEFORE_EXPOSURE
   > Responsibility: keep failover semantics explicit under in-memory observation ownership instead of implying durable truth transfer.
   > Verification: when a sink-bearing instance changes, the new instance starts with empty in-memory index state for its bound groups and performs rebuild for those groups.
   > Verification: rebuild lag is treated as observation lag against authoritative truth, not as truth deletion.
9. **OBSERVATION_ELIGIBILITY_BEFORE_EXPOSURE**: **fs-meta System** MUST require a newly activated generation or failover target to replay current authoritative truth and catch its observations up before it is treated as the trusted external result source.
   > Responsibility: prevent facade/API cutover from claiming observation success before materialized `/tree` and `/stats` readiness evidence shows the candidate generation has replayed current authoritative truth and completed the required first-audit/materialized-health catch-up for the active groups it serves.
   > Cross-ref: root `L1-CONTRACTS` `STATEFUL_APP_OBSERVATION_PLANE_OPT_IN`, `UNCERTAIN_STATE_MUST_NOT_PROMOTE`, and `POST_CUTOVER_STALE_OWNER_FENCING`.
   > Verification: cutover workflows describe authoritative replay first, initial audit completion plus materialized health catch-up for active scan-enabled primary groups second, and trusted external materialized `/tree` and `/stats` exposure only after the new generation reaches an app-owned `observation_eligible` point.
   > Verification: until `observation_eligible` is reached, fs-meta exposes either the previous eligible generation or explicit not-ready/degraded observation state for materialized `/tree` and `/stats` rather than silently treating partial rebuild output as current truth.
   > Verification: delayed query readability, partial rebuild output, or internal route availability do not promote a candidate generation to current truth for materialized `/tree` and `/stats` before app-owned `observation_eligible`.
   > Verification: `/on-demand-force-find` remains a freshness path and is not blocked by the initial-audit materialized-query gate.
   > Verification: before trusted materialized exposure moves to the newer generation, fs-meta emits stale-writer fence evidence that prevents older generations or stale local writers from re-exposing observations for older truth.
   > Covers L0: VISION.OBSERVATION_CONVERGENCE.OBSERVATION_ELIGIBILITY_BEFORE_EXPOSURE
10. **CROSS_RELATION_DRIFT_VISIBILITY**: **fs-meta System** MUST surface degraded or failure evidence when facade ownership, bind/run realization, and materialized observation state drift apart instead of silently presenting mixed-generation results.
   > Covers L0: VISION.OBSERVATION_CONVERGENCE.CROSS_RELATION_DRIFT_VISIBILITY
   > Verification: cutover and failover workflows describe explicit degraded/not-ready visibility when relation ownership, worker execution state, and current observation evidence disagree.
11. **GROUP_AWARE_SINK_BIND_RUN**: **fs-meta System** MUST treat sink execution as runtime group-aware bind/run realization instead of whole-app app-level facade routing policy.
   > Covers L0: VISION.APP_SCOPE.WORKER_ROLE_MODEL
   > Responsibility: preserve app-owned grouping while allowing runtime to realize per-group sink bind/run realization.
   > Verification: sink generation-control/runtime contract carries per-group execution shape and runtime returns per-instance bound groups.
   > Verification: sink state partitioning keys off `group_id` plus `object_ref`, not `concrete_root_id`.
   > Verification: sink-worker hosting may cover multiple local sink execution partitions, but query fanout and materialized result ownership are still defined per logical group.
12. **UNIT_CONTROL_ENVELOPE_FENCING**: **fs-meta System** MUST validate runtime unit control envelopes by unit contract (`unit_id`) and generation fencing (`generation`).
   > Covers L0: VISION.FAILURE_ISOLATION_BOUNDARY.EXPLICIT_EXECUTION_FAILURE_DOMAINS
   > Responsibility: keep unit dispatch deterministic and reject unknown/invalid execution units.
   > Verification: source/sink reject `ExecControl` or `UnitTick` envelopes with unknown `unit_id`.
   > Verification: source/sink ignore stale generation envelopes (`generation` regression) instead of rolling back newer unit state.
   > Verification: source/sink treat forward-moving `UnitTick` generations as current keepalive/control evidence for already-active routes; a tick whose generation is greater than the retained activation generation MUST NOT force worker reconnect or retained-state replay by itself.
   > Verification: accepted unit ids stay domain-bounded (`runtime.exec.source` / `runtime.exec.scan` / `runtime.exec.sink`).
   > Verification: source/sink share one gate implementation (`RuntimeUnitGate`) to enforce identical allowlist/fencing semantics.
   > Verification: source/sink consume one centralized fs-meta execution-unit registry instead of duplicating raw unit-id string literals.
   > Verification: fs-meta generation-control document keeps canonical execution identities in the `runtime.exec.*` namespace and mirrors that registry consistently.
   > Verification: unsupported non-`runtime.exec.*` unit ids are rejected by source/sink unit gate contract (fail-closed).
13. **CONTROL_FRAME_SIGNAL_TRANSLATION**: **fs-meta System** MUST treat `on_control_frame` as ingress-only boundary and translate envelopes into typed orchestration signals before business handlers run.
   > Covers L0: VISION.APP_SCOPE.DOMAIN_CONTRACT_CONSUMPTION_ONLY
   > Responsibility: separate orchestration parsing from source/sink business logic and keep control ingestion deterministic.
   > Verification: top-level app and module-local control handlers call shared orchestration adapter translation first; business handlers consume typed signal enums instead of decoding control envelopes inline.
   > Verification: same-worker source/sink control calls are serialized per handle; ticks and replay must not race retained-state replay on the same worker bridge, and transient tick followups are applied only after retained-state replay has had the first chance to recover.
   > Verification: after retained source replay succeeds, a forward-moving tick followup is handled as local current tick evidence instead of reopening the source worker path; generation-regressed ticks are ignored.
   > Verification: retryable source/sink control-reset recovery restarts or reacquires the affected worker path without waiting for the same in-flight control handoff that reported the reset.
   > Verification: source post-ack scheduled-group refresh separates total recovery budget from per-RPC attempt budget, and the total deadline covers every refresh lane including worker start, retained replay, client acquisition, grant refresh, and group fetch so one slow or dead worker bridge cannot consume all retry/reacquire time.
   > Verification: release cutover source waves that have already retained desired source state, including post-initial source activation waves split by runtime unit dispatch as source-only, source+facade, or mixed source/sink companion frames, MUST fail closed inside the app boundary on retryable source worker reset instead of blocking the process-level apply/quorum path on inline source replay.
   > Verification: a later tick, cleanup, empty replay, or repeated post-initial route-state control frame that wakes retained generation-cutover source or sink replay MUST use the same fail-closed process-boundary rule; it MUST preserve retained replay for the recovery lane rather than spend the process-level apply/quorum budget on inline retries. A same-generation source tick that matches retained source route state is a bounded source-repair recovery entrypoint and MAY replay current retained source state once; older or mismatched ticks remain process-apply evidence only. Runtime-unit exposure confirmation is an explicit bounded runtime-readiness probe and MAY replay retained sink state once after source replay is clear; ordinary empty/tick/cleanup frames MUST NOT do that replay. A later non-empty source recovery wave MAY perform one bounded apply of current desired state only after the runtime recovery lane has accepted ownership; repeated process-apply route-state waves while retained replay is still pending are not recovery waves. If retained sink route state is already present, a source-only deferred cutover MUST also preserve sink replay so sink recovery cannot be skipped by source-frame ordering.
   > Verification: mixed cleanup followups such as source `restart_deferred_retire_pending` or drained `deferred_retire` plus sink tick/cleanup are cleanup waves, not new business apply waves; retryable source or sink worker reset MUST fail closed at the app boundary and MUST NOT inline retained replay in the process-level apply/quorum path.
   > Verification: after that fail-closed release cutover, source/sink owners MUST keep or immediately republish bounded internal status recovery lanes for their assigned groups while materialized/query exposure remains closed; recovery MUST NOT depend on a later control tick or an external facade request landing on the same non-public owner.
   > Verification: while runtime control is already fail-closed, cleanup-only query and per-peer sink logical-roots deactivates MUST update retained route state locally and MUST NOT re-enter the sink worker bridge; cleanup cannot keep management writes unavailable behind retired worker RPCs. The sink events stream is the sink worker data-ingress route and MAY receive its own cleanup deactivate; that cleanup MUST NOT piggyback retained sink activates or turn app-side query/logical-roots cleanup into retained sink replay.
   > Verification: once a successor fixed-bind HTTP facade is actively serving, cleanup-only query/facade tails from the failed cutover MUST NOT withdraw the serving facade's core business read lanes (`find`, materialized query proxy) while runtime control is still fail-closed; stale auxiliary source-status/source-find lanes may still be withdrawn. During app-local source/sink retained replay, already-active core business read lanes are continuity lanes and MUST remain receive-capable while the retained source/sink state repairs underneath them; a generation cutover must not permanently close the reusable materialized query proxy route just because source/sink replay is pending. `sink-status` is not a facade-owned business lane; it follows the active sink runtime owners.
   > Verification: exhausted source/sink worker-control recovery fails closed inside the fs-meta app boundary and preserves retained replay state; worker-path timeout/channel-close/reset evidence MUST NOT be returned as a process-boundary `Timeout`/`ChannelClosed`/`TransportClosed` from `on_control_frame`. After retained sink replay has restored current scope but sink status is still catching up with `PendingMaterialization` or mixed ready/unready scheduled evidence, the app MUST defer gate reopening/republish completion instead of treating the control frame as a terminal replay failure.
   > Verification: source status/observability reads that find retained source replay pending MUST perform a bounded retained-state replay repair before reporting `not started` when no newer source control frame has arrived; recovery MUST NOT depend on a future runtime tick to reopen the source worker path.
   > Verification: source-status route ownership MUST follow active `runtime.exec.source` runtime-scope ownership. Query/facade lanes may aggregate source-status evidence, but a source-status endpoint MUST NOT require a query or query-peer facade route before serving source-owned runtime-scope readiness evidence. A source-owned source-status endpoint MUST stay available for its assigned groups even when the same process has a local HTTP facade handle whose published facade state is pending or unavailable; facade serving state may gate query/facade-owned status lanes, not source-owned runtime-scope evidence.
   > Verification: source runtime-scope summary is aggregate positive local ownership evidence. Release-upgrade source convergence gates validate coverage for current logical roots on the app-selected source/scan owner nodes; they MUST NOT pin acceptance to a fixed demo node list because source-primary selection is app-owned and may move with grants, topology, and generation cutover state. A peer-only activate wave MUST NOT clear previously accepted local source/scan scheduled-group evidence; only an explicit current-authority empty refresh for the known local node may clear stale scheduled groups. Empty live schedule snapshots are not app-visible ownership evidence and MUST NOT be published as if the local node owns an empty group set.
   > Verification: generic nonblocking source-status MUST publish current runtime-scope control-cache evidence before attempting retained replay or returning a control-inflight worker-unavailable fallback when that cache already has source/scan scheduled groups and control-route facts for the same Authority Epoch; this reports ownership without clearing the retained replay obligation. A pending post-rescan publication refresh MUST withhold live root-health, source-primary, and publication counters from that cache response, but MUST NOT erase source/scan ownership maps needed by release-upgrade and status fan-in gates. Once retained replay is current and a live worker client exists, normal source-status MUST try the bounded live source observation instead of using runtime-scope control cache as final data truth. That live source observation MUST be budgeted inside the caller's status-route collection attempt so the endpoint can still answer before the route collect deadline. If that bounded live observation fails or times out and current runtime-scope control-cache evidence still exists, generic source-status MUST return that ownership evidence with explicit runtime-scope cache provenance instead of downgrading it to worker-unavailable evidence. Stable status root-health cache may skip that live read only after the same runtime scope has already produced publication evidence. Public status `trusted_observation_readiness` MUST be judged from current per-root source/sink trusted evidence; a `source worker runtime scope served from control cache` marker remains explanatory provenance and MUST NOT keep the readiness plane closed once every current logical root has trusted source evidence and ready sink materialization. Cache-only worker status and pending-source markers remain blocking evidence. Manual-rescan pre-delivery probes and management-write readiness remain the blocking recovery paths after source state is current; while retained source replay or source-control apply is pending, manual-rescan source-status is still an observation read and MUST NOT initiate retained replay or endpoint rearm, and its runtime-scope/cache reply MUST withhold live source-primary/concrete-root delivery target truth so it cannot satisfy scoped delivery readiness.
   > Verification: management `/status` sink fan-in MUST NOT enter the generic worker start/retry loop. It may probe an already-started sink worker once inside the short status-route budget, but timeout, reset, or missing-client evidence MUST return cached/degraded sink evidence so a stale worker bridge cannot hold the HTTP facade open during fixed-bind handoff.
   > Verification: manual-rescan current-roots readiness MUST aggregate routed peer source-status evidence together with same-node source owner observation before deciding source runtime-scope readiness; peer fanout alone is not authoritative for the sender-local source owner, and peer fanout timeout MUST NOT mask current same-node source-primary target evidence that still has to pass scoped request/reply delivery.
   > Verification: recovery progress waits are satisfied only by runtime-change evidence or by a bounded observation window completing; a fast source/sink snapshot read without new progress evidence MUST NOT immediately complete the wait loop.
   > Verification: source-to-sink convergence pretrigger is an initial-boot assist only; replay-required recovery waves must let retained source/sink replay clear before follow-up rescan work is submitted.
   > Verification: a source-to-sink recovery rescan epoch is a local source-primary publication obligation. If the local source owns no source-primary scan roots for the current runtime scope, that epoch MUST complete as an observed no-op and MUST NOT block sink/facade recovery; cluster-wide publication remains owned by the source-primary nodes selected for each group.
   > Verification: internal query/status endpoint reply `Backpressure` is bounded transient pressure; endpoint loops preserve liveness instead of relying on route respawn churn.
   > Verification: consecutive worker-bridge transport-close evidence or persistent stale grant-attachment denial while receiving on an app-owned request endpoint is stale endpoint evidence, not reply pressure; the endpoint MUST exit with terminal reason evidence so the next runtime endpoint reconciliation can prune and rearm the same product route on the current boundary. A direct `TransportClosed` from the sidecar bridge is authoritative stale-boundary evidence; retryable `PeerError`/`Internal` transport-close wrappers remain bounded continuity gaps.
   > Verification: when an app-owned internal endpoint is cancelled or pruned for generation cutover, it MUST stop its receive loop without permanently closing the reusable request route or derived reply route; the replacement endpoint for the same route key MUST be able to serve the first management/API request in the same recovery turn.
   > Verification: app close MUST stop app-owned internal endpoint loops before deciding source/sink worker handles are still shared; endpoint-held worker references MUST NOT suppress external-worker shutdown during release retire.
   > Verification: runtime control-frame acceptance is the app-domain acknowledgement that current source/sink/facade control facts were retained or applied. It MUST NOT wait for app-owned request/status proxy endpoints to prove receive-arm readiness. Endpoint rearm and receive-arm markers are separate runtime-readiness evidence: status may expose current control-cache ownership while rearm is still catching up, and management delivery gates still require live receive-armed proof before accepting scoped writes.
   > Verification: steady control-frame, status-endpoint, and endpoint request lifecycle diagnostics MUST be opt-in or bounded state-change summaries by default; full control payloads and per-batch normal-path logs MUST NOT become steady CPU/IO workload during operations gates.
   > Verification: full real-NFS resource-budget gates MUST measure incremental steady API polling overhead against the same live runtime process set under an idle observation window. Background source audit, materialization catch-up, and file-count-dependent scan work are workload evidence and MUST NOT be charged against a fixed demo-data CPU budget. A budget failure must represent sustained polling/control churn after convergence, not a transient data-volume spike.
14. **ORCHESTRATION_TOKEN_PARSING_BOUNDARY**: **fs-meta System** MUST keep raw `unit_id/runtime.exec.*` parsing in orchestration adapter boundary modules only.
   > Covers L0: VISION.APP_SCOPE.DOMAIN_CONTRACT_CONSUMPTION_ONLY
   > Responsibility: avoid orchestration token leakage into pure source/sink compute paths.
   > Verification: source/sink business handlers use typed unit signals (`source/scan/sink`) and no longer branch on raw unit-id strings.

15. **DOMAIN_TRACEABILITY_CHAIN**: **fs-meta System** MUST keep domain L0/L1 items traceable to runtime workflows and executable verification anchors.
   > Covers L0: VISION.EVOLUTION_AND_OPERATIONS.DOMAIN_TRACEABILITY_CHAIN
---

## CONTRACTS.API_BOUNDARY

1. **FS_META_HTTP_API_BOUNDARY**: **fs-meta System** MUST expose fs-meta-domain management/observability HTTP API under one bounded resource-scoped namespace and keep API implementation inside fs-meta app module.
   > Covers L0: VISION.API_BOUNDARY.BOUNDED_PRODUCT_MANAGEMENT_NAMESPACE
   > Responsibility: keep external HTTP ingress owned by fs-meta domain/app package boundary without introducing runtime/daemon semantic coupling.
   > Verification: fs-meta app starts the HTTP API from app-owned API ingress modules; request handlers operate through fs-meta source/sink abstractions; API auth-init/bind failure causes explicit app startup failure.

2. **RESOURCE_SCOPED_DOMAIN_HTTP_FACADE**: **fs-meta System** MUST keep the public HTTP facade as a resource-scoped one-cardinality domain facade owned by fs-meta app package boundary rather than by kernel/runtime semantic layers.
   > Covers L0: VISION.QUERY_OUTCOME.RESOURCE_SCOPED_DOMAIN_HTTP_FACADE
   > Responsibility: keep external query/management ingress on an app-owned facade resource while internal query/find propagation stays on app-owned opaque channels.
   > Verification: external query ingress uses one active HTTP entrypoint selected from the facade resource scope on the fs-meta app package boundary; internal query/find/source/sink coordination remains on route-resolved opaque channels and does not promote extra multi-writer HTTP ingress.

3. **UNIX_STYLE_LOCAL_AUTH_IN_DOMAIN**: **fs-meta System** MUST treat Unix-style local user authentication as a domain-local credential-acquisition personality, not as a replacement for upstream signed control-submit and scope authority.
   > Covers L0: VISION.API_BOUNDARY.LOCAL_AUTH_WITHOUT_PLATFORM_BYPASS
   > Responsibility: provide product-local login UX without creating a domain-owned control-auth bypass.
   > Verification: login verifies passwd+shadow records, rejects locked/disabled users, and issues a session/token/signing context that is consumed by the upstream signed control-submit path rather than bypassing it.
   > Verification: protected API operations still enforce upstream signed submit and scope checks; domain-local auth only supplies product-local credential acquisition.

4. **ROLE_GROUP_ACCESS_GUARD**: **fs-meta System** MUST gate management API operations by the configured product management-session group and reject non-management sessions from the management surface.
   > Covers L0: VISION.API_BOUNDARY.SEPARATE_QUERY_AND_MANAGEMENT_AUTH_SUBJECTS
   > Responsibility: preserve one explicit management-session boundary for product API operations without inventing an independent domain authority lattice.
   > Verification: `/status`, `/runtime/grants`, `/monitoring/roots`, `/monitoring/roots/preview`, `PUT /monitoring/roots`, `POST /index/rescan`, and query-api-key management endpoints require a management session whose principal belongs to the configured `management_group`; non-management sessions are rejected.

5. **READINESS_PLANES_ARE_INDEPENDENT**: **fs-meta System** MUST keep API facade liveness, management write readiness, source repair readiness, and trusted observation readiness as independent readiness planes.
   > Responsibility: avoid treating HTTP listener reachability as permission to mutate business scope or as proof that trusted materialized observation is ready.
   > Verification: API facade liveness means the product HTTP namespace can accept and authenticate requests; it does not by itself authorize roots apply, rescan, query-api-key mutation, or trusted-materialized query success.
   > Verification: management write readiness requires an active current-epoch control stream capable of safely submitting management writes; `PUT /monitoring/roots` MUST fail closed with explicit not-ready semantics when this plane is closed. Manual rescan uses the narrower source repair readiness plane because it targets source-owned repair routes and still performs scoped delivery proof before accepting.
   > Verification: source repair readiness requires an active current-epoch control stream and current retained source replay state; while source control apply is still in flight, the source state is not current and Source Repair Ready MUST remain closed. It MUST NOT wait for sink replay or sink-status materialization readiness, and it MUST NOT authorize roots apply or other full management writes. A full management-write drain may block roots apply, but it MUST NOT prevent a bounded source-repair recovery from reopening manual rescan while full sink/materialization readiness remains closed. A caller-facing request timeout MAY bound the current HTTP response, but it MUST NOT cancel the service-level source-repair recovery it started; otherwise later retries can repeatedly discard the same retained replay progress. Once retained source replay is clear and no source control apply is in flight, a source-repair recovery check is an idempotent no-op and MUST NOT queue behind unrelated control-frame serialization, even if full management readiness or published Source Repair Ready is still closed behind sink/facade recovery. When an explicit manual-rescan current-roots readiness probe has already observed route activation/readiness gaps for the roots it is about to deliver, that evidence MAY trigger one bounded source-repair turn even if the previously published Source Repair Ready plane is still open; the readiness plane alone is not fresher than same-request route-gap evidence.
   > Verification: trusted observation readiness is governed by materialized readiness evidence and observation eligibility; it is not opened by management write readiness alone.
   > Verification: when trusted observation readiness is closed, trusted-materialized reads MUST fail closed with HTTP `503` and `NOT_READY`; tests MUST assert the closed readiness semantics instead of binding to one explanatory error sentence.
   > Verification: trusted-materialized root `/tree` fails closed with explicit not-ready evidence when no current sink schedule or cached sink status exists; on a routed facade, "no current sink schedule" is determined after peer sink-status fan-in, not from the local facade process alone. Source/sink status fan-in is collected independently so one slow status route does not serialize the other. For app-owned online logical roots, peer sink-status fan-in MUST address the current sink owner candidates for the still-active roots; the generic deployment-scoped sink-status route is only aggregate evidence and MUST NOT be the sole authority when current roots move beyond its bootstrap route scopes. Those current sink owner candidates MUST come first from the routed source-status/source-observability evidence that established the active read roots; local facade cache is fallback evidence only and MUST NOT remove a routed active root from peer sink-status fan-in. A partial routed source-status response MUST NOT reduce unscoped root trusted-read readiness below the app-authoritative current scan-enabled root set. A sink owner that is route-activated for owner-scoped sink-status MUST also run the matching app endpoint; route declaration without a serving endpoint is missing owner evidence, not readiness. Once a sink runtime accepts an active group schedule, its sink-status response MUST expose that runtime schedule; local host-grant projection may explain placement but MUST NOT erase a runtime-assigned group from schedule/readiness fan-in. A missing or timed-out sink-status peer may contribute degraded readiness evidence, but the public trusted-materialized read path MUST NOT enter the generic sink worker start/retry/retained-replay loop; absent trusted sink evidence closes the read as `NOT_READY`, not as an HTTP timeout.
   > Verification: trusted-materialized root `/tree` may rescue a selected owner route gap through bounded generic query-proxy evidence only when that evidence returns a non-empty same-group same-path materialized payload; proxy timeout, empty payload, or mismatched group/path remains fail-closed.
   > Verification: selected-group materialized owner selection treats stream-applied event counts as freshness evidence inside the current sink owner set only; stale stream evidence from a node that is not scheduled for the group and is not the group's current primary owner MUST NOT override current sink ownership.
   > Verification: management `/status` is an observation/convergence endpoint and MUST NOT hold facade request drain during fixed-bind handoff; it may remain reachable in degraded form while recovery proceeds and must report closed readiness planes explicitly instead of delaying the handoff it is observing.
   > Verification: steady management `/status` may skip remote status fan-in when same-node source root-health and same-node sink readiness already prove the active status groups; route-schedule maps are diagnostics and must not become mandatory work for every poll. Deployment-scoped fan-in and owner-scoped completion probes are short-budget observation attempts. When deployment-scoped source-status or sink-status returns only a subset of the app-authoritative active roots/groups, `/status` MUST attempt current owner-scoped source-status/sink-status routes derived from current roots/grants and routed owner evidence before publishing the view as complete. Slow or missing generic/owner replies MUST return degraded/partial/not-ready evidence within the status request budget instead of serializing multiple long status-route waits. Generic status success on the preferred runtime boundary MUST NOT prevent using the query boundary as a second chance for missing owner-scoped source/sink evidence.
   > Verification: management `/status` MUST NOT be a synchronous repair executor for source/sink route timeout, transport collapse, missing-client evidence, or retained generation-cutover replay that is already owned by a background/full recovery lane. Those cases are observation evidence and MUST return degraded, partial, or not-ready status within the request budget. A status-triggered sink observation repair is valid only after sink route collection succeeds and the accepted repair signature is not already owned by a sink-generation-cutover or full sink replay lane; repeated same-signature polls observe instead of replaying. This status-poll duplicate suppression does not bind the owning full sink-generation-cutover/background recovery lane: while retained sink replay remains open and owner-scoped sink evidence has not materialized, that lane may republish the same app-authoritative logical-roots declaration inside its own bounded deadline.
   > Verification: repair/replay lanes decoupled from `/status` MUST remain observable without reading logs. Successful and fail-closed `/status` responses MUST carry bounded repair-lane evidence for each lane touched by the request, including at least lane name, owner plane, state, trigger, whether the request blocked on it, a stable diagnostic signature for duplicate suppression/correlation, an update timestamp, and route outcome or reason when the lane was skipped, blocked, failed, timed out, or owned by another recovery lane. When the lane has a meaningful authority generation, retry attempt, or deadline, that evidence MUST be carried as structured fields rather than only in explanatory text.
   > Verification: `/status` and read-only diagnostics MAY remain available when management write readiness, source repair readiness, or trusted observation readiness is closed, as long as they report the closed plane explicitly.
   > Verification: management `/status` readiness planes MUST include API facade liveness, management write readiness, source repair readiness, and trusted observation readiness as separate booleans; source repair readiness MUST NOT be inferred from either full management write readiness or trusted observation readiness.
   > Covers L0: VISION.API_BOUNDARY.BOUNDED_PRODUCT_MANAGEMENT_NAMESPACE, VISION.OBSERVATION_CONVERGENCE.OBSERVATION_ELIGIBILITY_BEFORE_EXPOSURE

5A. **STATUS_OBSERVES_ASYNC_RECOVERY_LANES**: **fs-meta System** MUST keep `/status` as an observation endpoint while source/sink replay, roots-control repair, worker-control recovery, source repair, and sink materialization repair run in explicit recovery lanes.
   > Covers L0: VISION.API_BOUNDARY.STATUS_OBSERVES_ASYNC_RECOVERY
   > Responsibility: decouple user polling from repair execution so status remains low-latency and predictable while recovery still progresses and stays observable.
   > Verification: `/status` MUST NOT synchronously perform retained source replay, retained sink replay, worker start/retry, roots-control full replay, manual-rescan execution, scan/audit traversal, sink observation repair, or sink materialization catch-up. It MAY trigger only a nonblocking wake for an existing recovery lane, and the response MUST still return within the status request budget with lane evidence.
   > Verification: every asynchronous recovery lane that can affect API facade liveness, management write readiness, source repair readiness, trusted observation readiness, source/sink owner fan-in completeness, or materialized group visibility MUST publish bounded machine-readable state through `/status.repair_lanes[]` or an equivalent documented read-only diagnostics endpoint. Logs alone are not a valid observation interface.
   > Verification: lane evidence MUST identify the lane owner plane, state, trigger, diagnostic signature, blocking relationship to the current request, update timestamp, and route outcome/reason when applicable. If a lane has a meaningful authority generation, retry attempt, or deadline, those values MUST be structured fields.
   > Verification: `/status` nonblocking wakes MUST be single-flight per recovery lane diagnostic signature. If the same lane signature is already `scheduled` or `inflight`, a later status poll MUST return that existing lane evidence, or refresh only its observation timestamp/reason, without starting another retained replay, roots-control replay, worker recovery, source repair, or management-write recovery turn.
   > Verification: the duplicate-suppression signature used by status-observation repair is diagnostic correlation only. It MUST NOT suppress the owning background recovery lane's bounded retry policy, and it MUST NOT be treated as owner authority or trusted-readiness proof.
   > Verification: long-running source scan/audit, manual-rescan execution, sink materialization catch-up, worker-control recovery, and retained generation-cutover replay MUST transition through observable lane states such as scheduled, inflight, blocked, completed, failed, or timed-out instead of being represented only by a closed readiness boolean.

6. **ONLINE_ROOT_RECONFIG_WITHOUT_RESTART**: **fs-meta System** MUST support online logical-root reconfiguration through API without app restart while keeping roots/group definitions app-owned and bind/run realization runtime-owned.
   > Covers L0: VISION.API_BOUNDARY.ONLINE_SCOPE_MUTATION_AND_REPAIR
   > Responsibility: keep fs-meta root lifecycle mutable at runtime while preserving fanout correctness and current owner boundaries.
   > Verification: API root/group update applies validation (including rejecting legacy `roots[].source_locator`), updates app-owned authoritative roots/group definitions against current host object grants, consumes current runtime-returned bound scopes only for resulting source/sink refresh and compensatory rescan, and does not promote API/app code into bind/run semantic ownership.
   > Verification: roots apply requires Management Write Ready; HTTP facade liveness alone is insufficient to accept the write.
   > Verification: after an app-defined group is added or removed, projection `tree` / `force-find` group result sets converge to the new defined groups without requiring host restart or runtime auto-discovery of groups.
   > Verification: restoring a logical root MUST reopen that group for sink materialization even when the previous same-epoch state was still pending; roots apply MUST NOT limit restored groups to previously ready cached materialization evidence.
   > Verification: accepted logical roots are app-authoritative configuration truth and MUST be recoverable from the authoritative logical-roots cell; source/sink runtime route payloads are propagation hints, not the only source of truth for later query/status repair.
   > Verification: every runtime roots-control full declaration carries the authoritative logical-root generation for that declaration. Retries and followups for the same accepted roots write MUST reuse that generation, and source/sink owners MUST ignore older or already accepted roots-control generations instead of letting delayed stale declarations overwrite current roots.
   > Verification: online roots apply MAY submit a Capanix accepted-scope convergence followup after the app-authoritative roots write is accepted, but that followup is scope-only: it updates runtime-owned accepted `__cnx_runtime.app_scopes` / bound-scope projection from the accepted roots plus the facade scope while preserving the current static manifest config, startup path, worker runtime bindings, route plans, target generation, and process lifecycle fingerprint. It MUST NOT submit a full roots-bearing deploy intent, mutate static `config.roots`, or force app restart as the mechanism for scope convergence.
   > Verification: authoritative logical-root repair MUST NOT expand a sink instance's runtime placement. Placement remains owned by runtime scope evidence; roots repair may refresh the local root view and reconcile only the groups that the runtime already assigned to that sink.
   > Verification: trusted observation status fan-in MUST be published by source and sink owners for their assigned groups. Query/facade units may consume and aggregate this evidence, but MUST NOT be the sole publisher of source/sink readiness for groups they do not own. Online root replacement may change the current source/sink owner set without app restart; management `/status` MUST complete missing source roots and sink groups through current owner-scoped source-status/sink-status routes, and trusted-materialized reads MUST collect sink-status evidence from current root/grant owner candidates instead of assuming deployment-time generic status scopes still enumerate every active root. Current owner discovery MUST use routed source-observability evidence and app-authoritative roots/grants before consulting local cache. Each current sink owner MUST serve its owner-scoped sink-status request route after activation; otherwise online-root fan-in has no authoritative lane to observe that owner. Sink-status schedule evidence is the sink runtime's accepted active group schedule; it MUST NOT be narrowed again by a stale or partial local grant projection after runtime placement has already accepted the group. Missing owner fan-in is repaired by the owning source/sink recovery lanes; `/status` may report or nonblocking-kick that recovery, but source/sink timeout evidence does not grant `/status` authority to run competing synchronous replay.
   > Verification: online logical-root replacement is a full declaration, including deletions. If a resource grant is withdrawn before the replacement request removes its logical root, the initial replacement control wave MUST still target previously observed source/sink owner nodes discovered from routed status evidence, so retired roots are cleared from old runtimes instead of remaining as active readiness requirements.

7. **MANUAL_RESCAN_OPERATION**: **fs-meta System** MUST provide explicit manual rescan operation to repair index drift and watch-loss conditions.
   > Covers L0: VISION.API_BOUNDARY.ONLINE_SCOPE_MUTATION_AND_REPAIR
   > Responsibility: offer deterministic operator-triggered repair path independent of periodic audit cadence.
   > Verification: manual rescan API triggers source rescan on group-primary pipelines only and returns accepted operation response; scoped runtime delivery evidence targets source-primary runner nodes proven by current live source runtime-scope observation and live group-primary concrete-root ownership for the same root, not every host grant or every scheduled source member that matches the logical root. Degraded worker-cache source-status evidence may explain status but MUST NOT satisfy manual-rescan current-roots readiness or become scoped delivery target evidence, because it does not prove that the node currently owns the source-rescan endpoint. A manual-rescan pre-delivery source-status probe MUST request live delivery evidence; when the app runtime source state is current, a local source-status owner re-arms current source endpoints before replying, and that rearm completion means finished or terminal stale same-route source-rescan request consumers have been pruned and missing current-boundary consumers have been spawned inside the bounded endpoint lifecycle repair. In worker-backed mode, the externally addressed scoped source-rescan request lane is owned by the app runtime endpoint layer; source-status proves only that this app-owned lane is receivable for the current generation and that live source-status evidence still names the target as a source-primary owner for the current root. It MUST NOT run worker target-acceptance as a hidden status pre-check. Worker target acceptance belongs to the scoped source-rescan request/reply endpoint; if worker control/replay makes that acceptance unavailable, the scoped endpoint returns pending delivery evidence and the API surfaces retryable service-unavailable convergence. If retained source replay or source-control apply is pending, the source-status owner returns runtime-scope/cache observation without initiating retained replay or endpoint rearm, and API MUST NOT treat that response as pre-delivery readiness or target proof. Healthy already-armed source-rescan consumers MUST NOT be retired on every pre-delivery probe, but mere task presence is not fresh readiness proof: the source owner MUST observe the scoped request route enter a current receive-armed state during that rearm turn before refreshing the ready marker. A receive-armed ready marker MUST be recorded by the source owner as part of that rearm result; API/status aggregation layers MUST NOT synthesize scoped ready proof after a worker RPC ack, and every worker observability wrapper, including cache/override layers, MUST preserve the source-owned scoped route activation plus ready marker instead of replacing it with an older outer control summary. That ready marker is valid for a current root only while the same node also reports live source/scan runtime-scope ownership and group-primary concrete-root ownership for that root; a scoped route activation whose scopes mention the root, or a ready marker retained after source ownership moved away, MUST NOT cover roots the node no longer owns or cannot publish. For the same target node-scoped manual-rescan route, the source-owner ready marker generation MUST match that source owner's local activation generation for the same target route and root. Peer-origin activations for that target route are cluster-control facts that may name a candidate, but they MUST NOT invalidate the source owner's local receive-armed proof or become pre-delivery readiness without same-target source-owned ready evidence. Root-id scopes remain non-ordering across different target nodes. A nonblocking status cache is not scoped-route readiness evidence. A same-node worker-backed observation used by the API local readiness path has the same duty split: source-status may prove app-owned scoped lane receivability and current root ownership, while worker target acceptance is proven later by the scoped request/reply delivery. Live source snapshots and audit may refine trust or status but MUST NOT gate app-lane receivability, and the API MUST use that same-node source-status probe only when the local source node covers every current root; when it does cover every current root, the same-node probe MUST get the bounded live source-status route budget before peer control-cache candidate evidence is considered as explanation. Otherwise peer source-status fan-in owns the missing roots and the local node may contribute only bounded nonblocking evidence. The pre-delivery fan-in budget MUST cover the bounded live source-status probe it asks peer owners to run; a shorter caller timeout that can never observe the requested live probe is invalid readiness evidence. A `source worker status served from cache` marker is cache-only readiness explanation; even when it carries root-health, source/scan maps, or scoped route activation maps, it MUST NOT satisfy pre-delivery readiness or select a scoped manual-rescan target. A control-inflight status fallback is not the same as an unavailable-worker cache when it preserves configured root-health, source/scan runtime-scope maps, and route activation maps from the same authority epoch; that bounded fallback remains source-status ownership evidence until contradictory current-generation ownership appears, but it is not scoped delivery target evidence until a same-target live source-status observation proves the app-owned rescan route is receivable and live group-primary concrete-root ownership is present for that root; worker acceptance remains the scoped request/reply proof. Same-node source owner observation is part of the pre-delivery readiness authority: if routed peer source-status fanout times out or returns no usable aggregate, current same-node source-primary target evidence may satisfy readiness only under the same root-health, route-activation, group-primary concrete-root, and scoped-delivery proof requirements. If prior source-primary refs point outside current live group-primary concrete-root ownership, scoped delivery waits for corrected publisher evidence instead of choosing a stable non-primary runtime-scope node. A newer source/scan control generation by itself MUST NOT override current non-degraded group-primary domain evidence when the same source-status evidence also proves the primary node scoped manual-rescan route is activated; generation ordering is freshness evidence, not publication authority, so cached older source-status schedules cannot route rescan to a node that no longer owns the source-rescan endpoint. Cross-node generation pruning only applies to control scopes that name a concrete owner target for the group; root-id scopes such as `nfs1=>nfs1` are route activation liveness evidence and MUST NOT be compared as a global node ordering. When route-schedule maps for a current root are absent during cutover, source-primary domain evidence may name a scoped target only if source-status evidence returned by that same target node also proves live group-primary concrete-root ownership and node-scoped manual-rescan route activation for that node and root; when the source-status node identity contains a runtime-generated route-owner suffix for the same stable source node, target selection MUST preserve that exact route-owner identity for delivery and MUST NOT synthesize a stable node route key from the normalized ownership match. When pre-delivery fan-in derives a target set from same-target source-status origin evidence, the API MUST carry that exact origin-proven target set into scoped delivery and MUST NOT recompute or expand it from a merged aggregate snapshot that contains peer-observed route facts. Peer-origin route activations are cluster-control facts, not target receive proof or pre-delivery readiness. In a runtime-scope control-cache observation, that route activation may be observed by a peer because route activation maps are cluster-control facts, but target readiness still requires same-target source-status app-lane proof, and target acceptance still requires scoped request/reply proof. Source-primary refs alone are stale-prone target hints. Control scope alone is not ownership evidence unless the same node also reports live schedule ownership for that root.
   > Verification: worker `ObservabilitySnapshot` used by manual-rescan source-status/pre-delivery readiness MUST be read-only. It may report current cached/live evidence, but it MUST NOT initiate authoritative logical-root repair, runtime-root reconciliation, retained replay, scan, audit, or endpoint rearm before replying; source-status app-lane proof and scoped request/reply delivery own those separate readiness/acceptance steps. Explicit empty local source/scan schedule maps are meaningful observation facts: wrappers MUST preserve them and MUST NOT treat them as missing data that can be filled from older control summaries or status recovery.
   > Verification: when current roots/source target discovery yields a finite expected source-status owner set, manual-rescan pre-delivery fan-in MUST finish after every expected owner reply has arrived and MUST NOT spend idle grace waiting for unrelated extra replies. If an expected owner does not reply or the returned evidence still fails readiness, the normal bounded timeout/fail-closed behavior remains.
   > Verification: if manual-rescan current-roots fan-in resends the logical-roots control wave, any source-status scoped-delivery proof collected before that resend is stale for target acceptance and MUST be discarded. The API may keep target node hints for the next probe, but it MUST re-observe source-owned route-ready proof after the resent control wave instead of selecting a target that is still applying that control.
   > Verification: source-owned endpoint rearm MUST replay any retained source control state that can overwrite scoped manual-rescan route evidence before it records receive-armed ready proof, so the ready generation and route activation generation are from the same current source state.
   > Verification: if the same source-owned scoped manual-rescan request route advances generation while its managed app endpoint is still receive-armed, the source owner MAY refresh the ready marker to that current generation from the receive-armed endpoint state. A spawned route record or task presence without receive-armed state remains insufficient readiness proof.
   > Verification: node-scoped manual-rescan delivery is owned by the source node named by the scoped route. Non-target evidence may appear only as supplementary route-drain evidence and MUST NOT prove delivery, execution ownership, or readiness. App/runtime endpoint layers MAY attach explicit non-target drains for active peer-scoped manual-rescan request routes so fanout does not hang, but target `accepted` delivery still requires explicit source-owned evidence from the named source-primary runner.
   > Verification: worker-backed scoped manual-rescan acceptance is target-required. The target source worker may return `accepted` only after it owns at least one local source-primary scan root for the requested/current rescan scope; a local no-op path for non-primary or absent roots MUST fail closed for scoped delivery and MUST NOT mark that scoped delivery accepted. This scoped `accepted` reply is delivery/ownership proof plus durable target-local rescan intent submission; the scoped endpoint MUST enqueue the source-primary scan intent before replying but MUST NOT synchronously wait for scan-root readiness, scan/audit traversal, materialization completion, or other file-count-dependent work. In worker-backed mode, enqueue means recording the target-local rescan intent before the worker RPC reply is sent; any scan signal that can start file traversal/audit work MUST run after the worker RPC reply send, so target acceptance is not coupled to file-count-dependent execution time. Cluster manual-rescan signal publication remains the broad repair wave for all source-primary owners, but the broad runtime control stream MUST record only target-local manual-rescan intent before scoped/generic request-reply acceptance is proven; it MUST NOT wake scan/audit traversal ahead of that accepted delivery proof. Scoped target acceptance MUST NOT depend on a separate unproven control stream as the only scan trigger. When the current source-owned rearm-ack cache already proves the same target route is receive-armed and owns a local source-primary scan root, the scoped endpoint MAY use that proof only if it can still submit the target-local rescan intent before replying.
   > Verification: node-scoped manual-rescan delivery MUST use the standard fs-meta scoped route exchange, with the reply lane armed before sending the request. A direct one-off reply-channel poll is not sufficient delivery evidence for an accepted scoped rescan.
   > Verification: a zero-evidence scoped manual-rescan route exchange is route-convergence pending evidence, not final target rejection, while the operation's bounded delivery window remains open. The API MUST reissue the same idempotent target-scoped request inside that existing window and still fail closed unless the named source owner returns explicit `accepted` evidence.
   > Verification: if one bounded scoped collection observes duplicate target evidence for the same node-scoped manual-rescan request, explicit target `accepted` evidence is terminal for that target and MUST NOT be downgraded by another pending/rejected observation for the same target. Pending/rejected target evidence remains fail-closed only when no target `accepted` evidence exists in the same collection.
   > Verification: manual-rescan duplicate suppression MUST use a request identity that is stable across the cluster request, not cross-node wall-clock ordering. A lower `requested_at_us` from another node or a later facade process MUST still be accepted when its request identity is new; only the same request identity may be suppressed as a duplicate.
   > Verification: in a runtime-managed deployment, manual rescan is a cluster-level repair wave: the API MUST publish the cluster manual-rescan runtime control stream so every node that owns a source-primary root can rescan, and MUST NOT rely only on one selected runtime control route. After that stream is accepted and scoped/generic `source.rescan` request/reply delivery is proven, the API MUST NOT synchronously publish any additional process-local or worker-local duplicate outside the request/reply endpoint before returning; duplicate side signals are not acceptance evidence and can block behind unrelated source-worker work. If the runtime control stream is rejected by a stale drained PID before delivery, the scoped/generic request endpoint remains the bounded target-local fallback because it both proves delivery and submits the target-local rescan intent.
   > Verification: manual rescan republishes current root evidence for selected primary roots so newly reopened sink materialization paths can catch up even when file contents are unchanged.
   > Verification: manual rescan requires source repair readiness plus scoped source delivery evidence; HTTP facade liveness alone is insufficient to enqueue repair work, and full sink/materialization management-write readiness is not part of this source-owned repair precondition. A service-unavailable manual-rescan current-roots readiness failure or scoped source route pending failure is retryable convergence evidence for clients within their bounded management retry window; retry/failover MUST NOT become acceptance and MUST still require fresh source-owned readiness plus scoped delivery proof on the successful facade.
   > Verification: one manual-rescan HTTP request MUST spend a single bounded response budget across source-repair observation, current-roots roots/grants snapshot acquisition, current-roots roots-control replay submission, current-roots fan-in, scoped/generic source-rescan delivery, and any source-status delivery-confirmation fallback. That synchronous budget MUST leave client retry headroom and MUST terminate with retryable HTTP `503` / `NOT_READY` evidence before the transport request timeout. Source-status evidence that reports cache/degraded/pending state or a manual-rescan route that is not receive-armed is retryable not-ready evidence for the current attempt; it MUST NOT make the API continue waiting for unrelated owner probes after no partial scoped-delivery proof has been observed. The response budget bounds only the API reply: a source-repair recovery lane or same-generation current-roots roots-control replay lane that has already been scheduled/submitted MUST remain observable and continue under its owning service-level deadline instead of being canceled by the HTTP request returning. Same-roots/same-scope retries inside a management retry session MUST NOT periodically replay the same current-roots roots-control wave; route convergence must be observed through source-status/readiness evidence and background repair lanes instead of timer-based duplicate control sends.

8. **PRODUCT_API_NAMESPACE_STABILITY**: **fs-meta System** MUST expose a product-focused management namespace that is independent from legacy test/control helpers.
   > Covers L0: VISION.API_BOUNDARY.BOUNDED_PRODUCT_MANAGEMENT_NAMESPACE
   > Responsibility: keep console, independent `fsmeta` CLI, and automation bound to the same stable product API.
   > Verification: product management endpoints live on `/session/login`, `/status`, `/runtime/grants`, `/monitoring/roots`, and `/index/rescan`; legacy `/auth/login`, `/config/roots`, `/ops/rescan`, `/release/render`, `/exports`, and `/fanout` are absent from the public product API.

9. **API_NON_OWNERSHIP_OF_QUERY_FIND_CHANNEL_PATH**: **fs-meta System** API boundary MUST NOT introduce parallel `query/find` payload APIs in management namespace.
   > Covers L0: VISION.API_BOUNDARY.BOUNDED_PRODUCT_MANAGEMENT_NAMESPACE
   > Responsibility: keep management API and existing query/find channel paths semantically separated.
   > Verification: management API endpoints exclude file-tree query/find response contracts and only expose session, status, runtime-grant, root-management, and rescan payloads.
10. **QUERY_PATH_PARAMETERS_OWN_PAYLOAD_SHAPE**: **fs-meta System** MUST express query/find payload shaping through query-path parameters and path-specific response contracts, rather than management API request-body size fields.
   > Covers L0: VISION.QUERY_OUTCOME.QUERY_HTTP_FACADE
   > Responsibility: keep payload shaping in query semantics while preserving management namespace boundary.
   > Verification: management API contracts do not define query/find body limit fields; `/tree` and `/on-demand-force-find` shape subtree metadata with `group_page_size/group_after/entry_page_size/entry_after`, while `/stats` keeps its own aggregate envelope rules.
   > Verification: query-path contracts own bytes-safe path transport as well: callers use `path_b64` when raw path bytes are not valid UTF-8, and response payloads expose authoritative optional `path_b64` exactly in those lossy cases instead of relying on management-body escape hatches.
11. **GIANT_ROOT_PUBLIC_API_PARAMETER_GOVERNANCE**: **fs-meta System** MUST keep every public HTTP interface bounded and parameter-safe when any monitoring root may contain more than 1 billion files.
   > Covers L0: VISION.QUERY_OUTCOME.QUERY_HTTP_FACADE, VISION.API_BOUNDARY.BOUNDED_PRODUCT_MANAGEMENT_NAMESPACE, VISION.API_BOUNDARY.STATUS_OBSERVES_ASYNC_RECOVERY, VISION.API_BOUNDARY.ONLINE_SCOPE_MUTATION_AND_REPAIR
   > Responsibility: serve reasonable paged/observational/operator requests without letting invalid or unbounded parameters trigger full-tree traversal, unbounded fanout, duplicate background jobs, giant response bodies, or request-timeout-based control flow.
   > Verification: public query endpoints reject invalid, unknown, mutually exclusive, out-of-range, non-opaque cursor, or otherwise unbounded parameters with HTTP `400 Bad Request`; they MUST NOT silently widen the request, auto-clamp into a misleading complete result, or report permanent parameter invalidity as retryable `503`.
   > Verification: `/tree` and `/on-demand-force-find` root-recursive requests are valid only as bounded PIT/cursor observation sessions governed by group paging, entry paging, caller deadlines, per-group execution outcomes, and internal route byte budgets; they MUST NOT promise one-response full-root enumeration or one-request fresh traversal of a giant root.
   > Verification: `/stats` remains aggregate/materialized and MUST NOT introduce fresh full-tree traversal semantics to satisfy broad root-recursive requests; when aggregate evidence is unavailable it fails through the documented observation-readiness path instead of scanning synchronously.
   > Verification: `/index/rescan` is a background rescan-intent submission and delivery-proof interface. It MUST NOT wait for scan/audit traversal, sink materialization, or file-count-dependent work; duplicate same-generation/same-scope submissions observe or reuse the active job/lane evidence instead of starting repeated billion-file scans.
   > Verification: `/status` and diagnostics expose bounded readiness, recovery-lane, and job-progress summaries only. They MUST NOT execute file-count-dependent repair or query work, and they MUST surface enough state for operators to distinguish invalid client parameters, healthy bounded queries, backpressure, pending materialization, and source scan/audit progress.
12. **RUNTIME_GRANT_DISCOVERY_BOUNDARY**: **fs-meta System** MUST expose runtime-injected `host_object_grants` as the product-facing source for root selection.
   > Covers L0: VISION.API_BOUNDARY.ONLINE_SCOPE_MUTATION_AND_REPAIR
   > Responsibility: make root configuration depend on runtime grants instead of legacy exports/fanout diagnostics.
   > Verification: `GET /api/fs-meta/v1/runtime/grants` returns app-visible `GrantedMountRoot[]` descriptors carrying host/object metadata needed to compose selectors.
13. **ROOT_PREVIEW_BEFORE_APPLY**: **fs-meta System** MUST provide preview of root-to-grant matches and resolved monitor paths before persisting new roots.
   > Covers L0: VISION.API_BOUNDARY.ONLINE_SCOPE_MUTATION_AND_REPAIR
   > Responsibility: let operators verify selector coverage and concrete monitor paths before write-side mutation.
   > Verification: `POST /api/fs-meta/v1/monitoring/roots/preview` accepts draft roots, returns matched grants and resolved monitor paths, and keeps unmatched roots explicit.
   > Verification: `PUT /api/fs-meta/v1/monitoring/roots` revalidates draft roots against the current runtime grants and rejects any non-empty `unmatched_roots` set instead of persisting selector intent that currently has no visible grant match.
14. **EMPTY_ROOTS_VALID_DEPLOYED_STATE**: **fs-meta System** MUST allow deployed-but-unconfigured operation with zero monitoring roots.
   > Covers L0: VISION.API_BOUNDARY.ONLINE_SCOPE_MUTATION_AND_REPAIR
   > Responsibility: support “deploy first, discover grants later” without forcing placeholder roots.
   > Verification: startup config and `PUT /api/fs-meta/v1/monitoring/roots` both accept empty `roots`, while `/status` continues to report healthy runtime state.
15. **PRODUCT_CONSOLE_ACCESS_BOUNDARY**: **fs-meta System** MUST gate the product console with management sessions while keeping query execution on query api keys.
   > Covers L0: VISION.API_BOUNDARY.SEPARATE_QUERY_AND_MANAGEMENT_AUTH_SUBJECTS
   > Responsibility: keep interactive management traffic and distributed query-consumer traffic on separate auth subjects.
   > Verification: management session can access `/status`, `/runtime/grants`, `/monitoring/roots`, `/monitoring/roots/preview`, `PUT /monitoring/roots`, `POST /index/rescan`, `GET|POST /query-api-keys`, and `DELETE /query-api-keys/:key_id`.
   > Verification: management session bearer tokens are rejected on `/tree`, `/stats`, and `/on-demand-force-find`.
   > Verification: query api keys can access `/tree`, `/stats`, and `/on-demand-force-find`, but are rejected on management endpoints.

---

## CONTRACTS.APP_SCOPE

> Section summary only. Canonical item-level refs below use `VISION.APP_SCOPE.<ITEM>`.

1. **DOMAIN_CONTRACT_CONSUMPTION_ONLY**: **fs-meta System** MUST keep the app package downstream of domain/runtime contracts instead of creating a parallel package-local authority.
   > Covers L0: VISION.APP_SCOPE.DOMAIN_CONTRACT_CONSUMPTION_ONLY
   > Responsibility: keep app package semantics downstream of domain/runtime authority instead of creating a parallel contract set.
   > Verification: main specs trace root/domain Convergence Vocabulary without redefining package-local authority terms.
   > Verification: main specs explicitly state fs-meta app implementation consumes fs-meta domain specs and root Convergence Vocabulary rather than redefining `Authoritative Truth`, `Observation`, `Projection`, or `Observation-Eligible`.
   > Verification: stateful observation-facing business modules consume bounded upstream authoring, runtime, and deploy surfaces rather than owning helper-layer topology or low-level bridge semantics.
   > Verification: the developer-facing authoring surface stays bounded to fs-meta domain/types, while runtime artifact and deploy compilation remain sibling internal surfaces without becoming parallel product authority.
   > Verification: the product-facing deploy/tooling namespace remains bounded, while operational or test support modules may stay package-local without becoming product or platform authority.
   > Verification: exact dependency allowlists, package names, helper crates, file paths, and implementation tuning knob names live in engineering governance rather than in the formal app-scope contract.
   > Verification: bounded implementation tuning knobs may exist, but they do not redefine domain truth, cutover, or query-surface contracts.
2. **WORKER_ROLE_MODEL**: **fs-meta System** MUST present one app product container composed of three worker roles: `facade-worker`, `source-worker`, and `sink-worker`.
   > Covers L0: VISION.APP_SCOPE.WORKER_ROLE_MODEL
   > Responsibility: keep the downstream product model stable while making scan/audit source-side unit work rather than a fourth worker role.
   > Verification: app specs explicitly name the three worker roles and state that `query` remains inside `facade-worker` for now.
   > Verification: app specs state that initial full scan, periodic audit, and overflow/recovery rescans remain `source-worker` responsibilities carried by the source-side unit set rather than by a fourth worker role.
   > Verification: app specs justify the split as `3`, not `2`, by keeping sink/materialized ownership separate from source-side observation and keeping query orchestration inside `facade-worker`.
3. **WORKER_MODE_MODEL**: **fs-meta System** MUST use `embedded | external` as the only product-facing worker-mode vocabulary.
   > Covers L0: VISION.APP_SCOPE.WORKER_MODE_MODEL
   > Responsibility: keep product docs/config focused on worker roles and worker modes rather than realization mechanics.
   > Verification: product-facing L0-L2 specs use worker/mode vocabulary and do not present realization-mechanic terms as architecture terms.
   > Verification: app specs state the initial mode defaults as `facade-worker=embedded`, `source-worker=external`, and `sink-worker=external`.
   > Verification: operator-visible worker config stays worker-oriented through per-role mode fields rather than legacy realization vocabulary, while the app package consumes compiled runtime worker bindings and MUST reject raw realization-specific startup fields at app-load time.
4. **LOCAL_HOST_RESOURCE_PROGRAMMING_ONLY**: **fs-meta System** MUST keep resource-bound source behavior on the bound host through local-host programming targets rather than inventing remote-host operation contracts.
   > Covers L0: VISION.APP_SCOPE.LOCAL_HOST_RESOURCE_PROGRAMMING_ONLY
5. **RESOURCE_SCOPED_HTTP_FACADE_ONLY**: **fs-meta System** MUST host the resource-scoped external HTTP facade for the single fs-meta app package boundary without redefining product or platform ownership.
   > Covers L0: VISION.APP_SCOPE.RESOURCE_SCOPED_HTTP_FACADE_ONLY
   > Verification: app source hosts the bounded one-cardinality HTTP facade and does not promote it into product or platform authority or a separate roaming API boundary.
6. **OPAQUE_INTERNAL_PORTS_ONLY**: **fs-meta System** MUST treat internal ports and protocols as app-owned opaque semantics rather than platform-owned vocabulary.
   > Covers L0: VISION.APP_SCOPE.OPAQUE_INTERNAL_PORTS_ONLY
   > Verification: app architecture and workflows keep internal ports opaque and route/channel carriers platform-owned only as transport.
   > Verification: external-worker realization helpers remain confined to explicit worker runtime seams and do not define source/sink/query/API business module contracts.
   > Verification: the bridge-realization seam remains below the business-module contract boundary; worker bootstrap, retry clipping, lifecycle supervision, and canonical transport/error classification remain runtime-helper implementation rather than worker artifact or tooling responsibilities.
   > Verification: external-worker transport preserves canonical `Timeout` / `TransportClosed` categories and wall-clock total-timeout semantics instead of collapsing transport failures into a generic peer-error bucket.
7. **RELEASE_GENERATION_CUTOVER_CONSUMPTION_ONLY**: **fs-meta System** MUST consume release-generation cutover by replaying current authoritative truth inputs (`roots`, runtime grants, authoritative journal continuation) and rebuilding observation state rather than inventing package-local rollout semantics.
   > Covers L0: VISION.APP_SCOPE.RELEASE_GENERATION_CUTOVER_CONSUMPTION_ONLY
   > Verification: active scan-enabled primary groups reaching trusted materialized observation state are the readiness anchor for `trusted-materialized` `/tree` and `/stats` exposure.
   > Verification: `/on-demand-force-find` stays on the freshness path while materialized observation catches up.
   > Verification: `/on-demand-force-find` on the freshness path remains available before trusted materialized observation is promoted.
   > Verification: runtime-unit exposure confirmation is trust evidence, not a retained source/sink replay owner; when retained sink replay is pending after release cutover, exposure confirmation must not synchronously consume the sink worker control path whose own route readiness depends on app recovery.
8. **AUTHORITATIVE_TRUTH_CARRIER_CONSUMPTION_ONLY**: **fs-meta System** MUST consume authoritative truth carriers and revisions from domain/runtime boundaries rather than define a package-local competing truth source.
   > Covers L0: VISION.APP_SCOPE.AUTHORITATIVE_TRUTH_CARRIER_CONSUMPTION_ONLY
9. **OBSERVATION_ELIGIBILITY_GATE_OWNERSHIP**: **fs-meta System** MUST own the package-local `observation_eligible` evidence that reports when materialized `/tree` and `/stats` observations are trustworthy enough to be treated as current observation after cutover or rebuild.
   > Covers L0: VISION.APP_SCOPE.OBSERVATION_ELIGIBILITY_GATE_OWNERSHIP
   > Verification: app cutover workflow ties `observation_eligible` to the same package-local materialized observation evidence that feeds query `observation_status`, rather than to direct package-local trusted external exposure ownership in runtime or mere hosting readiness.
   > Verification: `/on-demand-force-find` remains available as a freshness path before `observation_eligible` is reached for materialized `/tree` and `/stats`.
   > Verification: app workflow treats `observation_eligible` as the trusted-materialized gate while `/on-demand-force-find` stays a freshness path.
   > Verification: status/health surfaces expose explicit source coverage, degraded state, audit timing, sink capacity signals, and optional facade-pending retry diagnostics so large-NFS monitoring can distinguish trusted exposure evidence from coarse host-boundary liveness or listener retry drift.
10. **STALE_WRITER_FENCE_BEFORE_EXPOSURE**: **fs-meta System** MUST fence stale generations before runtime can trust a newer exposure path or allow older observations to re-expose.
   > Covers L0: VISION.APP_SCOPE.STALE_WRITER_FENCE_BEFORE_EXPOSURE
11. **NO_PRODUCT_OR_PLATFORM_OWNERSHIP**: **fs-meta System** MUST NOT own product CLI/deploy semantics or platform bind/route/grant authority.
   > Covers L0: VISION.APP_SCOPE.NO_PRODUCT_OR_PLATFORM_OWNERSHIP
12. **WORKER_MODE_FAILURE_BOUNDARY_IS_EXPLICIT**: **fs-meta System** MUST describe failure isolation in worker-mode terms: `embedded` workers stay inside the shared host boundary, `external` workers run through isolated external worker hosting, and construction/bootstrap/runtime-task failures MUST surface through typed `CnxError` / `init_error` paths instead of routine production `panic!` / `expect!`.
   > Covers L0: VISION.APP_SCOPE.WORKER_MODE_FAILURE_BOUNDARY_IS_EXPLICIT
   > Verification: product-facing L0-L2 specs describe the baseline defaults as `facade-worker=embedded`, `source-worker=external`, and `sink-worker=external`.
   > Verification: app workflow and source continue to preserve `init_error` / join-failure handling and keep API/bootstrap construction failures on explicit typed error/log paths instead of routine `expect(...)`.
   > Verification: external-worker init retry is clipped by wall-clock total timeout even when one RPC attempt has a larger per-call timeout budget.

---

## CONTRACTS.CLI_SCOPE

> Section summary only. Canonical item-level refs below use `VISION.CLI_SCOPE.<ITEM>`.

1. **PRODUCT_DEPLOYMENT_CLIENT_ONLY**: **fs-meta System** MUST keep the fs-meta CLI as a product/operator client boundary for deploy/undeploy/local/grants/roots command workflows only; login/rescan remain bounded HTTP/API workflows rather than top-level `fsmeta` subcommands.
   > Covers L0: VISION.CLI_SCOPE.PRODUCT_DEPLOYMENT_CLIENT_ONLY
2. **RESOURCE_SCOPED_HTTP_FACADE_CONSUMPTION_ONLY**: **fs-meta System** MUST keep CLI requests pointed at the bounded resource-scoped fs-meta HTTP facade and deploy boundary rather than inventing parallel operator protocols.
   > Covers L0: VISION.CLI_SCOPE.RESOURCE_SCOPED_HTTP_FACADE_CONSUMPTION_ONLY
3. **AUTH_BOUNDARY_CONSUMPTION_ONLY**: **fs-meta System** MUST keep CLI auth behavior as credentialed consumption of the product auth boundary only and MUST NOT own session, role, scope, or bypass semantics.
   > Covers L0: VISION.CLI_SCOPE.AUTH_BOUNDARY_CONSUMPTION_ONLY
4. **RELEASE_GENERATION_DEPLOY_CONSUMPTION_ONLY**: **fs-meta System** MUST keep release-generation deploy/cutover on the same product boundary without exposing manual internal release-doc edits as operator workflow.
   > Covers L0: VISION.CLI_SCOPE.RELEASE_GENERATION_DEPLOY_CONSUMPTION_ONLY
5. **DOMAIN_BOUNDARY_CONSUMPTION_ONLY**: **fs-meta System** MUST keep CLI request composition built from fs-meta domain declarations, external `cnxctl` deploy/control commands, and shared kernel-owned auth vocabulary without redefining them.
   > Covers L0: VISION.CLI_SCOPE.DOMAIN_BOUNDARY_CONSUMPTION_ONLY
6. **NO_RUNTIME_OR_PLATFORM_OWNERSHIP**: **fs-meta System** MUST keep runtime policy, bind/run realization, route convergence, route target selection, and primitive semantics out of the CLI package.
   > Covers L0: VISION.CLI_SCOPE.NO_RUNTIME_OR_PLATFORM_OWNERSHIP
7. **NO_OBSERVATION_PLANE_OWNERSHIP**: **fs-meta System** MUST keep `state/effect observation plane` meaning such as `observation_eligible` out of the CLI package.
   > Covers L0: VISION.CLI_SCOPE.NO_OBSERVATION_PLANE_OWNERSHIP
8. **LOCAL_DEV_DAEMON_COMPOSITION_ONLY**: An optional tooling-only local-dev launcher MAY ship as an explicit feature, but that launcher MUST remain tooling-only composition over upstream daemon bootstrap seams and MUST NOT redefine daemon ingress, runtime planning, or kernel authority.
   > Covers L0: VISION.CLI_SCOPE.LOCAL_DEV_DAEMON_COMPOSITION_ONLY

---

## CONTRACTS.DATA_BOUNDARY

1. **TYPED_EVENT_CONTINUITY**: **fs-meta System** MUST preserve typed source-event continuity from source-origin ingest through sink materialization and query-visible envelopes.
   > Covers L0: VISION.DATA_BOUNDARY.TYPED_EVENT_CONTINUITY
   > Responsibility: keep source-origin event meaning stable across source, sink, and query boundaries without degrading into opaque transport-only blobs.
   > Verification: source/sink/query contracts continue to use typed event payloads and do not replace the query-visible envelope with untyped byte buckets or ad-hoc lossy projections.
2. **METADATA_CONTINUITY**: **fs-meta System** MUST preserve stable query metadata continuity across one logical observation snapshot until new ingest advances the materialized view.
   > Covers L0: VISION.DATA_BOUNDARY.METADATA_CONTINUITY
   > Responsibility: keep one logical observation snapshot internally self-consistent so metadata does not churn without corresponding ingest/change progression.
   > Verification: grouped query contracts and tests preserve stable snapshot metadata until new ingest, audit, or replay advancement explicitly updates the materialized observation.
3. **REFERENCE_DOMAIN_SLICE_CONNECTIVITY**: **fs-meta System** MUST preserve reference-domain slice connectivity so related file-system slice updates remain queryable through the same fs-meta domain boundary.
   > Covers L0: VISION.DATA_BOUNDARY.REFERENCE_DOMAIN_SLICE_CONNECTIVITY
   > Responsibility: keep related file-system slice updates connected inside one fs-meta query domain instead of fragmenting them across unrelated product surfaces.
   > Verification: reference-domain slice tests and grouped query contracts continue to present related slice updates through one fs-meta query boundary rather than splitting them into separate ad-hoc APIs or uncorrelated result families.

---

## CONTRACTS.FAILURE_ISOLATION_BOUNDARY

1. **EXECUTION_FAILURE_DOMAINS_ARE_EXPLICIT**: **fs-meta System** MUST explicitly distinguish non-isolating `embedded` execution from isolated `external` worker execution, and MUST NOT present them as one equivalent fault domain.
   > Covers L0: VISION.FAILURE_ISOLATION_BOUNDARY.EXPLICIT_EXECUTION_FAILURE_DOMAINS
   > Responsibility: keep worker-mode failure domains explicit so embedded host-boundary failures and external worker failures are not treated as the same operational event.
   > Verification: architecture and workflow specs describe `embedded` workers as sharing the host boundary and `external` workers as isolated external worker hosting; public docs do not present them as equivalent isolation shapes.

2. **INTERFACE_LOCAL_EXECUTION_OR_WORKER_FAILURE_CONTAINMENT_TARGET**: **fs-meta System** SHOULD isolate recoverable interface failures (for example watcher/audit local execution panic, query worker crash, or sink worker loss) to the narrowest local execution partition or worker scope when feasible, while preserving explicit degraded visibility.
   > Covers L0: VISION.FAILURE_ISOLATION_BOUNDARY.TASK_OR_WORKER_FAILURE_CONTAINMENT
   > Responsibility: keep recoverable local-execution and worker failures contained to the narrowest feasible scope while surfacing degraded/failure evidence.
   > Verification: managed endpoint loops and worker supervision keep source/sink/query failures explicit in status or degraded outputs instead of silently collapsing them into whole-app success.

3. **FAILURE_IMPACT_BY_EXECUTION_SHAPE_DECLARED**: **fs-meta System** MUST document that embedded crash semantics can take down all boundaries in the shared host boundary, while external worker failures are recovered through runtime lifecycle restart/rebind and rebuild paths rather than being mistaken for truth loss.
   > Covers L0: VISION.FAILURE_ISOLATION_BOUNDARY.FAILURE_IMPACT_DECLARED_BY_MODE
   > Responsibility: declare user-visible failure impact by worker mode so recovery expectations stay stable across product docs, tooling, and runtime behavior.
   > Verification: specs and runtime behavior state that embedded crash semantics can terminate the shared host boundary, while external worker failures recover through restart/rebind/rebuild rather than being interpreted as authoritative truth loss.

version: 2.20.1
---

# L1: fs-meta Domain Contracts

> Subject: `fs-meta System` at domain boundaries.
> Pattern: `[fs-meta System] MUST ...`

---

## CONTRACTS.DOMAIN_IDENTITY

1. **DOMAIN_CONSTITUTION_SCOPE_DISCIPLINE**: **fs-meta System** MUST keep domain-level specs focused on externally observable contracts and cross-component ownership.
   > Covers L0: VISION.FS_META_DOMAIN_CONSTITUTION

2. **ROLE_BOUNDARY_ENFORCEMENT**: **fs-meta System** MUST expose one app package boundary to runtime/deployment while encapsulating source/sink partitioning as implementation detail.
   > Covers L0: VISION.MODULE_ROLE_BOUNDARY
   > Verification: fs-meta is treated as one downstream app product container, not as a generic reusable shared library package.

3. **META_INDEX_DOMAIN_OWNERSHIP**: **fs-meta System** MUST keep `meta-index` lifecycle and state ownership inside fs-meta domain services, not kernel business state.

---

## CONTRACTS.KERNEL_RELATION

1. **KERNEL_DOMAIN_NEUTRAL_CONSUMPTION**: **fs-meta System** MUST consume only kernel thin runtime ABI plus generic route/channel-attach mechanisms, without requiring fs-meta-specific kernel primitives or kernel-owned fs-meta protocol verb interfaces.
   > Covers L0: VISION.GENERIC_KERNEL_MECHANISM_CONSUMPTION
   > Verification: fs-meta does not treat `kernel-api` as the canonical ordinary app-facing runtime boundary; any remaining low-level primitive usage is explicit and secondary to the `app-sdk -> runtime-api` path.

2. **HOST_DESCRIPTOR_GROUPING_IS_DOMAIN_POLICY**: **fs-meta System** MUST implement grouping from app-owned host descriptor selectors and object descriptors and MUST NOT require kernel/runtime hardcoded grouping semantics.
   > Covers L0: VISION.POLICY_OUTSIDE_KERNEL_FOR_GROUPING

3. **HOST_ADAPTER_SDK_TRANSLATION_BOUNDARY**: **fs-meta System** MUST delegate host-local facade/ABI adaptation to generic host-adapter SDK boundaries, including directory watch/listen primitives.
   > Covers L0: VISION.HOST_ADAPTER_SDK_TRANSLATION_OWNERSHIP
   > Responsibility: keep host-local adaptation semantics and distributed host-operation-contract avoidance out of domain code paths.
   > Verification: source watcher uses host-adapter watch/session boundaries instead of directly importing host watch libraries (for example inotify API types).
   > Verification: fs-meta domain crate dependency boundary MUST NOT directly depend on host watch library crates (for example `inotify`); host-specific watch backends remain in host-adapter-sdk.
   > Verification: source root-runtime assembly resolves one public `HostFsFacade` per granted host object via host-adapter-sdk and does not branch on local-vs-remote backend construction inline or model host calls as a distributed host-operation enumeration interface.
4. **RESOURCE_BOUND_SOURCE_LOCAL_HOST_PROGRAMMING**: **fs-meta System** MUST keep resource-bound source logic programming to public host-fs facade on the bound host and MUST NOT model host operations as distributed host-operation forwarding contract.
   > Covers L0: VISION.RESOURCE_BOUND_LOCAL_HOST_PROGRAMMING
   > Responsibility: preserve local resource execution while keeping cross-node coordination in runtime/kernel control relations only.
   > Verification: startup and adapter-boundary workflows describe source tasks consuming granted mount-root objects through public host-fs facade on the bound host; fs-meta specs do not require route-transparent remote host-operation interfaces.
5. **THIN_RUNTIME_ABI_CONSUMPTION**: **fs-meta System** MUST consume only thin runtime ABI state (`host_object_grants`, run context, generation/lease, control events, channel hooks) at its app/runtime boundary.
   > Covers L0: VISION.THIN_RUNTIME_ABI_CONSUMPTION
   > Responsibility: keep fs-meta below runtime orchestration and above domain protocol ownership.
   > Verification: fs-meta consumes the ordinary app-facing runtime surface through `capanix-app-sdk` authoring entrypoints and `capanix-runtime-api` typed boundaries; `capanix-kernel-api` remains only a low-level kernel-owned mirror or carrier vocabulary.
6. **APP_OWNS_OPAQUE_PORT_MEANING**: **fs-meta System** MUST keep query/find/source/sink rendezvous naming and protocol meaning app-owned over opaque channels.
   > Covers L0: VISION.APP_OWNS_OPAQUE_PORT_MEANING
   > Responsibility: prevent kernel/runtime/adapter layers from becoming owners of fs-meta protocol semantics.
   > Verification: fs-meta runs use and serve app-owned opaque ports for query/find/source/sink coordination, while kernel/runtime only resolve route/channel attachment and do not enumerate fs-meta protocol verbs.
7. **APP_DEFINED_GROUPS_RUNTIME_BIND_RUN**: **fs-meta System** MUST define groups in app/domain config and allow runtime to realize unit bind/run on those groups without interpreting group formation semantics.
   > Responsibility: keep grouping policy in fs-meta while allowing runtime to realize per-group unit bind/run realization.
   > Verification: group membership is computed from app config plus runtime-injected host object descriptors; runtime receives opaque groups and realizes per-group source/sink execution against those groups without upgrading any internal desired-state wiring into product vocabulary.
   > Verification: runtime host routing is resolved from `object_ref` and fs-meta app code does not parse node/member identifiers for host-fs calls.
   > Verification: single mount-root init/watch failure is isolated to that object's task and does not block other group pipelines.
8. **SOURCE_PRIMARY_EXECUTOR_APP_OWNED**: **fs-meta System** MUST keep the source-primary selection rule inside fs-meta app logic while leaving bind/run admission, execution gating, and generation/fencing semantics under runtime ownership.
   > Responsibility: keep per-group audit/sentinel authority in fs-meta domain without promoting fs-meta app code into runtime-owned execution policy.
   > Verification: fs-meta selects one bound source run per group using an app-owned deterministic rule.
   > Verification: runtime still owns whether a selected source run is admitted, activated, deactivated, fenced, or rebound; fs-meta only consumes the admitted runtime unit context and bound group realization.
   > Verification: all source members in the same group keep realtime watch/listen pipelines enabled when `watch=true`; periodic audit/sentinel loop authority is enabled only on that group's source-primary executor.
   > Verification: metadata from all members in the group is emitted to sink-side tree construction path; non-primary source instances are not excluded from data channel.

---

## CONTRACTS.QUERY_OUTCOME

1. **DUAL_QUERY_PATH_AVAILABILITY**: **fs-meta System** MUST expose both `query` (materialized) and `find` (fresh/live) user paths.
   > Covers L0: VISION.QUERY_PATH_AVAILABILITY, VISION.FIND_PATH_AVAILABILITY

2. **QUERY_PATH_OUTPUT_DISCIPLINE**: **fs-meta System** MUST keep query-path observation semantics consistent while allowing `/tree` and `/on-demand-force-find` to use path-specific response envelopes.
   > Responsibility: keep externally visible observations stable without confusing them with authoritative truth or forcing materially different paths into one fake uniform payload shape.
   > Verification: materialized `/tree` returns a grouped envelope with top-level `path/status/group_order/groups/group_page`; each group item carries its own `group/status/reliable/unreliable_reason/stability/meta` and optional `root/entries/entry_page`.
   > Verification: `/on-demand-force-find` remains a freshness path but also returns the same grouped-envelope family; its per-group `stability` shape stays explicit while `state` remains `not-evaluated`.
   > Verification: grouped response keys remain location-agnostic logical ids (for example `nfs1`) and MUST NOT expose absolute host mount paths.
   > Verification: returned envelopes are observation/projection outputs that may lag current authoritative truth; they do not redefine authoritative journal state by being readable.
   > Covers L0: VISION.UNIFIED_FOREST_RESPONSE, VISION.OBSERVATION_IS_NOT_TRUTH

3. **EXPLICIT_QUERY_STATUS_AND_METADATA_AVAILABILITY**: **fs-meta System** MUST represent observation availability explicitly instead of silently dropping failed or withheld results.
   > Covers L0: VISION.PARTIAL_FAILURE_ISOLATION
   > Verification: `/tree` always exposes explicit `status` and `meta.metadata_available`; withheld metadata uses explicit `withheld_reason` instead of silently dropping a returned group bucket from the response.
   > Verification: grouped query paths keep explicit per-group `ok`/`error` envelopes and `partial_failure/errors` visibility instead of silently dropping failed groups.

4. **CORRELATION_ID_CONTINUITY**: **fs-meta System** MUST preserve request-response correlation metadata on query paths.

5. **GROUP_ORDER_MULTI_GROUP_BUCKET_SELECTION**: **fs-meta System** MUST expose group bucket ordering as an explicit query axis instead of hiding it behind a single-group winner selection knob.
   > Responsibility: keep group ranking deterministic while avoiding API-level “pick exactly one winner” semantics.
   > Verification: `/tree` and `/on-demand-force-find` accept `group_order=group-key|file-count|file-age`; default is `group-key`.
   > Verification: clients that want one group use `group_page_size=1`; the server does not require `group=<id>` or `best=true`.
   > Verification: when `group_order=file-count`, groups are ordered by highest file count; when `group_order=file-age`, groups are ordered by newest file activity (`latest_file_mtime_us`); ties break deterministically by group key.

6. **QUERY_PARAMETER_AXES_REMAIN_ORTHOGONAL**: **fs-meta System** MUST keep group ordering, PIT session ownership, bucket pagination, entry pagination, stability evaluation, and metadata-shaping parameters orthogonal.
   > Responsibility: let callers freely combine group ordering (`group_order`), PIT session continuation (`pit_id`), group bucket pagination (`group_page_size`, `group_after`), per-group entry pagination (`entry_page_size`, `entry_after`), stability evaluation (`stability_mode`, `quiet_window_ms`), and response shaping (`metadata_mode`) without hidden semantic coupling.
   > Verification: `group_order` affects only group ranking, `pit_id` only binds one frozen query session, `group_page_size/group_after` affect only bucket slicing, `entry_page_size/entry_after` affect only per-group entry slicing, `stability_mode/quiet_window_ms` affect only stability evaluation, and `metadata_mode` affects only whether metadata is returned.
   > Verification: `group_order=file-age` does not implicitly mean “stable-only”, and `stability_mode=quiet-window` does not redefine group ranking.

7. **FORCE_FIND_GROUP_LOCAL_EXCLUSIVITY**: **fs-meta System** MUST keep `force-find` execution exclusive per group while allowing any bound source run in that group to perform the scan.
   > Responsibility: avoid duplicate diagnostic scans without overloading runtime with transient lease semantics.
   > Verification: each group keeps one in-flight `force-find` at a time through app-owned mutex/state.
   > Verification: runner selection uses round-robin across bound source runs in the target group, and failed runner selection falls back to the next bound source run.
   > Verification: `file-count` and `file-age` ordering paths MAY use lightweight phase-1 stats probe; they MUST NOT require full fanout materialized tree payload just to rank groups.
   > Covers L0: VISION.GROUP_ORDER_MULTI_GROUP_QUERY

8. **NO_CROSS_GROUP_ENTRY_MERGE**: **fs-meta System** MUST keep multi-group query payloads as independent group buckets and MUST NOT merge metadata from multiple groups into one synthetic tree page.
   > Responsibility: preserve per-group visibility, cursor correctness, and failure isolation semantics.
   > Verification: `/tree` and `/on-demand-force-find` return `groups[]`; each group item owns its own `root/entries/entry_page`.
   > Verification: `entry_after` continuation is per-group, and `group_after` continuation is top-level bucket paging inside one PIT; neither cursor family may be reinterpreted as a cross-group merged page.

9. **QUERY_HTTP_FACADE_DEFAULTS_AND_SHAPE**: **fs-meta System** MUST provide query HTTP facade with deterministic defaults and explicit grouped response shaping.
   > Responsibility: keep UI/CLI integration stable across `/tree` `/stats` `/on-demand-force-find`.
   > Verification: `/tree` default query params are `path=/`, `recursive=true`, `group_order=group-key`, `group_page_size=64`, `entry_page_size=1000`, `stability_mode=none`, and `metadata_mode=full`.
   > Verification: the first `/tree` request MAY omit `pit_id`; the response returns `pit.id` and `pit.expires_at_ms`. Any continuation using `group_after` or `entry_after` MUST supply that `pit_id`.
   > Verification: `/tree` accepts `group_after` only as an opaque PIT-owned group offset cursor, and accepts `entry_after` only as an opaque PIT-owned per-group entry cursor bundle; expired PIT returns explicit `PIT_EXPIRED` instead of stale materialized-revision errors.
   > Verification: `/tree` responses expose top-level `group_order`, `pit`, `groups`, and `group_page`; each group item exposes `stability`, and when metadata is available `root`, `entries`, and `entry_page`.
   > Verification: query paths accept either `path` (UTF-8 convenience input) or `path_b64` (authoritative raw-path bytes encoded as base64url), but never both in one request.
   > Verification: `/tree`, `/on-demand-force-find`, and `/stats` expose `path_b64` only when the authoritative raw path is not valid UTF-8; group metadata entries likewise expose optional `path_b64` in those lossy cases, while `path` remains the default display form.
   > Verification: `metadata_mode=status-only` returns stability without `root/entries/page`, and `metadata_mode=stable-only` withholds metadata until stability state is `stable`.
   > Verification: `/on-demand-force-find` defaults to `path=/`, `recursive=true`, `group_order=group-key`, `group_page_size=64`, and `entry_page_size=1000`, exposes the same grouped envelope family plus `pit`, and keeps every returned group's `stability.state` fixed to `not-evaluated`.
   > Verification: `/on-demand-force-find` uses the same PIT-only continuation model as `/tree`; live query executes once per PIT creation and later pages read frozen PIT results instead of rerunning the live scan.
   > Verification: `/stats` returns group envelopes with aggregate subtree stats and per-group `partial_failure/errors` visibility.
   > Covers L0: VISION.QUERY_HTTP_FACADE

10. **QUIET_WINDOW_STABILITY_TRACKS_OBSERVED_CHANGE_NOT_REFRESH_NOISE**: **fs-meta System** MUST derive quiet-window stability from observed materialized subtree change and coverage recovery evidence, not from raw periodic sync traffic or file mtime alone.
   > Responsibility: avoid resetting path stability on every audit/repair refresh while still reflecting real write-visible subtree change.
   > Verification: periodic sync-refresh updates that leave the effective materialized subtree result unchanged MUST NOT reset quiet-window timing.
   > Verification: periodic refresh that changes accepted query-visible fields such as `modified_time_us` is classified as write-significant rather than sync-refresh.
   > Verification: write-significant subtree changes and coverage recovery reset quiet-window timing; quiet-window state is not computed from latest file mtime alone.
   > Verification: `/on-demand-force-find` remains a freshness path and MUST NOT claim quiet-window stability semantics, even when it paginates metadata.

11. **ORIGIN_TRACE_DRIVEN_GROUP_AGGREGATION**: **fs-meta System** MUST derive query aggregation groups from per-request RPC `origin_id` traces and policy mapping, without precomputed peer registry coupling.
   > Responsibility: keep projection membership resolution request-scoped and kernel-routing driven.
   > Verification: projection groups are built from returned events' `origin_id` plus policy mapping (`mount-path`/`source-locator`/`origin`) and do not require stored peer inventory.
   > Covers L0: VISION.STATELESS_QUERY_PROXY_AGGREGATION

12. **GROUPED_QUERY_PARTIAL_FAILURE_HTTP_SUCCESS_ENVELOPE**: **fs-meta System** MUST isolate per-group decode/peer failures on grouped query paths and preserve successful group payloads in the same HTTP response.
   > Responsibility: avoid cascading single-group observation failure into full-request failure.
   > Verification: `/stats` keeps decode failure on one group as explicit `status:error` envelope while other groups remain `status:ok`; when a grouped path has mixed decode success/failure it sets `partial_failure=true` and keeps `errors[]`.
   > Covers L0: VISION.PARTIAL_FAILURE_ISOLATION

13. **QUERY_TRANSPORT_TIMEOUT_AND_ERROR_MAPPING**: **fs-meta System** MUST bound projection RPC waits and report transport/protocol failures as explicit structured HTTP errors.
   > Responsibility: prevent indefinite query hangs and keep failure diagnostics machine-readable.
   > Verification: projection enforces bounded timeouts (`query_timeout_ms` default `30000`, `force_find_timeout_ms` default `60000`).
   > Verification: query error responses include `error/code/path`; timeout/protocol/transport/peer failures map to explicit HTTP status families.

14. **QUERY_BOUND_ROUTE_METRICS_DIAGNOSTICS_BOUNDARY**: **fs-meta System** MUST expose projection bound-route transport counters for runtime diagnostics.
   > Responsibility: provide lightweight observability for query transport anomalies.
   > Verification: `/bound-route-metrics` returns `call_timeout_total`, `correlation_mismatch_total`, `uncorrelated_reply_total`, `recv_loop_iterations`, and `pending_calls`.
   > Covers L0: VISION.QUERY_TRANSPORT_DIAGNOSTICS

15. **QUERY_CALLER_FLOW_LIFECYCLE_IMPLEMENTATION_DEFINED**: **fs-meta System** MUST keep query correctness independent from caller channel reuse policy; baseline implementation MAY use per-request caller open/close lifecycle.
   > Responsibility: avoid coupling correctness semantics to a specific connection-pooling model.
   > Verification: route call path can remain correct when caller channel is opened/closed per request (`caller.open -> ask -> close`) and without long-lived peer cache state.

---

## CONTRACTS.INDEX_LIFECYCLE

1. **REALTIME_PLUS_BASELINE_PLUS_AUDIT_PLUS_SENTINEL**: **fs-meta System** MUST keep index construction and repair on four mechanisms: 实时监听 + 初始全盘扫描 + audit 修正 + 哨兵反馈。
   > Covers L0: VISION.REALTIME_LISTENING, VISION.INITIAL_FULL_SCAN, VISION.AUDIT_REPAIR, VISION.SENTINEL_FEEDBACK
   > Responsibility: prevent reduced-mode operation that silently drops realtime guarantees.
   > Verification: unsupported host platforms fail startup explicitly instead of running without realtime watch support.
   > Verification: when a granted mount-root object is unavailable at stream bootstrap, source retries availability probe with exponential backoff (`1s`..`300s`) until the object recovers or `close()`/shutdown cancels the pipeline.
   > Verification: initial baseline scan is executed only on group-primary members; baseline scan on primary keeps watch registration unified (`watch-before-read`) and MUST NOT require separate prescan/snapshot dual-walk.
   > Verification: when a directory/route is not realtime-covered (`watch=false` or watch bind/run unavailable), source MUST keep coverage semantics explicit and rely on audit reconciliation instead of claiming realtime attestation.
   > Verification: scan/audit traversal uses configurable parallel workers (`scan_workers`) and bounded re-batch emission (`batch_size`).
   > Verification: non-primary members keep realtime watch/listen hot set and metadata emission enabled, but do not execute epoch0 baseline/audit scan loops.
   > Verification: initial baseline includes root directory metadata; audit mtime-pruning uses cached directory mtime and emits `audit_skipped=true` heartbeat for silent directories.
   > Verification: scan traversal tracks visited directory identity `(dev, ino)` and MUST skip symlink loops with warning instead of recursive dead-loop.
   > Verification: watch `IN_IGNORED` invalidation MUST purge wd/path internal mapping; if invalidation is on root watch, source MUST terminate `pub_()` stream promptly (no silent hang).
   > Verification: watch queue overflow (`IN_Q_OVERFLOW`) MUST emit in-band overflow control marker and set sink group query response to `reliable=false` with `unreliable_reason=WatchOverflowPendingAudit`; overflow MUST NOT trigger immediate full rescan.
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
   > Responsibility: keep grouping semantics in fs-meta while making host/object descriptors available for policy selection.
   > Verification: invalid groups fail app config validation; startup returns explicit invalid-input errors; group config binds descriptor selectors plus optional `subpath_scope` and does not require runtime-visible export maps.
3. **DELETE_SEMANTICS_PRESERVATION**: **fs-meta System** MUST preserve delete semantics during aggregation and query projection.
4. **DELETE_SEMANTICS_SPLIT**: **fs-meta System** MUST keep delete semantics split as: Realtime delete => tombstone, MID cleanup => hard remove.
   > Responsibility: avoid zombie resurrection while keeping missing-item sweep deterministic.
   > Verification: realtime delete paths preserve tombstone protection window; MID only removes missing stale nodes directly.
   > Verification: tombstone reincarnation uses mtime tolerance window to reject stale-cache zombie events and accept genuine recreate events.
   > Verification: tombstone policy boundary is configurable (`sink_tombstone_ttl_ms`, `sink_tombstone_tolerance_us`), baseline defaults remain `90000ms` / `1000000us`.
   > Verification: `MID hard-remove` path may allow short-lived false-positive reappearance from delayed Scan in extreme races; this is an explicit accepted tradeoff and MUST converge by subsequent realtime/audit progression.
5. **GROUP_PARTITIONED_SINK_STATE**: **fs-meta System** MUST maintain sink materialization state partitioned by logical group.
   > Responsibility: prevent cross-group contamination and enable per-group lifecycle loops.
   > Verification: sink state organizes tree/clock/epoch by group only (single tree per group, no member sub-tree); same relative path from different members arbitrates into that group's single tree.
   > Verification: sink arbitration authority hierarchy is `Realtime atomic > Scan > Realtime non-atomic` for mtime dominance; equal/older compensation events do not rollback newer state.
   > Verification: sink applies compensation parent-staleness check (`parent_mtime_us`) before accepting first-seen Scan file insertions.
   > Verification: when a path changes type (`dir<->file`), sink purges cached descendants of that path before applying the new type to keep single-tree structure valid.
   > Verification: sink suspect-age evaluation uses shadow clock domain (`shadow_time_high_us` vs payload `modified_time_us`) and MUST NOT use local host wall-clock as arbitration freshness source.
   > Verification: Scan with unchanged `mtime` preserves existing `monitoring_attested`; only mtime-changed Scan may downgrade attestation.
   > Verification: cold start (`shadow_time_high_us=0`) with empty materialized tree keeps query response reliability permissive (`reliable=true` unless independent overflow marker is pending).
   > Verification: extreme future payload `modified_time_us` MUST NOT advance sink shadow clock; source-side drift estimator (`P999` outlier filter + jump guard) protects emitted `EventMetadata.timestamp_us`.
   > Verification: sink keeps integrity flags (`monitoring_attested`, `suspect`, `blind_spot`) and derives group-global reliability (`reliable/unreliable_reason`) from those flags rather than per-node response-level gating.
   > Verification: sink shadow clock tracks `EventMetadata.timestamp_us` high-water mark for NFS-domain freshness calculations; `logical_ts` is transport-preserved ordering metadata and is not used as sink arbitration time axis.
   > Covers L0: VISION.SINK_SINGLE_TREE_ARBITRATION
6. **IN_MEMORY_MATERIALIZED_INDEX_BASELINE**: **fs-meta System** MUST keep sink materialized tree as in-memory observation/projection state in current baseline architecture.
   > Responsibility: preserve the current low-latency projection baseline while keeping authoritative truth distinct from derived observation state across both in-process and worker-process execution shapes.
   > Cross-ref: root `L1-CONTRACTS` `AUTHORITATIVE_TRUTH_OBSERVATION_SEPARATION`, `STATEFUL_APP_OBSERVATION_PLANE_OPT_IN`, `STATEFUL_APP_OBSERVATION_PLANE_MINIMUM_DECLARATIONS`, and `OPTIONAL_STATE_CARRIER_RUNTIME_HOSTING`.
   > Verification: sink materialized tree lifecycle is process-bound (no durable snapshot dependency required for startup).
   > Verification: sink state access passes through explicit in-memory carrier boundary (`SinkStateCell`) rather than scattering raw lock ownership across business handlers.
   > Verification: source mutable runtime state access passes through explicit in-memory carrier boundary (`SourceStateCell`) to keep source state hosting orthogonal to business handlers.
   > Verification: source/sink carrier boundaries keep a bounded authoritative mutation journal separate from projection state so future StateCell backend changes can replace hosting without rewriting business handlers.
   > Verification: app-side authority commits are expressed only through the runtime-owned state-carrier boundary (`statecell_read/write/watch`) with explicit `state_class`.
   > Clarification: runtime-only cleanup helpers such as `runtime-api::RuntimeStateBoundaryRetire::{statecell_retire_binding}` remain runtime-owned carrier lifecycle seams; fs-meta domain consumes read/write/watch semantics but does not assign domain meaning to retire cleanup unless a later contract promotes it explicitly.
   > Verification: authoritative journal cells use `state_class=authoritative`; projection or runtime scratch cells use `state_class=volatile`.
   > Verification: authoritative journal commit acceptance advances the current `authoritative_revision`; in-memory tree/view refresh advances only after projection rebuild applies that accepted truth.
   > Verification: derived materialized observation state MUST NOT outrun or replace `authoritative_revision` as the domain truth source.
   > Verification: source/sink authoritative mutation recording passes through one shared commit-boundary abstraction, not duplicated runtime state-carrier write call paths.
   > Verification: fs-meta manifest config MUST reject removed authority carrier fields (`unit_authority_state_carrier`, `unit_authority_state_dir`) to prevent app-level carrier coupling.
   > Verification: if runtime state-carrier authority initialization fails, fs-meta startup MUST fail closed with explicit invalid-input error.
   > Verification: sink execution hosting is selected through worker-oriented config (`workers.sink.mode=embedded|external`); `external` hosts sink materialized tree in a dedicated sink-worker process, isolated from the main fs-meta process address space.
   > Verification: under `workers.sink.mode=external`, sink worker restart/failover rebuilds materialized tree via baseline scan/audit path; in-memory tree remains projection cache, not authoritative durable state.
7. **AUTHORITATIVE_JOURNAL_TRUTH_LEDGER**: **fs-meta System** MUST treat the bounded authoritative mutation journal as the domain truth ledger for state/effect convergence.
   > Responsibility: keep “what fs-meta currently recognizes as truth” separate from query-ready materialized views and scratch runtime state.
   > Cross-ref: root `L1-CONTRACTS` `AUTHORITATIVE_TRUTH_OBSERVATION_SEPARATION` and `STATEFUL_APP_OBSERVATION_PLANE_MINIMUM_DECLARATIONS`.
   > Verification: authoritative mutation commits pass through one shared commit boundary and one journal abstraction, rather than being inferred from query results or duplicated across source/sink business handlers.
   > Verification: the journal is the only boundary that mints or advances `authoritative_revision`; query/materialized-tree readability and projection refresh do not become competing revision authorities.
   > Verification: restart, failover, and worker-process rebuild paths replay or rebuild from authoritative journal inputs plus scan/audit progression; successful query observation alone is never the authority source.
   > Covers L0: VISION.AUTHORITATIVE_TRUTH_LEDGER
8. **BUSINESS_MODULE_ORCHESTRATION_TOKEN_FREE**: **fs-meta System** MUST keep orchestration tokens and runtime-unit identifiers out of source/sink business logic modules.
   > Responsibility: preserve local-style business coding model and keep orchestration concerns in dedicated ingress/gate adapters.
   > Verification: business modules (`sink/{arbitrator,clock,epoch,query,tree}` and `source/{scanner,watcher,drift,sentinel}`) do not contain `runtime.exec*`, orchestration-only `unit_id` carrier parsing, `unit_ids` activation summaries, or direct cluster-view coupling tokens.

---

## CONTRACTS.EVOLUTION_AND_OPERATIONS

1. **BINARY_APP_STARTUP_PATH**: **fs-meta System** MUST stay stable across runtime-managed worker startup and restart lifecycle.
   > Covers L0: VISION.BINARY_APP_RUNTIME

2. **INDEPENDENT_UPGRADE_CONTINUITY**: **fs-meta System** MUST allow higher-target-generation replacement of single-app binaries while keeping contracts stable.

3. **PRODUCT_CONFIGURATION_SPLIT**: **fs-meta System** MUST keep operator-visible product configuration split between thin deploy-time bootstrap config and online business-scope API configuration derived from runtime grants.
   > Covers L0: VISION.PRODUCT_CONFIGURATION_SPLIT
   > Responsibility: prevent operators from editing internal desired-state/runtime policy details as business configuration.
   > Verification: product deployment docs keep `fs-meta.yaml` limited to bootstrap API/auth concerns; deploy defaults to `roots=[]`; operators configure monitoring roots through `/runtime/grants`, `/monitoring/roots/preview`, and `/monitoring/roots`.
4. **RELEASE_GENERATION_CUTOVER**: **fs-meta System** MUST realize upgrades as higher-target-generation replacement on one fs-meta app boundary while preserving the product API base and replaying current monitoring roots/runtime grants into the new generation.
   > Covers L0: VISION.RELEASE_GENERATION_UPGRADE
   > Responsibility: make binary upgrades explicit at the app package boundary while keeping authoritative truth replay and externally visible observations separate.
   > Verification: upgrade workflows/docs describe submitting a higher target generation, replaying current monitoring roots/runtime grants as authoritative truth inputs, rebuilding in-memory observation/projection state through scan/audit/rescan on the candidate generation, reaching app-owned `observation_eligible` for materialized `/tree` and `/stats`, then cutting trusted external materialized observation exposure to the new generation while `/on-demand-force-find` remains a freshness path that may become available earlier, and issuing runtime-owned drain/deactivate/retire control intent to the old generation.
5. **SINGLE_ENTRYPOINT_DESIRED_STATE**: **fs-meta System** MUST allow app desired-state submission from one management entrypoint, while peer nodes can start from baseline config and receive distributed apply from runtime/kernel privileged mutation path.
6. **GLOBAL_HTTP_API_RESOURCE_SCOPED_APP_FACADE**: **fs-meta System** MUST keep one operator-facing HTTP API on a resource-scoped one-cardinality facade while internal source/sink execution evolves independently.
   > Responsibility: preserve one stable external URL through ingress-resource selection without introducing a separate roaming API boundary.
   > Verification: fs-meta generation-control document binds the HTTP facade through `api.facade_resource_id`, declares `runtime.exec.facade` as `resource_visible_nodes + one`, and does not define a standalone roaming HTTP execution role outside the app package boundary.
7. **API_FAILOVER_RESCAN_REBUILD**: **fs-meta System** MUST rebuild materialized sink/query state on newly bound instances after unit failover via baseline scan/audit/rescan path.
   > Responsibility: keep failover semantics explicit under in-memory observation ownership instead of implying durable truth transfer.
   > Verification: when a sink-bearing instance changes, the new instance starts with empty in-memory index state for its bound groups and performs rebuild for those groups.
   > Verification: rebuild lag is treated as observation lag against authoritative truth, not as truth deletion.
8. **OBSERVATION_ELIGIBILITY_BEFORE_EXPOSURE**: **fs-meta System** MUST require a newly activated generation or failover target to replay current authoritative truth and catch its observations up before it is treated as the trusted external result source.
   > Responsibility: prevent facade/API cutover from claiming observation success before materialized `/tree` and `/stats` readiness evidence shows the candidate generation has replayed current authoritative truth and completed the required first-audit/materialized-health catch-up for the active groups it serves.
   > Cross-ref: root `L1-CONTRACTS` `STATEFUL_APP_OBSERVATION_PLANE_OPT_IN`, `UNCERTAIN_STATE_MUST_NOT_PROMOTE`, and `POST_CUTOVER_STALE_OWNER_FENCING`.
   > Verification: cutover workflows describe authoritative replay first, initial audit completion plus materialized health catch-up for active scan-enabled primary groups second, and trusted external materialized `/tree` and `/stats` exposure only after the new generation reaches an app-owned `observation_eligible` point.
   > Verification: until `observation_eligible` is reached, fs-meta exposes either the previous eligible generation or explicit not-ready/degraded observation state for materialized `/tree` and `/stats` rather than silently treating partial rebuild output as current truth.
   > Verification: delayed query readability, partial rebuild output, or internal route availability do not promote a candidate generation to current truth for materialized `/tree` and `/stats` before app-owned `observation_eligible`.
   > Verification: `/on-demand-force-find` remains a freshness path and is not blocked by the initial-audit materialized-query gate.
   > Verification: before trusted materialized exposure moves to the newer generation, fs-meta emits stale-writer fence evidence that prevents older generations or stale local writers from re-exposing observations for older truth.
   > Covers L0: VISION.OBSERVATION_ELIGIBILITY_BEFORE_EXPOSURE
9. **CROSS_RELATION_DRIFT_VISIBILITY**: **fs-meta System** MUST surface degraded or failure evidence when facade ownership, bind/run realization, and materialized observation state drift apart instead of silently presenting mixed-generation results.
   > Verification: cutover and failover workflows describe explicit degraded/not-ready visibility when relation ownership, worker execution state, and current observation evidence disagree.
10. **GROUP_AWARE_SINK_BIND_RUN**: **fs-meta System** MUST treat sink execution as runtime group-aware bind/run realization instead of whole-app app-level facade routing policy.
   > Responsibility: preserve app-owned grouping while allowing runtime to realize per-group sink bind/run realization.
   > Verification: sink generation-control/runtime contract carries per-group execution shape and runtime returns per-instance bound groups.
   > Verification: sink state partitioning keys off `group_id` plus `object_ref`, not `concrete_root_id`.
11. **UNIT_CONTROL_ENVELOPE_FENCING**: **fs-meta System** MUST validate runtime unit control envelopes by unit contract (`unit_id`) and generation fencing (`generation`).
   > Responsibility: keep unit dispatch deterministic and reject unknown/invalid execution units.
   > Verification: source/sink reject `ExecControl` or `UnitTick` envelopes with unknown `unit_id`.
   > Verification: source/sink ignore stale generation envelopes (`generation` regression) instead of rolling back newer unit state.
   > Verification: accepted unit ids stay domain-bounded (`runtime.exec.source` / `runtime.exec.scan` / `runtime.exec.sink`).
   > Verification: source/sink share one gate implementation (`RuntimeUnitGate`) to enforce identical allowlist/fencing semantics.
   > Verification: source/sink consume one centralized fs-meta execution-unit registry instead of duplicating raw unit-id string literals.
   > Verification: fs-meta generation-control document keeps canonical execution identities in the `runtime.exec.*` namespace and mirrors that registry consistently.
   > Verification: unsupported non-`runtime.exec.*` unit ids are rejected by source/sink unit gate contract (fail-closed).
12. **CONTROL_FRAME_SIGNAL_TRANSLATION**: **fs-meta System** MUST treat `on_control_frame` as ingress-only boundary and translate envelopes into typed orchestration signals before business handlers run.
   > Responsibility: separate orchestration parsing from source/sink business logic and keep control ingestion deterministic.
   > Verification: top-level app and module-local control handlers call shared orchestration adapter translation first; business handlers consume typed signal enums instead of decoding control envelopes inline.
13. **ORCHESTRATION_TOKEN_PARSING_BOUNDARY**: **fs-meta System** MUST keep raw `unit_id/runtime.exec.*` parsing in orchestration adapter boundary modules only.
   > Responsibility: avoid orchestration token leakage into pure source/sink compute paths.
   > Verification: source/sink business handlers use typed unit signals (`source/scan/sink`) and no longer branch on raw unit-id strings.

14. **DOMAIN_TRACEABILITY_CHAIN**: **fs-meta System** MUST keep domain L0/L1 items traceable to runtime workflows and executable verification anchors.
---

## CONTRACTS.API_BOUNDARY

1. **FS_META_HTTP_API_BOUNDARY**: **fs-meta System** MUST expose fs-meta-domain management/observability HTTP API under one bounded resource-scoped namespace and keep API implementation inside fs-meta app module.
   > Covers L0: VISION.BOUNDED_PRODUCT_MANAGEMENT_NAMESPACE
   > Responsibility: keep external HTTP ingress owned by fs-meta domain/app package boundary without introducing runtime/daemon semantic coupling.
   > Verification: fs-meta app starts HTTP API from `fs-meta/app/src/api/*`; request handlers operate through fs-meta source/sink abstractions; API auth-init/bind failure causes explicit app startup failure.

2. **RESOURCE_SCOPED_DOMAIN_HTTP_FACADE**: **fs-meta System** MUST keep the public HTTP facade as a resource-scoped one-cardinality domain facade owned by fs-meta app package boundary rather than by kernel/runtime semantic layers.
   > Covers L0: VISION.RESOURCE_SCOPED_DOMAIN_HTTP_FACADE
   > Responsibility: keep external query/management ingress on an app-owned facade resource while internal query/find propagation stays on app-owned opaque channels.
   > Verification: external query ingress uses one active HTTP entrypoint selected from the facade resource scope on the fs-meta app package boundary; internal query/find/source/sink coordination remains on route-resolved opaque channels and does not promote extra multi-writer HTTP ingress.

3. **UNIX_STYLE_LOCAL_AUTH_IN_DOMAIN**: **fs-meta System** MUST treat Unix-style local user authentication as a domain-local credential-acquisition personality, not as a replacement for upstream signed control-submit and scope authority.
   > Covers L0: VISION.LOCAL_AUTH_WITHOUT_PLATFORM_BYPASS
   > Responsibility: provide product-local login UX without creating a domain-owned control-auth bypass.
   > Verification: login verifies passwd+shadow records, rejects locked/disabled users, and issues a session/token/signing context that is consumed by the upstream signed control-submit path rather than bypassing it.
   > Verification: protected API operations still enforce upstream signed submit and scope checks; domain-local auth only supplies product-local credential acquisition.

4. **ROLE_GROUP_ACCESS_GUARD**: **fs-meta System** MUST gate management API operations by the configured product management-session group and reject non-management sessions from the management surface.
   > Responsibility: preserve one explicit management-session boundary for product API operations without inventing an independent domain authority lattice.
   > Verification: `/status`, `/runtime/grants`, `/monitoring/roots`, `/monitoring/roots/preview`, `PUT /monitoring/roots`, `POST /index/rescan`, and query-api-key management endpoints require a management session whose principal belongs to the configured `management_group`; non-management sessions are rejected.

5. **ONLINE_ROOT_RECONFIG_WITHOUT_RESTART**: **fs-meta System** MUST support online logical-root reconfiguration through API without app restart while keeping roots/group definitions app-owned and bind/run realization runtime-owned.
   > Responsibility: keep fs-meta root lifecycle mutable at runtime while preserving fanout correctness and current owner boundaries.
   > Verification: API root/group update applies validation (including rejecting legacy `roots[].source_locator`), updates app-owned authoritative roots/group definitions against current host object grants, consumes current runtime-returned bound scopes only for resulting source/sink refresh and compensatory rescan, and does not promote API/app code into bind/run semantic ownership.
   > Verification: after an app-defined group is added or removed, projection `tree` / `force-find` group result sets converge to the new defined groups without requiring process restart or runtime auto-discovery of groups.

6. **MANUAL_RESCAN_OPERATION**: **fs-meta System** MUST provide explicit manual rescan operation to repair index drift and watch-loss conditions.
   > Covers L0: VISION.ONLINE_SCOPE_MUTATION_AND_REPAIR
   > Responsibility: offer deterministic operator-triggered repair path independent of periodic audit cadence.
   > Verification: manual rescan API triggers source rescan on group-primary pipelines only and returns accepted operation response.

7. **PRODUCT_API_NAMESPACE_STABILITY**: **fs-meta System** MUST expose a product-focused management namespace that is independent from legacy test/control helpers.
   > Responsibility: keep console, independent `fsmeta` CLI, and automation bound to the same stable product API.
   > Verification: product management endpoints live on `/session/login`, `/status`, `/runtime/grants`, `/monitoring/roots`, and `/index/rescan`; legacy `/auth/login`, `/config/roots`, `/ops/rescan`, `/release/render`, `/exports`, and `/fanout` are absent from the public product API.

8. **API_NON_OWNERSHIP_OF_QUERY_FIND_CHANNEL_PATH**: **fs-meta System** API boundary MUST NOT introduce parallel `query/find` payload APIs in management namespace.
   > Responsibility: keep management API and existing query/find channel paths semantically separated.
   > Verification: management API endpoints exclude file-tree query/find response contracts and only expose session, status, runtime-grant, root-management, and rescan payloads.
9. **QUERY_PATH_PARAMETERS_OWN_PAYLOAD_SHAPE**: **fs-meta System** MUST express query/find payload shaping through query-path parameters and path-specific response contracts, rather than management API request-body size fields.
   > Responsibility: keep payload shaping in query semantics while preserving management namespace boundary.
   > Verification: management API contracts do not define query/find body limit fields; `/tree` and `/on-demand-force-find` shape subtree metadata with `group_page_size/group_after/entry_page_size/entry_after`, while `/stats` keeps its own aggregate envelope rules.
   > Verification: query-path contracts own bytes-safe path transport as well: callers use `path_b64` when raw path bytes are not valid UTF-8, and response payloads expose authoritative optional `path_b64` exactly in those lossy cases instead of relying on management-body escape hatches.
10. **RUNTIME_GRANT_DISCOVERY_BOUNDARY**: **fs-meta System** MUST expose runtime-injected `host_object_grants` as the product-facing source for root selection.
   > Covers L0: VISION.ONLINE_SCOPE_MUTATION_AND_REPAIR
   > Responsibility: make root configuration depend on runtime grants instead of legacy exports/fanout diagnostics.
   > Verification: `GET /api/fs-meta/v1/runtime/grants` returns app-visible `GrantedMountRoot[]` descriptors carrying host/object metadata needed to compose selectors.
11. **ROOT_PREVIEW_BEFORE_APPLY**: **fs-meta System** MUST provide preview of root-to-grant matches and resolved monitor paths before persisting new roots.
   > Covers L0: VISION.ONLINE_SCOPE_MUTATION_AND_REPAIR
   > Responsibility: let operators verify selector coverage and concrete monitor paths before write-side mutation.
   > Verification: `POST /api/fs-meta/v1/monitoring/roots/preview` accepts draft roots, returns matched grants and resolved monitor paths, and keeps unmatched roots explicit.
   > Verification: `PUT /api/fs-meta/v1/monitoring/roots` revalidates draft roots against the current runtime grants and rejects any non-empty `unmatched_roots` set instead of persisting selector intent that currently has no visible grant match.
12. **EMPTY_ROOTS_VALID_DEPLOYED_STATE**: **fs-meta System** MUST allow deployed-but-unconfigured operation with zero monitoring roots.
   > Covers L0: VISION.ONLINE_SCOPE_MUTATION_AND_REPAIR
   > Responsibility: support “deploy first, discover grants later” without forcing placeholder roots.
   > Verification: startup config and `PUT /api/fs-meta/v1/monitoring/roots` both accept empty `roots`, while `/status` continues to report healthy runtime state.
13. **PRODUCT_CONSOLE_ACCESS_BOUNDARY**: **fs-meta System** MUST gate the product console with management sessions while keeping query execution on query api keys.
   > Covers L0: VISION.SEPARATE_QUERY_AND_MANAGEMENT_AUTH_SUBJECTS
   > Responsibility: keep interactive management traffic and distributed query-consumer traffic on separate auth subjects.
   > Verification: management session can access `/status`, `/runtime/grants`, `/monitoring/roots`, `/monitoring/roots/preview`, `PUT /monitoring/roots`, `POST /index/rescan`, `GET|POST /query-api-keys`, and `DELETE /query-api-keys/:key_id`.
   > Verification: management session bearer tokens are rejected on `/tree`, `/stats`, and `/on-demand-force-find`.
   > Verification: query api keys can access `/tree`, `/stats`, and `/on-demand-force-find`, but are rejected on management endpoints.

---

## CONTRACTS.APP_SCOPE

> Traces to: `VISION.APP_SCOPE`

1. **DOMAIN_CONTRACT_CONSUMPTION_ONLY**: **fs-meta System** MUST keep the app package downstream of domain/runtime contracts instead of creating a parallel package-local authority.
   > Responsibility: keep app package semantics downstream of domain/runtime authority instead of creating a parallel contract set.
   > Verification: main specs trace root/domain Convergence Vocabulary without redefining package-local authority terms.
   > Verification: main specs explicitly state fs-meta app implementation consumes fs-meta domain specs and root Convergence Vocabulary rather than redefining `Authoritative Truth`, `Observation`, `Projection`, or `Observation-Eligible`.
   > Verification: ordinary app-facing imports and boundary-conversion seams flow through `capanix-app-sdk`; the app package does not keep a direct `capanix-runtime-api` or `capanix-kernel-api` dependency for business or infra modules.
   > Verification: `fs-meta/app` and other package-local authoring surfaces MUST NOT directly depend on or reference `capanix-kernel-api`, `capanix-unit-entry-macros`, or `capanix-unit-sidecar`; those remain limited to dedicated artifact/runtime crates, explicit bridge seams, or test/dev fixtures.
   > Verification: `fs-meta/app` remains the only product app package for the fs-meta container, stays `publish = false`, owns package-local business/runtime composition, and does not present a generic reusable fs-meta library API.
   > Verification: bounded `product` remains the product-facing CLI/tooling namespace, while public `query`, `product::release_doc`, and `workers` support surfaces may remain package-local operational/test support modules without becoming product or platform authority.
   > Verification: package-local implementation tuning seams (`FS_META_SOURCE_SCAN_WORKERS`, `FS_META_SOURCE_AUDIT_INTERVAL_MS`, `FS_META_SOURCE_THROTTLE_INTERVAL_MS`, `FS_META_SINK_TOMBSTONE_TTL_MS`, `FS_META_SINK_TOMBSTONE_TOLERANCE_US`, `FS_META_SOURCE_AUDIT_DEEP_INTERVAL_ROUNDS`) are documented as bounded implementation knobs and do not redefine domain truth, cutover, or query-surface contracts.
2. **WORKER_ROLE_MODEL**: **fs-meta System** MUST present one app product container composed of four worker roles: `facade-worker`, `source-worker`, `scan-worker`, and `sink-worker`.
   > Responsibility: keep the downstream product model stable before internal crate splitting or realization changes.
   > Verification: app specs explicitly name the four worker roles and state that `query` remains inside `facade-worker` for now.
   > Verification: app specs justify the split as `4`, not `3`, by keeping heavy scan/audit work separate from live source/watch work.
   > Verification: app specs justify the split as `4`, not `5`, by keeping query orchestration inside `facade-worker` until traffic or ownership pressure proves a fifth worker is needed.
3. **WORKER_MODE_MODEL**: **fs-meta System** MUST use `embedded | external` as the only product-facing worker-mode vocabulary.
   > Responsibility: keep product docs/config focused on worker roles and worker modes rather than realization mechanics.
   > Verification: product-facing L0-L2 specs use worker/mode vocabulary and do not present realization-mechanic terms as architecture terms.
   > Verification: app specs state the initial mode defaults as `facade-worker=embedded` and `source-worker=external`, `scan-worker=external`, `sink-worker=external`.
   > Verification: operator-visible worker config stays worker-oriented through `workers.facade.mode`, `workers.source.mode`, `workers.scan.mode`, and `workers.sink.mode` rather than legacy realization vocabulary.
4. **LOCAL_HOST_RESOURCE_PROGRAMMING_ONLY**: **fs-meta System** MUST keep resource-bound source behavior on the bound host through local-host programming targets rather than inventing remote-host operation contracts.
5. **RESOURCE_SCOPED_HTTP_FACADE_ONLY**: **fs-meta System** MUST host the resource-scoped external HTTP facade for the single fs-meta app package boundary without redefining product or platform ownership.
   > Verification: app source hosts the bounded one-cardinality HTTP facade and does not promote it into product or platform authority or a separate roaming API boundary.
6. **OPAQUE_INTERNAL_PORTS_ONLY**: **fs-meta System** MUST treat internal ports and protocols as app-owned opaque semantics rather than platform-owned vocabulary.
   > Verification: app architecture and workflows keep internal ports opaque and route/channel carriers platform-owned only as transport.
   > Verification: external-worker realization helpers remain confined to explicit worker runtime seams and do not define source/sink/query/API business module contracts.
   > Verification: the upstream bridge-realization seam remains only the low-level external-worker bridge carrier; worker child-process bootstrap, log/socket ownership, retry clipping, and lifecycle supervision remain downstream product-owned runtime-support concerns rather than worker artifact or tooling responsibilities.
   > Verification: external-worker transport preserves canonical `Timeout` / `TransportClosed` categories and wall-clock total-timeout semantics instead of collapsing transport failures into a generic peer-error bucket.
7. **RELEASE_GENERATION_CUTOVER_CONSUMPTION_ONLY**: **fs-meta System** MUST consume release-generation cutover by replaying current authoritative truth inputs (`roots`, runtime grants, authoritative journal continuation) and rebuilding observation state rather than inventing package-local rollout semantics.
   > Verification: active scan-enabled primary groups reaching initial-audit-complete materialized observation state are the readiness anchor for trusted `/tree` and `/stats` exposure.
   > Verification: `/on-demand-force-find` stays on the freshness path while materialized observation catches up.
   > Verification: `/on-demand-force-find` on the freshness path remains available before trusted materialized observation is promoted.
8. **AUTHORITATIVE_TRUTH_CARRIER_CONSUMPTION_ONLY**: **fs-meta System** MUST consume authoritative truth carriers and revisions from domain/runtime boundaries rather than define a package-local competing truth source.
9. **OBSERVATION_ELIGIBILITY_GATE_OWNERSHIP**: **fs-meta System** MUST own the package-local `observation_eligible` evidence that reports when materialized `/tree` and `/stats` observations are trustworthy enough to be treated as current observation after cutover or rebuild.
   > Verification: app cutover workflow ties `observation_eligible` to materialized `/tree` and `/stats` readiness evidence rather than to direct package-local trusted external exposure ownership in runtime or mere process readiness.
   > Verification: `/on-demand-force-find` remains available as a freshness path before `observation_eligible` is reached for materialized `/tree` and `/stats`.
   > Verification: app workflow treats `observation_eligible` as materialized-query evidence while `/on-demand-force-find` stays a freshness path.
   > Verification: status/health surfaces expose explicit source coverage, degraded state, audit timing, sink capacity signals, and optional facade-pending retry diagnostics so large-NFS monitoring can distinguish trusted exposure evidence from coarse process liveness or listener retry drift.
10. **STALE_WRITER_FENCE_BEFORE_EXPOSURE**: **fs-meta System** MUST fence stale generations before runtime can trust a newer exposure path or allow older observations to re-expose.
11. **NO_PRODUCT_OR_PLATFORM_OWNERSHIP**: **fs-meta System** MUST NOT own product CLI/deploy semantics or platform bind/route/grant authority.
12. **WORKER_MODE_FAILURE_BOUNDARY_IS_EXPLICIT**: **fs-meta System** MUST describe failure isolation in worker-mode terms: `embedded` workers share a host process, `external` workers run in dedicated worker processes, and construction/bootstrap/runtime-task failures MUST surface through typed `CnxError` / `init_error` paths instead of routine production `panic!` / `expect!`.
   > Verification: product-facing L0-L2 specs describe the baseline defaults as `facade-worker=embedded` and `source-worker=external`, `scan-worker=external`, `sink-worker=external`.
   > Verification: app workflow and source continue to preserve `init_error` / join-failure handling and keep API/bootstrap construction failures on explicit typed error/log paths instead of routine `expect(...)`.
   > Verification: external-worker init retry is clipped by wall-clock total timeout even when one RPC attempt has a larger per-call timeout budget.

---

## CONTRACTS.CLI_SCOPE

> Traces to: `VISION.CLI_SCOPE`

1. **PRODUCT_DEPLOYMENT_CLIENT_ONLY**: **fs-meta System** MUST keep the fs-meta CLI as a product/operator client boundary for deploy/undeploy/local/grants/roots command workflows only; login/rescan remain bounded HTTP/API workflows rather than top-level `fsmeta` subcommands.
2. **RESOURCE_SCOPED_HTTP_FACADE_CONSUMPTION_ONLY**: **fs-meta System** MUST keep CLI requests pointed at the bounded resource-scoped fs-meta HTTP facade and deploy boundary rather than inventing parallel operator protocols.
3. **AUTH_BOUNDARY_CONSUMPTION_ONLY**: **fs-meta System** MUST keep CLI auth behavior as credentialed consumption of the product auth boundary only and MUST NOT own session, role, scope, or bypass semantics.
4. **RELEASE_GENERATION_DEPLOY_CONSUMPTION_ONLY**: **fs-meta System** MUST keep release-generation deploy/cutover on the same product boundary without exposing manual internal release-doc edits as operator workflow.
5. **DOMAIN_BOUNDARY_CONSUMPTION_ONLY**: **fs-meta System** MUST keep CLI request composition built from fs-meta domain declarations, external `cnxctl` deploy/control commands, and shared kernel-owned auth vocabulary without redefining them.
6. **NO_RUNTIME_OR_PLATFORM_OWNERSHIP**: **fs-meta System** MUST keep runtime policy, bind/run realization, route convergence, route target selection, and primitive semantics out of the CLI package.
7. **NO_OBSERVATION_PLANE_OWNERSHIP**: **fs-meta System** MUST keep `state/effect observation plane` meaning such as `observation_eligible` out of the CLI package.
8. **LOCAL_DEV_DAEMON_COMPOSITION_ONLY**: An optional tooling-only local-dev launcher MAY ship the product-scoped `capanixd-fs-meta` binary as an explicit feature, but that launcher MUST remain tooling-only composition over `capanix-daemon` bootstrap seams and MUST NOT redefine daemon ingress, runtime planning, or kernel authority.

---

## CONTRACTS.DATA_BOUNDARY

1. **TYPED_EVENT_CONTINUITY**: **fs-meta System** MUST preserve typed source-event continuity from source-origin ingest through sink materialization and query-visible envelopes.
   > Responsibility: keep source-origin event meaning stable across source, sink, and query boundaries without degrading into opaque transport-only blobs.
   > Verification: source/sink/query contracts continue to use typed event payloads and do not replace the query-visible envelope with untyped byte buckets or ad-hoc lossy projections.
2. **METADATA_CONTINUITY**: **fs-meta System** MUST preserve stable query metadata continuity across one logical observation snapshot until new ingest advances the materialized view.
   > Responsibility: keep one logical observation snapshot internally self-consistent so metadata does not churn without corresponding ingest/change progression.
   > Verification: grouped query contracts and tests preserve stable snapshot metadata until new ingest, audit, or replay advancement explicitly updates the materialized observation.
3. **REFERENCE_DOMAIN_SLICE_CONNECTIVITY**: **fs-meta System** MUST preserve reference-domain slice connectivity so related file-system slice updates remain queryable through the same fs-meta domain boundary.
   > Responsibility: keep related file-system slice updates connected inside one fs-meta query domain instead of fragmenting them across unrelated product surfaces.
   > Verification: reference-domain slice tests and grouped query contracts continue to present related slice updates through one fs-meta query boundary rather than splitting them into separate ad-hoc APIs or uncorrelated result families.

---

## CONTRACTS.FAILURE_ISOLATION_BOUNDARY

1. **EXECUTION_FAILURE_DOMAINS_ARE_EXPLICIT**: **fs-meta System** MUST explicitly distinguish non-isolating in-process loadable boundaries from worker-process execution boundaries, and MUST NOT present them as one equivalent fault domain.
   > Covers L0: VISION.EXPLICIT_EXECUTION_FAILURE_DOMAINS
   > Responsibility: keep worker-mode failure domains explicit so embedded host-process failures and external worker-process failures are not treated as the same operational event.
   > Verification: architecture and workflow specs describe `embedded` workers as sharing a host process and `external` workers as dedicated worker processes; public docs do not present them as equivalent isolation shapes.

2. **INTERFACE_TASK_OR_WORKER_FAILURE_CONTAINMENT_TARGET**: **fs-meta System** SHOULD isolate recoverable interface failures (for example watcher/audit task panic, query worker crash, or sink worker loss) to task or worker scope when feasible, while preserving explicit degraded visibility.
   > Covers L0: VISION.TASK_OR_WORKER_FAILURE_CONTAINMENT
   > Responsibility: keep recoverable interface-task and worker failures contained to the narrowest feasible scope while surfacing degraded/failure evidence.
   > Verification: managed endpoint tasks and worker supervision keep source/sink/query failures explicit in status or degraded outputs instead of silently collapsing them into whole-app success.

3. **FAILURE_IMPACT_BY_EXECUTION_SHAPE_DECLARED**: **fs-meta System** MUST document that in-process crash semantics can take down all boundaries in that host process, while worker-process failures are recovered through runtime lifecycle restart/rebind and rebuild paths rather than being mistaken for truth loss.
   > Covers L0: VISION.FAILURE_IMPACT_DECLARED_BY_MODE
   > Responsibility: declare user-visible failure impact by worker mode so recovery expectations stay stable across product docs, tooling, and runtime behavior.
   > Verification: specs and runtime behavior state that embedded crash semantics can terminate the shared host process, while external worker failures recover through restart/rebind/rebuild rather than being interpreted as authoritative truth loss.

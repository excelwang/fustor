---
version: 3.0.0
---

# L0: fs-meta Domain Vision

> Mission: 在 Capanix 上以单一 fs-meta 域 authority 与单一 app 产品边界提供文件元数据能力，
> 对外暴露稳定产品接口与可审计的 observation 结果；内部执行职责、隔离形态与升级/故障恢复路径可以演进，
> 但 domain meaning 继续由 fs-meta 自身拥有。

## In-Scope

### VISION.DOMAIN_IDENTITY

1. **FS_META_DOMAIN_CONSTITUTION**: User wants one fs-meta 域级 authority/宪法层，统一约束 `app/shared-types` 的跨组件语义，避免重复定义和漂移。
2. **MODULE_ROLE_BOUNDARY**: User wants fs-meta 对外表现为一个整体产品边界；内部职责可以拆分为 facade/query ingress、live source observation、scan/audit 和 materialized index maintenance，但不应把这些内部边界升级成多产品语义。
3. **BINARY_APP_RUNTIME**: User wants runtime 以共享进程或专属进程两类隔离形态承载这些职责，保证升级与故障隔离能力，同时不把 realization-mechanic 词汇提升为产品语义。
4. **META_INDEX_DOMAIN_OWNERSHIP**: User wants `meta-index` 归属 fs-meta 域服务，不进入 kernel 业务状态。

### VISION.KERNEL_RELATION

1. **GENERIC_KERNEL_MECHANISM_CONSUMPTION**: User wants fs-meta 只建立在通用的 runtime ABI、bind/route/grant 和 channel-attach 机制之上，不依赖 fs-meta-specific platform primitives。
2. **POLICY_OUTSIDE_KERNEL_FOR_GROUPING**: User wants“按挂载路径分组”作为域策略而不是 kernel 语义。
3. **HOST_ADAPTER_SDK_TRANSLATION_OWNERSHIP**: User wants宿主本地 facade/ABI 适配归 host-adapter 层，不放在 fs-meta 域策略里，也不把宿主 host-operation 当作分布式契约。
4. **RESOURCE_BOUND_LOCAL_HOST_PROGRAMMING**: User wants资源绑定 source 行为在绑定宿主上通过公共 host-fs facade 编程，而不是依赖远程宿主操作语义。
5. **THIN_RUNTIME_ABI_CONSUMPTION**: User wants fs-meta run 只消费薄 runtime ABI，而不是依赖宽而杂的 platform verb 目录。
6. **APP_OWNS_OPAQUE_PORT_MEANING**: User wants fs-meta 内部 query、find、source、sink 协调保持 app-owned opaque port 语义；kernel/runtime 只负责 route 与 channel。

### VISION.QUERY_OUTCOME

1. **QUERY_PATH_AVAILABILITY**: User wants稳定低延迟 `query` 路径，面向物化索引。
2. **FIND_PATH_AVAILABILITY**: User wants `find` 路径支持实时探测/强制新鲜视图。
3. **UNIFIED_FOREST_RESPONSE**: User wants查询路径共享一致的 observation semantics、selection vocabulary 与 failure visibility，便于上层 CLI/UI 复用；不同路径可以保留各自的 payload 细节，但必须属于同一家 grouped-result 模型。
4. **PARTIAL_FAILURE_ISOLATION**: User wants单组失败不会拖垮整次查询，系统返回可观测部分成功结果。
5. **GROUP_ORDER_MULTI_GROUP_QUERY**: User wants `/tree` 与 `/on-demand-force-find` 直接返回多组结果；分组排序应该是显式查询轴，而不是隐藏的单组选主逻辑。
6. **QUERY_HTTP_FACADE**: User wants query path 通过稳定 HTTP 入口输出 JSON，并保持参数默认行为可预测。
7. **RESOURCE_SCOPED_DOMAIN_HTTP_FACADE**: User wants对外 HTTP facade 由 facade resource 驱动的 one-cardinality facade 承担，固定归属于 fs-meta 域 app authority，而不是提升为 kernel/runtime 语义 authority。
8. **STATELESS_QUERY_PROXY_AGGREGATION**: User wants投影层按请求实时转发与聚合，不维护成员注册表或缓存式 peer 状态。
9. **QUERY_TRANSPORT_DIAGNOSTICS**: User wants投影层暴露 RPC 传输诊断计数，便于排查 timeout/correlation 异常。

### VISION.INDEX_LIFECYCLE

1. **REALTIME_LISTENING**: User wants持续监听增量变化，保持索引新鲜度。
2. **INITIAL_FULL_SCAN**: User wants启动期执行主挂载全盘基线扫描。
3. **AUDIT_REPAIR**: User wants周期性 audit 对索引漂移执行修正。
4. **SENTINEL_FEEDBACK**: User wants哨兵信号触发自愈与降级可见性。
5. **SINK_SINGLE_TREE_ARBITRATION**: User wants sink 在每个组内使用单树进行仲裁/构建，对外不暴露 member 子层。

### VISION.OBSERVATION_CONVERGENCE

1. **AUTHORITATIVE_TRUTH_LEDGER**: User wants fs-meta 明确区分 authoritative truth 与对外 observation，并让权威变更有正式 truth ledger 可追踪。
2. **OBSERVATION_IS_NOT_TRUTH**: User wants `/tree`、`/stats`、`/on-demand-force-find` 等对外结果被视为 observation/projection，而不是自动等同于当前权威真值。
3. **OBSERVATION_ELIGIBILITY_BEFORE_EXPOSURE**: User wants新 generation 或新实例只有在 replay 当前 truth 并把 observation 追平到可接受状态后，才对外承担可信结果暴露。
4. **CROSS_RELATION_DRIFT_VISIBILITY**: User wants fs-meta 在 facade ownership、bind/run 结果和 materialized observation plane 对可信结果发生分歧时显式暴露 degraded/failure evidence，而不是静默输出半新半旧结果。

### VISION.API_BOUNDARY

1. **BOUNDED_PRODUCT_MANAGEMENT_NAMESPACE**: User wants fs-meta 管理 API 收口到一个稳定的产品命名空间，统一承载 session/status/runtime grants/monitoring roots/index rescan/query-api-key 管理，而不是散落为旧 helper 或多套入口。
2. **SEPARATE_QUERY_AND_MANAGEMENT_AUTH_SUBJECTS**: User wants 查询流量与管理流量使用不同的凭据主体，避免管理 session 变成 query 令牌，也避免 query api key 获得管理写权限。
3. **LOCAL_AUTH_WITHOUT_PLATFORM_BYPASS**: User wants Unix-style 本地登录只作为产品侧凭据获取方式，而不是绕过上游 signed submit / scope authority。
4. **ONLINE_SCOPE_MUTATION_AND_REPAIR**: User wants runtime grants 查询、roots preview/apply、empty-roots deployed state、以及 manual rescan 都作为显式在线运维工作流存在，而不是依赖重启或隐藏内部工具。

### VISION.EVOLUTION_AND_OPERATIONS

1. **PRODUCT_CONFIGURATION_SPLIT**: User wants正式部署只暴露薄 bootstrap 配置面，而把业务监控范围配置留给产品 API 在运行时基于 runtime grants 完成，不把 platform-owned config/intention semantics 重新发明成 fs-meta 私有配置模型。
2. **RELEASE_GENERATION_UPGRADE**: User wants新版本 fs-meta 通过单 app 边界的 release-generation cutover 升级，而不是手工编辑内部 desired-state 文档或节点 manifest。

### VISION.APP_SCOPE

> Traceability: fs-meta app implementation must consume fs-meta domain specs and root Convergence Vocabulary. `Authoritative Truth`, `Observation`, `Projection`, and `Observation-Eligible` retain their upstream meanings unless this main spec explicitly narrows them.

1. **DOMAIN_CONTRACT_CONSUMPTION_ONLY**: User wants package-local fs-meta app implementation limited to consuming domain and platform contracts rather than redefining them.
2. **WORKER_ROLE_MODEL**: User wants the product-facing app package to present one `fs-meta` app product container whose execution responsibilities cover facade/query ingress, live source observation, scan/audit, and sink/materialized-index maintenance; `query` remains with the ingress/query responsibility until a later split is justified.
3. **WORKER_MODE_MODEL**: User wants product-facing execution described only by shared-process versus dedicated-process isolation choices, without exposing realization-mechanic terminology as architecture vocabulary.
4. **LOCAL_HOST_RESOURCE_PROGRAMMING_ONLY**: User wants resource-bound source and other app-internal behavior implemented through bound-host local-host programming targets without redefining platform semantics.
5. **RESOURCE_SCOPED_HTTP_FACADE_ONLY**: User wants the package-local app implementation to host one bounded external HTTP facade for the single fs-meta app boundary.
6. **OPAQUE_INTERNAL_PORTS_ONLY**: User wants the app package to keep internal coordination on app-owned opaque protocols rather than redefining platform vocabulary.
7. **RELEASE_GENERATION_CUTOVER_CONSUMPTION_ONLY**: User wants the app package to consume release-generation cutover semantics rather than invent package-local rollout semantics.
8. **AUTHORITATIVE_TRUTH_CARRIER_CONSUMPTION_ONLY**: User wants the app package to consume authoritative truth carriers and revisions from runtime/domain boundaries rather than inventing its own truth source.
9. **OBSERVATION_ELIGIBILITY_GATE_OWNERSHIP**: User wants the app package to own the local readiness evidence that determines when rebuilt materialized results are trustworthy enough for current external use.
10. **STALE_WRITER_FENCE_BEFORE_EXPOSURE**: User wants stale generations fenced before runtime can promote a newer generation or allow older observations to re-expose.
11. **NO_PRODUCT_OR_PLATFORM_OWNERSHIP**: User wants product, deploy, and platform authority kept outside the package-local app implementation.
12. **WORKER_MODE_FAILURE_BOUNDARY_IS_EXPLICIT**: User wants failure isolation described in terms of shared-process versus dedicated-process execution boundaries, and constructor/bootstrap/join failures to surface as typed app errors rather than unexpected crashes.

### VISION.CLI_SCOPE

1. **PRODUCT_DEPLOYMENT_CLIENT_ONLY**: User wants fs-meta CLI limited to product/operator workflows.
2. **RESOURCE_SCOPED_HTTP_FACADE_CONSUMPTION_ONLY**: User wants CLI to target the bounded resource-scoped fs-meta HTTP facade and deploy entrypoint rather than inventing a parallel operator boundary.
3. **AUTH_BOUNDARY_CONSUMPTION_ONLY**: User wants CLI auth to remain credentialed consumption of the product auth boundary only.
4. **RELEASE_GENERATION_DEPLOY_CONSUMPTION_ONLY**: User wants CLI to drive release-generation deployment/cutover from the same product boundary rather than by manual internal release-doc edits.
5. **DOMAIN_BOUNDARY_CONSUMPTION_ONLY**: User wants CLI requests built from domain and platform boundary types instead of redefining them.
6. **NO_RUNTIME_OR_PLATFORM_OWNERSHIP**: User wants runtime policy and platform authority kept out of the CLI package.
7. **NO_OBSERVATION_PLANE_OWNERSHIP**: User wants `route` convergence, route target selection, and `state/effect observation plane` meaning kept out of the CLI package.
8. **LOCAL_DEV_DAEMON_COMPOSITION_ONLY**: User wants any fs-meta-specific local-dev daemon launcher kept as optional product tooling, separate from the main app runtime surface, rather than creating a new platform daemon authority.

### VISION.DATA_BOUNDARY

1. **TYPED_EVENT_CONTINUITY**: User wants source-origin events to remain typed and traceable all the way into query-visible fs-meta outputs.
2. **METADATA_CONTINUITY**: User wants one logical observation snapshot to keep stable metadata until new ingest advances the materialized view.
3. **REFERENCE_DOMAIN_SLICE_CONNECTIVITY**: User wants related file-system slice updates to remain connected inside the same fs-meta domain boundary instead of fragmenting across unrelated views.

### VISION.FAILURE_ISOLATION_BOUNDARY

1. **EXPLICIT_EXECUTION_FAILURE_DOMAINS**: User wants shared-process 与 dedicated-process execution boundary 的故障域被明确区分，而不是被表述成同一种失败语义。
2. **TASK_OR_WORKER_FAILURE_CONTAINMENT**: User wants 可恢复的 interface task 或 execution-role 故障尽量局限在 task/role 范围内，并保持 degraded/failure evidence 可见。
3. **FAILURE_IMPACT_DECLARED_BY_MODE**: User wants 文档与系统行为明确说明不同执行形态的失败影响：shared-process 故障可能影响同宿主进程内全部工作，而 dedicated-process 故障通过 restart/rebind/rebuild 恢复，而不是被误当成 truth 丢失。

## Out-of-Scope

### VISION.NON_GOALS

1. **NON_GOAL_FILE_CONTENT** (Context): fs-meta 不负责文件内容传输与内容查询，仅负责文件元数据。
2. **NON_GOAL_KERNEL_POLICY_OWNER** (Context): fs-meta 不拥有 kernel/runtime 的 bind/run/route/grant 语义 authority。
3. **NON_GOAL_HOST_ABI_BRIDGE_IN_DOMAIN** (Context): fs-meta 不直接承载宿主本地适配/host-local adaptation 或分布式宿主调用契约细节。

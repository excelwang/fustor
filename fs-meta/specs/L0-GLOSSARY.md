---
version: 3.0.0
---

# L0 Glossary: fs-meta Domain Terms

> Traceability: fs-meta domain terminology extends root `L0-GLOSSARY` Convergence Vocabulary.
> Unless fs-meta explicitly narrows them, `Authoritative Truth`, `Observation`,
> `Projection`, `Authoritative Revision`, and `Observation-Eligible` retain
> their root meanings.

1. **Materialized Query Path**: 面向 fs-meta 已收敛物化观测状态的查询路径，目标是稳定、低延迟地读取当前对外 observation。
2. **Force-Find Query Path**: 面向 fresher 结果的查询路径，通过触发按组 live probe 或定向 fresh scan 获取更实时的视图。
3. **Grouped Tree/Find Envelope**: `/tree` 与 `/on-demand-force-find` 共享的多组结果体家族。顶层表达当前请求与分组结果，每个 group item 再独立携带该组的 root、entries、分页、稳定性与元数据结果。
4. **Mount-Root Object**: runtime 授予 fs-meta 的宿主挂载点对象。fs-meta 在绑定宿主上通过公共 host-fs facade 消费它，并使用其宿主与对象描述字段进行分组和策略判断。
5. **Delete-Aware Aggregation**: 一种优先保留删除语义、避免已删节点被迟到事件错误回生的聚合规则。
6. **Domain Constitution**: fs-meta 域级规格层，只承载跨组件 ownership、边界和域级不变量。
7. **Opaque Port**: fs-meta app 自有的 rendezvous 名称，用于内部 source、sink、query 和 find 协调；平台层承载连接但不拥有其业务含义。
8. **Thin Runtime ABI**: runtime 注入给 fs-meta run 的窄上下文边界，承载 run 身份、`host_object_grants`、generation/lease 证据、控制事件和 channel hooks。
9. **Resource-Scoped Domain Facade**: fs-meta 对外 one-cardinality HTTP facade，由 facade resource scope 驱动，语义 owner 仍是 fs-meta 域边界。
10. **Thin Deploy Config**: 正式部署时由操作员维护的最小 bootstrap 输入，只覆盖产品上线所必需的 API 和认证初始项。
11. **Release Generation**: 单 fs-meta app 边界上的一代发布版本；升级通过 generation cutover 完成，同时保留稳定的产品入口。
12. **Quiet-Window Stability**: 一种面向 materialized observation 的稳定性概念，用来表示某个 path 或 subtree 在有效变更与覆盖恢复都沉静之后进入可可信读取的状态。
13. **Point-in-Time Query Session**: 一次查询创建的冻结视图，会把当次 group 选择与分组结果固定下来，以便后续分页在同一视图内继续读取。
14. **Authority Epoch**: fs-meta 当前业务监控范围的一次权威世代，由 monitoring roots signature、runtime grants signature、source stream generation、sink materialization generation、facade/runtime generation 共同确定。跨 Authority Epoch 的观察证据、runner 绑定证据和 readiness 证据不能互相复用；其中任一组成项变化都形成新的 Authority Epoch。
15. **Runner Binding Evidence**: 在某个 Authority Epoch 内，fs-meta 根据当前 monitoring roots 与 runtime grants 得出的 group 到可执行 fresh 查找的 source runner 集合的证据。它属于 fs-meta 领域策略，不属于 runtime/kernel 业务语义。
16. **Force-Find Ready**: 某个目标 group 在当前 Authority Epoch 下存在有效 Runner Binding Evidence，且没有被同 group 的并发 fresh scan gate 阻塞的状态。该状态不要求 materialized observation 已经可信。
17. **Selected-Runner Fresh Execution**: 某个 Force-Find Ready group 已由 fs-meta 选定一个 runner 后，该 group 的 fresh 查找只等待该 runner 的一次成功、失败或超时结果。未指定单个 group 的多 group 查询仍可按 group 汇总多个独立结果，但每个 group 都由自己的选定 runner 决定。
18. **Management Write Ready**: fs-meta 管理写路径具备当前 Authority Epoch 的 active control stream，可安全提交会改变业务 scope、repair 动作或凭据状态的管理写入。它不同于 HTTP facade 可访问，也不同于 trusted materialized observation 可读。
19. **Readiness Plane**: fs-meta 对外 readiness 的领域维度，包括 API Facade Liveness、Management Write Ready、Trusted Observation Ready。三个维度可以独立变化，不能压成一个布尔 ready。
20. **Materialized Readiness Evidence**: sink/source status fan-in 汇总出的 group 级 materialized observation 证据，用于判断 `/tree`、`/stats` 的 materialized/trusted-materialized 可读性。该证据在同一 Authority Epoch 内按单调规则使用；跨 epoch 必须重新建立。
21. **AuditCoverageMode**: group/root 当前 observation 覆盖方式的粗粒度枚举：`realtime_hotset_plus_audit`、`audit_only`、`audit_with_metadata`、`audit_without_file_metadata`、`watch_degraded`。它用于解释覆盖来源，不替代 query read_class 或 trusted observation 判定。
22. **ObservationCoverageCapability**: group/root 当前可声明的 observation 能力位，包括 `exists_coverage`、`file_count_coverage`、`file_metadata_coverage`、`mtime_size_coverage`、`watch_freshness_coverage`。能力位关闭时，查询结果必须显式 withheld/degraded，不能伪造对应 metadata。
23. **RuntimeArtifactEvidence**: fs-meta runtime/app 实际加载 artifact 的可验证证据，至少包含 artifact path 与 content hash，并可与部署期 expected hash 对比。它是部署/测试一致性证据，不是业务 observation truth。

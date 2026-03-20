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

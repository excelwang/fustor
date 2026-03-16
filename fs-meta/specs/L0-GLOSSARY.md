version: 2.0.2
---

# L0 Glossary: fs-meta Domain Terms

> Traceability: fs-meta domain terminology extends root `L0-GLOSSARY` Convergence Vocabulary. Unless fs-meta explicitly narrows them, `Authoritative Truth`, `Observation`, `Projection`, `Authoritative Revision`, and `Observation-Eligible` retain their root meanings and are only refined for fs-meta-specific query/index semantics below.

1. **Materialized Query Path**: 面向 fs-meta app 已收敛状态的查询路径，目标是低延迟稳定读取。
2. **Force-Find Query Path**: 通过 fs-meta app 的实时扫描路径查询，目标是更高新鲜度读取。
3. **Grouped Tree/Find Envelope**: `/tree` 与 `/on-demand-force-find` 查询响应的顶层多组结果体。它显式携带 `groups[]` 与 `group_page`，每个 group item 再单独携带自己的 `root`、`entries`、`entry_page`、`stability` 与 `meta`。
4. **Group Order Axis**: 查询参数 `group_order`，取值仅 `group-key`、`file-count`、`file-age`。它只定义 group bucket 的排序方式，不定义 member 级筛选，也不改变稳定性或元数据可见性语义。
5. **Mount-Root Object**: runtime 授予给 fs-meta 的宿主挂载点对象。对象通过公共 host-fs facade 在绑定宿主本地被消费，并同时携带宿主描述字段（如 `mount_point`、`fs_source`、`fs_type`、`host_ip`），供 app 进行分组与策略判断。
6. **Delete-Aware Aggregation**: 在 raw 事件聚合中确保删除语义优先且不复活已删节点的规则。
7. **Domain Constitution**: fs-meta 域级规格层，只定义跨组件权责和域级不变量。

8. **Opaque Port**: fs-meta app 自有的 rendezvous 名称，用于内部 source/sink/query/find 协议接线。kernel/runtime 只解析 use/serve 与 channel attach/accept，不解释其业务含义。
9. **Thin Runtime ABI**: runtime 注入到 fs-meta run 的窄上下文边界，包含 run 身份、`host_object_grants`、generation/lease、控制事件与 channel hooks。
10. **Resource-Scoped Domain Facade**: fs-meta 对外 one-cardinality 的 HTTP facade。它由 facade resource scope 驱动，属于 fs-meta 域 app 边界，而不是 kernel/runtime 的语义 authority。
11. **Thin Deploy Config**: 正式部署时暴露给操作员的最小 bootstrap 配置面，仅覆盖 API 监听与初始认证等启动必需项，不承载内部 desired-state/runtime policy 细节。
12. **Release Generation**: 单 fs-meta app 边界上的一代二进制发布版本。升级通过新 generation cutover 完成，同时保留既有产品 API base 与业务配置来源。
13. **Stability Evaluation Axis**: 查询参数轴 `stability_mode` / `quiet_window_ms`，只定义是否评估路径稳定性以及使用什么 quiet-window 阈值；它独立于 `group_order`、group bucket 分页、以及 per-group entry pagination 语义。
14. **Metadata Mode Axis**: 查询参数轴 `metadata_mode`，取值仅 `full`、`status-only`、`stable-only`；它只控制是否返回元数据、仅返回稳定状态、或仅在稳定时返回元数据，不改变组选择或稳定性计算规则。
15. **Sync-Refresh Update**: 周期性 scan/audit/repair/update 事件中，那些仅重新确认或重放当前 materialized subtree 结果、而未改变对外可见 subtree 元数据结果的更新。它们 MAY 改善 coverage/evidence，但 MUST NOT 仅因收到该事件就重置 quiet-window 计时。若周期性 refresh 发现并落地了会改变对外结果的 `modified_time_us` 或其他查询可见字段变化，则该事件不再属于 sync-refresh update。
16. **Write-Significant Update**: 被 fs-meta 观察到并改变某 path/subtree 对外 materialized 结果的变化，例如 create/delete、rename 产生的 delete+upsert、目录/文件类型变化、或影响查询 payload 的元数据字段变化。周期性 refresh 中发现并接受的 `modified_time_us` 变化，若会改变对外 tree payload、subtree stats、或 `group_order=file-age` 的排序依据，也属于 write-significant update。Write-significant update 会重置受影响 path 及其祖先 subtree 的 quiet-window 计时。
17. **Quiet-Window Stability**: 某 path/subtree 仅在所选 materialized group 处于可信 coverage，且自 `max(subtree_last_write_significant_change_at, last_coverage_recovered_at)` 起持续静默超过给定 `quiet_window_ms` 时，才视为 quiet-window stable。它不由 `best` 选择、单个文件 mtime、或单次 query readability 单独定义。
18. **PIT Query Pagination Axis**: `/tree` 与 `/on-demand-force-find` 查询参数轴 `pit_id` / `group_page_size` / `group_after` / `entry_page_size` / `entry_after`。首次请求创建 point-in-time 查询会话并返回 `pit.id`；`group_*` 与 `entry_*` 只在该 PIT 内定义 bucket 与 per-group entry 的继续分页。它们不改变 `group_order`、quiet-window 稳定性、或 metadata withholding 规则。

version: 2.17.2
---

# L3 Runtime: fs-meta Workflows

## [workflow] StartupAndIndexBootstrap

**Steps**

1. 管理入口节点提交 fs-meta app 期望状态（单点声明）。
2. app config 通过 group/roots 逻辑声明监控分组与子路径范围（每组含 id、descriptor 选择规则、watch/scan，不含运行时节点枚举）。
3. runtime/kernel privileged mutation path 将期望状态传播，并向已调度 run 注入薄 runtime ABI 上下文；resource-scoped HTTP API 继续附着在 one-cardinality fs-meta app facade unit 上，source/sink 的内部执行策略可独立演进。
4. app 使用 runtime 注入的 `host_object_grants`、宿主描述字段以及绑定宿主上的公共 host-fs facade 形成 app-owned groups。
5. app 为每个已调度 mount-root object 启动独立 watch 管线并并流输出事件；同一 source group 内所有 member 都执行监听并维护 hot watch set。
6. app 启动实时监听增量流并持续更新索引。
7. app 仅在各 group 的 source-primary executor 上执行 epoch0 初始基线扫描与周期 audit/sentinel 循环；非 primary source instances 不执行组级扫描循环，但仍持续上送元数据事件。
8. 哨兵反馈触发降级/修复路径并形成可观测状态。
9. 若 `start` 阶段失败（例如平台不支持 realtime watch 或 auth/api 初始化失败），激活必须失败并回传显式错误，不得报告成功运行；`roots=[]` 本身是合法的已部署未接入状态。
10. 物化索引基线保持进程内内存态；进程重启或 owner 切换后必须通过扫描/审计链路重建。
11. unit 作用域 authoritative commit journal 统一通过 runtime-owned `statecell_*` state-carrier boundary 提交；app 仅声明 `state_class`（`authoritative`/`volatile`），不声明底层 carrier 介质。
12. sink 在每个 logical group 内维持单树仲裁/构建状态，对外查询响应不暴露 member 子层。
13. `workers.sink.mode=external` 时，sink materialized tree 由独立 sink-worker 进程承载；主 fs-meta 进程通过 kernel data route 转发 sink 读写与控制帧，worker 仅消费相同的 runtime ABI、route-resolved opaque channels 与内部 sink protocol，worker 重启后走扫描/审计重建。

## [workflow] UnifiedAuditScanWorkflow

**Steps**

1. source 在统一扫描中仅由 source-primary executor 对 `watch=true` 目录执行 `watch-before-read`：先尝试注册 watch，再读取目录内容。
2. 初始基线扫描必须产出根目录自身的目录元数据记录。
3. 扫描遍历使用 `scan_workers` 并行执行；事件输出按 `batch_size` 重新分批，避免超大批次突发。
4. 审计阶段对目录启用 mtime 差分：命中静默目录时跳过文件级 stat，并发出 `audit_skipped=true` 心跳；仍递归子目录。
5. source 在每轮 baseline/audit 扫描前后都发出 in-band 审计边界控制事件：`EpochStart(Audit, epoch_id)` 与 `EpochEnd(Audit, epoch_id)`。
6. source 为 scan/realtime/control 事件统一写入 `EventMetadata.timestamp_us = shadow_now_us(drift)`，该时间轴与 payload `modified_time_us` 解耦。
7. 若 drift 样本采集不可用（权限受限/metadata 不可得/瞬时 I/O 失败），source 仍必须继续启动并发流；drift 基线保持 `0`，并记录降级告警。
8. scan/watch 过程中的单条目错误（如 `ENOENT`、metadata/read_dir 失败）仅影响该条目，执行 `log+skip`，不得中断整条管线。
9. watcher 必须订阅并处理 `IN_ATTRIB`；该类事件按 `Update + is_atomic_write=false` 发出，不得误标为原子写。
10. force-find 诊断错误标记事件不参与上述时间轴约束，可保留轻量诊断时间戳实现。
11. root stream 启动前先探测 granted mount-root object 可用性；若根不可用则按 `1s→2s→...→300s` 指数退避重试，直到根恢复或收到 shutdown/close。
12. 扫描遍历维护目录身份集合 `(dev, ino)`；命中回环 symlink 时记录告警并跳过，避免无限递归。
13. watcher 收到 `IN_Q_OVERFLOW` 时发出 in-band `ControlEvent::WatchOverflow` 并置 sink 组级 `overflow_pending_audit`，查询响应立即标记 `reliable=false`。
14. watcher 收到 `IN_IGNORED` 时必须立即清理 wd/path 映射；若对应根 watch 被移除（根被删除或卸载），立即关闭流并退出任务。
15. overflow 处理在当前基线中不触发即时全盘补扫；`overflow_pending_audit` 仅在下一次 primary audit `EpochEnd` 后清除，并恢复可靠性标记。
16. audit 扫描产生的文件级 `FileMetaRecord` 必须携带 `parent_path` 与 `parent_mtime_us`，供 sink 进行 parent-staleness 拒绝判定。
17. source 通过 schema 约定输出 MessagePack `FileMetaRecord`：`path` 为 raw-bytes 且在 in-root emission 中保持 leading-slash 相对路径；`source` 字段按产出轨道显式标记 `Realtime/Scan`。

## [workflow] GroupFormationFromHostObjectDescriptorsWorkflow

**Steps**

1. source 读取 app config 中的 group/roots 逻辑声明。
2. source 读取 `__cnx_runtime.host_object_grants`，其中每个 `mount-root` object 带 `object_ref`、`host_ref`、`host_ip`、`mount_point`、`fs_source`、`fs_type` 等 descriptors。
3. app 仅使用 descriptors + 管理配置形成 groups，不依赖 runtime 解释路径或节点含义。
4. app 可使用 host descriptor selectors（例如 `host_ip`、`host_name`、`site/zone`）与 object descriptors（例如 `mount_point`、`fs_source`、`fs_type`）决定分组。
5. on runtime grants-changed control event，source 按版本号执行在线增量重收敛（新增 object 启动新的本地执行分区，移除 object 有界回收旧分区），并触发定向重扫。
6. mount-root object 生命周期独立，单 object 失败不阻塞同组其他 objects；禁止“仅更新内存映射但不生效任务变更”。

## [workflow] SourcePrimaryExecutorSelectionWorkflow

**Steps**

1. app 从当前已调度 source instances 与 group membership 计算每组候选执行者集合。
2. app 使用稳定顺序选择一个 source-primary executor；该规则由 fs-meta 拥有，不由 runtime 解释。
3. source-primary executor 负责该组 audit/sentinel 周期循环，非 primary source instances 不执行组级循环。
4. 同组所有 member 都保留 watch/listen；source-primary executor 仅控制周期循环职责，不控制数据上送资格。
5. 同组所有 member 的 fs 元数据事件都进入 sink 的目录树构建管线。
6. 当已调度 source instances 集合变化时，app 在组内重算 source-primary executor，并在不中断其他组的前提下完成漂移收敛。

## [workflow] ForceFindGroupLocalExclusivity

**Steps**

1. 每个 group 维护一个 app-owned in-flight mutex，确保同组同一时刻最多一个 `force-find`。
2. `force-find` runner 采用 round-robin 在当前已调度 source instances 中选择，不要求与 source-primary executor 相同。
3. 若被选 runner 调用失败，app 回退到下一个 bound source run。
4. facade/query 先确定目标 groups；若调用方未指定 group，则 fan-out 目标集合为当前 path 命中的 groups。
5. facade/query 对每个目标 group 启动一个独立 `force-find` 执行分支，并在 facade 侧汇总多组结果信封。
6. source 只执行自己被选中的本地 group 范围工作，不得把该请求再转发给 remote source 执行分支。
7. 该排他语义完全在 app 内实现，不向 runtime 引入临时 lease 语义。

## [workflow] GroupPartitionedEpochMidWorkflow

**Steps**

1. sink 按 logical group 维护独立 state（tree/clock/epoch）。
2. EpochStart/EpochEnd 在对应 group 维度推进，不跨组共享 epoch 状态。
3. MID 仅对当前 group 执行缺失回收（hard-remove），不得影响其他组。
4. Realtime delete 保持 tombstone 语义；MID 不创建 tombstone。

## [workflow] SinkArbitrationAuthorityWorkflow

**Steps**

> Boundary: 此处 `group_id` 仅表示 fs-meta 领域内的业务分组/投影分区，不是 runtime 平台 bind/run scope carrier。

1. sink 收到 `FileMetaRecord` 后先按 `group_id` 路由到组内单树。
2. 对于 `Scan` 事件，先执行祖先 tombstone 拦截与 `parent_mtime_us` 过期检查。
3. 若同一路径发生类型变更（`dir<->file`），先清理该路径下已缓存的后代节点，再应用当前事件。
4. 执行 LWW 仲裁：默认拒绝 `mtime` 回退或相等更新。
5. 仅当“`incoming=Realtime atomic` 且 `existing=Scan`”时，允许在相同/更旧 `mtime` 情况下覆盖。
6. `audit_skipped=true` 的 Scan 心跳可绕过普通 mtime 拒绝，用于保持 epoch 可观测性与 MID 保护上下文。

## [workflow] TombstoneReincarnationWorkflow

**Steps**

1. Realtime delete 将节点标记为 tombstone，并写入 TTL 到期时间。
2. Tombstone 生效窗口内，`Scan` 与 `Realtime non-atomic` 事件需通过 mtime tolerance 判定；落在 tolerance 内视为 zombie，拒绝回生。
3. 若 mtime 差异超过 tolerance，视为真实重建，清除 tombstone 并接受更新。
4. tombstone 期满后允许回生；过期 tombstone 在 MID 周期清理。
5. tombstone 策略参数由配置提供：`sink_tombstone_ttl_ms` 与 `sink_tombstone_tolerance_us`，默认 `90000ms` / `1000000us`。

## [workflow] IntegrityFlagsWorkflow

**Steps**

1. Realtime atomic update: `monitoring_attested=true`，清理 `suspect`，清理 `blind_spot`。
2. Realtime non-atomic update: `monitoring_attested=true`，设置 `suspect` 窗口，清理 `blind_spot`。
3. Scan update 且 `mtime_changed=true`: `monitoring_attested=false`；当 `epoch>=1` 设置 `blind_spot=true`；根据 shadow-time 与文件 mtime 计算是否设置 `suspect`。
4. Scan update 且 `mtime_changed=false`（含 `audit_skipped`）: 保留既有 attested/suspect/blind-spot 状态，不得降级既有 attestation。
5. suspect 年龄判定仅使用 shadow-time 轴（`shadow_time_high_us`）与 payload `modified_time_us`，不得读取本机 wall-clock 作为仲裁时间源。
6. query/reply 将组内节点标志聚合成 group-global `reliable/unreliable_reason`。

## [workflow] MidHardRemoveWorkflow

**Steps**

1. EpochEnd(Audit) 到达后，仅在 `epoch>=1` 触发 MID 缺失检测。
2. MID 跳过 tombstone 节点、`audit_skipped` 节点与其后代、以及审计窗口内被 Realtime 再确认的节点。
3. 对仍判定缺失的节点执行 `hard-remove`（不打 tombstone）。
4. MID 同轮执行 tombstone 过期清理。
5. 已知权衡：`hard-remove` 在极端竞态下可能出现短暂假阳性（迟到 Scan 回生），由后续 realtime/audit 收敛。

## [workflow] LogicalClockAndShadowClockWorkflow

**Steps**

1. source 为每个发出的 event 写入递增 `logical_ts`（单 source 实例内单调）。
2. kernel/runtime transport 层保留 `logical_ts`，不做业务重写。
3. sink 每组维护 `shadow_time_high_us = max(EventMetadata.timestamp_us)` 作为 freshness 时间轴。
4. sink 仲裁与完整性评估使用 shadow-time 与 payload mtime；不使用 `logical_ts` 作为仲裁时间轴。
5. 冷启动时 `shadow_time_high_us=0`；在无节点场景下，查询可靠性保持宽松（`reliable=true`），仅由独立异常标记（如 overflow）改变。
6. payload `modified_time_us` 极端未来值不会直接推进 shadow clock；source 通过 drift `P999` 样本过滤与 graduated anti-jump 抑制异常时间污染 `EventMetadata.timestamp_us`。

## [workflow] SingletonHttpApiFacade

**Steps**

1. fs-meta 对外 HTTP API 继续附着在由 facade resource 驱动的 one-cardinality domain facade 上，而不是单独声明 `runtime.exec.api` execution identity。
2. runtime 只需保持整个 fs-meta app 对外入口在 facade resource scope 内的单活 bind/run 落位；不额外为 projection/API 维护 roaming bind/run 语义。
3. active app 实例保留 API manifest/监听职责；non-active 节点不暴露 fs-meta HTTP API，并执行 stale-process 回收。
4. 对 stateful cutover，只有完成 authoritative replay、projection rebuild，并达到 semantic-eligible / app-owned `observation_eligible` 的 active generation 才承担“可信 observation”暴露；未追平 generation 只能保留旧 active 或显式降级结果。
5. app failover 时，新 active 实例重新拉起同一 HTTP facade；对外 URL 模型保持不变。
6. 内部 query/find、sink worker 与 host object 协调继续优先走 route/channel/opaque-channel/facade 边界，而不是多节点 HTTP 漫游或宿主 host-operation 转发。

## [workflow] QueryPath (Materialized)

**Steps**

1. caller issues `query`.
2. facade/query 以 logical group 为 fan-out 单位，只向拥有该 group 的 sink-side execution partition 发起 materialized query。
3. sink-side group execution 从 `meta-index` 读取该 group 的物化 observation/projection 结果。
4. facade/query 汇总 per-group sink 结果，并按统一树响应模型返回 grouped envelopes：top-level 默认公开 `path`，只有当 query path 不是合法 UTF-8 时才额外公开 `path_b64`；每个 group 再公开 `root/entries/entry_page/stability/meta`。
5. 当 metadata 可用时，`root` 与 `entries` 默认公开 `path`；仅当对应 raw bytes 不是合法 UTF-8 时，额外公开 authoritative bytes-safe 字段 `path_b64`。
6. 若某 group 全部解码失败，仍返回该 group 的 `status:error` envelope，而不是伪造成功 metadata。

## [workflow] FindPath (Fresh)

**Steps**

1. caller issues `find`.
2. facade/query 先确定目标 groups，并对每个目标 group 启动一个 fresh-find 执行分支。
3. 每个 group 仅在该组内选择一个 source runner 执行 live probe / targeted fresh scan；source 只执行本地 group 范围工作，不做 source-to-source forwarding。
4. facade/query 执行 delete-aware 聚合并输出与 QueryPath 完全一致的 grouped observation 模型（同 envelope、同 bytes-safe path/name 字段）。

## [workflow] ProjectionHttpQueryFacade

**Steps**

1. caller 通过 `/api/fs-meta/v1/tree`、`/api/fs-meta/v1/stats`、`/api/fs-meta/v1/on-demand-force-find` 发起查询。
2. projection 参数归一化：`path` 与 `path_b64` 二选一；`path_b64` 是 authoritative raw-path bytes，`path` 只是 UTF-8 convenience 输入。缺省路径为 `/`，`recursive` 缺省为 `true`；`/tree` 与 `/on-demand-force-find` 额外归一化 `group_order=group-key`、`group_page_size=64`、`entry_page_size=1000`；其中 `/tree` 还归一化 `stability_mode=none`、`metadata_mode=full`。
3. projection/query client 对 app-owned query ports 使用通用 channel attach / query request，而不是直接拼装宿主对象路由。
4. `/tree` 与 `/on-demand-force-find` 输出 top-level `group_order`、`path`、`groups[]` 与 `group_page`；仅当 raw query path 不是合法 UTF-8 时才额外输出 `path_b64`。每个 group item 再单独携带 `group/root/entries/entry_page/stability/meta`，其中 `path_b64` 只在对应 raw bytes 不是合法 UTF-8 时出现，字符串路径字段保持默认 display 形态。
5. projection 使用请求返回事件中的 `origin_id` + policy mapping（`mount-path`/`source-locator`/`origin`）构建 group key，不依赖预置 peer 注册表。
6. 首次 `/tree` 或 `/on-demand-force-find` 请求先按 `group_order` 生成当前 group bucket 序列，并把该次 grouped 结果冻结到一个短 TTL PIT；随后根据 `group_page_size` 取本次 bucket page。
7. `/tree` 与 `/on-demand-force-find` 的 `group_after`/`entry_after` 都是 projection-owned opaque PIT cursors；续页必须带同一个 `pit_id`，服务端直接从 PIT 读取 page offsets，而不是重新计算 group 排序或重跑 live force-find。
8. `group_order=file-count|file-age` 时，projection MAY 用 lightweight `stats` probe 完成 group ranking；客户端若只想要一个 top-ranked group，使用 `group_page_size=1`。
9. `group_order` 只负责 bucket 排序；quiet-window stability、metadata withholding、以及 dual-cursor pagination 由独立参数轴控制，不隐式附着在 group ranking 上。
10. transport/protocol/timeout 异常返回结构化错误体：`error/code/path`，并按错误类型映射到明确 HTTP 状态。
11. projection 暴露 `/api/fs-meta/v1/bound-route-metrics` 读取 bound-route 传输计数（timeout/correlation/uncorrelated/pending 等）。
12. 调用方通道生命周期与 correctness 解耦；当前实现可采用每次 route call `caller.open -> ask -> close` 的基线策略。

## [workflow] QueryPayloadShapedByQueryPathParameters

**Steps**

1. caller 在 projection query/find 路径使用该路径自己的 query-shaping 参数；materialized `/tree` 使用 `pit_id/group_page_size/group_after/entry_page_size/entry_after` 约束 bucket page 与 per-group metadata page，而不是复用旧 `limit` 语义。
2. projection query/find 执行层按各自路径合同约束结果体大小；`/tree` 与 `/on-demand-force-find` 分页 subtree metadata，`/stats` 保持 aggregate subtree stats envelope。
3. 管理 API 命名空间 `/api/fs-meta/v1` 不承担 query payload-shaping 语义，也不通过 request body 字段表达该约束。

## [workflow] GroupOrderedBucketQuery

**Steps**

1. caller sends query/find with explicit `group_order`.
2. projection/client derives current candidate groups inside the request scope.
3. when `group_order=file-count|file-age`, projection/client MAY execute a lightweight phase-1 `stats` probe to compute the ranking metric.
4. projection/client orders groups deterministically:
   1. `group-key`: ascending group key.
   2. `file-count`: descending total files, then ascending group key.
   3. `file-age`: descending latest file mtime, then ascending group key.
5. projection/client slices the ordered list with `group_page_size` and freezes the grouped result into one PIT.
6. continuation uses `pit_id` plus `group_after`/`entry_after` offsets to read the frozen PIT page instead of rebuilding ranking or rerunning force-find.
7. app returns grouped response with one `entry_page` per returned group plus top-level `pit`.

## [workflow] QuietWindowStableTreeQuery

**Steps**

1. caller MAY request materialized tree query with `stability_mode=quiet-window`; caller MAY independently choose `group_order`, `pit_id/group_page_size/group_after`, `entry_page_size/entry_after`, and `metadata_mode`.
2. group bucket ordering runs first; stability parameters do not alter the bucket-order algorithm.
3. sink/projection evaluates path stability from materialized subtree evidence, not from raw query readability or `latest_file_mtime_us` ranking data.
4. sink distinguishes two classes of incoming update:
   1. sync-refresh update: periodic scan/audit/repair refresh that leaves the effective materialized subtree result unchanged.
   2. write-significant update: create/delete/rename/type mutation or metadata-visible subtree delta that changes the effective materialized result.
5. periodic refresh that discovers and accepts a query-visible `modified_time_us` or equivalent payload delta is treated as write-significant, because the effective materialized subtree result changed.
6. write-significant updates reset the affected node and all ancestor subtree quiet-window anchors; sync-refresh updates that do not change the effective subtree result do not reset the quiet window.
7. coverage loss or catch-up uncertainty (for example overflow pending audit, blind spots, degraded root coverage, or equivalent recovery-in-progress) prevents `stable` result and yields `unknown` or `degraded`.
8. quiet-window outcome is computed from `max(subtree_last_write_significant_change_at, last_coverage_recovered_at)` against caller-supplied `quiet_window_ms`.
9. `metadata_mode=full` returns paged subtree metadata (`root` + `entries` + `page`) plus stability report; `metadata_mode=status-only` returns stability report without metadata; `metadata_mode=stable-only` returns metadata only when stability state is `stable`, otherwise reports explicit metadata withholding.
10. `group_page_size/group_after` paginate bucket selection inside one PIT and `entry_page_size/entry_after` paginate per-group metadata only; stability remains scoped to each returned subtree, not to the current page slice.
11. `/on-demand-force-find` stays a freshness path and rejects quiet-window stability controls instead of pretending live scan equals stable materialized observation; pagination只切分 live subtree metadata，不把该结果升级为 materialized snapshot。

## [workflow] DescriptorDrivenGroupingPolicy

**Steps**

1. policy engine 读取域策略（例如按 `mount_point`、`fs_source`、`fs_type`、`host_ip`、`site/zone/labels` 分组）。
2. app 对 group 结果执行策略分组与聚合（无 member 子层）。
3. app 返回每组 `ok/error` 可见结果，单组失败不阻塞其他组。

## [workflow] GroupAwareSinkBindRun

**Steps**

1. sink 分发主路径采用“单 fs-meta app package boundary + runtime group-aware sink bind/run realization”，而不是 whole-app single-instance sharding。
2. app 为每个 group 声明 sink 执行形状；这些声明细节仅用于内部 bind/run realization，而不是产品层术语。
3. runtime 返回每个 sink instance 的 `bound_scopes[]`，sink 仅处理自己当前 bound_scopes 中的 groups。
4. worker 进程只承载本机 sink-side execution partitions；group 查询 fanout 单位为 logical group 及其状态分区，不以 app 实例或 worker 进程为 fanout 单位。
5. source/sink unit gate 对未知 `unit_id` 与 stale generation 进行 fail-closed/fencing，确保 unit 合约前置收敛。

## [workflow] UnitControlFenceWorkflow

**Steps**

1. runtime 通过 `ExecControl(activate/deactivate)` 与 `UnitTick` 下发执行门控信号，信号必须携带 `unit_id + generation`。
2. source/sink 按域内 unit 合约校验 `unit_id`；未知 unit 视为非法控制帧并拒绝。
3. source/sink 对同一 unit 维护代际高水位；`generation` 回退的控制帧被判定为 stale 并忽略。
4. unit 控制帧仅更新执行门控状态，不改写 query/find 或业务 payload 语义。
5. sink/source 仅接受显式域内 unit（source: `runtime.exec.source`/`runtime.exec.scan`; sink: `runtime.exec.sink`）。

## [workflow] LinuxOnlyRealtimeEnforcement

**Steps**

1. source startup validates host watch bridge availability.
2. on unsupported platforms, source startup fails with explicit unsupported-platform error.
3. app does not enter degraded mode that omits realtime watch stream.

## [algorithm] Delete-Aware Aggregation Rule

**Rationale**

在多来源事件汇聚时保持删除语义优先，避免同时间戳更新与删除冲突导致“幽灵文件”回生。

**Type Signature**

```rust
fn delete_aware_aggregate(records: &[FileMetaRecord]) -> Vec<FsMetaQueryNode>
```

1. Group raw records by normalized path.
2. Keep latest by `modified_time_us`.
3. Tie-break: `Delete` wins on equal timestamp.
4. Exclude deleted terminal nodes from final tree.
5. Derive stats from filtered tree nodes.

## [workflow] AdapterBridgeBoundary

**Steps**

1. fs-meta 业务层通过 public host-fs facade 在绑定宿主本地发出低层元数据/目录/watch 请求。
2. `host-adapter-sdk` 消费 runtime 已解析的 bound-host seam / post-bind dispatch 输入并执行宿主本地 facade/ABI 适配；它不是分布式宿主 host-operation forwarding 契约，也不拥有 locality resolution 或 target selection authority。
3. kernel/runtime 仅承载通用 route/channel、post-bind dispatch、与控制关系传播；host-facing 工作由绑定宿主上的 facade 消费。
4. fs-meta 业务层仅处理域策略与域数据融合，不处理 host-operation ABI 细节，也不拼装位置透明的远程宿主调用目录。
5. host-object descriptors 到 groups 的映射由 fs-meta app 维护；runtime/kernel 不实现业务 fanout。

## [workflow] BinaryUpgradeContinuity

**Steps**

1. operator submits a higher target generation for the same fs-meta app boundary.
2. runtime activates the candidate generation under normal bind/run and fencing rules, but does not yet treat it as the trusted external materialized observation owner; trusted external materialized `/tree` and `/stats` exposure remains on the previous eligible generation, or on explicit degraded/not-ready observation state when no eligible generation is available, until the candidate reaches app-owned `observation_eligible`. `/on-demand-force-find` remains a freshness path and may become externally available earlier.
3. app replays current monitoring roots and runtime grants into the candidate generation as authoritative truth inputs.
4. app rebuilds in-memory observation state through scan/audit/rescan and tracks whether the active scan-enabled primary groups have completed their required first-audit/materialized catch-up against current authoritative truth inputs.
5. only after `observation_eligible` is reached may runtime cut trusted external materialized `/tree` and `/stats` exposure on the resource-scoped one-cardinality facade over to the new generation.
6. after cutover, runtime sends drain/deactivate/retire intent to the old generation through the thin runtime control path; old-generation instances fence stale observation or writer paths during retirement.

## [workflow] ObservationEligibilityCutover

**Steps**

1. new generation receives current authoritative truth inputs: monitoring roots revision, runtime grants view, and authoritative journal continuation.
2. source/sink rebuild observation state until the active scan-enabled primary groups have replayed current authoritative truth inputs into materialized state and satisfied the required first-audit/materialized health catch-up.
3. status/health surfaces expose the current observation evidence through source audit timing, degraded roots, and per-group materialized readiness markers such as `initial_audit_completed` and `overflow_pending_audit`.
4. if facade activation is still pending after runtime proof or listener retry, status/health surfaces additionally expose optional `facade.pending` diagnostics (`reason`, `runtime_exposure_confirmed`, retry counters, and last error timing) so operators can tell cutover waiting from generic liveness.
5. app computes `observation_eligible` from first-audit completion on active scan-enabled primary groups, materialized degraded/overflow state, and stale-writer fencing state.
6. if `observation_eligible` is not yet satisfied, fs-meta keeps materialized `/tree` and `/stats` on the previous eligible generation or returns explicit not-ready/degraded observation state; query readability, internal route availability, or partial rebuild output are not sufficient to treat the generation as current truth. `/on-demand-force-find` stays a freshness path.
7. once `observation_eligible` is satisfied, trusted external materialized `/tree` and `/stats` exposure is promoted to the new generation and older generation observations are no longer authoritative.
8. stale generation control/data paths remain fenced so old observations cannot be re-promoted after cutover.
9. stale generations remain fenced after cutover so older observations cannot reclaim trusted external facade/query ownership.

## [workflow] ProductConfigSplit

**Steps**

1. operator runs `fsmeta deploy` with thin deploy config covering bootstrap API/auth concerns only.
2. deploy helper generates internal release desired-state material, runtime subscriptions, execution-shape material, and startup material without exposing those details as product knobs.
3. deployed service starts validly with `roots=[]`.
4. operator discovers runtime grants, previews monitoring roots, and applies online business monitoring scope through bounded product APIs.
5. manual rescan remains an explicit repair action instead of being folded into deploy-time bootstrap config.

## [workflow] ApiSessionLogin

**Steps**

1. caller sends `POST /api/fs-meta/v1/session/login` with username/password.
2. api loads passwd/shadow records from fs-meta local files.
3. api validates user existence, locked/disabled flags, and shadow password hash.
4. api issues session token with ttl and returns principal projection.

## [workflow] ApiAuthzGuard

**Steps**

1. protected api endpoint parses bearer token.
2. api resolves session principal and validates expiry.
3. for management endpoints, api verifies the session principal belongs to the configured `management_group`; query endpoints use query-api-key authorization instead of management-session role splitting.
4. unauthorized request fails with explicit auth/access error response.

## [workflow] ApiRuntimeGrantDiscovery

**Steps**

1. caller sends `GET /api/fs-meta/v1/runtime/grants`.
2. api authorizes a management session and returns current runtime-injected `host_object_grants`.
3. caller or console uses the returned descriptors to propose `roots` candidates.

## [workflow] ApiRootsPreviewAndApply

**Steps**

1. caller reads current roots through `GET /api/fs-meta/v1/monitoring/roots`.
2. caller sends draft roots to `POST /api/fs-meta/v1/monitoring/roots/preview`.
3. api validates the payload, resolves matching grants, and returns per-root monitor path previews.
4. caller applies the approved set with `PUT /api/fs-meta/v1/monitoring/roots`.
5. api revalidates the submitted roots against the current runtime grants; if any root has no current grant match, api rejects the whole write with explicit unmatched root ids and preserves the previous authoritative root set.
6. once revalidation passes, api updates the app-owned authoritative roots/group definition set in-memory.
7. source refreshes group membership against current host object grants, and sink refreshes projection partitioning against the same roots plus current bound scopes.
8. runtime remains the owner of bind/run realization for affected execution scopes; api/app consume the resulting runtime state and do not become bind/run semantic owners.
9. if source/sink refresh against current grants or bound scopes fails, api returns explicit internal error and reverts to the previous authoritative root set.
10. empty roots remain a valid deployed-but-unconfigured state.

## [workflow] ApiManualRescan

**Steps**

1. caller sends `POST /api/fs-meta/v1/index/rescan`.
2. api authorizes admin role and forwards trigger to source-primary executor rescan path.
3. source sends manual rescan signal only to each group source-primary executor pipeline.
4. api returns accepted response.

## [workflow] QueryApiKeyLifecycle

**Steps**

1. caller authenticates as a management-session subject with `admin` capability.
2. caller reads current query-consumer credentials through `GET /api/fs-meta/v1/query-api-keys`.
3. caller creates a new query-consumer credential through `POST /api/fs-meta/v1/query-api-keys`; api persists the key metadata and returns the one-time `api_key` secret together with its summary row.
4. query consumers use that `api_key` only on `/tree`, `/stats`, and `/on-demand-force-find`; management endpoints continue to reject query-api-key bearer tokens.
5. caller revokes a credential through `DELETE /api/fs-meta/v1/query-api-keys/:key_id`.
6. after revocation, subsequent query requests with that key are rejected; revocation does not grant management-session access or mutate roots/runtime grants by itself.

---
version: 3.0.0
---

# L3 Runtime: fs-meta Workflows

User-visible and operator-visible state names consumed by these workflows are owned by [STATE_MODEL.md](./STATE_MODEL.md). Workflow-local replay, retained, tick, republish, and timeout classifiers are implementation diagnostics only unless and until they are folded into one of the domain state machines defined there.

## [workflow] StartupAndIndexBootstrap

**Steps**

1. 管理入口节点提交 fs-meta app 期望状态（单点声明）。
2. app config 通过 group/roots 逻辑声明监控分组与子路径范围（每组含 id、descriptor 选择规则、watch/scan，不含运行时节点枚举）。
3. runtime/kernel privileged mutation path 将期望状态传播，并向已调度 run 注入薄 runtime ABI 上下文与编译后的 runtime worker bindings；resource-scoped HTTP API 继续附着在 one-cardinality fs-meta app facade unit 上，source/sink 的内部执行策略可独立演进，而 local-style app construction 保持 deployment-neutral embedded 默认。
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
14. worker-mode reconfiguration is generation-based: operator changes to `workers.source.mode` or `workers.sink.mode` are compiled into a new generation’s runtime worker bindings and take effect only through normal generation rollout, not through same-generation hot switching inside one running host.

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
13. watcher 收到 `IN_Q_OVERFLOW` 时发出 in-band `ControlEvent::WatchOverflow` 并置 sink 组级 `overflow_pending_materialization`，查询响应立即标记 `reliable=false`。
14. watcher 收到 `IN_IGNORED` 时必须立即清理 wd/path 映射；若对应根 watch 被移除（根被删除或卸载），立即关闭流并退出任务。
15. overflow 处理在当前基线中不触发即时全盘补扫；`overflow_pending_materialization` 仅在下一次 primary audit `EpochEnd` 后清除，并恢复可靠性标记。
16. audit 扫描产生的文件级 `FileMetaRecord` 必须携带 `parent_path` 与 `parent_mtime_us`，供 sink 进行 parent-staleness 拒绝判定。
17. source 通过 schema 约定输出 MessagePack `FileMetaRecord`：`path` 为 raw-bytes 且在 in-root emission 中保持 leading-slash 相对路径；`source` 字段按产出轨道显式标记 `Realtime/Scan`。

## [workflow] CoverageEvidenceAggregation

**Steps**

1. source reports each group/root coverage using one `AuditCoverageMode` and the five `ObservationCoverageCapability` booleans.
2. when realtime watch is healthy and audit is active, source reports `realtime_hotset_plus_audit` with watch freshness coverage enabled.
3. when realtime watch freshness is unavailable but audit continues, source reports `audit_only`; metadata completeness remains expressed by capability bits.
4. when metadata collection is complete through audit, source may report `audit_with_metadata`; when file metadata collection is disabled or unavailable, source reports `audit_without_file_metadata` and disables `file_metadata_coverage` plus `mtime_size_coverage`.
5. sink/query propagate disabled capabilities into `meta.metadata_available=false`, `withheld_reason`, degraded reason, or partial evidence rather than manufacturing metadata.
6. status exposes the same mode/capability evidence so operators can distinguish count/existence coverage from full metadata coverage.

## [workflow] HostFsTimeoutBackpressureEvidence

**Steps**

1. source performs host-fs scan, stat, list, and watch setup through bounded/cancellable facade operations.
2. a host-fs timeout or backpressure event is recorded as observation evidence for the affected root or group.
3. isolated timeout may produce partial/degraded evidence for that root/group without aborting unrelated groups.
4. repeated timeout updates status with root/group degraded reason `HOST_FS_TIMEOUT`; repeated backpressure updates status with root/group degraded reason `HOST_FS_BACKPRESSURE`; either prevents trusted-materialized promotion for the affected coverage until recovery evidence arrives.
5. source does not use unbounded thread spawn as the routine way to wait out slow host-fs operations.

## [workflow] GroupFormationFromHostObjectDescriptorsWorkflow

**Steps**

1. source 读取 app config 中的 group/roots 逻辑声明。
2. source 读取 runtime 注入的 host-object grants，其中每个 `mount-root` object 带 `object_ref`、`host_ref`、`host_ip`、`mount_point`、`fs_source`、`fs_type` 等 descriptors。
3. app 仅使用 descriptors + 管理配置形成 groups，不依赖 runtime 解释路径或节点含义。
4. app 可使用 host descriptor selectors（例如 `host_ip`、`host_name`、`site/zone`）与 object descriptors（例如 `mount_point`、`fs_source`、`fs_type`）决定分组。
5. on runtime grants-changed control event，source 按版本号执行在线增量重收敛（新增 object 启动新的本地执行分区，移除 object 有界回收旧分区），并触发定向重扫。
6. 若 grant 的 `fs_source` 表示远端挂载源（例如 `host:/export`），app 必须使用 mount-root guarded host-fs facade；该失败闭合边界由远端源形态决定，不依赖 `fs_type` 标签是否写成 `nfs`。
7. mount-root object 生命周期独立，单 object 失败不阻塞同组其他 objects；禁止“仅更新内存映射但不生效任务变更”。

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
3. active app 实例保留 API manifest/监听职责；non-active 节点不暴露 fs-meta HTTP API，并执行 stale worker-host 回收。
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
2. projection 参数归一化：`path` 与 `path_b64` 二选一；`path_b64` 是 authoritative raw-path bytes，`path` 只是 UTF-8 convenience 输入。缺省路径为 `/`，`recursive` 缺省为 `true`；`/tree` 与 `/on-demand-force-find` 额外归一化 `group_order=group-key`、`group_page_size=64`、`entry_page_size=1000`；其中 `/tree` 与 `/stats` 还归一化 `read_class=trusted-materialized`。
3. projection/query client 对 app-owned query ports 使用通用 channel attach / query request，而不是直接拼装宿主对象路由。
4. `/tree` 与 `/on-demand-force-find` 输出 top-level `read_class`、`observation_status`、`group_order`、`path`、`groups[]` 与 `group_page`；仅当 raw query path 不是合法 UTF-8 时才额外输出 `path_b64`。每个 group item 再单独携带 `group/root/entries/entry_page/stability/meta`，其中 `path_b64` 只在对应 raw bytes 不是合法 UTF-8 时出现，字符串路径字段保持默认 display 形态。
5. projection 使用请求返回事件中的 `origin_id` + policy mapping（`mount-path`/`source-locator`/`origin`）构建 group key，不依赖预置 peer 注册表。
6. 首次 `/tree` 或 `/on-demand-force-find` 请求先按 `group_order` 生成当前 group bucket 序列，并把该次 grouped 结果冻结到一个短 TTL PIT；随后根据 `group_page_size` 取本次 bucket page。
7. `/tree` 与 `/on-demand-force-find` 的 `group_after`/`entry_after` 都是 projection-owned opaque PIT cursors；续页必须带同一个 `pit_id`，服务端直接从 PIT 读取 page offsets，而不是重新计算 group 排序或重跑 live force-find。
8. `group_order=file-count|file-age` 时，projection MAY 用 lightweight `stats` probe 完成 group ranking；客户端若只想要一个 top-ranked group，使用 `group_page_size=1`。
9. `group_order` 只负责 bucket 排序；`read_class`、observation gating、以及 dual-cursor pagination 由独立参数轴控制，不隐式附着在 group ranking 上。
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

## [workflow] NamedReadClassQuery

**Steps**

1. caller selects `read_class=fresh|materialized|trusted-materialized` instead of hand-composing stability or metadata modes.
2. group bucket ordering runs first; `read_class` does not alter the bucket-order algorithm.
3. `fresh` delegates to the live/freshness path and reports `observation_status.state=fresh-only`; it may be useful before materialized observation catches up, but it does not claim current trusted observation.
4. `materialized` reads the current materialized projection and returns explicit `observation_status`; degraded coverage, missing initial audit, or overflow evidence are surfaced as `materialized-untrusted` reasons instead of forcing the caller to infer trust from parameter combinations.
5. `trusted-materialized` consumes the same package-local observation evidence as cutover `observation_eligible`; until that evidence is trusted enough, the request fails closed with explicit `NOT_READY`.
6. `group_page_size/group_after` paginate bucket selection inside one PIT and `entry_page_size/entry_after` paginate per-group metadata only; `read_class` does not change PIT ownership or cursor meaning.
7. `/on-demand-force-find` stays a freshness path; it is the dedicated force-find alias for `read_class=fresh` rather than a second free-form stability model.
8. For a trusted-materialized root `/tree`, a selected sink owner route gap may be rescued only by bounded generic query-proxy evidence for the same selected group and same path; the result still fails closed if that proxy evidence is missing, empty, or from another group/path.
9. Selected-group materialized owner routing uses current sink ownership evidence. Stream-applied counts are freshness/materialization evidence only; they may choose between currently scheduled or primary sink owners, but they must not override the current sink schedule or primary owner and route a query to a stale node.

## [workflow] DescriptorDrivenGroupingPolicy

**Steps**

1. policy engine 读取域策略（例如按 `mount_point`、`fs_source`、`fs_type`、`host_ip`、`site/zone/labels` 分组）。
2. app 对 group 结果执行策略分组与聚合（无 member 子层）。
3. app 返回每组 `ok/error` 可见结果，单组失败不阻塞其他组。

## [workflow] GroupAwareSinkBindRun

**Steps**

1. sink 分发主路径采用“单 fs-meta app package boundary + runtime group-aware sink bind/run realization”，而不是 whole-app single-instance sharding。
2. app 为每个 group 声明一个 sink 物化负责人；负责人必须从该监控 group 的成员节点中选择一个，而不是从无关的资源可见节点中选择。
3. runtime 返回每个 sink instance 的 `bound_scopes[]`，sink 仅处理自己当前 bound_scopes 中的 groups。
4. “已分配物化负责人”只表示运行放置已就绪；只有对应 group 已产出同 epoch 的 sink 物化证据并达到 ready，才可以把 trusted observation readiness 判定为 ready。
5. worker 进程只承载本机 sink-side execution partitions；group 查询 fanout 单位为 logical group 及其状态分区，不以 app 实例或 worker 进程为 fanout 单位。
6. source/sink unit gate 对未知 `unit_id` 与 stale generation 进行 fail-closed/fencing，确保 unit 合约前置收敛。

## [workflow] UnitControlFenceWorkflow

**Steps**

1. runtime 通过 `ExecControl(activate/deactivate)` 与 `UnitTick` 下发执行门控信号，信号必须携带 `unit_id + generation`。
2. source/sink 按域内 unit 合约校验 `unit_id`；未知 unit 视为非法控制帧并拒绝。
3. source/sink 对同一 unit 维护代际高水位；`generation` 回退的控制帧被判定为 stale 并忽略，前进的 `UnitTick` 只表示当前 keepalive/control evidence，不能单独触发 worker reconnect 或 retained-state replay。
4. unit 控制帧仅更新执行门控状态，不改写 query/find 或业务 payload 语义。
5. sink/source 仅接受显式域内 unit（source: `runtime.exec.source`/`runtime.exec.scan`; sink: `runtime.exec.sink`）。

## [workflow] RuntimeWorkerControlResetRecovery

**Steps**

1. source/sink control frame observes retryable worker-control reset evidence such as channel close, timeout-like stale bridge, or retryable bridge peer failure.
2. app marks runtime control uninitialized, republishes facade unavailable, and retains the last accepted control state for replay.
3. recovery restarts or reacquires the affected worker path without waiting for the same in-flight worker control handoff that reported the reset. Retiring the stale worker client is cleanup work; it must not spend the bounded control retry budget needed to acquire and apply on the replacement worker path.
4. retained source/sink control state is replayed after the worker path is live; steady ticks do not reopen worker initialization when no replay is required, and a forward-moving source tick after replay is accepted locally instead of being treated as generation skew.
5. post-initial source post-ack scheduled-group refresh uses one bounded total recovery window plus a shorter per-RPC attempt cap; worker start, retained-control replay, client acquisition, grant refresh, and group refresh are all inside that total deadline, so no stale bridge lane can keep source control in flight after the refresh budget expires. Source Repair retained-control replay is a current route-state apply, not a scheduled-group refresh probe: it uses the standard bounded source-control RPC budget while still returning to the app boundary after one retryable reset.
6. post-initial source activation waves produced by release cutover are accepted into retained source state and fail-closed at the app boundary before source-worker replay; this applies whether they arrive as source-only, source+facade, mixed source/sink companion frames, after cleanup-only frames have already left runtime control uninitialized while retained source route state still exists, or as the first generation > 1 candidate wave in a cold successor process. Numeric runtime generation values do not distinguish fresh source bootstrap from cutover recovery. Fresh initial source bootstrap is identified by no successfully applied source route state, no armed retained replay, and an activation-only desired-state batch; pre-apply journaling of that same desired batch does not make it a post-initial wave. That apply uses the standard bounded source-control RPC budget, and after the worker ACK the app may publish control-derived source/scan ownership evidence without waiting for a live scheduled-group refresh or audit completion. Source audit duration is proportional to current filesystem contents and environment behavior, so audit progress affects observation trust/status fields rather than runtime-control initialization. The short existing-client cap is reserved for post-initial/cutover/replay/reset seams. A cold successor may make one bounded worker apply attempt, but a retryable worker reset/timeout must return at the app boundary with replay retained instead of looping inside the process-level apply/quorum path.
7. post-initial sink route-state waves produced by release cutover follow the same app-boundary rule: when runtime was already initialized, retained sink route state exists, retained replay is already pending, or the process boundary is already fail-closed from a previous cutover, the app records the desired sink state, closes only the sink replay lane, and leaves worker replay to bounded recovery/status entrypoints. A retryable sink worker reset/timeout during that candidate wave must not be retried inline until the process-level apply/quorum budget is exhausted. Numeric runtime generation values do not distinguish fresh bootstrap from cutover recovery. Fresh initial sink bootstrap is identified by no retained sink route state, no armed retained replay, and an activation-only desired-state batch; that apply uses the standard bounded sink-control RPC budget. The short existing-client cap is reserved for post-initial/cutover/replay/reset seams.
8. later tick, cleanup, exposure confirmation, or empty replay wakeups that find retained generation-cutover source or sink replay still pending follow the same fail-closed rule; once replay is armed, retained route state follows this rule regardless of route generation, because initial bootstrap is distinguished by the absence of replay-required state. These wakeups do not spend the process-level apply/quorum budget on inline replay retries or reinitialization loops. Runtime-unit exposure confirmation is route trust evidence, not a retained worker replay owner; it must not synchronously repair a sink route whose own trusted exposure is blocked by app readiness. A same-generation source tick that matches retained source route state is a bounded source-repair entrypoint and may perform one current desired-state source apply; older or mismatched ticks stay on the fail-closed process-boundary path. Later non-empty source recovery waves may perform one bounded apply of the current desired state and still fail closed at the app boundary if the worker path is not ready.
9. mixed cleanup followups such as source `restart_deferred_retire_pending` or drained `deferred_retire` plus sink tick/cleanup are cleanup waves, not new business apply waves; retryable source or sink worker reset preserves retained replay and returns at the app boundary without inline replay retries.
10. after that fail-closed release cutover, assigned source/sink owners keep bounded internal status recovery lanes live while materialized/query exposure stays closed, so a source primary can repair retained replay without waiting for a later control tick or for an external facade request to hit that same node. Background source repair completion is keyed by retained source replay being clear; background sink repair completion is keyed by retained sink replay being clear. These lanes must not spin waiting for the local fixed-bind HTTP facade to be serving, because many source/sink owners are not the fixed-bind facade owner. Source Repair recovery proves control-plane currentness by replaying retained source control state only; it must not require a live source observability snapshot, scheduled-group refresh, or source audit completion before clearing source replay. Live source-status and audit evidence remain the later pre-delivery/read-trust proof, because their duration depends on current filesystem contents and environment behavior. HTTP Source Repair Ready is a separately published API readiness plane and still requires an active current-epoch control stream plus settled source control apply; source state is not current while source apply is in flight.
11. retained sink replay recovery treats source/scan runtime-scope evidence as node-local ownership evidence and sink runtime-scope evidence as materialization coverage. A source owner is not required to schedule every logical root that the sink materializes; distributed real-NFS roots converge when local source/scan ownership is a valid subset of expected roots and sink coverage reaches the expected materialized groups.
12. source-to-sink recovery rescan epochs are scoped to local source-primary scan roots. A node that owns only non-primary source membership must record the epoch as an observed no-op instead of waiting for publication it is not allowed to perform; the matching group-primary nodes still own real publication through their scoped manual-rescan delivery.
13. manual-rescan delivery target selection may keep worker status-cache evidence only when it includes current group-primary concrete root evidence for the same root and scoped route target. A cached `source_primary` reference without matching group-primary root evidence is not a delivery-ready source owner; the app must keep bounded readiness fan-in running instead of sending scoped rescan to that cached owner.
14. while runtime control is already fail-closed, cleanup-only query and per-peer sink logical-roots deactivates are absorbed into retained route state locally; they do not re-enter the sink worker bridge and do not keep management writes unavailable behind retired worker RPCs. Cleanup-only sink events stream deactivate is different: it belongs to the sink worker data-ingress route and may be sent to the sink worker by itself, but it must not carry retained sink activate replay or drag app-side query/logical-roots cleanup into the worker lane.
15. when source/sink worker-control recovery exhausts its bounded retry budget, the app keeps the process boundary alive, marks runtime control uninitialized, retains replay state, and reports degraded/unavailable evidence through app status/query gates instead of returning a process-level `Timeout`, `ChannelClosed`, or `TransportClosed` from `on_control_frame`.
16. source status/observability access paths and management-write readiness waits are also recovery entrypoints: if retained source/sink replay is pending and no control op is already in flight, the access path performs one bounded retained-state replay repair before returning `not started` or management-write `not ready`; source/sink recovery does not rely on a later runtime tick after a generation cutover. Generic nonblocking source-status must first return current runtime-scope control-cache evidence when it already has source/scan scheduled groups and control-route facts for the same authority epoch; that status response is ownership evidence, not replay completion, and it must not consume retained replay or classify control-inflight ownership evidence as worker-unavailable before publishing the current runtime scope. A pending post-rescan publication refresh must withhold live root-health, source-primary, and publication counters from that cache response, but it must not erase source/scan ownership maps needed by release-upgrade and status fan-in gates. After retained replay is current and a live worker client exists, runtime-scope control cache is no longer final data truth; normal source-status takes a bounded live observation inside the caller's status-route collection attempt budget. If that bounded live observation fails or times out while current runtime-scope control-cache evidence still exists, generic source-status returns the runtime-scope ownership evidence with explicit cache provenance instead of erasing ownership as worker-unavailable. Stable root-health cache may skip the live read only after publication evidence exists for the cached runtime scope. Management `/status` sink fan-in is nonblocking facade evidence, not a worker lifecycle repair loop: it may use one existing-client sink probe inside the short status-route budget, but it must return cached/degraded sink evidence on timeout, reset, or missing-client evidence instead of starting/retrying the worker behind a fixed-bind handoff.
17. source-to-sink convergence pretrigger runs only for the first mixed source/sink boot wave with no retained replay pending; recovery waves do not submit that synchronous pretrigger before retained replay has reopened the control gate.
18. after source control fail-closes inside a mixed source/sink generation-cutover frame, the app returns from the current control frame with retained source/sink replay armed; it must not continue into same-frame sink replay or sink cleanup RPCs behind the failed source lane.
19. after that fail-closed cutover, later tick, cleanup, or empty process wakeups that still carry retained post-initial source or sink replay must keep the latest desired state and return without inline worker replay RPCs; retained replay includes active routes and ordinary deactivating route state. A restart-deferred retire cleanup that removes the last active source route is absorbed at the app endpoint gate and clears source worker replay, because no source worker route remains to repair. A later non-empty source recovery wave may run one bounded source apply; retained sink replay is driven by bounded recovery/status entrypoints instead of process apply/quorum wakeups.
20. if that fail-closed app owns a fixed-bind HTTP facade, it records the failed-owner state with the active bind owner; when a successor later registers a pending claim for the same bind address, the successor releases that stale owner through the bounded fixed-bind handoff path instead of leaving a not-ready facade bound forever.
21. if the fail-closed app is the pending successor and the predecessor is already only continuity-retaining a runtime-deactivated fixed listener, the successor releases the retained predecessor through the same bounded handoff path and exposes its own fail-closed app boundary, so new API requests do not continue through a drained/fenced predecessor PID. While the successor has a pending fixed-bind facade that is not runtime-exposure-confirmed, older cleanup-only facade deactivates must retain the currently active listener instead of shutting down the only reachable API facade. During this cutover window, manual-rescan readiness failures that explicitly report drained/fenced stale grant attachments are retryable facade-reconfiguration evidence for multi-facade operator clients; retry/failover may only move the request to another candidate facade and MUST NOT accept repair without fresh source-owned readiness and scoped delivery proof.
22. after a successor fixed-bind HTTP facade is actively serving, cleanup-only query/facade tails from the failed cutover may withdraw stale auxiliary source-status/source-find lanes, but they must not withdraw the active facade's core business read lanes (`find`, materialized query proxy, sink-status) while runtime control is still fail-closed; those lanes belong to the serving facade and must survive until retained replay either recovers them or a later initialized business route change supersedes them.
23. steady-state control, status, and endpoint request diagnostics are opt-in or bounded summaries by default; full signal payloads and per-batch normal-path logs are debug evidence, not part of the operations workload being measured by CPU gates.
24. while retained source or sink replay is pending, tick/cleanup wakeups must not reopen normal runtime initialization before the retained replay recovery lane clears the app gate; otherwise a fail-closed owner can rebuild endpoints on every steady tick and turn recovery bookkeeping into steady CPU workload.
25. once runtime control is already initialized, steady tick/keepalive waves must not re-enter the runtime initialization path; initialization is reserved for first activation or bounded recovery, not normal keepalive processing.
26. after a post-reset recovery, current retained-generation source ticks may drive one bounded retained-state repair and must keep the tick evidence; this is distinct from steady keepalive processing and from older retained generation-cutover wakeups.
27. if runtime control remains fail-closed only because sink replay is still pending, source current-generation keepalive ticks are retained gate evidence only; they must not re-enter source-worker control while source replay is already clear.
28. status-route fan-in may reach the same source worker through multiple active route units; same-node nonblocking source observability reads that overlap in one status cycle share one live worker read and use the resulting fresh cache for followers, instead of multiplying source-worker RPCs per status request. A nonblocking source-status read that can answer from current runtime-scope control cache must not block behind retained replay or an already-running control turn. If a post-rescan live publication refresh or first live publication proof is still required, the cache response may report only runtime-scope ownership and route-control provenance; it must not report that pending publication as live root-health or materialized data readiness. Manual-rescan pre-delivery probes and management-write readiness remain the blocking recovery paths.
29. when a retained source or sink replay is deliberately deferred at the app boundary, the app marks only that lane as still needing replay; deferring sink replay must not re-arm already-cleared source replay, and deferring source replay must not re-arm already-cleared sink replay.
30. management status, write-readiness, and explicit runtime readiness probes are bounded recovery entrypoints: if retained source or sink replay is pending, the app must run one bounded replay repair before returning `NotReady` and clear the app-level replay flag once the worker no longer needs retained replay; empty control frames, cleanup frames, and ordinary tick frames remain process-apply evidence and do not own this repair. Runtime-unit exposure confirmation is an explicit runtime-readiness probe: after source replay is clear, it may run the retained sink replay repair once instead of deferring it again as a process-apply wakeup. If a concurrent recovery/control turn has already made the worker retained-replay state current, the management-write recovery clears the app gate from that worker replay flag before issuing blocking status snapshots, because a stale status/read bridge is not allowed to keep management writes unavailable after replay ownership has already converged.
31. recovery progress waits use source/sink snapshots as evidence probes inside a bounded observation window; a probe that returns quickly without a runtime-change notification is not progress and must not turn retained replay recovery into a tight poll loop.
32. when an authoritative retained sink control wave successfully covers the current retained sink desired state after a worker reset, the sink worker bridge marks that retained replay current. Later steady ticks then stay on the keepalive fast path instead of replaying the full retained sink state on every tick/status cycle.
33. sink keepalive ticks may expand into retained sink route state only when app-level or worker-level retained sink replay is explicitly required; a normal current-generation tick is liveness evidence, not authority to replay every retained sink route.

## [workflow] RuntimeEndpointReplyPressureLiveness

**Steps**

1. internal query/status endpoints receive request batches and send replies over route reply channels.
2. reply send `Timeout`, `Backpressure`, not-ready, channel-close, and retryable link errors are transient endpoint pressure signals.
3. endpoint loops retry those transient reply send failures within bounded sleeps and continue serving later batches.
4. non-transient send failures preserve terminal reason evidence before runtime app prunes or respawns the route.
5. endpoint cancellation or generation cutover stops the app receive loop without permanently closing the reusable request route or derived reply route; the same runtime route key must remain usable by the replacement endpoint in the same control-frame recovery turn.
6. request/reply endpoint startup readiness means the endpoint has entered its first request receive poll for the owned route; a spawned thread that has not armed receive interest is not delivery-ready evidence for management writes.
7. consecutive worker-bridge transport-close evidence or persistent stale grant-attachment denial while receiving request batches marks the endpoint boundary stale. The endpoint records terminal reason evidence and exits so the next runtime endpoint reconciliation can prune the old task and rearm the same product route on the current worker boundary. A direct `TransportClosed` from the sidecar bridge is authoritative stale-boundary evidence; retryable `PeerError`/`Internal` transport-close wrappers remain bounded continuity gaps. This stale-endpoint exit does not apply to bounded reply backpressure/timeouts, which remain liveness-preserving retry signals.
8. manual-rescan live source-status pre-delivery rearm is an explicit route-local reconciliation turn: source must prune finished or terminal stale source-rescan request consumers and spawn missing replacements on the current boundary, while preserving healthy already-armed consumers for the same product routes. Stale grant retry exhaustion is repaired by terminal endpoint evidence and pruning; healthy endpoint consumers must not be torn down on every pre-delivery probe, because that turns readiness checking into route churn and can consume the caller's bounded live-probe budget. Preserved consumers still have to prove current receive-armed state during the rearm turn before source refreshes scoped ready evidence; task presence alone is not delivery readiness. This is a bounded endpoint lifecycle repair, not a timeout increase or acceptance bypass.
9. idle request/reply and stream endpoint receives are event-driven waits with a bounded quiet window and cancellation-aware shutdown. They must not use high-frequency empty polling as the normal steady path, because full five-node operations keep many runtime routes armed even when no request is present.

## [workflow] TrustedMaterializedReadinessFanIn

**Steps**

1. trusted-materialized `/tree` and `/stats` first check whether root-level sink schedule or cached sink status evidence exists for the current authority epoch. In a routed facade, local sink schedule absence is not authoritative because the current materialized owner may be a peer sink; the routed path must perform peer sink-status fan-in before deciding that no current schedule exists.
2. if neither local authoritative evidence nor routed peer fan-in evidence exists, the read returns explicit not-ready/degraded evidence without entering expensive materialized PIT work.
3. when readiness fan-in is needed, source-status and sink-status route reads run independently and are combined after both bounded attempts finish.
4. one slow or missing status route may contribute degraded evidence, but it must not serialize the other route or consume the full query budget before the other readiness plane is observed.
5. once source-status has identified the active readiness groups and same-node sink evidence already covers those groups as ready/live, a missing sink-status route is supplementary degraded evidence; the request must return after a bounded local completion grace instead of waiting for the full sink route timeout.
6. if peer sink-status times out, resets, or reports no live client during trusted-materialized readiness fan-in, the public `/tree`/`/stats` read path may use bounded nonblocking same-process sink evidence, but it must not start/retry the sink worker or run retained sink replay from that public read. Missing trusted sink evidence closes the trusted-materialized read with explicit `NOT_READY` rather than surfacing the worker recovery gap as HTTP `TIMEOUT`.
7. steady request-scoped sink-status refresh for trusted-materialized `/tree`/`/stats` must not re-run a missing sink-status route when same-epoch loaded readiness evidence and same-node sink evidence already prove all active groups ready/live; it may reuse that evidence until an authority-epoch change or contradictory failure evidence invalidates it.
8. management `/status` may use source logical-root/concrete-root health as status-readiness group evidence when route-level source schedule maps are absent during a generation cutover; if same-node sink evidence covers those groups as ready/live, a missing status route is a degraded diagnostic and must use bounded local completion instead of steady full-route waits.
9. once a source worker has returned complete same-authority root-health evidence for the configured roots, steady management `/status` does not need to re-enter the source-worker observability RPC on every poll only to restore optional route-schedule debug maps; it may reuse that cached root-health evidence for status readiness until control/grant/root authority changes or contradictory health evidence arrives.
10. when same-node source root-health evidence identifies the active status groups and same-node sink evidence already proves those groups ready/live, steady management `/status` may complete from local domain evidence and mark remote status fan-in as skipped; route-schedule maps remain diagnostics, not mandatory work for every poll.
11. steady trusted-materialized `/tree` first-page PIT/session reads and `/stats` reads may reuse the latest successful same-parameter materialized read snapshot when the source root/grant authority signature, source rescan/read epoch, request source evidence, and sink status/materialized-revision evidence are unchanged; repeated observation reads consume the same materialized snapshot and must not reopen selected-group route/proxy fanout until that authority or materialized evidence changes. Empty, non-content, or partially materialized `/tree` PIT/session snapshots are materialization catch-up evidence, not steady reusable snapshots, and must not mask later post-rescan content for any returned group.
11. operations validation that checks source/scan coverage must prefer domain source logical-root health, concrete-root health, runner binding evidence, and materialized/query evidence over route-schedule debug maps; route-schedule maps are diagnostics and may be absent or delayed during generation cutover without closing the source/scan coverage plane when domain evidence is complete.
12. operations validation that checks post-grant-contraction sink coverage must prefer active runtime grant authority plus materialized/query evidence over status sink schedule maps or activation route scope debug; route and schedule maps are diagnostics and may retain broad process-continuity detail or be absent during cutover even when grant authority and materialized coverage have converged.

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

## [workflow] RuntimeArtifactEvidenceValidation

**Steps**

1. deploy records the expected fs-meta runtime/app artifact hash for each participating node.
2. runtime/app startup records the actually loaded artifact path and content hash as RuntimeArtifactEvidence.
3. status or deployment diagnostics expose expected-vs-actual artifact evidence without making it business observation truth.
4. deploy validation compares expected and actual hashes across all nodes before treating the environment as artifact-consistent.
5. demo/test harness stale-artifact checks cover both fs-meta workspace inputs and upstream capanix path dependencies linked into the runtime/app artifact before reusing an existing cdylib.
6. demo/test harness fails closed when the run/bin artifact and actually loaded runtime artifact differ.

## [workflow] DemoEvidenceDiscipline

**Steps**

1. mini demo environments may reduce file counts for fast debugging, but preserve the full demo topology, roots/grants shape, and runtime parameters.
2. mini demo evidence and full demo evidence remain separate; mini success is not full demo acceptance.
3. `/stats` aggregation output is display evidence only unless `/status` also proves artifact consistency, roots/grants activation, source coverage, and sink materialization readiness.
4. disabled metadata audit is not an allowed full-demo acceleration path; metadata coverage loss may be reported only as degraded evidence.
5. full demo acceptance fails when artifact mismatch, host-fs timeout/backpressure, missing metadata coverage, or materialization unreadiness remains.
6. full real-NFS operations use a runtime channel budget sized for the full five-node route set plus one release-upgrade generation overlap; mini/debug channel caps must not be reused for full operations gates.
7. release-upgrade operations gates that trigger manual rescan or steady CPU sampling must wait for app-visible source runtime-scope evidence or accepted manual-rescan delivery evidence for the current roots; source/scan process liveness alone is not convergence evidence. Source runtime-scope evidence proves coverage by the app-selected owner nodes for the current logical roots, not by a fixed demo node list, because source-primary selection is app-owned and may move with grants, topology, and generation cutover state. Source runtime-scope evidence is aggregate positive local ownership across partial peer route activations: a peer-only activation may update route state, but it must not erase local source/scan schedule evidence learned from another current wave. Empty live schedule observations are withheld from app-visible ownership; stale schedules are cleared only by explicit current-authority empty refresh for the known node. Cross-node generation comparison is stale-owner evidence only when the control scope names a concrete owner target for the group; root-id route scopes are per-node liveness facts and are not a global ordering across source owners.
8. full real-NFS operations gates must preserve capanix's bounded relation-target quorum budget for multi-node startup and release cutover; fs-meta demo harnesses must not shorten that budget for the full five-node operations environment. A quorum timeout remains a fail-closed product signal, but a test-local budget below the runtime contract is not valid full-demo acceptance evidence.

## [workflow] BinaryUpgradeContinuity

**Steps**

1. operator submits a higher target generation for the same fs-meta app boundary.
2. runtime activates the candidate generation under normal bind/run and fencing rules, but does not yet treat it as the trusted external materialized observation owner; trusted external materialized `/tree` and `/stats` exposure remains on the previous eligible generation, or on explicit degraded/not-ready observation state when no eligible generation is available, until the candidate reaches app-owned `observation_eligible`. `/on-demand-force-find` remains a freshness path and may become externally available earlier.
3. app replays current monitoring roots and runtime grants into the candidate generation as authoritative truth inputs.
4. app rebuilds in-memory observation state through scan/audit/rescan and tracks whether the active scan-enabled primary groups have completed their required first-audit/materialized catch-up against current authoritative truth inputs.
5. only after `observation_eligible` is reached may runtime cut trusted external materialized `/tree` and `/stats` exposure on the resource-scoped one-cardinality facade over to the new generation.
6. after cutover, runtime sends drain/deactivate/retire intent to the old generation through the thin runtime control path; old-generation instances fence stale observation or writer paths during retirement.

## [workflow] FixedBindFacadeHandoff

**Steps**

1. a successor generation that cannot bind the fixed HTTP facade records a pending fixed-bind handoff and keeps facade-dependent routes suppressed.
2. during normal handoff, the predecessor keeps serving until successor runtime exposure is confirmed, then drains requests and releases the bind address through the fixed-bind handoff path.
3. if the predecessor has already failed closed, its active fixed-bind owner record carries a failed-owner marker; when a pending successor exists, the successor releases the bind address even if successor exposure confirmation has not arrived yet.
4. if the predecessor has accepted runtime deactivate and is only retained for continuity, and the pending successor later fail-closes after accepting desired state, the successor releases the retained predecessor and binds a fail-closed successor facade rather than leaving the old runtime PID to handle new requests.
5. after release, the retired listener's process-local claim is cleared by bind address before successor retry, because the listener handle rather than the app instance id is the concrete owner of that port. The successor then refreshes fixed-bind lifecycle facts before evaluating facade-dependent query publication; stale pre-release claim snapshots cannot continue suppressing current-generation routes.
6. after owner release, successor publication runs as an after-release fixed-bind handoff. It retries bind/promotion within the fixed handoff deadline and may bind a fail-closed HTTP boundary before successor runtime exposure confirmation, so the API endpoint is reachable while management writes and trusted materialized reads stay closed behind readiness gates. During control-failure handling, this failed-owner release and successor publication attempt happen before uninitialized query/status cleanup; cleanup cannot spend the recovery window while the only HTTP listener is still the drained predecessor. A failed-owner release does not wait for stale predecessor facade read requests to drain, because those requests can be blocked on the same recovery handoff; in-flight predecessor requests may fail/reconnect while bounded successor publication restores the operator API boundary. If the failed owner later receives cleanup-only facade follow-up frames while still uninitialized, that frame first runs the same failed-owner fixed-bind release path before returning as cleanup-only. Once the fail-closed successor HTTP boundary is serving, later cleanup-only facade deactivates do not clear the `facade-control` route or shut down the only operator API boundary.
7. if the bind blocker is outside the current process, the successor still treats its unconfirmed fixed-bind facade publication as incomplete and keeps facade-dependent query routes suppressed until the bind is actually acquired.
8. when fixed-bind facade publication becomes complete, previously suppressed sink-owned facade-dependent materialized/query route activates are replayed from retained sink desired state using fresh post-publication fixed-bind lifecycle facts; retained core facade read lanes (`find`, materialized query proxy, `sink-status`) suppressed by the same publication barrier are replayed in the same tail. A stale pre-publication/pending-facade snapshot must not re-suppress those routes or clear the replay marker. A source-only recovery follow-up must not leave trusted-materialized routes unpublished behind a completed facade.
9. fixed-bind publication continuation, including a pending handoff and the post-bind replay tail for suppressed dependent routes, is not a source/sink generation cutover. Source or sink control waves in that window apply retained desired state and publication tail work; they must not be diverted into fail-closed generation-cutover replay.
10. a fresh request that is already accepted by the predecessor before shutdown must settle with an HTTP response; a new connection during the unavoidable no-listener bind gap is not treated as proof of app-level query correctness.

## [workflow] ObservationEligibilityCutover

**Steps**

1. new generation receives current authoritative truth inputs: monitoring roots revision, runtime grants view, and authoritative journal continuation.
2. source/sink rebuild observation state until the active scan-enabled primary groups have replayed current authoritative truth inputs into materialized state and satisfied the required first-audit/materialized health catch-up.
3. status/health surfaces expose the current observation evidence through source audit timing, degraded roots, and per-group materialized readiness markers such as `initial_audit_completed` and `overflow_pending_materialization`.
4. if facade activation is still pending after runtime proof or listener retry, status/health surfaces additionally expose optional `facade.pending` diagnostics (`reason`, `runtime_exposure_confirmed`, retry counters, and last error timing) so operators can tell cutover waiting from generic liveness.
5. app computes `observation_eligible` from first-audit completion on active scan-enabled primary groups, materialized degraded/overflow state, and stale-writer fencing state.
6. if `observation_eligible` is not yet satisfied, fs-meta keeps materialized `/tree` and `/stats` on the previous eligible generation or returns explicit not-ready/degraded observation state; query readability, internal route availability, or partial rebuild output are not sufficient to treat the generation as current truth. `/on-demand-force-find` stays a freshness path.
7. once `observation_eligible` is satisfied, trusted external materialized `/tree` and `/stats` exposure is promoted to the new generation and older generation observations are no longer authoritative.
8. stale generation control/data paths remain fenced so old observations cannot be re-promoted after cutover.
9. stale generations remain fenced after cutover so older observations cannot reclaim trusted external facade/query ownership.

## [workflow] ProductConfigSplit

**Steps**

1. operator runs `fsmeta deploy` with thin deploy config covering bootstrap API/auth concerns only.
2. deploy helper consumes shared upstream config-loading, manifest-discovery, and intent-compilation semantics, then generates internal release desired-state material, runtime subscriptions, execution-shape material, and startup material without exposing those details as product knobs.
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
5. api checks Management Write Ready for the current Authority Epoch; if the HTTP facade is reachable but the active control stream is not ready, api rejects the write with explicit `NOT_READY` and preserves the previous authoritative root set.
6. api revalidates the submitted roots against the current runtime grants; if any root has no current grant match, api rejects the whole write with explicit unmatched root ids and preserves the previous authoritative root set.
7. once revalidation passes, api updates the app-owned authoritative roots/group definition set in-memory.
8. source refreshes group membership against current host object grants, and sink refreshes projection partitioning against the same roots plus current bound scopes.
9. runtime remains the owner of bind/run realization for affected execution scopes; api/app consume the resulting runtime state and do not become bind/run semantic owners.
10. if source/sink refresh against current grants or bound scopes fails, api returns explicit internal error and reverts to the previous authoritative root set.
11. empty roots remain a valid deployed-but-unconfigured state.

## [workflow] ApiManualRescan

**Steps**

1. caller sends `POST /api/fs-meta/v1/index/rescan`.
2. api authorizes admin role.
3. api checks Source Repair Ready for the current Authority Epoch; if the HTTP facade is reachable but the active control stream, retained source replay state, or current source control apply is not ready, api rejects the write with explicit `NOT_READY`. Source Repair Ready is narrower than full Management Write Ready and does not wait for retained sink replay or sink-status materialization readiness. A full management-write drain does not own source-repair recovery: roots apply remains closed, but manual rescan may run bounded Source Repair recovery and proceed only if that plane reopens. If retained source replay is already clear and no source control apply is in flight, the source-repair recovery check is an idempotent no-op and must not queue behind unrelated control-frame serialization, even when full management readiness or published Source Repair Ready is still closed by sink/facade recovery. If an earlier online roots apply returned before peer runtime-scope followup finished, api performs a bounded current-roots source readiness fan-in before scoped manual-rescan delivery; it must not turn a not-yet-activated source route into an accepted rescan. Before that blocking current-roots fan-in, the manual-rescan API path must publish the current roots control wave for those roots so peer source-status probes observe the same roots the rescan is about to deliver. Source-status readiness evidence is served by active `runtime.exec.source` owners for their runtime-scope groups; query/facade lanes aggregate that evidence but are not a prerequisite for the source-owned status endpoint. Manual-rescan current-roots readiness must collect source-owned status on node-scoped source-status request routes for the grant/discovery source nodes; the generic aggregate source-status route is supplementary evidence and must not be the only proof when current roots may be owned by multiple source nodes. The current-roots fan-in is evidence-driven: each generic, node-scoped, or same-node source-status reply is merged as it arrives, and once the accumulated source-owned proof covers every current root, api returns without waiting for unrelated aggregate replies or node-scoped probes that are not needed for the selected target set. A source-owned source-status endpoint is tied to source runtime-scope ownership, so it must answer for its groups even if a local HTTP facade handle exists and the published facade state is pending or unavailable; only query/facade-owned status lanes are gated by facade serving state. Manual-rescan readiness fan-in marks its source-status request as a pre-delivery probe; when the app runtime source state is current, the source-status owner must reconcile current worker source endpoints before returning delivery evidence. If retained source replay or source-control apply is still pending, source-status returns runtime-scope/cache observation only, must not initiate retained replay or endpoint rearm, must withhold live source-primary/concrete-root delivery target truth, and that response is not scoped delivery readiness. Endpoint rearm owns the pre-delivery proof: after the source owner records the receive-armed ready marker, source-status may return a rearm-ack delivery snapshot with explicit cache provenance and must not wait for live observability or audit completion. Live source snapshots and audit results are read-trust evidence only and may be used when available inside the caller budget; their absence is not a reason to withhold rearm-ack delivery evidence. Generic cached nonblocking status without the same source-owned ready marker remains explanation only for that probe. Endpoint rearm must not consume an independent worker-control timeout that exceeds the caller's route collect budget. Endpoint rearm is health-aware: it prunes finished or terminal stale source-rescan endpoint tasks and starts missing replacements, but it must preserve healthy already-armed source-rescan endpoints instead of forcing route teardown on every source-status probe. A rearmed scoped manual-rescan endpoint is not ready merely because a task exists or was spawned: the source owner must wait through stale grant-attachment gaps until the scoped request route is receive-armed in the current rearm turn before reporting pre-delivery evidence. The source owner records the receive-armed ready marker as part of endpoint rearm; API/status aggregation layers must not synthesize scoped ready proof after a worker RPC ack, and every worker observability wrapper, including cache/override layers, must preserve the source-owned scoped route activation plus ready marker instead of replacing it with an older outer control summary. That ready marker is valid for a current root only while the same node also reports source/scan runtime-scope ownership for that root; a scoped route activation whose scopes mention the root, or a ready marker retained after source ownership moved away, must not cover roots the node no longer owns. For the same target node-scoped manual-rescan route, the source-owner ready marker generation must match that source owner's local activation generation for the same target route and root. Peer-origin activations for that target route are cluster-control facts that may name a candidate, but they must not invalidate the source owner's local receive-armed proof or become pre-delivery readiness without same-target source-owned ready evidence. Root-id route scopes are still not a global ordering across different target nodes. Source-status evidence used to choose scoped manual-rescan delivery targets must include that node-owned receive-armed proof; a route activation frame or cached runtime-scope map alone may name a candidate but must not become target proof. The API may use routed aggregate source-status as delivery target proof only when that aggregate contains the target node's source-owned receive-armed scoped-route proof for every current root; it must not issue a second probe that discards already-collected proof and then fail on a partial cutover observation. Roots-put second-wave readiness wrappers that issue the same manual-rescan pre-delivery source-status probe must preserve that manual-rescan delivery budget and must not cap it with the generic short status-route timeout. A same-node worker observation used by the API local readiness path follows the same rule: re-arm current worker source endpoints first, then use rearm-ack delivery evidence; live source snapshots and audit can refine trust/status but are not required for scoped delivery proof. The API uses that blocking same-node probe only when the local source node covers every current root, otherwise peer source-status fan-in owns the missing roots and the local node may contribute only bounded nonblocking evidence. When the local source node covers every current root, same-node local observation is not a leftover-budget shortcut: it must receive the bounded source-status route budget before peer control-cache candidate evidence is considered as explanation, while still staying inside the single current-roots fan-in deadline. If that local proof does not satisfy readiness, peer fan-in continues only with the remaining bounded budget and the operation fails closed when target evidence never appears. The pre-delivery fan-in timeout must be long enough to observe the bounded source-status probe it requested from peer owners; a shorter caller budget that can only collect partial local evidence is not valid readiness proof. Degraded unavailable-worker source-status evidence is status explanation only; it is not source readiness evidence and must be ignored for this pre-delivery gate. A `source worker status served from cache` marker is also cache-only readiness explanation: even if it carries root-health, source/scan maps, or scoped route activation maps, it must not satisfy pre-delivery readiness or select a scoped manual-rescan target. If routed peer aggregation later merges in live source evidence, the merged live observation drops the cache marker before it can be used as delivery readiness. A control-inflight status fallback is valid ownership evidence only when it preserves configured root-health, source/scan runtime-scope maps, and route activation maps from the same authority epoch; it remains bounded source-status evidence while the source worker is applying control, but loses to any newer observed source/scan schedule owner for the same root and must not be converted into scoped rescan target evidence until a live source-status observation from the same target node or the scoped request/reply probe proves the node-owned rescan route is receive-armed. Older source/scan control-generation schedules are stale readiness evidence once a newer generation for the same root has been observed from a node that also reports schedule ownership for that root. Because runtime remote fanout is peer delivery, the API aggregation combines routed peer source-status evidence with the same-node source owner observation before judging source readiness; if routed peer fanout times out or yields no usable aggregate, same-node current source-primary target evidence may satisfy pre-delivery readiness only under the same root-health, route-activation, and scoped-delivery proof requirements. Same-node fallback observation must not resurrect source/scan schedule or concrete-root ownership for a root that the routed aggregate already reports as `serving-degraded` or waiting. If a current root has no route-schedule debug map during generation cutover, current source-primary domain evidence may name the scoped target for that root only when that root itself has non-degraded source status and source-status evidence returned by the same target node proves the node-scoped manual-rescan route is activated for that node/root; peer-origin aggregated route-control evidence may name a candidate but must not become target proof or pre-delivery readiness. When the observation is explicitly `source worker runtime scope served from control cache`, the route activation map is a cluster-control fact and may be observed from a peer origin, but it only names the candidate target and does not prove delivery readiness. Source-primary refs alone are not live route ownership. Serving-degraded aggregate status alone or unavailable-worker cache status is explanation evidence, not target evidence. The scoped request/reply delivery remains the final acceptance proof and must fail closed if that target route is not live. Sink materialization readiness is not part of this precondition because manual rescan delivery targets source request routes, so this source-only gate must not wait on sink-status route replies.
   Worker-backed clarification: when this step refers to same-node worker endpoint rearm, the externally addressed scoped source-rescan request lane is app-owned. The app-owned lane must be current-generation receive-ready, while the worker proves target acceptance for the current source-primary root; source-status must not wait for the worker to re-arm an external request lane it does not own.
4. api resolves current authoritative logical roots for manual-rescan target selection before scoped delivery. In worker-backed mode, if the live logical-roots snapshot loses a stale source-worker handoff after the worker generation has changed, api may use the cached current-roots view for this target-selection step only; ordinary live logical-roots snapshots still fail closed and must not silently downgrade to cache.
5. api publishes the cluster manual-rescan stream notification and performs bounded `source.rescan` request/reply collects against the current source-primary runner node-scoped routes for the current authoritative logical roots, where each scoped route is addressed by the source-primary runner identity proven by current live source runtime-scope observation, not by every active host grant that matches the logical root. The scoped collect uses the standard fs-meta route exchange and arms the reply lane before sending the request; direct one-off reply-channel polling is not scoped delivery evidence. For a current root, source/scan runtime-scope schedule is liveness evidence only; scoped delivery targets are publisher-ready only when the same source-status evidence reports non-degraded live group-primary concrete-root ownership for that root and the node-scoped manual-rescan route is activated and receive-armed on that same node. If explicit primary evidence points outside current live group-primary concrete-root ownership, the app continues bounded fan-in or fails closed; it must not choose a stable runtime-scope node that cannot publish that root. A newer source/scan control generation by itself MUST NOT override current non-degraded group-primary domain evidence when the same source-status evidence also proves the primary node scoped manual-rescan route is activated; generation ordering is only freshness evidence, not publication authority. Control scope alone is not runtime-scope ownership evidence unless the same node also reports live source/scan schedule ownership for that root. Cached older source-status schedules from query/facade aggregation are freshness hints only; degraded worker-cache schedules must not satisfy readiness or route manual rescan to a node that no longer owns the source-rescan endpoint. A same-target node-scoped route ready marker that lags the same source owner's newer local activation for that same target route is stale, even if the source owner still reports live root health; peer-origin activations may name a candidate but do not stale the source owner's local receive-armed proof. If source-status proves stable source ownership through a runtime-generated route-owner node id, the API must carry that exact route-owner id into the scoped route address and must not rebuild the route from the normalized stable node label. When readiness returns a target set from same-target source-status origin proof, the API must use that exact target set for scoped delivery; it must not merge the snapshots and then expand targets from peer-observed route activations in the aggregate view. The API must fail closed or continue bounded fan-in instead of sending scoped delivery to a locally stale or synthesized route. Active grants that no longer match the current authoritative logical roots, matching grants that are not the observed source-primary runner for that root, or source-primary refs outside current live group-primary concrete-root ownership are stale or standby runtime evidence and must not become required manual-rescan delivery targets. If no active source scoped target exists for the current roots, the generic `source.rescan` route is the fallback delivery probe.
6. source request/reply endpoint records the manual rescan intent and replies with explicit delivery evidence before rescan execution completes; execution is still owned by each group source-primary executor pipeline and waits for root readiness internally. A node-scoped source-rescan request route is accepted only by the source node named by that route. In worker-backed scoped delivery, acceptance is target-required: the target source worker must own a local source-primary scan root for the requested/current scope before it can reply `accepted`; a non-primary/no-root local no-op must fail closed and must not be converted into delivery evidence. The node-scoped endpoint MUST return this `accepted` delivery proof without synchronously starting the scan; scan execution is triggered by the cluster manual-rescan signal, not by the scoped proof endpoint. App/runtime endpoint layers may attach explicit non-target drains for active peer-scoped manual-rescan request routes so route fanout receives a bounded `not-target` answer instead of hanging, but those drains are supplementary only and never prove delivery readiness, execution ownership, or acceptance. Deploy-time route plans for node-scoped source-rescan request lanes MUST target the named node on the request lane and keep the reply lane reachable by current route-plan nodes; all-peer request fanout is not valid node-scoped delivery evidence. If stale or transport-level non-target evidence is observed, it is supplementary only and never proves execution ownership.
7. scoped manual-rescan collection treats any non-target drain evidence as supplementary only; it must keep waiting within the existing route timeout for the target source node's explicit delivery evidence, and short idle grace after non-target replies must not complete the target delivery decision. If a fanout send only reaches non-target evidence while the target endpoint is still becoming receive-armed, api reissues the same scoped delivery probe within the original route timeout instead of accepting non-target evidence or extending the operation budget. A zero-evidence scoped collect is different: the original request may still be queued at the source owner, so api must not blindly issue a new correlation after a short empty receive window. It keeps the first scoped collect open for the full bounded delivery budget and fails closed when target evidence never appears.
8. after a source control wave activates source-owned manual-rescan routes, the source runtime reconciles request endpoints again in the same control-frame turn so newly active local target routes can serve the first management request after activation, and app-level active peer-scoped routes can return explicit `not-target` drain evidence instead of timing out.
9. if retained source control replay is pending when source endpoint rearm is requested, the source worker client replays the retained state before asking the source owner to record receive-armed ready proof; a ready marker recorded before replay is stale because replay may replace the route-control evidence it must match. When bounded source-repair recovery clears retained replay without a live source-status rearm, it must restore the current-generation scoped source-rescan ready proof for the local target routes it just made current, otherwise the next management request can observe current state but still fail scoped delivery.
10. if any required active source scoped route cannot be reached, or if the generic fallback cannot be reached when there are no scoped targets, api fails closed instead of accepting a rescan that only reached the facade-local source.
9. api returns accepted response after the cluster route delivery and local source signal are published.

## [workflow] QueryApiKeyLifecycle

**Steps**

1. caller authenticates as a management-session subject with `admin` capability.
2. caller reads current query-consumer credentials through `GET /api/fs-meta/v1/query-api-keys`.
3. api checks Management Write Ready before credential creation or revocation; if the HTTP facade is reachable but the active control stream is not ready, api rejects the write with explicit `NOT_READY`.
4. caller creates a new query-consumer credential through `POST /api/fs-meta/v1/query-api-keys`; api persists the key metadata and returns the one-time `api_key` secret together with its summary row.
5. query consumers use that `api_key` only on `/tree`, `/stats`, and `/on-demand-force-find`; management endpoints continue to reject query-api-key bearer tokens.
6. caller revokes a credential through `DELETE /api/fs-meta/v1/query-api-keys/:key_id`.
7. after revocation, subsequent query requests with that key are rejected; revocation does not grant management-session access or mutate roots/runtime grants by itself.

## [workflow] ForceFindAvailability

**Steps**

1. caller sends `GET /api/fs-meta/v1/on-demand-force-find` with a query API key.
2. api identifies the current Authority Epoch from roots signature, grants signature, source stream generation, sink materialization generation, and facade/runtime generation.
3. if monitoring roots are empty, api returns explicit `NOT_READY` with `monitoring_roots_empty`.
4. if runtime grants are unavailable for the current Authority Epoch, api returns explicit `NOT_READY` with `runtime_grants_unavailable`.
5. api resolves Runner Binding Evidence for each requested group inside the current Authority Epoch; cached evidence may be used only when it belongs to that same Authority Epoch.
6. when route-schedule debug maps, configured-root snapshots, or per-snapshot active-member counters are absent/partial during cutover but same-epoch logical-root health and active grants prove group membership, Runner Binding Evidence derives candidates from those active group grants; debug underreporting must not collapse force-find onto source-primary only.
7. status fan-in must preserve selected-runner evidence produced by fresh execution even when route-schedule debug maps are absent; missing debug maps may fail-close incomplete root/debug fields, but they must not discard same-epoch runner evidence that proves force-find execution and fallback behavior.
8. successful force-find response origin evidence is authoritative runner evidence for management status; source-status route debug maps are supplementary diagnostics, and a bounded source-status timeout must not erase runner evidence already proven by the fresh execution response.
9. if runner binding has not yet been built for the current Authority Epoch, api returns explicit `NOT_READY` with `runner_binding_not_ready` instead of using older evidence.
10. if a requested group has no runner candidate in the current Authority Epoch, the group returns explicit `GROUP_UNBOUND` while other groups continue independently.
11. if a requested group already has an in-flight fresh scan, fs-meta applies the group-local exclusivity policy and either joins that in-flight work within the request budget or returns an explicit group-local conflict/unavailable result.
12. once a group is Force-Find Ready, fs-meta selects one runner bound to that group and dispatches fresh execution to that selected runner; it does not wait for source initial audit completion, sink materialization readiness, or trusted materialized observation eligibility.
13. the group enters Selected-Runner Fresh Execution: the group result is decided by the selected runner's success, explicit failure, or timeout, and fs-meta does not keep that group open to wait for unrelated runners.
14. for multi-group force-find, fs-meta repeats the same selected-runner decision per group and assembles independent group buckets; one group's wait or failure does not redefine another group's result.
15. successful results return `read_class=fresh` and `observation_status.state=fresh-only`; they do not promote `/tree` or `/stats` materialized observation to trusted state.
16. fresh execution failures remain explicit group-level errors such as `RUNNER_UNAVAILABLE` or `HOST_FS_UNAVAILABLE`; fs-meta does not silently substitute materialized results for failed fresh execution.
17. if a selected runner's remote mount-root is no longer mounted, the host-fs facade must fail closed with `HOST_FS_UNAVAILABLE`; when another current-epoch bound runner exists for the same logical group, fs-meta retries that group's fresh execution on the next bound runner before rendering unavailable evidence.
18. a same-group runner retry is still one group-level fresh execution decision; it must not use materialized data, unrelated group replies, or a generic fallback that can omit the requested group.

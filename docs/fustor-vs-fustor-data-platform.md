# `fustor + capanix` 与前身 `fustor-data-platform` 对比分析

分析时间基准：2026-04-07  
当前仓库基准：本地 `/root/repo/fustor` 与相邻 `/root/repo/capanix`  
前身仓库基准：`gh` 读取 `excelwang/fustor-data-platform` 的 `master` 分支

## 总结

结论先行：当前 `fustor` 不是对 `fustor-data-platform` 的同范围线性续作，而是一次**范围收缩 + 语义强化 + 平台外移**后的重构。

- `fustor-data-platform` 的顶层目标是做一个通用数据融合平台：`Agent + Fusion + Core + Extensions`，强调控制面与数据面解耦、插件化、多协议、多数据源。
- 当前 `fustor` 的核心只剩一个单域产品 `fs-meta`，并且它显式建立在 `capanix` 这个外部平台之上；运行、路由、bind/run、runtime ABI、worker 承载、deploy/release 等通用机制被上移到平台侧。
- 所以如果按“平台广度”看，当前项目明显更窄；如果按“文件元数据域产品的定义清晰度、查询语义、管理边界、发布切换、可信观测”看，当前项目反而更完整、更严格。

## 证据范围

当前体系主要依据：

- `README.md`
- `fs-meta/specs/L0-VISION.md`
- `fs-meta/specs/L2-ARCHITECTURE.md`
- `fs-meta/specs/L3-RUNTIME/API_HTTP.md`
- `fs-meta/specs/L3-RUNTIME/WORKFLOWS.md`
- `../capanix/README.md`
- `../capanix/specs/L2-ARCHITECTURE.md`

前身体系主要依据：

- `README.md`
- `pyproject.toml`
- `specs/L0-VISION.md`
- `specs/01-ARCHITECTURE.md`
- `specs/02-CONSISTENCY_DESIGN.md`
- `specs/07-DATA_ROUTING_AND_CONTRACTS.md`
- `specs/09-MULTI_FS_VIEW.md`
- `agent/README.md`
- `fusion/docs/README.md`
- `docs/archive/MANAGEMENT_V1_SPEC.md`

## 一、整体对比：设计目标

| 维度 | `fustor-data-platform` | `fustor + capanix` | 判断 |
| --- | --- | --- | --- |
| 顶层定位 | 通用“数据融合/同步平台”，覆盖 Agent、Fusion、Core、SDK、Source/Sender/Receiver/View 扩展 | 单一 `fs-meta` 域产品，依赖 `capanix` 提供平台底座 | 从“平台产品”转为“域产品 + 外部平台” |
| 核心口号 | “The Control Plane must survive the Data Plane” | “single fs-meta domain authority”“observation is not truth”“single app product boundary” | 关注点从“进程可存活/远程修复”转为“语义权威/可信观测/产品边界” |
| 控制与执行 | Agent/Fusion 之间靠 heartbeat、session、命令队列维持远程控制和热修复 | `fs-meta` 只消费薄 runtime ABI；bind/run、route、grant、worker 托管由 `capanix` 负责 | 通用控制机制平台化，不再由产品自带 |
| 真值模型 | `Fusion` 内存树是仲裁后的“最终状态”，文档中明确有“视图即真相”表述 | `fs-meta` 明确区分 authoritative truth ledger 与 observation/projection，并引入 `observation_eligible` | 当前语义更强，避免把查询结果直接等同真值 |
| 配置与运维 | YAML 配置、热重载、远程推配置、远程升级、远程扫描 | 薄 bootstrap 配置 + 在线 `runtime grants`/`monitoring roots` 管理 + generation cutover | 当前更偏产品化运维闭环，少做远程进程操控 |
| 可扩展策略 | 以插件化扩展 source/sender/receiver/view/schema 为核心 | 以单域产品边界稳定 + 平台能力复用为核心 | 扩展点从“业务插件”转向“平台分层” |

依据：

- 前身 `specs/L0-VISION.md`、`specs/01-ARCHITECTURE.md`
- 当前 `fs-meta/specs/L0-VISION.md`、`fs-meta/specs/L2-ARCHITECTURE.md`
- 平台边界见 `../capanix/specs/L2-ARCHITECTURE.md`

## 二、整体对比：功能完整性

这里必须分开看“广度”和“深度”。

### 1. 广度：前身明显更大

`fustor-data-platform` 的 `uv` workspace 直接列出：

- `agent`
- `fusion`
- `core`
- `agent-sdk`
- `fusion-sdk`
- 多个 `source-*`
- 多个 `sender-*`
- 多个 `receiver-*`
- 多个 `view-*`
- `demo`

它显式覆盖：

- 多数据源：FS、MySQL、OSS、Elasticsearch
- 多传输端：HTTP、OpenAPI、Echo 等 sender/receiver
- 多视图：`view-fs`、`view-fs-forest`
- Agent/Fusion 双端产品与 SDK
- 管理模块和历史 UI

当前 `fustor` 则只保留 `fs-meta` 相关的作者面、runtime、deploy、tooling；它不再尝试在本仓库内维持通用 source/sender/receiver/view 平台。

所以如果问题是“当前项目是否覆盖了前身整个平台的功能面”，答案是：**没有**。

### 2. 深度：当前在 `fs-meta` 域内更完整

如果只看“文件元数据观测与查询产品”，当前项目做了前身没有系统化完成的几件事：

| 方面 | `fustor-data-platform` | 当前 `fustor + capanix` |
| --- | --- | --- |
| 管理面边界 | 历史上有 management module，但文档自己列出状态易失、并发风险、共享 key 粗粒度鉴权等问题，且该模块已在 `v0.9.x` 被移除 | 管理面被重新收口为稳定命名空间：`/session/login`、`/status`、`/runtime/grants`、`/monitoring/roots`、`/index/rescan`、`/query-api-keys` |
| 查询面语义 | 主要是 `/api/v1/views/{view_id}/tree|stats`，森林视图提供 `members`/`best` 聚合 | `/tree`、`/stats`、`/on-demand-force-find` 共享 grouped-result 模型，带 `read_class`、`observation_status`、`pit`、分页 cursor、`group_order` |
| 读写权限模型 | 旧管理面依赖 `X-Management-Key`，数据面主要依赖 API key | 显式分离 management session 和 query API key，两种主体互斥 |
| 在线 scope 管理 | 旧设计更偏 fusion.yaml / agent.yaml 配置与热重载 | 当前以 `runtime grants -> roots preview -> roots apply` 形成在线 scope 管理闭环 |
| 新鲜度/可信度 | 有 tombstone、suspect、blind-spot、readiness，但“视图即真相”仍是顶层表述 | 显式区分 `fresh`、`materialized`、`trusted-materialized`，并把 trusted exposure 挂到 `observation_eligible` |
| 发布/切换 | 更偏守护进程热重载、命令下发、配置推送 | generation-based release cutover，旧 generation 与新 generation 的观测切换有明确语义 |

判断：

- 旧平台在**能力面广度**上更完整。
- 当前产品在**单域产品定义、访问边界、查询语义、发布切换、可信观测**上更完整。

### 3. 一个关键变化：不是简单删功能，而是重组功能

当前 `fs-meta` 的 specs 明确要求移除旧式 public helper 路径，例如：

- `/auth/login`
- `/config/roots`
- `/ops/rescan`
- `/release/render`
- `/exports`
- `/fanout`

这说明当前路线并不是“还没来得及补齐旧功能”，而是在主动拒绝旧接口模型，改用新的产品 API 和新的职责划分。

## 三、整体对比：架构与职责归属

### 架构/职责归属对照表

| 关注点 | `fustor-data-platform` | 当前 `fustor + capanix` |
| --- | --- | --- |
| 主体结构 | Python workspace：`agent`、`fusion`、`core`、多扩展 | Rust workspace：`fs-meta`、`fs-meta-runtime`、`fs-meta-deploy`、`fs-meta-tooling`；平台在外部 `capanix` |
| 产品边界 | Agent 和 Fusion 是两个顶层产品，View/Receiver/Source/Sender 再插件化组合 | 对外只暴露一个 `fs-meta` app 产品边界，内部再分 `facade/source/sink` worker 角色 |
| 运行时机制 | 产品内部自己承载 session、heartbeat、命令队列、Receiver/View 路由、配置热重载 | `capanix-runtime` 负责 bind/run、route convergence、runtime ABI injection、worker 承载；`fs-meta` 只拥有业务语义 |
| 数据/契约模型 | `schema`、`event_schema`、Handler/ViewDriver 是核心抽象；靠 `FusionPipe + ViewManager` 做两级路由 | 共享类型只保留域内 MessagePack carrier 和查询 DTO；app 通过 opaque port、host grants、meta-index 实现域语义 |
| 部署方式 | `pip/uv` + YAML + 热重载/命令派发 | deploy compiler + thin bootstrap config + runtime grants + generation cutover |
| Host / adapter | 多数适配逻辑仍在产品/扩展体系里展开 | host-local 适配明确下沉到 host-adapter SDK，域层不拥有 host operation 语义 |
| 观测与真值 | Fusion 内存树承担仲裁后最终视图 | truth ledger、projection、observation eligibility 分层明确 |

依据：

- 前身 `specs/01-ARCHITECTURE.md`、`specs/07-DATA_ROUTING_AND_CONTRACTS.md`、`fusion/docs/README.md`
- 当前 `fs-meta/specs/L2-ARCHITECTURE.md`、`../capanix/specs/L2-ARCHITECTURE.md`

## 四、谱系深挖：`Fusion + FS View + Agent` 到 `fs-meta`

如果只比较前身里最接近当前 `fs-meta` 的一条链路，那么它并不是凭空重来，而是发生了明显的“继承后重写”。

### 1. 延续下来的问题域

两代系统都关心：

- 文件系统元数据，而不是文件内容本身
- 内存态物化索引/树结构
- 实时事件、基线扫描、周期审计的组合
- 多源挂载/多节点视角下的统一查询
- 删除回生、热点文件、盲区修复等一致性问题

这一点在前身的 `view-fs` / `forest view` 文档，以及当前 `fs-meta` 的 index lifecycle、query/find、group planner 设计里是一致的。

### 2. 关键继承关系

| 谱系点 | 前身 | 当前 |
| --- | --- | --- |
| 物化树 | `Fusion` 内存 hash tree / FSViewDriver | `sink-worker` 持有 materialized tree / meta-index |
| 全量与审计 | Leader Agent 负责 snapshot/audit/sentinel | 每组 `source-primary executor` 负责 baseline/audit/sentinel |
| 多源聚合 | `forest view` 以 `fusion_pipe_id` 维护多树，对外返回 `members` 或 `best` | `fs-meta` 以 logical group 聚合，对外返回 `groups[]`、`group_order`、`pit` |
| 一致性标记 | tombstone、suspect、blind-spot | tombstone、integrity flags、observation status、trusted-materialized gate |

### 3. 关键断裂点

#### 3.1 从“View 即真相”到“Observation 不是 Truth”

前身一致性设计把 Fusion 内存树视为仲裁后的最终状态；当前 `fs-meta` 明确反对这一点，要求：

- truth ledger 与 observation 分开
- `/tree`、`/stats`、`/on-demand-force-find` 都只是 observation/projection
- 只有达到 `observation_eligible` 才能作为可信当前结果暴露

这是当前与前身最大的语义断裂。

#### 3.2 从“全局 Leader/Follower”到“按组主执行者”

前身是 session 驱动的 Leader/Follower：

- 第一个 AgentPipe 成为 Leader
- Leader 执行 Snapshot/Audit/Sentinel
- Follower 只负责 Realtime

当前是 group-aware 的 source planner：

- 所有组员都可 watch/listen
- 每个 group 单独选择 source-primary executor
- 周期扫描职责按 group 分配，而不是整个 view 只有一个 leader

这比前身更贴合多组、多挂载、分区执行的场景。

#### 3.3 从“动态 View API”到“固定产品 API”

前身的查询入口是：

- `/api/v1/views/{view_id}/tree`
- `/api/v1/views/{view_id}/stats`

当前的查询入口是固定产品命名空间：

- `/api/fs-meta/v1/tree`
- `/api/fs-meta/v1/stats`
- `/api/fs-meta/v1/on-demand-force-find`
- `/api/fs-meta/v1/bound-route-metrics`

这意味着当前系统已经不再把“view”当作可任意增殖的产品单位，而是把 `fs-meta` 当作单产品边界。

#### 3.4 从“产品自带控制平面”到“平台提供运行时底座”

前身把 heartbeat、命令队列、远程扫描、配置推送、热升级都做进产品内部。  
当前这些“运行/部署/worker 路由/host grants”相关能力被抽到 `capanix`，`fs-meta` 本身只保留：

- 领域查询与聚合
- 根范围选择
- 观测可信度判断
- 组规划与索引生命周期

换句话说，旧平台里一部分“系统软件职责”已经不再归 `fustor` 自己承担。

## 五、公共接口边界变化

### 前身的接口重心

- 数据面：
  - `POST /ingestor-api/v1/events/`
  - `POST /ingestor-api/v1/sessions/`
  - `GET /api/v1/views/{view_id}/tree`
  - `GET /api/v1/views/{view_id}/stats`
- 历史管理面：
  - `GET /api/v1/management/dashboard`
  - `POST /api/v1/management/agents/{agent_id}/command`
  - `GET/POST /api/v1/management/config`

它的特点是：Fusion 既是 ingest hub，又是 query server，还曾经兼做 cluster management center。

### 当前的接口重心

- 管理面：
  - `/api/fs-meta/v1/session/login`
  - `/api/fs-meta/v1/status`
  - `/api/fs-meta/v1/runtime/grants`
  - `/api/fs-meta/v1/monitoring/roots`
  - `/api/fs-meta/v1/index/rescan`
  - `/api/fs-meta/v1/query-api-keys`
- 查询面：
  - `/api/fs-meta/v1/tree`
  - `/api/fs-meta/v1/stats`
  - `/api/fs-meta/v1/on-demand-force-find`
  - `/api/fs-meta/v1/bound-route-metrics`

变化本质不是“接口数量变多/变少”，而是：

- 从“平台式、混合式入口”转为“单产品稳定命名空间”
- 从“共享 key + 管理辅助接口”转为“管理主体/查询主体分离”
- 从“视图导向查询”转为“域导向查询”

## 六、在线服务不停机：新架构的关键收益

如果只看“技术栈升级”或“单域化”，很容易低估当前架构真正想解决的问题。  
当前 `fustor + capanix` 的一个核心收益，其实是把原来那些会把在线查询直接打成不可用的变更窗口，尽量改造成：

- 旧 owner 继续对外服务
- 新 owner 在后台 catch-up
- 必要时只暴露显式的 `degraded/not-ready`
- 或者让 `fresh` 路径先可用，而不是整站直接 `503`

这和前身的可用性模型差异很大。前身并不是完全没有“在线变更”能力，它支持 `SIGHUP` 热重载，也有“动态扩容”文档；但旧方案的公开契约同时明确写了：

- 初始快照阶段，View API 会返回 `503 Service Unavailable`
- Live 模式下无活跃 Session 时，View 会自动 reset 并返回 `503`
- 权威 Agent 心跳丢失时，Fusion 必须立即把对应 View 切成 `503`
- Hot reload 只支持 Pipe 的增加/删除；已有 Pipe 的配置变更通常要“删除再添加”或直接重启

所以旧方案更像“进程尽量不死、控制面尽量在线”；当前方案更像“产品入口尽量不停机，必要重建放到后台，切换只在新结果可信后发生”。

### 1. 旧方案的典型不可用窗口

| 场景 | `fustor-data-platform` 中的不可用窗口 | 当前 `fustor + capanix` 的对应设计 |
| --- | --- | --- |
| 新 NFS 上线 | 典型流程是改 `fusion.yaml` / `agent.yaml` 再 `reload`。旧 hot reload 只支持增删 Pipe，不支持原 Pipe 配置就地变更；新源接入后还要经历 Session 建立、初始 Snapshot、队列清空。旧文档明说：初始快照期间 View API 返回 `503`。 | 新挂载先以 `runtime grants` 暴露，再通过 `/monitoring/roots/preview` 和 `/monitoring/roots` 在线纳入；`grants-changed` 会触发 source 在线增量重收敛和定向重扫。已有 root 集合保持有效，新增失败不会覆盖旧 truth。 |
| 旧 NFS 退役 | 旧模型强依赖 Leader/活跃 Session。Leader 心跳丢失或最后一个活跃 Session 关闭时，Live View 会 reset 并返回 `503`。Leader 迁移后，新 Leader 还会补做 Snapshot 并清空 audit cache，期间存在 catch-up 窗口。 | 当前按 `runtime grants-changed` 和 online roots 管理做有界回收；移除 object 不要求整服务重启。查询路径本身支持 group 级 partial failure isolation，单组问题不应拖垮整次查询。`roots=[]` 也是合法已部署状态，不要求“无 root = 服务失败”。 |
| `fustor` 自身升级 | 旧文档没有定义 old/new generation 并存和可信结果切换契约；管理模块更多是 `reload`、`upgrade`、重载配置、命令下发。按旧 readiness 规则，一旦 Fusion 重启、Session 重建或 Leader 切换，View 可能落入 `503` 直到 snapshot end。 | 当前升级是 release-generation cutover：候选代先重放当前 `monitoring roots`、`runtime grants` 和 authoritative journal，再后台重建 observation。可信的 materialized `/tree` 和 `/stats` 继续留在上一代，直到新代达到 `observation_eligible` 才切换。 |
| 配置失配或变更失败 | 旧管理模块自己承认配置更新缺少锁和版本控制，容易有并发风险；而且 Agent 配置缓存是内存态，Fusion 重启后会丢。 | 当前 `roots` 更新先 `preview` 再 `apply`，`PUT /monitoring/roots` 会按当前 runtime grants 重新校验；只要有 unmatched roots，整次写入拒绝，旧 authoritative roots 保持不变。 |

### 2. 分场景的针对分析

#### 6.1 新 NFS 上线

旧方案理论上支持“动态扩容”，但它的在线性主要体现在“可以不手工停掉整个守护进程”，而不是“查询面没有不可用窗口”。

旧方案的风险点在于：

1. Fusion 和 Agent 侧都要改 YAML，再通过 `SIGHUP` reload。
2. hot reload 只按 Pipe ID 做 added/removed diff；如果是改已有 Pipe 配置，不会被检测到，通常只能删旧 Pipe、加新 Pipe，或者直接重启。
3. 新接入源需要重新建立 Session，并经历初始快照。
4. 旧 `Fusion` 文档直接规定：初始快照阶段 View API 返回 `503 Service Unavailable`。

所以旧方案在“新 NFS 上线”场景下的典型风险不是进程一定宕机，而是**在线查询会进入 not ready/503 窗口**。

当前方案对此的改法是：

1. 把“可见到哪些挂载”从静态配置改成 runtime 注入的 `runtime grants`。
2. 把“要不要监控这些挂载”改成产品 API 上的在线 roots 管理。
3. source 收到 `grants-changed` 后在线增量重收敛，新 object 启动新的本地执行分区，并触发定向重扫。
4. 若新增 roots 校验失败，旧 roots 集合保持不变，不会把在线服务一起写坏。

重点不是“新 NFS 立刻就完全可查”，而是**新增范围的 catch-up 不应该拖垮原有已服务范围**。

#### 6.2 旧 NFS 退役

旧方案里，NFS 退役通常会表现为：

- Agent 下线
- Leader 心跳超时
- Session 消失
- 相关 Pipe 被删除或重载

而旧文档对 Live 模式的规定又很硬：

- 无活跃 Session 就 reset + `503`
- 权威 Agent 心跳丢失就立即 `503`

这意味着旧 NFS 退役并不只是“少一个数据源”，而可能把对应 View 直接打成不可用，直到新的 Leader/Session 把状态重新追上。

当前方案试图把这个窗口缩到更小：

- group 是独立规划和独立查询聚合单位
- 单组失败不应拖垮整次查询
- `runtime grants-changed` 会做在线增量收敛和有界回收
- 若某些组暂时未追平，materialized 查询可以继续留在上一代 eligible owner，或者返回显式 degraded/not-ready，而不是把整个产品入口直接打成粗粒度 `503`

这里的提升本质是：**从“资源失联 => 整个 View 不可用”转为“资源失联 => 只影响对应 group 或对应 generation 的可信暴露”**。

#### 6.3 `fustor` 自身升级

这是新旧差异最大的场景。

旧方案里虽然有 `upgrade` 命令和 reload 机制，但缺的不是“怎么发升级命令”，而是缺：

- old/new generation 并存语义
- 何时允许新版本接管外部查询
- 旧版本何时 drain
- 新版本何时算“结果可信”

因此，旧方案一旦发生 Fusion 重启、会话重建、Leader 迁移，本质上还是要重新走 readiness，公开 contract 允许查询面直接 `503`。

当前方案则把升级定义成共享生命周期：

- candidate generation 先进入 catch-up
- 重放当前 roots / runtime grants / authoritative journal
- 后台 rebuild materialized observation
- 在 `observation_eligible` 前，trusted materialized `/tree` 与 `/stats` 继续由上一代提供，或者显式 not-ready/degraded
- `/on-demand-force-find` 作为 freshness path 可以更早外放
- 只有新代达到 `observation_eligible` 后，trusted external materialized exposure 才切过去
- cutover 后旧代进入 drain/retire，并被 stale fencing，不能再偷偷抢回 owner 身份

这实际上是在把“升级窗口”从**服务中断窗口**改造成**后台追平窗口**。

### 3. 可用性模型的根本变化

这两代系统对“服务不停机”的理解并不一样：

- 旧方案强调的是：Agent/Fusion 守护进程别死，控制链路别断，出问题后还能远程修。
- 新方案强调的是：即使后台在重建、切换、收敛，对外产品入口也尽量不要因为内部 catch-up 而整体失效。

更具体地说，当前架构把旧方案里常见的粗粒度不可用：

- `503 Service Unavailable`
- Session 空窗
- Leader 切换后重新 Snapshot
- reload 触发的配置窗口

尽量改造成以下几类更细粒度状态：

- 上一代 eligible generation 继续服务
- 只对新范围/新组返回 not-ready 或 degraded
- 只让 materialized path 延后切换，而保留 fresh path
- 用 stale fencing 保证旧 owner 不会在切换后重新对外暴露错误结果

这也是为什么当前 specs 反复强调：

- `ONLINE_SCOPE_MUTATION_AND_REPAIR`
- `RELEASE_GENERATION_CUTOVER`
- `observation_eligible`
- “until eligible, keep previous eligible generation or explicit degraded state”

它们共同服务的目标就是：**把在线变更变成连续服务下的后台重收敛，而不是把用户请求暴露在 reload/restart/snapshot 的裸窗口里**。

## 七、当前重构进展：按 24 条 E2E 职责面看

下面这一节有两层事实来源，需要分开看：

- **仓库内可直接验证的结构事实**：`fs-meta/tests/fs_meta_api_e2e.rs` 当前挂了 24 条 ignored real-NFS E2E。
- **2026-04-07 的联调进展快照**：来自你提供的另一 session 摘要，尚未全部回写到仓库内 `TODO.md` 或重新刷 authoritative full-suite。

### 1. 24 条 E2E 的组成

当前 `fs_meta_api_e2e` 里的 24 条 ignored real-NFS E2E，可以精确拆成 4 个职责簇：

| 职责簇 | 数量 | 对应测试 | 主要在验什么 |
| --- | --- | --- | --- |
| API 契约与查询矩阵 | 3 | `fs_meta_http_api_matrix_real_nfs`、`fs_meta_http_api_matrix_query_baseline_real_nfs`、`fs_meta_http_api_matrix_live_only_rescan_real_nfs` | 登录、状态、runtime grants、roots preview/apply、rescan、`/tree` `/stats` `/on-demand-force-find`、PIT、分页、`bound-route-metrics` |
| Activation Scope Capture | 3 | `activation_scope_capture_preserved_layout_distributed`、`activation_scope_capture_nfs2_visibility_contracted_to_node_a`、`activation_scope_capture_force_find_preserved_pre_force_find` | runtime 激活后 source/scan/sink scope 是否落在正确节点，visibility 收缩后 scope 是否正确收敛，force-find 前后 scope capture 是否保持稳定 |
| 在线运维与资源变更 | 8 | `fs_meta_operational_scenarios_real_nfs`、`fs_meta_operational_force_find_execution_semantics_real_nfs`、`fs_meta_operational_new_nfs_join_real_nfs`、`fs_meta_operational_root_path_modify_real_nfs`、`fs_meta_operational_visibility_change_and_sink_selection_real_nfs`、`fs_meta_operational_sink_failover_real_nfs`、`fs_meta_operational_facade_failover_and_resource_switch_real_nfs`、`fs_meta_operational_nfs_retire_real_nfs` | 新 NFS 上线、根路径修改、可见性收缩、sink/facade 故障切换、NFS 退役、在线 force-find 执行语义 |
| Release-Generation 升级与连续性 | 10 | `fs_meta_release_upgrade_real_nfs`、`fs_meta_release_upgrade_peer_source_control_completion_real_nfs`、`fs_meta_release_upgrade_facade_claim_continuity_real_nfs`、`fs_meta_release_upgrade_roots_persist_real_nfs`、`fs_meta_release_upgrade_tree_stats_stable_real_nfs`、`fs_meta_release_upgrade_tree_materialization_real_nfs`、`fs_meta_release_upgrade_sink_control_roles_real_nfs`、`fs_meta_release_upgrade_source_control_roles_real_nfs`、`fs_meta_release_upgrade_window_join_real_nfs`、`fs_meta_release_upgrade_cpu_budget_real_nfs` | generation cutover、source/sink/facade continuity、roots 持久、树与 stats 稳定性、升级窗口并发、CPU 预算、升级后 materialization 完整性 |

这 24 条的意义不是“回归测试数量很多”，而是它们几乎把当前 `fs-meta + capanix` 重构最重要的 4 个目标面都钉住了：

1. 产品 API 不能漂
2. 在线 scope/resource 变更不能把服务打穿
3. runtime scope 规划与 group 落位不能乱
4. generation cutover 不能靠“运气”成功

### 2. 进展快照：官方计分板仍是 18/24，但本地前线已经前移

按你给的联调摘要，**最新 authoritative full-suite 口径仍是 18/24 = 75.0%**。  
但同一份摘要也明确说明：这个数字还没有吸收最近一轮本地 closure，因为这些 closure 之后还没有重新跑 authoritative full-suite 刷官方分数。

所以当前更准确的表述不是“仍然只有 18/24”，而是：

- **官方计分板暂时停在 18/24**
- **本地 exact 前线已经继续前移**

### 3. 已经明确洗绿的 5 条关键 exact

根据你提供的联调快照，本地已明确打绿的 exact 有 5 条：

- `fs_meta_http_api_matrix_query_baseline_real_nfs`
- `fs_meta_http_api_matrix_real_nfs`
- `fs_meta_operational_nfs_retire_real_nfs`
- `fs_meta_release_upgrade_cpu_budget_real_nfs`
- `fs_meta_release_upgrade_facade_claim_continuity_real_nfs`

这 5 条横跨了 3 个最关键职责簇：

| 已绿 exact | 所属职责簇 | 说明 |
| --- | --- | --- |
| `matrix_query_baseline` | API 契约与查询矩阵 | 说明基础查询面、roots/status/grants、materialized query 基线已经更稳了 |
| `matrix_real` | API 契约与查询矩阵 | 说明完整 HTTP API contract 与 query matrix 在真实 NFS 下已经站住一块 |
| `nfs_retire` | 在线运维与资源变更 | 直接对应“旧 NFS 退役时不要把服务打崩”的核心目标 |
| `cpu_budget` | Release-Generation 升级与连续性 | 不只是性能指标，实际上在卡“升级/恢复路径不能因 continuity seam 卡死或超时” |
| `facade_claim_continuity` | Release-Generation 升级与连续性 | 直接验证新架构里 facade owner handoff 的连续性和 fail-closed 纪律 |

从职责面看，这 5 条已绿的意义比“+5 分”更大，因为它们刚好覆盖了：

- 产品 API 基线
- 在线退役场景
- 升级连续性
- facade claim handoff
- CPU/timeout 级别的收敛质量

也就是说，当前重构不是只在“单元测试更绿”，而是在**最接近真实用户感知的场景责任面上变绿**。

### 4. 这轮 closure 具体说明了什么

你给的联调摘要里提到两条刚新增并验证过的 owning closure：

- [runtime_app.rs](/root/repo/fustor/fs-meta/app/src/runtime_app.rs#L20328)
  `mark_control_uninitialized_clears_retained_active_facade_continuity_before_later_query_cleanup`
- [api.rs](/root/repo/fustor/fs-meta/app/src/query/api.rs#L13631)
  `build_tree_pit_session_trusted_materialized_fails_closed_when_later_ranked_group_returns_empty_root_after_first_group_decodes`

这两条 closure 很能说明当前重构的实际方向。

第一条 closure 的含义是：

- 当控制面被标记为 uninitialized 时，必须先清掉保留的 active facade continuity 状态
- 后续 query cleanup 不能继续背着旧 continuity 残影运行

它对外对应的不是“内部变量更整洁”，而是：

- stale facade continuity 不能拖住后续 handoff
- 旧 owner 的残影不能继续污染 later query cleanup
- `facade_claim_continuity` 这种 upgrade/cutover 场景要么继续正确服务，要么明确 fail-closed

第二条 closure 的含义是：

- trusted-materialized 的 `/tree` PIT 构建过程中，如果前面某个 group 已经 decode 成功，而后面排名更靠后的 group 却返回空 root/payload，系统必须 fail-closed
- 不能把它渲染成一个表面 `ok` 的空树，也不能把它拖成 504 TIMEOUT

这和你摘要里的描述是对上的：  
它把 `cpu_budget` exact 中原本更脏的“node-d fixed-bind continuity timeout + /tree 504 TIMEOUT”，推进成了更干净的 trusted fail-close，最终再把 `cpu_budget` 带绿。

换句话说，这轮进展并不是“把 timeout 调长了”，而是：

- 把 continuity seam 从模糊 timeout 收敛成明确 fail-closed
- 把“看起来卡死”收敛成“可诊断、可拒绝、不冒充成功”

这非常符合当前 `fs-meta` 的总路线：  
**宁可显式 not-ready / degraded，也不接受半切换、半成功、假成功。**

### 5. 从 24 条职责面看，当前卡点已经进一步收缩

如果按这 24 条职责面来理解当前重构进展，可以把状态概括成：

#### 7.1 API 契约面

- `matrix_query_baseline` 和 `matrix_real` 已本地洗绿
- 说明 API 命名空间、查询形态、roots/grants/status、materialized query 主链路已有明显收口
- 尚未从你给的快照中看到 `live_only_rescan` 的新 authoritative 结果，因此这一块更像“基线稳住了，大 live-only 边角还有待刷新”

#### 7.2 在线变更面

- `nfs_retire` 已本地洗绿，这一点很重要
- 因为它不是单纯资源删除，而是在验证“退役时 query 和 owner 切换还能不能维持正确行为”
- 但 `new_nfs_join`、`root_path_modify`、`visibility_change_and_sink_selection`、`sink_failover`、`facade_failover_and_resource_switch` 是否都已跟进打绿，当前摘要没有给出新的 authoritative 结论

#### 7.3 Activation Scope 面

- 这 3 条测试不直接产出用户 API 成败，但它们是在验证 runtime.exec.source / scan / sink 的 scope capture 是否正确
- 这一层其实是 `capanix` 平台能力与 `fs-meta` 产品语义接缝处的关键护栏
- 当前摘要没有显示它们有新的 wash-green 结果，所以可以理解为：平台语义侧的护栏还在，但最近的突破主要不在这里

#### 7.4 Release-Generation Cutover 面

- 这是当前剩余风险最密集的一簇，也是重构主战场
- 好消息是 `cpu_budget` 和 `facade_claim_continuity` 已经从 sharp exact 集里掉出去
- 这意味着升级后的 facade continuity 和 timeout/fail-close 处理已经有实质前进
- 当前最合理的下一个前线，按你给的摘要，已经转移到 `tree_materialization` 这一类 exact

### 6. 当前最准确的阶段判断

把上面的信息合起来，当前更准确的阶段判断是：

1. `fs-meta + capanix` 的重构已经越过“只是架构重命名”的阶段，开始在 24 条真实 E2E 职责面上稳定收口。
2. 收口最明显的三块是：
   - HTTP/query API 基线
   - NFS 退役这类在线变更
   - upgrade/cutover 下的 facade continuity 和 CPU/timeout fail-close
3. 官方 18/24 计分板已经落后于本地 exact 前线。
4. 下一步最合理的工作面，正如你给的摘要所说，仍然是继续从 [fs_meta_api_e2e.rs](/root/repo/fustor/fs-meta/tests/fs_meta_api_e2e.rs) 这条主套件往前推，尤其是 `tree_materialization` 及相邻的 upgrade/materialization exact，然后再回 authoritative full-suite 刷新正式通过数。

如果只用一句话概括当前进度：

> 当前重构已经把一部分最危险的 continuity / timeout / facade handoff 问题洗成了明确 fail-closed，并开始在真实 NFS E2E 上兑现“在线变更不停服务”的设计目标；官方分数还没刷新，但本地前线已经继续向前推进。

## 八、最终判断

### 1. 设计目标

当前项目的设计目标已经不是“重做一个更大的 Fustor 数据平台”，而是“把文件元数据能力收敛成一个单域产品，并把通用平台能力交给 `capanix`”。

### 2. 功能完整性

- 若按前身整个平台比较：当前是不完整的，因为它不再覆盖多 source/sender/receiver/view 平台。
- 若按 `fs-meta` 这条谱系比较：当前在产品边界、查询语义、权限模型、运行时切换、可信观测方面更完整。

### 3. 架构

前身是“Python 插件平台 + 双端产品”；当前是“Rust 单域产品 + 外部平台分层”。

## 九、明确的推断

下面这条是**推断**，不是仓库中文字面原句：

> 当前 `fustor` 很像是把前身中仍然有长期价值的“FS 元数据观测/聚合”问题抽出来，重新安放到 `capanix` 平台之上；也就是说，它不是前身的全量继承者，而是前身某条核心产品线在新平台上的重构版本。

推断依据：

- 当前 workspace 只保留 `fs-meta` 相关 crate
- 大量通用能力依赖 `capanix-*` crate
- 当前 specs 反复强调“single app boundary”“thin runtime ABI”“runtime owns bind/run/route/grant”
- 当前 contracts 显式废弃 legacy helper API，而不是试图兼容回旧平台接口

如果只用一句话概括：

> `fustor-data-platform` 更像一个可扩展的数据融合平台；当前 `fustor + capanix` 更像一个建立在新平台之上的、语义更严格的文件元数据产品。

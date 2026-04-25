# fs-meta Demo 产品说明书

本说明书面向 demo 用户、售前、产品经理和现场讲解人员。它不讲部署细节，只讲：

- fs-meta 是什么
- 它解决什么问题
- demo 里应该看什么
- 每个页面/接口代表什么产品语义

权威依据来自 `fs-meta/specs/`，不是实现细节。

如果本次 demo 采用当前 5 节点环境，则运行组件位于 `10.0.82.144~148`。
控制机或仓库机只负责部署和演示组织，不属于默认产品运行面。

## 1. 产品是什么

`fs-meta` 是一个面向文件系统元数据的产品。它把“挂载在不同节点上的文件目录”统一抽象成一个单一产品边界，对外提供：

- 文件树查询
- 聚合统计查询
- 新鲜实时探测
- 状态监控
- 在线监控范围配置

它不是通用对象存储，也不是文件内容服务。当前产品只处理“文件和目录的元数据”。

## 2. 产品核心价值

### 2.1 统一看见文件树

用户不需要登录每个节点逐一查看目录结构。fs-meta 会把分布式文件源组织成逻辑 group，再统一提供：

- `/tree`
- `/stats`
- `/on-demand-force-find`

### 2.2 统一配置监控范围

产品不会要求用户在部署阶段就写死所有业务目录。标准流程是：

1. 先看 runtime grants
2. 再 preview roots
3. 再 apply roots

这样“能监控什么”来自当前 runtime 授权，而不是历史静态配置。

### 2.3 同时提供两种读取语义

这是 demo 里最重要的产品概念之一。

#### materialized 语义

`/tree` 和 `/stats` 默认读的是 sink 持有的 materialized observation。
这条路径更适合：

- 稳定读
- 列表/统计展示
- 面向用户的常规查询

#### fresh 语义

`/on-demand-force-find` 是 freshness path。
这条路径更适合：

- 刚改完文件后立即确认
- materialized 结果还没追平时，先看实时事实
- 演示“同步链路还在追，但 fresh query 已经可见”

specs 明确要求 `/on-demand-force-find` 的可用性窗口宽于 trusted materialized 查询。

## 3. demo 里会看到哪些产品对象

### 3.1 runtime grants

这是“当前 runtime 授权给 fs-meta 看见的资源列表”。
它回答的问题是：

- 现在系统有权访问哪些 mount roots
- 这些资源来自哪个 host/object
- 当前业务可选范围是什么

这不是历史导出清单，而是当前可用真值。

### 3.2 monitoring roots

这是“产品当前真正监控的业务范围”。
它回答的问题是：

- 哪些资源已经被纳入 fs-meta 监控
- 这些 root 最终会映射成哪些 monitor paths

用户在 demo 里可以看到：

- preview 结果
- apply 后的当前 roots

### 3.3 status

这是 demo 里最重要的监控面。它告诉用户：

- source 当前覆盖到了哪些授权 root
- sink 当前是否已经 materialize 完成
- facade 当前是否在 serving
- 每个 group 当前是否可靠

status 的意义不是“内部 debug 面板”，而是产品级 observation evidence。

### 3.4 tree

`/tree` 代表按 group 返回的文件树结果。
它适合 demo：

- 展示目录层级
- 展示多 group 聚合
- 展示 trusted materialized 的正常读取结果

### 3.5 stats

`/stats` 代表按 group 返回的聚合统计。
重点字段包括：

- `total_nodes`
- `total_files`
- `total_dirs`
- `total_size`
- `latest_file_mtime_us`

demo 里它适合解释“文件总量统计”和“目录聚合统计”。

如果现场要讲“总共汇聚了 10 亿文件”，推荐做法是：

- 保留 5 个 logical groups
- 每个 group 对应一个独立 NFS 数据源
- 用 `/stats` 读取 5 个 group 的 `total_files`
- 现场做总和展示

不要把这一步讲成“单一超级目录树已经被物理合并”。产品正确语义是：

- 一个 fs-meta 产品边界
- 统一接入 5 个业务 groups
- 统一提供树查询、统计查询和 fresh 查询
- 统计总量时可以跨 groups 汇总

### 3.6 on-demand-force-find

这是 demo 里最适合展示“刚改文件马上看见”的接口。
它返回的是 fresh-only 语义，不等同于 trusted materialized。

## 4. demo 里用户应该理解的几个状态概念

### 4.1 `trusted-materialized`

表示：

- sink 里的 materialized observation 已达到可被信任的门槛
- `/tree` 和 `/stats` 可以按 trusted materialized 语义对外提供

这是 demo 中“正常稳定状态”的核心标志。

### 4.2 `materialized-untrusted`

表示：

- materialized 结果已经有了
- 但还没达到 trusted 门槛

这类状态适合讲“系统已经在收敛，但还没完全达标”。

### 4.3 `fresh-only`

表示：

- 当前结果来自 freshness path
- 适合快速确认，不代表 trusted materialized 已经追平

这就是 `/on-demand-force-find` 的标准语义。

### 4.4 `reliable / unreliable_reason / stability`

这些字段告诉用户：

- 当前 group 是否可靠
- 如果不可靠，是为什么
- 当前稳定性属于什么模式

在 demo 主链里，正常 group 应该是可靠且稳定的。

## 5. 现场 demo 推荐讲法

### 第 1 段：先讲“系统能看到什么”

先展示：

- 登录
- `/runtime/grants`
- `/monitoring/roots/preview`
- `/monitoring/roots`

讲清楚：

- runtime grants 是可选范围
- monitoring roots 是真正启用的范围

### 第 2 段：再讲“系统现在状态如何”

展示 `/status`，重点讲：

- facade 是否 serving
- source 是否已经接入目标 roots
- sink group 的 `initial_audit_completed`
- sink group 的 `materialization_readiness`
- `shadow_lag_us`

讲清楚：

- 这不是底层日志
- 这是产品级 readiness/health 证据

### 第 3 段：再讲“稳定查询结果”

展示 `/tree` 和 `/stats`：

- `/tree` 展示文件树
- `/stats` 展示文件总量和目录总量

这一步是“稳定结果面”。

如果这次 demo 的目标是强调规模，建议把 `/stats` 当成主证据：

1. 先展示 5 个 groups 都已经进入 serving/readable 状态。
2. 再展示每个 group 的 `total_files`。
3. 最后把 5 个 group 的 `total_files` 相加，说明当前合计约 10 亿文件。

现场推荐话术：

- “我们不是把 5 个来源讲成 1 个物理目录，而是在 1 个 fs-meta 产品边界内统一管理 5 个 groups。”
- “每个 group 当前大约是 2 亿文件，5 个 group 汇总大约 10 亿文件。”
- “这个总量以 `/stats` 的 group 级统计作为主证据。”

### 第 4 段：最后讲“实时性”

在 NFS 或数据目录里改一个文件，然后立即调用 `/on-demand-force-find`。
讲清楚：

- fresh path 可以先看到变化
- materialized 路径会在短时间内追平

这是整个 demo 里最能体现产品价值的部分。

## 6. demo 用户最容易误解的点

### 误解 1：force-find 就是最终结果

不是。force-find 是 freshness path，用来证明“实时可见”，不是用来替代 trusted materialized。

### 误解 2：status 只是调试页面

不是。status 是正式产品监控面，代表当前 observation evidence。

### 误解 3：roots 是部署时写死的

不是。roots 是在线配置，不是 deploy config 的一部分。

### 误解 4：查询用户和管理用户是同一类账号

不是。管理用户使用 management session；查询用户使用 query API key。

## 7. 这版 demo 的边界

这版 demo 优先保证：

- 同步
- 状态监控
- 查询
- 配置

不把下面内容当作现场重点：

- 平滑升级
- 复杂 failover
- external worker 复杂恢复
- 长尾回放/切代细节

这不是说它们不存在，而是这版 demo 不把它们当成主叙事。

## 8. 建议的现场用语

如果你要给非工程观众解释，可以直接用下面这套说法：

- “`runtime grants` 表示系统当前被授权能看到哪些文件资源。”
- “`monitoring roots` 表示我们实际选择纳入监控的业务目录。”
- “`status` 告诉我们系统是不是已经把这些目录稳定地接进来了。”
- “`/tree` 和 `/stats` 是稳定查询面。”
- “`/on-demand-force-find` 是实时确认面，适合刚改完文件时立即看结果。”
- “这次规模展示按 5 个 group 汇总统计，总量约 10 亿文件。”

## 9. 关联文档

- [DEPLOYMENT_ENGINEER_MANUAL.md](./DEPLOYMENT_ENGINEER_MANUAL.md)
- [PRODUCT_DEPLOYMENT.md](./PRODUCT_DEPLOYMENT.md)
- [API_HTTP.md](../specs/L3-RUNTIME/API_HTTP.md)
- [STATE_MODEL.md](../specs/L3-RUNTIME/STATE_MODEL.md)

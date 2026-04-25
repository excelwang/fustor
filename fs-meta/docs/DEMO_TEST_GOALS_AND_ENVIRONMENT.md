# fs-meta Demo 测试目标与测试环境说明

## 1. 文档目的

这份文档面向接手 demo 环境的人，目标是让对方快速理解：

- 这次 demo 想验证什么
- 环境由哪些节点组成
- 运行边界和限制条件是什么
- 哪些结果算“达标”

本文档不记录每一步排障细节，重点是说明测试目标和测试环境本身。

## 2. 测试目标

本次 demo 的目标分成两层。

### 2.1 基础目标

确保 `fs-meta` 在当前 5 节点环境中具备以下能力：

- `capanix` 5 节点底座正常运行
- `fs-meta` 成功 deploy
- `fs-meta` 管理 API 可访问
- 管理登录、状态查询、query key 管理可用
- `runtime grants`、`monitoring roots preview/apply` 这条产品路径可验证

### 2.2 完整 demo 目标

在基础目标之上，进一步达成：

- 5 个 NFS 数据源全部进入当前 demo 的业务范围
- 形成 5 个 logical groups
- 可以展示 `/status`
- 可以展示 `/stats`
- 最终能够按 5 个 group 汇总出约 `10 亿文件`

当前关于“10 亿文件”的对外表述应为：

- 每个数据源约 `2 亿文件`
- 5 个 groups 汇总约 `10 亿文件`

## 3. 测试环境

### 3.1 运行节点

当前运行节点固定为：

- `10.0.82.144`
- `10.0.82.145`
- `10.0.82.146`
- `10.0.82.147`
- `10.0.82.148`

控制机、仓库机、制品分发机不算运行面。

### 3.2 当前产品边界

`fs-meta` 对外始终按一个单一产品边界理解。

内部角色包括：

- `facade-worker`
- `source-worker`
- `sink-worker`

但本次 demo 中不把它们讲成三个独立产品。

### 3.3 worker 运行方式

本次环境约束已经明确：

- `facade = embedded`
- `source = embedded`
- `sink = embedded`

因此对外不按 external worker 模式解释。

### 3.4 当前 NFS 数据源

当前业务数据源来源于这 5 个 NFS 目录关系：

- `panda144` 观测 `/mnt/fustor-peers/nfs145`
- `panda145` 观测 `/mnt/fustor-peers/nfs146`
- `panda146` 观测 `/mnt/fustor-peers/nfs147`
- `panda147` 观测 `/mnt/fustor-peers/nfs148`
- `panda148` 观测 `/mnt/fustor-peers/nfs144`

这些是当前真实环境中的数据目录来源，不应再改动现有挂载结构。

### 3.5 当前 facade 入口

当前 `fs-meta` facade 入口已绑定在：

- `10.0.82.145:18080`

这也是当前产品管理 API 的主要访问入口。

## 4. 当前产品面验证路径

当前 demo 产品面应按下面顺序理解和验证：

1. `POST /api/fs-meta/v1/session/login`
2. `GET /api/fs-meta/v1/status`
3. `GET /api/fs-meta/v1/runtime/grants`
4. `GET /api/fs-meta/v1/monitoring/roots`
5. `POST /api/fs-meta/v1/monitoring/roots/preview`
6. `PUT /api/fs-meta/v1/monitoring/roots`
7. `POST /api/fs-meta/v1/query-api-keys`
8. 查询面：
   - `/tree`
   - `/stats`
   - `/on-demand-force-find`

## 5. 成功标准

### 5.1 底座成功标准

以下条件同时满足，才算底座成功：

- 5 节点 `cluster status` 为 `cluster_ready`
- 5 节点 `reachable=true`
- 各节点 control socket 正常

### 5.2 产品成功标准

以下条件同时满足，才算 `fs-meta` 产品面成功：

- `deploy` 返回 `status=ok`
- `/session/login` 返回 `200`
- `/status` 返回 `200`
- `facade.state = serving`
- 可成功签发 query key

### 5.3 完整 5-group demo 成功标准

以下条件同时满足，才算“完整 demo 环境正常”：

- `/runtime/grants` 中可见 5 个数据源对应资源
- 5 条 `monitoring roots` 都能 preview 命中
- 5 条 `monitoring roots` 能成功 apply
- `/status` 中出现 5 个 groups
- `/stats` 可返回 5 个 groups 的聚合结果
- 汇总 `total_files` 后得到约 `10 亿`

## 6. 当前边界与注意事项

### 6.1 不应改动的内容

当前已明确不应改动：

- 现有集群挂载结构
- 现有 5 个 NFS 数据目录组织方式

### 6.2 当前最容易误解的点

最容易误解的不是 API 或 deploy，而是：

- `fs-meta` 虽然已经正常起起来
- 但“5 个数据源是否都进入 active app 的 runtime grants”是另一条独立链路

因此：

- 单机产品面可用
- 不等于 5 组完整 demo 已打通

## 7. 推荐阅读顺序

建议接手人按下面顺序阅读：

1. `PRODUCT_DEPLOYMENT.md`
2. `DEPLOYMENT_ENGINEER_MANUAL.md`
3. `DEMO_PRODUCT_GUIDE.md`
4. `DEMO_ENV_STATUS_AND_GAP.md`

其中：

- 本文档负责解释“目标和环境”
- `DEMO_ENV_STATUS_AND_GAP.md` 负责解释“当前做到哪一步、还差什么”

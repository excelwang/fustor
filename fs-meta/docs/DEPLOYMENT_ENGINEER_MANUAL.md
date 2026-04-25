# fs-meta 部署工程师手册

本手册面向部署工程师，目标是帮助你把 `fs-meta` 以分布式方式部署到 demo 或正式环境，并明确：

- 分布式部署架构是什么
- 需要准备哪些输入
- 管理认证信息如何预填充
- 部署后如何把系统带到可用状态

本手册里的“部署节点”指真正承载 `facade-worker` / `source-worker` / `sink-worker` 的运行节点。
如果代码仓库或制品仓库放在独立控制机上，该控制机不自动成为运行节点。

本手册只解释当前产品路径。权威依据始终是 `fs-meta/specs/`，尤其是：

- [L2-ARCHITECTURE.md](../specs/L2-ARCHITECTURE.md)
- [L3-RUNTIME/API_HTTP.md](../specs/L3-RUNTIME/API_HTTP.md)
- [L3-RUNTIME/WORKFLOWS.md](../specs/L3-RUNTIME/WORKFLOWS.md)
- [L1-CONTRACTS.md](../specs/L1-CONTRACTS.md)

## 1. 先讲清楚：fs-meta 的分布式部署架构

### 1.1 产品边界

`fs-meta` 对外始终是一个单一产品边界，不是三个独立产品。内部执行被组织成三类 worker 角色：

- `facade-worker`
- `source-worker`
- `sink-worker`

见 [L2-ARCHITECTURE.md](../specs/L2-ARCHITECTURE.md) 的 `Single fs-meta App Boundary` 与 `WORKER_ROLE_TO_ARTIFACT_MAP`。

### 1.2 当前基线的角色职责

- `facade-worker`
  - 对外暴露 fs-meta 产品 HTTP API
  - 是登录、状态、runtime grants、monitoring roots、rescan、query API key 管理以及查询入口的统一门面
- `source-worker`
  - 消费 runtime grants 和 monitoring roots
  - 负责 baseline scan、listen、audit、manual rescan
  - 负责把文件系统事实转成 fs-meta 的 authoritative truth inputs / observation inputs
- `sink-worker`
  - 持有 in-memory materialized tree
  - 服务 `/tree`、`/stats` 的 materialized 查询
  - 维护 per-group materialization readiness、shadow lag、可靠性等状态

### 1.3 对部署工程师可见的 worker 模式

specs 明确把产品可见执行词汇限制在：

- `embedded`
- `external`

见 [L2-ARCHITECTURE.md](../specs/L2-ARCHITECTURE.md)。

当前基线下：

- `facade-worker` 固定 `embedded`
- `source-worker` 可 `embedded|external`
- `sink-worker` 可 `embedded|external`

如果没有特别要求，demo 环境建议统一使用：

- `facade=embedded`
- `source=embedded`
- `sink=embedded`

原因是启动路径最短、进程数最少、现场风险最低。

### 1.4 正式部署的两段式架构

部署顺序不是“直接写 roots 然后启动 fs-meta”，而是：

1. 先准备并启动 `capanix` 底座
2. 在线公告 NFS 或本地文件系统资源
3. 再部署 `fs-meta`
4. 通过 fs-meta 管理 API 读取 `runtime grants`
5. 再通过 `monitoring roots preview/apply` 决定业务监控范围

这不是实现习惯，而是产品 contract。见 [WORKFLOWS.md](../specs/L3-RUNTIME/WORKFLOWS.md) 的：

- `ProductConfigSplit`
- `ApiRuntimeGrantDiscovery`
- `ApiRootsPreviewAndApply`

### 1.5 对外网络与入口

fs-meta 不要求运维直接维护一批静态监听地址。正式路径只需要在 deploy config 中指定：

- `api.facade_resource_id`

它表示 fs-meta 应该挂接哪个被 runtime 公告的 facade listener 资源。见 [API_HTTP.md](../specs/L3-RUNTIME/API_HTTP.md) 的 `ProductConfigSurfaceSplit`。

### 1.6 本次 5 节点 demo 的实际落点

如果本次环境沿用当前 5 台业务节点，则运行组件全部部署在：

- `10.0.82.144`
- `10.0.82.145`
- `10.0.82.146`
- `10.0.82.147`
- `10.0.82.148`

控制机、代码仓库机、制品分发机即使存在，也不承载 `fs-meta` 运行组件，除非被显式纳入 runtime 部署目标。

## 2. 部署输入与交付物

### 2.1 你需要准备的最小输入

对部署工程师来说，最小输入只有三类：

1. capanix 集群 bootstrap 物料
2. fs-meta 产品 deploy config
3. 运行时资源公告信息

### 2.2 fs-meta deploy config 的最小形态

示例见 [examples/fs-meta.yaml](./examples/fs-meta.yaml)：

```yaml
api:
  facade_resource_id: fs-meta-tcp-listener
auth:
  bootstrap_management:
    username: admin
```

当前产品 config 的原则是“薄 bootstrap 配置”，只允许部署时声明：

- `api.facade_resource_id`
- `auth.bootstrap_management`
- 可选的 worker mode

不应该把下面这些内容写进 deploy config：

- 业务 roots
- runtime grants
- query 参数
- source/sink 内部运行拓扑
- desired-state 文档细节

见 [L1-CONTRACTS.md](../specs/L1-CONTRACTS.md) 的 `PRODUCT_CONFIGURATION_SPLIT`。

### 2.3 deploy 完成后 CLI 会返回什么

`fsmeta deploy` 成功后会返回：

- `app_id`
- `api_facade_resource_id`
- `bootstrap_management.username`
- `bootstrap_management.password`
- `state_dir`

这意味着部署工程师可以把“第一次可登录的管理账号”直接交付给环境负责人，无需再手工进入节点写密码文件。

## 3. 管理认证信息怎么预填充

这一节是最重要的运维细节。

### 3.1 认证体系分两类

fs-meta 有两套认证面：

- 管理会话认证
  - 用于 `/session/login`、`/status`、`/runtime/grants`、`/monitoring/roots`、`/index/rescan`、`/query-api-keys`
- 查询 API key
  - 用于 `/tree`、`/stats`、`/on-demand-force-find`

这两类认证不能混用。见 [API_HTTP.md](../specs/L3-RUNTIME/API_HTTP.md) 的 `ProductAccessBoundary`。

### 3.2 管理登录文件的物化方式

管理登录依赖三个本地文件/参数：

- `passwd_path`
- `shadow_path`
- `management_group`

实现默认值来自 [fs-meta/lib/src/api/config.rs](../lib/src/api/config.rs)：

- `passwd_path=./fs-meta.passwd`
- `shadow_path=./fs-meta.shadow`
- `query_keys_path=./fs-meta.query-keys.json`
- `management_group=fsmeta_management`

### 3.3 预填充方法 A：在 deploy config 里显式给出 bootstrap 管理员

这是正式环境最推荐的方法。

你可以在 `fs-meta.yaml` 中写：

```yaml
api:
  facade_resource_id: fs-meta-tcp-listener
auth:
  bootstrap_management:
    username: admin
    password: <strong-initial-password>
```

行为是：

1. deploy CLI 把这组 bootstrap 凭据编进 release config
2. runtime 在 auth 文件不存在时，自动物化 `passwd` 和 `shadow`
3. 该用户被写入 `management_group`
4. 部署完成后可以立即通过 `POST /session/login` 登录

这套自动物化逻辑在 [fs-meta/lib/src/api/config.rs](../lib/src/api/config.rs) 的 `ApiAuthConfig::ensure_materialized()`。

### 3.4 预填充方法 B：只给用户名，由 CLI 自动生成一次性密码

这是 demo 环境或临时环境更方便的方法。

```yaml
api:
  facade_resource_id: fs-meta-tcp-listener
auth:
  bootstrap_management:
    username: admin
```

行为是：

1. `fsmeta deploy` 发现你没有显式提供密码
2. CLI 自动生成随机密码
3. deploy 输出里直接返回 `bootstrap_management.password`
4. runtime 仍按这组生成后的凭据物化 `passwd/shadow`

这条逻辑在 [fs-meta/tooling/src/bin/fsmeta.rs](../tooling/src/bin/fsmeta.rs) 的 `build_auth_config(...)`。

适用场景：

- demo 环境
- 一次性联调环境
- 现场临时搭建

不适用场景：

- 需要纳入正式密码管理系统的长期环境

### 3.5 预填充方法 C：本地单机模式自动写入工作目录

`fsmeta local start` 会在 workdir 下直接生成：

- `fs-meta.passwd`
- `fs-meta.shadow`
- `credentials.json`

并把登录信息写回：

- `api_base_url`
- `username`
- `password`

这条路径只适用于本地体验和前端联调，不是正式分布式部署方案。

### 3.6 query API key 的预填充策略

query API key 不应该通过 deploy config 预填充。

正确流程是：

1. 先用管理账号登录
2. 调用：
   - `GET /api/fs-meta/v1/query-api-keys`
   - `POST /api/fs-meta/v1/query-api-keys`
3. 把返回的 `api_key` 发给查询侧用户或 demo 前端

理由：

- specs 明确把 query key 生命周期建模为在线管理工作流，而不是 deploy bootstrap 配置的一部分
- query key 只用于 query 面，不能替代管理会话

见 [WORKFLOWS.md](../specs/L3-RUNTIME/WORKFLOWS.md) 的 `QueryApiKeyLifecycle`。

## 4. 标准分布式部署步骤

### 第 1 步：准备 capanix 集群

参考 [PRODUCT_DEPLOYMENT.md](./PRODUCT_DEPLOYMENT.md)：

```bash
cnxctl cluster scaffold --spec fs-meta/fixtures/examples/capanix-cluster.yaml --out ./cluster-out
source ./cluster-out/admin.env
./cluster-out/start-all.sh
```

### 第 2 步：公告资源

示例：

```bash
cnxctl -s ./cluster-out/nodes/capanix-node-1/home/core.sock \
  resource announce \
  --node-id capanix-node-1 \
  --source /absolute/path/to/data
```

对 demo 环境，如果数据来自 NFS 挂载点，这里的 `--source` 就是挂载后的目录。

### 第 3 步：部署 fs-meta

```bash
fsmeta deploy \
  --socket ./cluster-out/nodes/capanix-node-1/home/core.sock \
  --config fs-meta/docs/examples/fs-meta.yaml
```

建议把 deploy 输出完整保存：

```bash
fsmeta deploy ... --output json | tee ./fsmeta-deploy-result.json
```

这样你可以明确拿到：

- `bootstrap_management` 用户名/密码
- `state_dir`

### 第 4 步：登录管理面

```bash
curl -sS -X POST "$API_BASE/api/fs-meta/v1/session/login" \
  -H 'content-type: application/json' \
  -d '{"username":"admin","password":"<password>"}'
```

### 第 5 步：读取 runtime grants

```bash
curl -sS "$API_BASE/api/fs-meta/v1/runtime/grants" \
  -H "authorization: Bearer $TOKEN"
```

### 第 6 步：preview roots

```bash
curl -sS -X POST "$API_BASE/api/fs-meta/v1/monitoring/roots/preview" \
  -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -d @roots.json
```

### 第 7 步：apply roots

```bash
curl -sS -X PUT "$API_BASE/api/fs-meta/v1/monitoring/roots" \
  -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -d @roots.json
```

### 第 8 步：必要时补一次 rescan

```bash
curl -sS -X POST "$API_BASE/api/fs-meta/v1/index/rescan" \
  -H "authorization: Bearer $TOKEN"
```

### 第 9 步：为查询侧签发 query key

```bash
curl -sS -X POST "$API_BASE/api/fs-meta/v1/query-api-keys" \
  -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -d '{"label":"demo-reader"}'
```

### 第 10 步：按 5 个 group 接入 5 个 NFS 数据源

如果这次环境的目标是演示“总共汇聚了 10 亿文件”，推荐直接采用 5 个 logical groups，而不是把 5 个来源强行讲成单树合并。

推荐做法：

- 一个 NFS 数据源对应一个 root
- 一个 root 对应一个 logical group
- selector 只约束到唯一 grant，优先使用 `host_ip + mount_point`
- `subpath_scope=/`

样例文件见：

- [examples/monitoring-roots-5group.json](./examples/monitoring-roots-5group.json)

写入前先 preview：

```bash
curl -sS -X POST "$API_BASE/api/fs-meta/v1/monitoring/roots/preview" \
  -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -d @fs-meta/docs/examples/monitoring-roots-5group.json
```

确认 preview 里 5 条 roots 都命中当前 grants 后，再 apply：

```bash
curl -sS -X PUT "$API_BASE/api/fs-meta/v1/monitoring/roots" \
  -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -d @fs-meta/docs/examples/monitoring-roots-5group.json
```

### 第 11 步：用 `/stats` 汇总展示 10 亿文件

当前 demo 口径建议是：

- 每个 NFS 数据源约 `200,000,000` 文件
- 5 个 groups 合计约 `1,000,000,000` 文件

这里必须区分两个结果：

- **展示口径**：`/stats` 能按 group 汇总出当前观测到的文件数，可用于现场说明“5 个来源在一个产品边界内聚合展示”。
- **验收口径**：只有 `/status` 同时证明 roots/grants 生效、artifact hash 一致、source coverage 明确、sink materialization readiness 达标，才表示真实 full-NFS demo 通过。

full demo 不允许为了大 NFS 加速而关闭文件 metadata audit。metadata coverage 缺失只能作为诊断降级证据，不能作为 demo 通过结果。

这一步不要要求 `/tree` 返回一个超大单树去证明“汇聚”。正式证据应来自：

- `GET /status`
- `GET /stats`

建议先确认每个 group 的：

- `service_state`
- `initial_audit_completed`
- `materialization_readiness`
- `coverage_mode`
- `coverage_capabilities.file_metadata_coverage`

然后再用查询 key 调 `/stats`。样例脚本见：

- [examples/demo-aggregate-10e8.sh](./examples/demo-aggregate-10e8.sh)

## 5. demo 环境推荐拓扑

对于 demo，推荐使用最小稳定拓扑：

- 一个 facade listener 资源
- 小规模 capanix 集群
- `facade=embedded`
- `source=embedded`
- `sink=embedded`
- 如果目标是展示 10 亿文件汇聚，则固定使用 5 个 groups，对应 5 个 NFS 数据源

原因：

- 启动更快
- materialization readiness 更快达到
- `/status`、`/tree`、`/stats`、`/on-demand-force-find` 更稳定
- 更少暴露 external worker 与升级语义

## 6. 部署工程师最常见的交付物

部署工程师至少应向现场或上层交付：

1. `api_base_url`
2. bootstrap 管理员用户名/密码
3. demo 查询用 query API key
4. 当前 roots 文件或 roots 记录
5. demo 数据目录路径
6. 当前使用的二进制版本与 commit
7. `/stats` 的 group 级总量截图或导出结果

## 7. 常见误区

### 误区 1：把 roots 写进 deploy config

不对。roots 属于在线业务范围配置，必须走：

- `runtime grants`
- `monitoring roots preview`
- `monitoring roots apply`

### 误区 2：把 query 用户也做成 management session

不对。query 用户应该使用 query API key，而不是管理会话。

### 误区 3：把 deploy 输出里的 bootstrap 密码丢掉

如果你采用“用户名预填充、密码自动生成”的方式，deploy 输出就是第一次登录凭据的唯一便捷来源。正式环境应立即把它导入密码管理系统。

### 误区 4：把 local start 当成正式部署方式

不对。`fsmeta local start` 只用于单机联调和本地体验。

## 8. 关联文档

- [PRODUCT_DEPLOYMENT.md](./PRODUCT_DEPLOYMENT.md)
- [examples/fs-meta.yaml](./examples/fs-meta.yaml)
- [L2-ARCHITECTURE.md](../specs/L2-ARCHITECTURE.md)
- [API_HTTP.md](../specs/L3-RUNTIME/API_HTTP.md)
- [WORKFLOWS.md](../specs/L3-RUNTIME/WORKFLOWS.md)

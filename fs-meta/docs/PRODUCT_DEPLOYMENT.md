# fs-meta 产品部署指南

关联文档：

- [部署工程师手册](./DEPLOYMENT_ENGINEER_MANUAL.md)
- [Demo 产品说明书](./DEMO_PRODUCT_GUIDE.md)

## 概览

fs-meta 的正式用户路径分成两段：

1. 先部署 `capanix` 底座集群，并在线登记 mount-root 资源导出。
2. 再部署 `fs-meta` 产品，登录 fs-meta 管理面，发现 runtime grants，并配置 `roots`。

正式路径固定为：用户维护 bootstrap config、在线资源公告、fs-meta 产品配置与版本投放；roots 通过 `ui/` 在线选择，不要求手填 selector。

如果存在独立控制机或代码仓库机，它只负责下发命令、保存仓库或保存构建产物；是否成为运行节点，取决于它是否被纳入 runtime 部署目标。
对当前 5 节点 demo，`fs-meta` 运行组件应全部部署在 `10.0.82.144~148`，不应默认落在仓库所在机器。

## 第 1 步：准备 capanix 底座

最小集群规格文件只保留：

```yaml
domain_id: local
nodes:
  - node_id: capanix-node-1
    addr: 127.0.0.1:19401
  - node_id: capanix-node-2
    addr: 127.0.0.1:19402
```

生成完整节点物料：

```bash
cnxctl cluster scaffold --spec fs-meta/fixtures/examples/capanix-cluster.yaml --out ./cluster-out
source ./cluster-out/admin.env
./cluster-out/start-all.sh
```

生成后的每节点 `config.yaml` 仍然存在，但它是派生产物，用于审计和导出，不是正式长期维护入口。

## 第 2 步：在线登记资源导出

集群启动后，再在线登记资源目录，而不是把 `announced_resources` 写进 bootstrap 配置：

```bash
cnxctl -s ./cluster-out/nodes/capanix-node-1/home/core.sock \
  resource announce \
  --node-id capanix-node-1 \
  --source /absolute/path/to/data
```

该命令会自动推导：

1. `resource_kind=fs`
2. `mount_hint=source`
3. `owner_uid` / `owner_gid` / `mode`
4. 稳定的 `resource_id`

查看当前资源目录：

```bash
cnxctl -s ./cluster-out/nodes/capanix-node-1/home/core.sock resource list --output json
```

## 第 3 步：部署 fs-meta

正式部署入口：

```bash
fsmeta deploy --socket ./cluster-out/nodes/capanix-node-1/home/core.sock --config fs-meta/docs/examples/fs-meta.yaml
```

`fs-meta.yaml` 正式配置面默认只需要：

```yaml
api:
  facade_resource_id: fs-meta-tcp-listener
auth:
  bootstrap_management:
    username: admin
```

如需通过新 generation 调整 worker realization，可额外声明：

```yaml
workers:
  source:
    mode: embedded
```

说明：

1. `fsmeta deploy` 默认以 `roots=[]` 启动服务。
2. `fsmeta deploy` 消费共享的 `capanix-config` 配置/manifest/intention 编译语义以及 daemon/runtime ingress，而不是重新发明第二套平台配置语义。
3. `workers.source.mode` / `workers.sink.mode` 属于产品 deploy 配置；`scan` 在当前基线是 `source-worker` 内的 source-side unit，而不是独立 worker role。配置变更会编译成下一代 `__cnx_runtime.workers` bindings，通过正常 generation rollout 生效，而不是在同一运行代内热切换。
4. app target、manifest、desired-state document、runtime subscriptions、source/sink execution wiring、auth 文件路径等内部细节由 CLI 自动生成。
5. deploy 完成后会输出 `api_facade_resource_id`、一次性的 bootstrap 管理员凭据和内部 state 目录。

## 第 4 步：登录并勾选 monitoring roots

登录入口：

```text
POST /api/fs-meta/v1/session/login
```

登录后，先读取 runtime grants：

```text
GET /api/fs-meta/v1/runtime/grants
```

正式产品前端流程固定为：

1. 从 grants 列表勾选资源。
2. 保存前调用 preview，确认命中的 grants 和最终 monitor paths。
3. 保存 roots。
4. 需要时再触发 rescan。

当前正式写接口：

```text
GET  /api/fs-meta/v1/monitoring/roots
POST /api/fs-meta/v1/monitoring/roots/preview
PUT  /api/fs-meta/v1/monitoring/roots
POST /api/fs-meta/v1/index/rescan
```

正式 UI 不再暴露 `mount_point / host_ref / host_ip / fs_source / fs_type / watch / scan / audit_interval_ms` 的手工编辑表单。

### 5-group / 10 亿文件演示示例

如果这次部署的目标是现场展示“总共汇聚了 10 亿文件”，推荐直接采用：

- 5 个 runtime grants
- 5 条 monitoring roots
- 5 个 logical groups

而不是把 5 个来源讲成单一物理树。

样例 roots 文件见：

- [examples/monitoring-roots-5group.json](./examples/monitoring-roots-5group.json)

样例做法是：

1. 每条 root 用 `host_ip + mount_point` 约束到唯一 grant。
2. 每条 root 的 `subpath_scope=/`。
3. apply 完 roots 后，等 `/status` 中每个 group 达到可读状态。
4. 再用查询 key 调 `/stats`，读取 5 个 group 的 `total_files` 并汇总。

样例汇总脚本见：

- [examples/demo-aggregate-10e8.sh](./examples/demo-aggregate-10e8.sh)

这个脚本只生成 `/stats` 展示口径，不代表 full-NFS 验收完成。full demo 验收必须同时检查 `/status` 中的 artifact evidence、roots/grants、source `coverage_mode`/`coverage_capabilities`、sink `materialization_readiness`。如果 metadata audit 被关闭或不可用，结果应标记为 metadata 降级，而不是完整通过。

推荐对外表述：

- fs-meta 把 5 个独立文件源纳入一个统一产品边界
- `/stats` 按 group 给出稳定统计结果
- 当前 5 个 groups 合计约 10 亿文件

## 业务参数分层

在当前基线中，操作员真正配置的是三类东西：

1. **部署期 bootstrap 参数**：`api.facade_resource_id` 与初始 `auth.bootstrap_management`。
2. **在线业务范围参数**：通过 `runtime grants -> monitoring roots preview/apply` 决定监控哪些 mount-root resources。
3. **运行期修复动作**：通过 `POST /api/fs-meta/v1/index/rescan` 触发显式补扫。

以下内容不是正式业务参数面：

1. app target / desired-state 细节。
2. runtime subscriptions / source-sink execution wiring / internal execution identities。
3. 任何仅用于内部 bind/run realization 的声明 carrier 字段。
4. 任何要求用户直接手改 node manifest 或 release desired-state 文档的流程。

## 升级版本

fs-meta 的升级以单 app 边界的 release generation cutover 为主，而不是手工编辑内部 desired-state 文档：

1. 准备新的 fs-meta 二进制发布物。
2. 仍通过同一 `fsmeta deploy` 入口提交新版本。
3. runtime 在同一 app 边界下激活新 generation，但可信对外 exposure 继续停留在上一代 eligible generation，直到新 generation 完成追平。
4. 当前 monitoring roots 与 runtime grants 被重放为新 generation 的 authoritative truth 输入，不要求用户重新录入。
5. 新 generation 的内存 observation/projection state 通过 baseline scan / audit / manual rescan 路径重建；对 active scan-enabled primary groups，只有首次 audit 完成并清除 materialized degraded/overflow 阻断后，才达到 app-owned `observation_eligible`。新 generation 上出现部分 query/readability 并不代表已经具备 materialized `/tree`/`/stats` cutover readiness。
6. 只有在 `observation_eligible` 达成后，可信对外 materialized `/tree` 和 `/stats` 结果才切换到新 generation；`/on-demand-force-find` 作为 freshness path 可以更早可用；对外 `api_base_url` 与产品 API 命名空间保持稳定。

## 本地体验

本地单机体验入口仍然保留：

```bash
fsmeta local start --workdir .fsmeta-local --config fs-meta/docs/examples/fs-meta.yaml
```

该模式用于单机试跑和前端联调，不替代正式的 capanix + fs-meta 两段式部署路径。

## 非正式路径

当前产品路径不再暴露静态监听地址。operator 通过 `api.facade_resource_id` 选择被公告的 `tcp_listener` 资源，并继续使用 deploy / `ui/` / runtime grants 路径。

## 关联样例

- [examples/fs-meta.yaml](./examples/fs-meta.yaml)
- [examples/monitoring-roots-5group.json](./examples/monitoring-roots-5group.json)
- [examples/demo-aggregate-10e8.sh](./examples/demo-aggregate-10e8.sh)

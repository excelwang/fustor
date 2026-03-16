> TEST-ONLY: 本文档服务于 contract tests 与迁移校验，不是 fs-meta 的产品部署手册。

# fs-meta Contract Test Config Notes

## Files

1. `fs-meta-contract-tests.cluster.node-a.config.yaml`: Node-A 基线配置（不声明业务 app）。
2. `fs-meta-contract-tests.cluster.node-b.config.yaml`: Node-B 基线配置（不声明业务 app）。
3. `fs-meta.release-v2.yaml`: 单入口 apps-only 业务期望状态文档。

## Current Shape

1. 业务 app 期望状态通过单入口 desired-state 文档提交，节点 baseline 配置不承载业务 app 期望状态。
2. `source/sink/facade` 作为 fs-meta app 内能力面运行；group-aware bind/run 由 runtime 执行，但分组语义由 fs-meta app 声明。
   - HTTP API 保持在 resource-scoped one-cardinality fs-meta app facade 上；source/sink 可按组多实例调度。
3. 对外业务接口语义是 `query/find`。
4. 当前传输层使用 `find:v1.find` / `on-demand-force-find:v1.on-demand-force-find` 作为 wire routing 名称，不改变接口主语义。
5. 资源目录基线样例包含两节点四导出：`node-a:/mnt/nfs1,/mnt/nfs2` + `node-b:/mnt/nfs1,/mnt/nfs2`。
6. runtime 注入 `host_object_grants`，每个 `mount-root` object 都携带：`object_ref`、`host_ref`、`host_ip`、`mount_point`、`fs_source`、`fs_type` 等描述字段。
7. fs-meta app 配置正向模型使用 group/roots 逻辑声明与 descriptor selectors（含 host descriptor selectors）；`root_path` 与 `source_locator` 为禁用字段。
8. runtime 不注入 `visible_exports`、`concrete_root_id`、`local_member_id` 或任何 export 反查表。
9. runtime 按 app-defined groups 执行 sink/source bind/run realization；release-v2 文档中的内部执行形状仅用于 fixture wiring，runtime 不自动解释组形成原因。

## Bind/Run & Fixture Shape

1. `apps[*].runtime` 下的执行单元目录字段：承载运行单元 bind/run 元数据。
   - 命名基线收敛到 `runtime.exec.*`（例如 `runtime.exec.scan`）。
2. `apps[*].policy`: 仅保留 release generation 输入。
   - `apps[*].policy.generation` 为必填且 `>= 1`（策略声明硬切，无隐式旧版代际模式）。
3. `apps[*].manifest`: port/use/serve 发现入口（fixture: `fs-meta/fixtures/manifests/capanix-app-fs-meta.yaml`）。
4. `apps[*].policy` 下的执行启用列表字段对 fs-meta 主声明为必填非空数组；不允许隐式回退。
5. fs-meta source/sink 对 runtime unit 控制帧执行 `unit_id + generation` 合约校验：未知 unit 拒绝、过期代际信号忽略。
6. source/sink realization 由 runtime 按 `eligibility + cardinality` 执行，但 runtime 不内建 fs-meta 组语义或 primary 规则。

## Policy-Only Rules

1. `schema_version: v5`。
2. 禁止顶层 `routes`。
3. 禁止 `apps[*].bindings`。
4. 配置文件不得暴露内部实现路由。

# fs-meta 测试阶梯运行手册

本文用于让新接手的工程师或智能体按同一套顺序推进测试。权威矩阵见
`fs-meta/docs/TEST_MATRIX.md`。

## 1. 目标

测试阶梯不是“把一批 suite 都跑一遍”，而是逐层证明：

1. 产品语义正确。
2. 单进程闭环正确。
3. 本地 runtime/control 正确。
4. 真实 NFS 环境前提正确。
5. 真实集群上线闭环正确。

高层失败必须下沉到最低可复现层根修，不能在 L5 反复试错。

## 2. 公共入口

从 fustor 仓库根目录运行：

```bash
fs-meta/docs/examples/test-matrix-commands.sh <suite>
```

新阶梯入口：

```bash
fs-meta/docs/examples/test-matrix-commands.sh contracts-fast
fs-meta/docs/examples/test-matrix-commands.sh single-process-closed-loop
fs-meta/docs/examples/test-matrix-commands.sh runtime-local-multinode
fs-meta/docs/examples/test-matrix-commands.sh nfs-environment-gate mini
FSMETA_FULL_NFS_ROOTS_FILE=/path/to/roots.json \
  fs-meta/docs/examples/test-matrix-commands.sh nfs-environment-gate full
FSMETA_FULL_NFS_ROOTS_FILE=/path/to/roots.json \
  fs-meta/docs/examples/test-matrix-commands.sh real-cluster-acceptance
```

旧入口仍可用：`business-fast`、`business-mini-nfs`、
`environment-full-nfs`、`operations-local`、`operations-real-nfs`。

## 3. 阶梯定义

### L1：`contracts-fast`

证明纯语义，不依赖 runtime 或真实 NFS。

必须覆盖：

- formal specs、CLI/API contract。
- `subpath_scope` 只选择宿主子树，物化路径保持根内相对。
- sink 在 root 覆盖范围变化时重置物化状态。
- 管理凭据和 query api key 边界。

### L2：`single-process-closed-loop`

证明 app 内闭环，不引入真实集群变量。

必须覆盖：

- roots apply/rescan。
- source 初始 audit 可以稳定产生事件。
- sink materialization/readiness。
- `/tree`、`/stats`、force-find 的本地语义。

### L3：`runtime-local-multinode`

证明 capanix runtime/control 边界。

必须覆盖：

- stream send/recv direction。
- source/sink owner-scoped status fan-in。
- source/sink worker control replay。
- handoff、route scope、runtime scope。

### L4：`nfs-environment-gate`

证明真实 NFS 环境，不承担运维恢复场景。

子门：

- `mini`：小规模 5-node mini real-NFS，验证业务 API 在真实 NFS 上闭环。
- `full`：完整 5-node / 5-NFS demo roots，验证 grants、roots asset、coverage mode 和 bounded readiness。

规则：

- mini 和 full exports 必须隔离。
- full NFS 数据量不可预测，不能用固定文件数或固定大 timeout 当通过条件。
- full roots asset 通过 `FSMETA_FULL_NFS_ROOTS_FILE` 注入，不提交具体环境。

### L5：`real-cluster-acceptance`

证明真实集群已经可用。默认按 7 段执行：

1. `preflight`
2. `deploy-upgrade`
3. `management-api`
4. `source-audit`
5. `sink-materialization`
6. `query`
7. `resilience`

每段打印：

```text
[fs-meta-test-matrix] l5_stage=x/7
[fs-meta-test-matrix] boundary=<name>
```

旧 24 个 atomic L5 stage 仍可按原 stage 名称调用，用于精确回归，不再作为默认 L5 叙事。

## 4. 失败处理

报告必须包含：

```yaml
current_ladder_position: Lx/<suite>/<stage>
status: PASS | FAIL_NEEDS_LOCALIZATION | FAIL_LOCALIZED | BLOCKED_BY_ENV_PREREQ | NOT_RUN
first_raw_boundary: source | sink | root-authority | group-authority | facade | management-api | query-api | route | runtime | worker-host | host-fs | nfs-env
first_raw_error: <first unretried raw error>
domain_state: <roots/groups/routes/grants/readiness summary>
progress_effect: <does this block current rung or only a diagnostic branch?>
next_validation: <lowest next command>
```

不要写：

- “L5 failed” 但没有 stage/boundary。
- “flaky” 但没有重复频率和第一 raw error。
- “环境问题” 但没有缺失的具体前提。
- “基线问题” 但没有证明当前分支和基线同边界同错误。

## 5. 从 L5 下沉的规则

- 路径语义、root/scope/sink materialization 问题下沉到 L1/L2。
- source 初始 audit、roots apply、query key、status trusted 问题下沉到 L2。
- route direction、owner fan-in、control replay、worker handoff 问题下沉到 L3 或 capanix owner-layer。
- NFS mount、grants、roots asset、data-volume readiness 问题停在 L4。
- deploy/upgrade、真实集群重启、真实 failover 问题才留在 L5。

修复后从最低受影响 rung 重新向上跑，不能只重跑最后失败的 L5 stage。

## 6. 环境信息规则

- 真实 host、密码、query key、token、NFS 本地路径不得提交。
- 使用 env/template 注入真实环境。
- 如果必须记录环境状态，只写在本地工作日志或外部部署记录里。

## 7. 当前状态模板

```yaml
overall_gate: <Lx_PASS | Lx_BLOCKED | ALL_PASS>
L1_contracts_fast: <status>
L2_single_process_closed_loop: <status>
L3_runtime_local_multinode: <status>
L4_nfs_environment_gate:
  mini: <status>
  full: <status>
L5_real_cluster_acceptance:
  preflight: <status>
  deploy_upgrade: <status>
  management_api: <status>
  source_audit: <status>
  sink_materialization: <status>
  query: <status>
  resilience: <status>
next_lowest_validation: <command>
```

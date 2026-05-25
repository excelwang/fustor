# fs-meta 测试任务指导

本文纠正常见误区：L1~L5 不是并列 suite，也不是越高越“更完整”的重复测试。每一级只增加一种风险，并且失败要能定位到具体边界。

## 1. 正式顺序

1. L1 `contracts-fast`
2. L2 `single-process-closed-loop`
3. L3 `runtime-local-multinode`
4. L4 `nfs-environment-gate`
5. L5 `l5`

旧 L1~L4 suite 名称仍可运行，但旧 L5 名称不再保留；报告必须使用新
L1~L5 名称。

## 2. 什么时候不能继续往上报进度

- L1 未过：不能声明业务语义稳定。
- L2 未过：不能进入 runtime/NFS 结论。
- L3 未过：不能把 route/control/fan-in 问题归因给 NFS 或真实集群。
- L4 未过：不能声明真实 NFS 环境有效。
- L5 未过：只能说明真实集群验收未完成，不能反向削弱低层已验证语义。

允许并行做诊断，但总体 gate 只能按正式顺序推进。

## 3. 边界分类

失败报告必须选一个第一 raw boundary：

| Boundary | 含义 | 下沉层 |
| --- | --- | --- |
| `source` | source root task、audit、rescan、event publication | L2 |
| `sink` | materialization、readiness、query projection | L1/L2 |
| `root-authority` | monitoring roots、authoritative generation | L2 |
| `group-authority` | runtime group schedule、sink owner set | L3 |
| `facade` | API facade liveness or drain | L3/L5 |
| `management-api` | login、roots PUT、query-key management | L2/L5 |
| `query-api` | `/tree`、`/stats`、force-find data-plane | L2/L5 |
| `route` | send/recv direction、route publication、owner-scoped lane | L3/capanix |
| `runtime` | activation、upgrade generation、control loop | L3/capanix |
| `worker-host` | external worker launch/artifact/binary | L3/capanix |
| `host-fs` | mount-root facade、path identity、watch backend | L1/capanix |
| `nfs-env` | real mount、exports、permissions、roots asset | L4 |

## 4. 必须前移的部署问题

这次真实部署暴露的问题必须在低层防回归：

- `subpath_scope`：L1 必须证明它只选择宿主子树，不成为物化路径前缀。
- source 初始 audit：L2 必须证明 root task 不依赖短生命周期 runtime 才能醒来。
- sink materialization：L2 必须证明 roots → source → sink → trusted/query 闭环。
- route direction/fan-in：L3 必须证明 owner-scoped source/sink status 可被 fan-in 获取。
- 数据量不可预测：L4/L5 禁止硬编码文件数预算或固定耗时预算。

## 5. 报告格式

```yaml
current_ladder_position: Lx/<suite>/<stage>
status: PASS | FAIL_NEEDS_LOCALIZATION | FAIL_LOCALIZED | BLOCKED_BY_ENV_PREREQ | NOT_RUN
first_raw_boundary: <boundary>
first_raw_error: <raw error text>
domain_state:
  roots: <summary>
  grants: <summary>
  groups: <summary>
  routes: <summary>
progress_effect: <blocks current rung / diagnostic only / unblocks next rung>
next_validation: <exact command>
```

L5 还必须写：

```yaml
l5_stage: x/12
l5_phase: acceptance | ops
boundary: preflight | deploy-upgrade | management-api | source-audit | sink-materialization | query | resilience | foundation-real-runtime | upgrade-core | topology-change | recovery-switch | resource-budget
```

## 6. 禁止写法

- “跳过 L2/L3，因为需要真实 NFS”。
  - 应写：`BLOCKED_BY_ENV_PREREQ`，并列出缺失的 env、mount、roots asset 或 worker-host。
- “L5-5 失败”。
  - 应写：`l5_stage`、`boundary`、第一 raw error、当前卡住的领域对象。
- “基线问题”。
  - 应写：基线 commit、当前 commit、同一 raw boundary、同一 raw error。
- “flaky”。
  - 应写：重复次数、失败比例、第一 raw error、是否阻塞当前 rung。

## 7. 修复原则

- 先补 owner-layer 红测，再修实现。
- 能下沉到 capanix 的通用能力必须下沉。
- app 层保持领域词汇：root、group、source、sink、facade、query，不引入晦涩技术绕法。
- 不允许 workaround：跳断言、放宽语义、固定 sleep、假 ready、demo-only branch 都不接受。
- specs 不完整或错误时，先修 specs，再修测试和实现。

## 8. 推荐执行流

```bash
fs-meta/docs/examples/test-matrix-commands.sh contracts-fast
fs-meta/docs/examples/test-matrix-commands.sh single-process-closed-loop
fs-meta/docs/examples/test-matrix-commands.sh runtime-local-multinode
fs-meta/docs/examples/test-matrix-commands.sh nfs-environment-gate mini
FSMETA_FULL_NFS_ROOTS_FILE=/path/to/roots.json \
  fs-meta/docs/examples/test-matrix-commands.sh nfs-environment-gate full
FSMETA_FULL_NFS_ROOTS_FILE=/path/to/roots.json \
  fs-meta/docs/examples/test-matrix-commands.sh l5
```

如果任一步失败，停止向上给通过结论，先定位第一 raw boundary。

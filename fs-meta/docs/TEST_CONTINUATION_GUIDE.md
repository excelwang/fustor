# fs-meta 测试继续推进说明书

本文档面向一个全新的智能体，用于快速接手 `fs-meta` 阶梯测试推进工作。它说明整体测试方案、当前真实进度、已知卡点、下一步计划和修复原则。

## 1. 接手目标

目标不是“把某个测试跑绿”，而是把 `fs-meta` 的阶梯测试稳定推进到全部通过：

1. 先证明本地业务语义正确。
2. 再证明小规模 real-NFS 环境正确。
3. 再证明完整 5 节点 / 5 NFS demo 环境正确。
4. 再证明本地运行控制、重启、replay、handoff 正确。
5. 最后证明完整 real-NFS 运维场景，包括升级、拓扑变化、故障切换、激活范围和资源预算。

任何失败都必须根修。不能通过跳过断言、放宽语义、加 blind sleep、硬编码 demo 环境、伪造 readiness、缓存过期成功、或把通用运行时问题藏进 fustor app 层来通过测试。

## 2. 当前仓库状态

当前 `master` 已包含：

- `fs-meta` 逻辑修正。
- L1~L5 测试矩阵重组。
- L5 内部阶段外显，特别是原先粗粒度的 L5-5 已拆成 7 个 diagnostic substages。
- demo 文档和 demo 二进制资产。
- demo 环境设置已改为本地环境变量和模板注入，仓库内不应包含具体 demo host 地址。

当前已移除本地 `p0-demo-validation-assets` 分支。若远端仍存在同名 tracking branch，不代表本地还应继续使用 p0。后续测试推进应基于 `master`。

完整 real-NFS 测试需要本地未提交的 demo roots JSON。生成方式：

```bash
cp fs-meta/docs/examples/demo-env.example.sh /tmp/fsmeta-demo.env
$EDITOR /tmp/fsmeta-demo.env
source /tmp/fsmeta-demo.env

fs-meta/docs/examples/render-monitoring-roots-5group.sh \
  fs-meta/docs/examples/monitoring-roots-5group.template.json \
  "$FSMETA_DEMO_ROOTS_FILE"

export FSMETA_FULL_NFS_ROOTS_FILE="$FSMETA_DEMO_ROOTS_FILE"
```

具体 demo 地址、用户名、mount point、运行目录、控制 socket 都只应进入本地 env 文件，不应提交到仓库。

## 3. 测试阶梯

权威矩阵见 `fs-meta/docs/TEST_MATRIX.md`。公共入口统一使用：

```bash
fs-meta/docs/examples/test-matrix-commands.sh <suite>
```

### L1：business-fast

目的：证明本地合同、API、查询、force-find、status、roots apply/rescan 语义正确。

命令：

```bash
fs-meta/docs/examples/test-matrix-commands.sh business-fast
```

### L2：business-mini-nfs

目的：证明同一业务/API 语义可以在小规模 5-node mini real-NFS 上闭环。mini NFS 只能使用独立小 exports，不能复用或修改 full demo 数据。

命令：

```bash
fs-meta/docs/examples/test-matrix-commands.sh business-mini-nfs
```

### L3：environment-full-nfs

目的：证明完整 5 节点 / 5 NFS demo 环境的 grants、roots、bounded readiness、coverage mode、degraded evidence 正确。不要把 full data 的完整 source audit 当成同步阻塞条件。

命令：

```bash
export FSMETA_FULL_NFS_ROOTS_FILE=/path/to/local/rendered-roots.json
fs-meta/docs/examples/test-matrix-commands.sh environment-full-nfs
```

### L4：operations-local

目的：在本地证明 source/sink worker control、control replay、handoff、runtime scope 行为正确。L5 失败如果落在 source/sink/facade/runtime 控制边界，应先回到 L4 或 owner-layer 单测修复。

命令：

```bash
fs-meta/docs/examples/test-matrix-commands.sh operations-local
```

### L5：operations-real-nfs

目的：在完整 real-NFS 环境证明运维场景。完整 L5 共 24 个外部 gate，按下面五组执行：

```bash
fs-meta/docs/examples/test-matrix-commands.sh operations-real-nfs foundation-real-runtime
fs-meta/docs/examples/test-matrix-commands.sh operations-real-nfs upgrade-core
fs-meta/docs/examples/test-matrix-commands.sh operations-real-nfs topology-change
fs-meta/docs/examples/test-matrix-commands.sh operations-real-nfs recovery-switch
fs-meta/docs/examples/test-matrix-commands.sh operations-real-nfs resource-budget
```

也可以运行单个 atomic stage，例如：

```bash
fs-meta/docs/examples/test-matrix-commands.sh operations-real-nfs upgrade-cpu-budget
```

## 4. L5 内部顺序

L5 必须按“基础路径先于长尾运维”的顺序推进：

1. `foundation-real-runtime`
   - `runtime-selected-group-proxy`
   - `runtime-source-worker-manual-rescan`
   - `ops-force-find-smoke`
   - `ops-force-find-semantics`
   - `ops-visibility-sink-selection`
2. `upgrade-core`
   - `upgrade-apply-generation-two`
   - `upgrade-generation-two-http`
   - `upgrade-peer-source-control`
   - `upgrade-sink-scope`
   - `upgrade-runtime-scope`
   - `upgrade-roots-persist`
   - `upgrade-tree-stats`
   - `upgrade-tree-materialization`
3. `topology-change`
   - `ops-new-nfs-join`
   - `ops-root-path-modify`
   - `ops-nfs-retire`
   - `upgrade-window-join`
4. `recovery-switch`
   - `ops-sink-failover`
   - `ops-facade-resource-switch`
   - `upgrade-facade-continuity`
   - `ops-activation-preserved`
   - `ops-activation-visibility`
   - `ops-activation-force-find`
5. `resource-budget`
   - `upgrade-cpu-budget`

### L5-5 diagnostic substages

原先粗粒度的 `ops-visibility-sink-selection` 已外显为 7 个 diagnostic substages。它们用于定位失败边界，不改变最终产品 gate：

1. `ops-visibility-roots-narrowed`
2. `ops-visibility-source-delivery-ready`
3. `ops-visibility-manual-rescan-accepted`
4. `ops-visibility-grants-visible`
5. `ops-visibility-withdraw-converged`
6. `ops-visibility-sink-holder-moved`
7. `ops-visibility-facade-live`

这些 substage 会打印 `l5_subprogress=x/7` 和有效 L5 进度，便于观察失败卡在 roots、source delivery、manual rescan、grants、withdraw、sink holder 还是 facade liveness。

## 5. 当前真实进度

截至本文档编写时，当前可确认的进度是：

- 测试矩阵已重组为 L1~L5，执行顺序已明确。
- L5 已拆成 24 个 atomic gates，L5-5 已拆成 7 个 diagnostic substages。
- demo 环境相关配置已改成 env/template 注入，主分支不应再提交具体 demo host 地址。
- 最近一次完整复跑已经证明 L1~L5 全部通过，且 `operations-real-nfs` 按 `1/24..24/24` 顺序闭环。
- 当前没有 active blocker；后续主要工作是防回归和增量变更后的再验证。

当前真实状态应按下面方式表述：

> L1~L5 的测试组织、L5 进度外显和完整矩阵闭环都已完成；当前没有待解的阶梯测试卡点，后续只需要在变更后重新跑对应层级。

## 6. 当前已知卡点

当前没有已知阻塞性卡点。

如果后续出现新失败，先记录 first raw boundary，再决定是 fustor 还是 capanix 侧根修；不要直接加超时、sleep、阈值放宽或测试绕过。

## 7. 下一步计划

### Step 1：准备环境

1. 确认 `master` clean。
2. 准备 capanix worker host：

```bash
export CAPANIX_WORKER_HOST_BINARY=/absolute/path/to/capanix_worker_host
```

如果未显式设置，矩阵脚本会优先尝试解析 sibling `../capanix/target/debug/capanix_worker_host`。如二进制不存在，先在 capanix 仓库构建。不要把 capanix 通用能力问题绕到 fustor app 层。

3. 准备 full demo roots：

```bash
source /tmp/fsmeta-demo.env
fs-meta/docs/examples/render-monitoring-roots-5group.sh \
  fs-meta/docs/examples/monitoring-roots-5group.template.json \
  "$FSMETA_DEMO_ROOTS_FILE"
export FSMETA_FULL_NFS_ROOTS_FILE="$FSMETA_DEMO_ROOTS_FILE"
```

### Step 2：先做快速基线

先跑便宜且能暴露明显回归的阶段：

```bash
fs-meta/docs/examples/test-matrix-commands.sh business-fast
fs-meta/docs/examples/test-matrix-commands.sh operations-local
```

如果这里失败，不要继续 L5。先按 owner-layer 修复。

### Step 3：确认完整环境

```bash
fs-meta/docs/examples/test-matrix-commands.sh environment-full-nfs
```

如果 L3 失败，优先检查：

- `FSMETA_FULL_NFS_ROOTS_FILE` 是否存在且包含至少 5 个 roots；
- roots 中 mount point 是否当前机器可见、可读、且是 active mount；
- runtime grants 是否覆盖预期 host/mount；
- source/sink status 是否提供 degraded evidence，而不是等待完整大数据 audit。

### Step 4：按层级复测

在变更后先按影响面重新跑对应层级：

```bash
fs-meta/docs/examples/test-matrix-commands.sh business-fast
fs-meta/docs/examples/test-matrix-commands.sh business-mini-nfs
fs-meta/docs/examples/test-matrix-commands.sh environment-full-nfs
fs-meta/docs/examples/test-matrix-commands.sh operations-local
fs-meta/docs/examples/test-matrix-commands.sh operations-real-nfs
```

如果某层失败，再按该层内部顺序定位，不要跳过前置层去猜后面的原因。

```bash
fs-meta/docs/examples/test-matrix-commands.sh operations-real-nfs foundation-real-runtime
fs-meta/docs/examples/test-matrix-commands.sh operations-real-nfs upgrade-core
fs-meta/docs/examples/test-matrix-commands.sh operations-real-nfs topology-change
fs-meta/docs/examples/test-matrix-commands.sh operations-real-nfs recovery-switch
fs-meta/docs/examples/test-matrix-commands.sh operations-real-nfs resource-budget
```

如果失败，记录第一 raw failure，不要直接重复全量 L5。

### Step 5：失败后回退到最小边界

若 `upgrade-cpu-budget` 仍失败在 manual rescan scoped source delivery：

1. 跑 L5 stage 2：

```bash
fs-meta/docs/examples/test-matrix-commands.sh operations-real-nfs runtime-source-worker-manual-rescan
```

2. 跑 L4 source control / sink control 相关 owner-layer 测试。
3. 若 source worker local 正常但 real runtime delivery 超时，把边界指向 capanix worker-host/route delivery。
4. 若 source acceptance 本身异常，在 fustor source owner 层补 red test 后修复。

## 8. 数据量与资源预算原则

full real-NFS 环境的数据量可能非常大，且不同环境中文件数量、目录层级、NFS 延迟、缓存状态、host CPU、IO 状态都不可预测。因此：

- 不允许用固定文件数完成作为 full demo readiness 阻塞条件。
- 不允许用固定时长 sleep 证明 source audit 完成。
- 不允许把 CPU budget 写成与某个 demo 数据量强绑定的硬编码预算。
- 必须使用 bounded readiness、coverage mode、degraded evidence、明确的 observation evidence。
- `source audit` 可以作为后台收敛事实，但不能让完整大数据扫描成为所有业务/API 测试的同步前置条件。
- CPU budget 测试应验证“升级/收敛/轻载采样期间没有持续失控”，而不是验证“在任意数据量下完整扫描必须在固定时间内结束”；短暂 tail spike 只能作为诊断证据，不能单独作为失败门槛。

如果测试目标本身把不可预测的数据量当成硬条件，应先修订 specs/test matrix，再改测试或实现。

## 9. 修复归属规则

每个失败必须先定位第一 raw boundary，再决定修复仓库：

| 第一失败边界 | 首选归属 |
| --- | --- |
| fs-meta root/group/source/sink/facade/domain state | fustor |
| management roots API、query API、status semantics | fustor |
| source/sink retained control replay 的领域状态 | fustor |
| generic worker-host process lifecycle | capanix |
| generic route delivery / retry / cancel / scoped transport | capanix |
| generic artifact loading / host filesystem / runtime channel capacity | capanix |
| test helper/oracle 自身错误 | fustor test helper owner |

如果能力可复用到其他 stateful app，应下沉到 capanix；不要在 fustor 中写 demo 专用绕路。

## 10. 工作日志格式

每次非平凡失败或修复后，都写一条短日志。建议格式：

```text
date:
branch/head:
suite:
stage:
l5_progress:
environment:
command:
log_file:
first_raw_error:
observed_domain_state:
first_boundary:
owner_repo:
spec_used_or_changed:
fix_summary:
validation:
next_step:
```

要求：

- L5 必须写 `l5_progress`；若是 L5-5 substage，还要写 `l5_subprogress`。
- `first_raw_error` 必须是最早的失败，不是最终 timeout 包装。
- `observed_domain_state` 用产品领域词：root、group、source、sink、facade、management write、observation evidence、runtime route。
- `next_step` 必须是最小复测命令，不要默认全量重跑。

## 11. 执行注意事项

- 保持 shell 进程数量低。不要同时开很多长跑测试；复用命令或等当前命令结束。
- full real-NFS 和 L5 都必须 `--ignored` 且 `CAPANIX_REAL_NFS_E2E=1`；使用 matrix 脚本可避免漏参。
- 如果 L3/L5 结束得异常快，先确认测试没有被过滤空跑。
- 保存失败日志到 `fs-meta/.tmp/`，文件名包含 suite、stage、日期和 red/green。
- 修复后先跑最低受影响 rung，再向上爬，不要直接声明全阶梯稳定。

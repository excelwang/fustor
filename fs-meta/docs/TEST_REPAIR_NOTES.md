# fs-meta 测试修订注意事项

本文用于说明当前 `fustor` worktree 后续修订需要遵守的边界。

测试任务理解和报告格式详见 `fs-meta/docs/TEST_TASK_GUIDANCE.md`。L2/L3 运行步骤详见 `fs-meta/docs/TEST_LADDER_RUNBOOK.md`。后续 L1-L5 报告必须按该文档区分 `PASS`、`FAIL_NEEDS_LOCALIZATION`、`FAIL_LOCALIZED`、`BLOCKED_BY_ENV_PREREQ`、`NOT_RUN`，不能用“基线问题”“flaky”“环境问题”替代第一失败边界定位。

## 1. 提交边界

不要提交本机环境文件：

- `.claude/settings.local.json`
- `fsmeta-demo.env`
- `fsmeta-demo-monitoring-roots-5group.json`

这些文件只属于本机测试环境。真实 demo 地址、NFS 挂载点、SSH 用户、运行目录必须通过本机 env 文件、`/tmp` 资产或 CI secret 注入，不能作为仓库代码提交。

提交前必须检查：

```bash
git status --short
git diff --cached --name-only
git grep -n '10\.0\.82'
```

如果 `git diff --cached --name-only` 里出现上述本机文件，先取消暂存再提交。

## 2. sink 状态语义

`scheduled_groups_by_node` 和 `groups` 不是同一种事实：

- `scheduled_groups_by_node` 表示“这个节点被调度到哪些 group”，用于诊断和控制面可见性。
- `groups` 表示“sink 已经拿到可见状态行”，用于 readiness、materialization 和用户可见状态。

因此不能只因为调度缓存里有 group，就伪造 `PendingMaterialization` 的 visible group row。没有真实 row 证据时，只能保留调度事实，不能把它升级成可见状态事实。

后续如果测试需要 visible group row，测试输入必须提供真实的 row 证据；如果只有调度缓存，测试应断言不会凭空生成 row。

## 3. 测试环境边界

`CAPANIX_WORKER_HOST_BINARY` 不要写死到 `/root/repo/...`。测试脚本当前规则是：

1. 如果显式设置 `CAPANIX_WORKER_HOST_BINARY`，必须是绝对路径并且可执行。
2. 如果未设置，优先解析当前 `fustor` worktree 的 sibling `../capanix/target/debug/capanix_worker_host`。
3. 最后才从 `PATH` 查找。

这可以避免当前用户因为无法访问 `/root` 而把代码问题误判为环境权限问题。

## 4. 测试报告要求

报告失败时不要只写“基线失败”或“环境问题”。必须写清楚：

- 测试层级和 suite。
- 具体失败测试名。
- 第一条原始失败信息。
- 归因：代码回归、规格/测试目标问题、环境前提缺失，三者只能选证据最强的一类。
- 当前修复动作和下一次验证命令。

worker-host 路径修正后，原先归类为权限问题的 L1/L4 失败必须重跑再归因。

## 5. 修复原则

遇到失败时只做根修：

- 不加 blind sleep。
- 不跳过断言。
- 不把真实 demo 地址写进代码。
- 不在 fustor app 层隐藏 capanix 通用运行时问题。
- 不用“让测试先绿”的临时逻辑替代领域语义。

如果发现测试目标不合理，先修订 specs/test matrix 并记录理由，再改测试或实现。

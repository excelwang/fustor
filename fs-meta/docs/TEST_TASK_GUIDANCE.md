# fs-meta 测试任务理解与报告规范

本文用于纠正当前 L1-L5 测试推进中的误区。测试目标不是“统计哪些 suite 绿了”，而是证明每一级 gate 的产品语义、运行时前提和环境前提都被清楚验证。任何红项都必须进入可复现、可定位、可修复的状态。

L2/L3 的具体执行步骤见 `fs-meta/docs/TEST_LADDER_RUNBOOK.md`。遇到 L2/L3 不要再写“缺少信息”或“跳过”，先运行对应 preflight。

## 1. 正确的任务目标

L1-L5 的任务不是并列跑一批测试，而是按阶梯建立信心：

1. L1 先证明本地业务语义正确。
2. L2 再证明最小 real-NFS 环境正确。
3. L3 再证明完整 real-NFS 数据面正确。
4. L4 再证明本地运维控制、replay、handoff 正确。
5. L5 最后证明完整 real-NFS 运维场景正确。

下一级不能替代上一级。某个辅助 suite 通过，也不能抵消同级核心 suite 的失败。

## 2. 报告状态只能使用五类

每个 suite 只能归入以下状态之一：

- `PASS`：完整执行，零失败，ignored 数量符合预期。
- `FAIL_NEEDS_LOCALIZATION`：已失败，但还没有定位第一 raw boundary。这是临时状态，不能作为关闭结论。
- `FAIL_LOCALIZED`：失败已定位到第一 raw boundary，并明确 owner repo。
- `BLOCKED_BY_ENV_PREREQ`：环境前提缺失，已经给出证据和解除步骤。
- `NOT_RUN`：还没有运行或运行条件没有打开。

不要使用这些模糊归因作为最终状态：

- “基线预存问题”
- “时序敏感 flaky”
- “环境权限问题”
- “同上”
- “33+~38 passed”

这些只能作为临时观察，不能作为测试结论。

## 3. 顺序执行规则

L1-L5 有两条线，不能混在一起：

- **正式 gate 线**：只能按 `L1 -> L2 -> L3 -> L4 -> L5` 给出最终通过结论。
- **诊断线**：在 L1 已经通过、real-NFS 环境暂时不可用时，可以继续分析 L4 本地失败，但不能把总体进度写成已经越过 L2/L3。

因此，L2/L3 没有运行不是“缺少信息”，而是明确状态：

| 层级 | 当前没有 real-NFS 环境时的状态 | 应写的下一步 |
| --- | --- | --- |
| L2 | `BLOCKED_BY_ENV_PREREQ` | 准备 mini real-NFS 前提，然后运行 `business-mini-nfs` |
| L3 | `NOT_RUN` | 等 L2 通过，再准备 full roots asset 并运行 `environment-full-nfs` |
| L4 | 可以继续诊断，但不能作为总体 gate 通过结论 | 只处理本地 operations 失败 |
| L5 | `NOT_RUN` | 等 L2、L3、L4 都关闭后再运行 |

L2 和 L3 不是同一个环境门：

- L2 `business-mini-nfs` 验证小规模 real-NFS 上的业务语义，不要求 full 5-NFS roots asset。
- L3 `environment-full-nfs` 验证完整 5 节点 / 5 NFS demo 环境，必须有 `FSMETA_FULL_NFS_ROOTS_FILE`。

顺序执行命令以 `fs-meta/docs/examples/test-matrix-commands.sh` 为准：

```bash
bash fs-meta/docs/examples/test-matrix-commands.sh business-fast
bash fs-meta/docs/examples/test-matrix-commands.sh business-mini-nfs-preflight
bash fs-meta/docs/examples/test-matrix-commands.sh business-mini-nfs
FSMETA_FULL_NFS_ROOTS_FILE=/path/to/local-roots.json bash fs-meta/docs/examples/test-matrix-commands.sh environment-full-nfs-preflight
FSMETA_FULL_NFS_ROOTS_FILE=/path/to/local-roots.json bash fs-meta/docs/examples/test-matrix-commands.sh environment-full-nfs
bash fs-meta/docs/examples/test-matrix-commands.sh operations-local
CAPANIX_REAL_NFS_E2E=1 FSMETA_FULL_NFS_ROOTS_FILE=/path/to/local-roots.json bash fs-meta/docs/examples/test-matrix-commands.sh operations-real-nfs
```

如果没有 real-NFS 环境，报告应该写：

```text
overall_gate: L1_PASS_L2_BLOCKED
diagnostic_track: L4_LOCAL_FAILURE_ANALYSIS_IN_PROGRESS
L2: BLOCKED_BY_ENV_PREREQ
L3: NOT_RUN
L5: NOT_RUN
```

不要写“缺少 L2/L3 信息”。信息在 test matrix 里；缺的是环境前提或执行结果。

## 4. 对当前报告的具体纠偏

### L1 contracts / selected_group

当前写法是：

> 环境权限问题 — `/tmp/fs-meta-control-frame-leases/` 下锁文件被 root 锁定，Permission denied

这还不能算完整归因。必须补齐：

- 哪两个具体测试失败。
- 第一条原始错误路径和 errno。
- `id` 输出。
- `/tmp/fs-meta-control-frame-leases` 的 owner/mode。
- 失败文件的 owner/mode。
- 清理或隔离后是否复现。

如果只是 root 以前跑测试留下了 stale lock，这是一次本机清理问题；清理后必须重跑。  
如果测试或代码固定使用全局共享 `/tmp/fs-meta-control-frame-leases`，导致不同用户互相污染，那是可根修的问题：测试 lease 目录应按用户、worktree 或 test-run 隔离，不能靠手动删文件长期维持。

禁止用以下方式“修复”：

- 在测试里调用 `sudo`。
- 把目录改成全局可写来掩盖冲突。
- 跳过失败测试。
- 把 Permission denied 直接记成通过外的已知问题。

### L2 / L3

当前写法是：

> 跳过 — 需要真实 NFS 环境 + `CAPANIX_REAL_NFS_E2E=1`

这应该拆开写，不是合并成一个“跳过完成”：

- L2：`BLOCKED_BY_ENV_PREREQ`，需要 `CAPANIX_REAL_NFS_E2E=1`、mini NFS 环境可用、worker-host 可执行。
- L3：`NOT_RUN`，依赖 L2 通过，并额外需要 full roots asset、5 个 active NFS mount、full/demo exports 可读。

只有这些检查完成后，才能进入 L2 mini real-NFS。L2 通过前不要把 L3/L5 写成“已验证”。

### L4 status_snapshot_nonblocking

当前写法是：

> 2 个一致失败为基线预存问题，其余为时序敏感 flaky

这不是合格测试报告。必须把每个失败拆成独立条目：

- 测试名。
- 命令。
- 日志文件。
- 第一条 raw error。
- 观察到的领域状态：root、group、source、sink、facade、route、control frame。
- 第一失败边界。
- owner repo。
- 下一步最小复测命令。

“flaky”不是根因。只有同一个测试重复运行后，能证明失败边界随时间变化，才可以临时标记为 flaky；即便如此，也要继续定位为什么测试允许非确定性进入断言。

如果某个测试单跑通过、整 suite 中 `5/5` 失败，这不是 flaky，也不是“基线已知即可关闭”。它说明存在 suite 顺序依赖或共享状态污染，状态应写为：

```text
status: FAIL_NEEDS_LOCALIZATION
observed_pattern: single-test PASS, module-suite FAIL 5/5
suspected_boundary: test isolation / shared runtime state / retained control hook / temp path / worker-host global state
next_action: bisect preceding tests and locate leaked state
```

必须继续做顺序依赖定位：

1. 保存整 suite 失败日志，摘出真实 assertion diff 或 panic，不要写“单跑通过，suite 失败”作为 raw error。
2. 用 `--test-threads=1` 保持顺序稳定。
3. 列出该 module 下所有测试名，记录失败测试前面运行过哪些测试。
4. 通过缩小运行范围或临时只运行相邻测试，定位最小污染者。
5. 第一边界通常是 fustor 测试 fixture 没有清理全局 hook、lease 目录、runtime control state、cached snapshot、route binding；只有证据指向 worker-host 通用行为时才归到 capanix。

### republish_scheduled_groups

这 4 个测试通过是有效进展，但它只证明一个局部语义闭环：

- `scheduled_groups_by_node` 是调度事实。
- `groups` 是可见状态事实。
- 不能从纯调度缓存伪造 visible group row。

它不能替代 L4 `status_snapshot_nonblocking` 的失败归因，也不能说明 L4 已稳定。

### 当前最新报告应该重写成这样

```text
overall_gate: L1_PASS_L2_BLOCKED

L1 business-fast:
  status: PASS
  note: contracts / selected_group / force_find 已通过；lease 目录 UID 隔离修复需要附命令和日志。

L2 business-mini-nfs:
  status: BLOCKED_BY_ENV_PREREQ
  missing: mini real-NFS env not confirmed, CAPANIX_REAL_NFS_E2E not enabled
  next_validation: bash fs-meta/docs/examples/test-matrix-commands.sh business-mini-nfs-preflight

L3 environment-full-nfs:
  status: NOT_RUN
  blocked_by: L2 not passed
  additional_prereq: FSMETA_FULL_NFS_ROOTS_FILE and 5 active readable NFS mounts

L4 operations-local / status_snapshot_nonblocking:
  status: FAIL_NEEDS_LOCALIZATION
  evidence: target tests pass alone but fail 5/5 in full module
  not_accepted_as: baseline closed, flaky closed, non-code regression
  next_validation: isolate suite-order contaminator and first raw assertion

L5 operations-real-nfs:
  status: NOT_RUN
  blocked_by: L2, L3, and L4 not closed
```

## 5. 下一轮执行顺序

下一轮不要继续扩大范围，先把已有红项变成可定位状态。

### Step 1: 清掉 L1 环境噪声

先确认 `/tmp/fs-meta-control-frame-leases` 是否只是 stale root-owned 文件。

记录以下信息：

```bash
id
ls -ld /tmp/fs-meta-control-frame-leases
find /tmp/fs-meta-control-frame-leases -maxdepth 1 -mindepth 1 -ls
```

如果确认是 stale 文件，手动清理或换成用户隔离目录后，重新跑 L1 contracts 和 selected_group。报告里必须写清理前后结果。

### Step 2: 定位 L4 suite 顺序依赖

当前两个测试单跑通过、整 suite 失败，不能再只跑失败测试。要找污染者：

```bash
cargo test -p fs-meta-runtime --lib workers::sink::tests::status_snapshot_nonblocking -- --list
cargo test -p fs-meta-runtime --lib workers::sink::tests::status_snapshot_nonblocking -- --nocapture --test-threads=1
```

然后按失败测试之前的测试顺序缩小范围。目标不是证明“单跑通过”，而是找出“哪个前置测试或共享 fixture 让它在 suite 中失败”。

### Step 3: 把 flaky 变成清单

不要写“其余 flaky”。必须列成表：

| test | first failing run | first raw error | repeated? | boundary | next action |
| --- | --- | --- | --- | --- | --- |

没有清单，就等于没有完成分析。

### Step 4: 再推进 L2

L1 真实通过、L4 核心失败完成归因后，再打开 real-NFS。先 L2 mini，不要直接上 full 5-NFS。

如果 real-NFS 环境已经准备好，也可以先跑 L2；但报告必须分两条线写：

- formal gate：L1 -> L2 -> L3 -> L4 -> L5。
- local diagnostic：L4 suite 顺序依赖继续定位。

不要让 L2/L3 的环境缺失成为停止 L4 本地定位的理由，也不要让 L4 的局部测试通过替代 L2/L3 的正式 gate。

## 6. 合格报告模板

每个 suite 用这个格式，不要用模糊摘要：

```text
level:
suite:
status: PASS | FAIL_NEEDS_LOCALIZATION | FAIL_LOCALIZED | BLOCKED_BY_ENV_PREREQ | NOT_RUN
command:
exact_result:
ignored_count:
log_file:
failure_inventory:
  - test:
    first_raw_error:
    observed_domain_state:
    first_boundary:
    owner_repo:
    fix_or_env_action:
    next_validation:
progress_effect:
next_suite:
```

`progress_effect` 必须说明这次结果对阶梯进度的影响，例如：

- “L1 仍阻塞，不能进入 L2 结论。”
- “L4 局部语义已修复，但 status_snapshot_nonblocking 仍阻塞 L4。”
- “L2 env prerequisites 未满足，状态是 NOT_RUN。”

## 7. 判断是否真正理解任务

如果报告里出现下面情况，说明还没有按测试任务推进：

- 用 pass 数量掩盖 fail 项。
- 把 ignored 当成无影响。
- 把未运行写成跳过完成。
- 把 Permission denied 直接归为环境问题，但没有 owner/mode/清理后复测。
- 把“基线问题”当作关闭状态。
- 把“flaky”当作根因。
- 没有给每个失败写第一 raw boundary。
- 没有说明失败属于 fustor、capanix、测试 helper 还是环境前提。

测试推进的最小完成单位不是一个表格，而是一个失败从发现到定位、修复、最小复测通过的闭环。

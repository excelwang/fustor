# Fustor 性能基准测试工具 (Benchmark)

该模块是 Fustor 平台的自动化压力测试和性能量化工具。它通过模拟大规模文件系统元数据，量化 Fusion API 相比于操作系统原生文件系统调用的性能优势。

## 核心设计目标

1.  **量化优势**: 对比 Fusion 内存索引与 Linux 原生 `find` 命令在递归元数据检索下的延迟与吞吐量。
2.  **百万级规模**: 支持生成并同步超过 1,000,000 个文件的元数据。
3.  **全自动流程**: 自动编排 Registry、Fusion 和 Agent，实现一键式从环境部署到报告生成。
4.  **生产环境巡检**: 支持对接已有的 Fustor 集群进行性能实时量化。

## 目录结构规范与安全

为了保护生产数据免受误删，Benchmark 实施了严格的路径校验：

*   **运行沙箱**: 所有的日志、中间数据库和压测报告都固定在当前目录下的 **`fustor-benchmark-run/`**。
*   **安全逻辑**: `generate` 命令如果发现目标目录非空，会强制停止并报错，防止覆盖已有数据。

## 快速使用

### 1. 数据生成
数据生成器通过以下三个维度的乘积来确定最终的文件总量：
*   `--num-dirs` (默认 1000): UUID 目录的数量。
*   `--num-subdirs` (默认 4): 每个 UUID 目录下的子目录数量。
*   `--files-per-subdir` (默认 250): 每个子目录下的文件数量。

**默认规模**: $1000 \times 4 \times 250 = 1,000,000$ 个文件。

构建一个包含 1000 个 UUID 目录（总计 100 万文件）的测试集：
```bash
uv run fustor-benchmark generate fustor-benchmark-run/data --num-dirs 1000
```

### 2. 执行压测 (全自动模式)
此模式会自动在本地启动 Registry, Fusion 和 Agent，并在压测结束后自动停止：
```bash
uv run fustor-benchmark run fustor-benchmark-run/data -d 5 -c 20 -n 100
```

### 3. 执行压测 (外部服务模式)
对接已在运行的 Fustor 集群（如分布式部署环境）：
```bash
uv run fustor-benchmark run /mnt/nfs_data \
    --fusion-api http://10.0.0.1:18102 \
    --api-key YOUR_SECRET_KEY \
    -d 5 -c 50 -n 1000
```

## 参数说明

| 参数 | 缩写 | 默认值 | 说明 |
| :--- | :--- | :--- | :--- |
| `TARGET-DIR` | - | (必填) | 目标数据路径（本地或 NFS 挂载点） |
| `--concurrency` | `-c` | 20 | 并发工作进程/线程数 |
| `--num-requests` | `-n` | 200 | 总压测迭代次数 |
| `--target-depth` | `-d` | 5 | 探测并测试的目录相对深度 |
| `--fusion-api` | - | - | 外部 Fusion API 地址（跳过本地服务启动） |
| `--api-key` | - | - | 外部 Fusion API 的身份验证 Key |

## 报告与指标

压测完成后，将在 `fustor-benchmark-run/results/` 下生成以下产出：

1.  **`stress-find.html`**: 交互式可视化报表。
    *   **Latency Distribution**: 展示 Avg, P50, P95, P99 的柱状对比。
    *   **Latency Percentiles**: 展现延迟分布曲线。
    *   **Speedup Factor**: 自动计算 Fusion 相比 OS 的加速倍数。
2.  **`stress-find.json`**: 结构化的指标数据，包含所有原始延迟序列。

## 安全保护说明

Benchmark 及其底层驱动 `source_fs` 遵循 **100% 纯读取** 准则。压测过程仅通过 API 获取元数据或执行 `find` 命令，绝对不会修改用户的生产数据或在监控目录下创建任何临时文件。
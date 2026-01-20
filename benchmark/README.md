# Fustor 性能基准测试工具 (Benchmark)

该模块是 Fustor 平台的自动化压力测试和性能量化工具。它通过模拟大规模文件系统元数据，量化 Fusion API 相比于操作系统原生文件系统调用的性能优势。

## 核心设计目标

1.  **量化优势**: 对比 Fusion 内存索引与 Linux 原生 `find` 命令在递归元数据检索下的延迟与吞吐量。
2.  **百万级规模**: 支持生成并同步超过 1,000,000 个文件的元数据。
3.  **全自动流程**: 自动编排 Registry、Fusion 和 Agent，实现一键式从环境部署到报告生成。

## 目录结构规范与安全

为了保护生产数据免受误删，Benchmark 实施了严格的路径校验：

*   **路径白名单**: 压测主目录（`run-dir`）必须以 **`fustor-benchmark-run`** 结尾。
*   **结构定义**:
    *   `{run-dir}/data/`: 存放生成的数百万个模拟文件。
    *   `{run-dir}/.fustor/`: 存放压测期间的独立配置文件、SQLite 数据库、日志以及最终报告。

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

### 2. 执行压测
运行全链路同步并执行并发性能对比：
```bash
# 执行压测 (必须指定数据路径)
uv run fustor-benchmark run fustor-benchmark-run/data -d 5 -c 20 -n 100

# 使用外部 NFS 生产数据
uv run fustor-benchmark run /mnt/nfs_data -d 5
```
*   `-d`: 探测深度。
*   `-c`: 并发数。
*   `-n`: 总请求次数。
*   `-m`: 生成模式 (`auto`, `force`, `skip`)。

## 报告与指标

压测完成后，将在 `fustor-benchmark-run/.fustor/` 下生成以下产出：

1.  **`report.html`**: 交互式可视化报表。
    *   **Latency Distribution**: 展示 Avg, P50, P95, P99 的柱状对比。
    *   **Latency Percentiles**: 展现延迟分布曲线。
    *   **Speedup Factor**: 自动计算 Fusion 相比 OS 的加速倍数。
2.  **`benchmark_results.json`**: 结构化的指标数据，包含所有原始延迟序列。

## 安全保护说明

Benchmark 会在 `run-dir` 下执行 `shutil.rmtree` 操作以清理旧环境。**请务必确保指定的目录不包含任何重要业务数据**。如果尝试在非 `fustor-benchmark-run` 后缀目录下运行，程序将强制退出。

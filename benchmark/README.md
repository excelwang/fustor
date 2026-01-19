# Fustor Benchmark Tool

A dedicated CLI tool for benchmarking Fustor's performance against standard Linux utilities.

## Benchmark Scenario: "The Million File Challenge"

This benchmark simulates a high-density, sharded file storage system typical of massive data lakes or sharded object storage backends.

### Data Structure
*   **Total Files**: 1,000,000 files.
*   **Directory Layout**: Deeply nested, sharded by UUID prefix.
    *   Path format: `.../upload/submit/{char1}/{char2}/{uuid}/`
*   **Distribution**: 1,000 top-level UUID shards, each containing 1,000 files.

## The Contestants

1.  **Baseline (Linux `find`)**:
    *   **Path Only**: `find benchmark_data -type f`
    *   **With Metadata**: `find benchmark_data -type f -ls` (simulating real-world usage where file details like size/mtime are needed).
    *   **Mechanism**: Traverses the OS VFS. Performance scales linearly $O(N)$ with file count. Syscall overhead and string formatting become dominant as $N$ grows.

2.  **Fustor Fusion (In-Memory Tree)**:
    *   **Mechanism**: Metadata is pre-indexed in an optimized in-memory Tree structure.
    *   **Query**: HTTP GET `/views/fs/tree` returns full JSON metadata.
    *   **Advantage**: Lookup performance is nearly $O(1)$ regarding total dataset size. Zero disk I/O at query time.

## Installation & Usage

```bash
uv sync

# Generate 1M files
fustor-benchmark generate --num-dirs 1000

# Run benchmark
fustor-benchmark run --concurrency 20 --requests 200
```

## Results: 1,000,000 Files

| Metric | Find (Path Only) | Find (With Meta) | Fustor Fusion API | Improvement vs Meta |
| :--- | :--- | :--- | :--- | :--- |
| **Full Scan Latency** | ~326 ms | ~3,246 ms | **~2.3 ms** | **~1,400x Faster** |
| **Concurrent Latency** (1k files) | ~4.2 ms | ~25.3 ms | **~17.1 ms** | **~1.5x Faster** |
| **Throughput (QPS)** | 2,770 | 679 | **958** | **~1.4x Higher** |

### Key Takeaways
1.  **Massive Scale Stability**: While `find`'s latency jumped 10x when moving from 100k to 1M files, **Fusion's latency remained constant (~2ms)**.
2.  **The Metadata Tax**: Standard tools are heavily penalized when retrieving full file details. Fusion provides this data "for free" as it's already part of the memory node.
3.  **Concurrency Tipping Point**: At 1M files, Fusion's architectural efficiency starts to beat local syscalls even for smaller concurrent queries, as OS VFS management becomes more strained.
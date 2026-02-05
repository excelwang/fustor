# Quality Assurance Strategy

> **Goal**: To ensure the reliability, performance, and correctness of Fustor V2 through a layered testing strategy.

## 1. Testing Pyramid

### 1.1 Unit Tests (L1)
- **Scope**: Individual functions, classes, and modules.
- **Tools**: `pytest`, `unittest`.
- **Location**: `tests/`, `packages/*/tests/`.
- **Coverage Target**: 80%+.
- **Responsibility**: Developer.

### 1.2 Contract Tests (L2)
- **Scope**: Interface compliance (e.g., `Sender`, `Receiver`, `SourceHandler`).
- **Tools**: `pytest`.
- **Location**: `tests/contract/`.
- **Responsibility**: Architect/Developer.

### 1.3 Integration Tests (L3)
- **Scope**: Interaction between components (e.g., Agent <-> Fusion via Docker).
- **Tools**: `pytest`, `docker-compose`.
- **Location**: `it/`.
- **Focus**:
    - **Consistency**: Leader election, split-brain recovery, eventual consistency.
    - **Resilience**: Network partitions, process crashes.
    - **Correctness**: Data integrity across the pipeline.

## 2. Integration Test Optimization (The "Concise Work" Strategy)

> **Philosophy**: Integration tests are expensive. Run fewer, but smarter tests.

### 2.1 Environmental Reuse
- **Problem**: Spinning up Docker containers is slow.
- **Solution**: Use a persistent `docker-compose` environment.
    - Calculate hash of dependencies (`pyproject.toml`, `Dockerfile`, `docker-compose.yml`).
    - If hash matches, reuse existing containers.
    - Only restart services if code changes (via volume mounts).

### 2.2 Scenario Consolidation
- Instead of 50 small tests, run 5 complex "Life-cycle Scenarios".
- **Example Scenario**:
    1. Start Agent A (Leader).
    2. Start Agent B (Follower).
    3. Write file on NFS.
    4. Kill Agent A.
    5. Verify Agent B becomes Leader.
    6. Verify file update propagates.

### 2.3 White-box State Verification
- Don't just check the output (black-box).
- Query internal state (white-box) to verify correctness faster.
- **Mechanism**: Fusion exposes `/api/v1/debug/state` or similar to dump internal logical clocks, view trees, etc.

## 3. Continuous Integration
- Run L1/L2 on every commit.
- Run L3 on merge request (or nightly).

## 4. Performance Benchmarking
- Separate from functional IT.
- Focus on Throughput (Events/sec) and Latency (End-to-end).
- Location: `benchmark/`.

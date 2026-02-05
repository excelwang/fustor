# Ticket: Optimize Integration Test Framework

> **Status**: Backlog
> **Type**: Refactor
> **Workstream**: refactor/it-optimization

## 1. Context
The current `it/` directory contains a heavyweight Docker Compose setup. We need to optimize it to "do less but concise work" as per `specs/20-QUALITY_ASSURANCE.md`.

## 2. Requirements

### A. Environmental Optimization
- [ ] Implement smart container reuse based on dependency hashing (`it/utils/docker.py`?).
- [ ] Ensure volume mounts allow code hot-reloading without container rebuilds.

### B. Test Scenarios
- [ ] Consolidate existing granular consistency tests into a few robust scenarios.
- [ ] Ensure tests use "White-box State Verification" (query Fusion state directly).

### C. Developer Experience
- [ ] Provide a single command to run IT (e.g., `uv run pytest it/`).
- [ ] Add explicit logs for when environments are reused vs rebuilt.

## 3. Implementation Plan
1.  **Audit**: Review `it/conftest.py` and `it/docker-compose.yml`.
2.  **Refactor**: Update fixture scope (session-scoped containers).
3.  **Verify**: Run tests twice; second run should be instant.

## 4. Acceptance Criteria (DoD)
- [ ] Second run of `pytest it/` takes < 5s setup time.
- [ ] Tests pass reliably.

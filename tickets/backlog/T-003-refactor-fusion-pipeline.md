# Ticket: Refactor Fusion Pipeline Architecture (V2)

> **Status**: Backlog
> **Type**: Refactor
> **Workstream**: refactor/fusion-pipeline-v2

## 1. Context
Phase 2 of V2 Architecture requires `FusionPipeline` to support:
- 1 Receiver -> N Pipelines -> N Views architecture.
- Session Management integration.
- LogicalClock arbitration.

## 2. Requirements

### A. Functional (Must Have)
- [ ] Verify `FusionPipeline` (in `fusion/src/fustor_fusion/runtime/fusion_pipeline.py`) matches V2 topology.
- [ ] Ensure it correctly routes events from Receiver to multiple Views.
- [ ] Verify `on_event` handling uses `LogicalClock` for consistency.

## 3. Implementation Plan
1.  **Audit**: Read `fusion_pipeline.py`.
2.  **Refactor**: Align with Spec.

## 4. Acceptance Criteria (DoD)
- [ ] Fusion Pipeline V2 compliance.

# Gap Analysis Report: Fustor V2 Architecture

> **Date**: 2026-02-04
> **Ref Specs**: specs/01-ARCHITECTURE.md, specs/02-CONSISTENCY_DESIGN.md, specs/03-LOGICAL_CLOCK_DESIGN.md, specs/04-Detailed_Decisions.md
> **Code Path**: /home/huajin/fustor_monorepo/

## 1. Executive Summary
The system has successfully transitioned to the V2 architecture in terms of core structural elements: the `fustor-core` package exists, and both `fustor-agent` and `fustor-fusion` use the new Pipeline/Handler/Transport abstractions. The Robust Logical Clock and Session Management are also implemented and aligned with the specs.

However, several implementation details violate the project's coding principles (e.g., file size limits), and some planned features (gRPC transport) are still missing. The `source-fs` driver specifically requires refactoring to reduce complexity and redundancy.

## 2. Discrepancies (Gaps)

| ID | Spec Reference | Code Location | Description of Gap | Severity |
| :--- | :--- | :--- | :--- | :--- |
| G-001 | GEMINI.md:Rule 9 | `packages/source-fs/src/fustor_source_fs/__init__.py` | File is too large (697 lines, 31KB), violating the 800-line warning threshold and general maintainability rules. | High |
| G-002 | 01-ARCHITECTURE.md:3.4 | `packages/` | gRPC Sender and Receiver packages are missing despite being planned for Phase 3. | Medium |
| G-003 | todo.md:5 | `packages/source-fs/__init__.py` | Redundant directory scanning logic between `_perform_pre_scan_and_schedule` and `get_snapshot_iterator`. | Medium |
| G-004 | 02-CONSISTENCY_DESIGN.md | `fusion/src/fustor_fusion/main.py` | Router registration logic is messy and contains deprecated "Legacy mode" comments. | Low |
| G-005 | 03-LOGICAL_CLOCK_DESIGN.md | `packages/core/src/fustor_core/clock/logical_clock.py` | Double import of `SessionObsoletedError` in `agent_pipeline.py`. | Low |

## 3. Recommended Actions
- [ ] Refactor `FSDriver` into smaller logical components (e.g., `SnapshotManager`, `AuditManager`).
- [ ] Initialize gRPC transport packages (`fustor-sender-grpc`, `fustor-receiver-grpc`).
- [ ] Merge the scan workers in `source-fs` to share a common recursive scanning implementation.
- [ ] Clean up `fustor-fusion/main.py` and standardize router inclusion.

## 4. Proposed Tickets
- TICKET_V2_001_REF_SOURCE_FS: Refactor Source-FS Driver for maintainability.
- TICKET_V2_002_IMPL_GRPC_TRANSPORT: Implement gRPC Transport layer.
- TICKET_V2_003_CLEANUP_FUSION_MAIN: Clean up Fusion entry point and API routing.

# Ticket: Cleanup Fusion Main Entry Point

> **Status**: Backlog
> **Type**: Refactor
> **Workstream**: refactor/fusion-main

## 1. Context
The `AUDIT_REPORT.md` (G-004) identified that `fustor-fusion/main.py` contains messy router registration logic and deprecated "Legacy mode" comments. This needs to be standardized.

## 2. Requirements

### A. Functional (Must Have)
- [ ] Refactor `main.py` to use a cleaner `create_app()` factory pattern.
- [ ] Remove all "Legacy mode" toggle code and comments.
- [ ] Standardize router inclusion using `APIRouter` include_router method in a centralized `routers/__init__.py`.

### B. Non-Functional (Should Have)
- [ ] Code Readability: Follow PEP8.

## 3. Implementation Plan

> **Strategy**: Refactor

1.  **Phase 1: Standardization**
    - Inspect `fustor_fusion/main.py`.
    - Create `fustor_fusion/api/routers.py` if missing to centralize routing.
    - Move startup event logic to `lifespan` handler (Modern FastAPI).

## 4. Acceptance Criteria (DoD)
- [ ] `uvicorn fustor_fusion.main:app` starts without deprecated warnings.
- [ ] All API endpoints remain accessible (verify via `fustor-benchmark`).

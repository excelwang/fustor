# Step 2: Fix Incremental Audit (Performance)

## Goal
Fix Critical Issues D-05 and U-02: Restore incremental audit capability by maintaining an `mtime_cache` context in AgentPipeline and correctly updating it during the audit phase, including handling silent directories.

## Tasks
- [x] **1. AgentPipeline Context**
    - [x] Add `self.audit_context: Dict[str, Any] = {}` to `AgentPipeline.__init__`.
    - [x] Pass `self.audit_context` to `run_audit_sync`.

- [x] **2. Update Audit Logic (phases.py)**
    - [x] Update `fustor_agent/runtime/pipeline/phases.py`:
        - `run_audit_sync` should accept `audit_context`.
        - Pass `audit_context` to `pipeline.source_handler.get_audit_iterator`.
        - **Critical Fix**: When iterating, if `event` is `None` but `mtime_update` is present, UPDATE the `audit_context`!
        - Current logic `if event is None: continue` causes data loss (silence ignored).

- [x] **3. Update Mocks**
    - [x] Ensure `MockSourceHandler.get_audit_iterator` returns tuples `(Event, dict)` to be compatible with `phases.py`.

- [x] **4. Verify**
    - [x] Create `agent/tests/runtime/test_agent_pipeline_audit.py`.
    - [x] Verify that `audit_context` is populated after an audit run.
    - [x] Verify that `None` events correctly update the context.

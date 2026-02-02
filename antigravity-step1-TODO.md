# Step 1: Restore Resume Capability

## Goal
Fix Critical Issues D-04 and D-06: Ensure AgentPipeline resumes from the last committed index stored in Fusion, preventing data loss or duplication on restart.

## Tasks
- [x] **1. Extend Sender Interface**
    - [x] Update `fustor_core/pipeline/sender.py`: Add `get_latest_committed_index(session_id)` to `SenderHandler` ABC.
    - [x] Update `fustor_sender_http` (or relevant implementation) to implement this method (it was likely present in `pusher-fusion` driver but needs to be exposed via Handler adapter).

- [x] **2. Update Pipeline Logic**
    - [x] Update `AgentPipeline._run_control_loop` (or `_run_message_sync`) to query `latest_committed_index` from sender after session creation.
    - [x] Persist this `start_possition` in `AgentPipeline` state for use in sync phases.

- [x] **3. Fix Driver Mode Resume**
    - [x] Update `agent/src/fustor_agent/runtime/pipeline/phases.py`:
        - Change `run_driver_message_sync` signature to accept `start_position`.
        - Pass `start_position` to `pipeline.source_handler.get_message_iterator`.

- [x] **4. Verify**
    - [x] Check `agent/tests/runtime/test_agent_pipeline.py` or create a new test case to verify `get_latest_committed_index` is called.

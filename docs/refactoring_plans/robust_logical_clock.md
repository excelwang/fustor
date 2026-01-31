# Comprehensive Refactoring Plan: Robust Logical Clock

## Objective
Implement the "Robust Logical Clock" algorithm as defined in `docs/LOGICAL_CLOCK_DESIGN.md` to solve Clock Skew and Future Timestamp issues.

## Scope
-   **Package**: `fustor_common` (`logical_clock.py`)
-   **Package**: `view_fs` (`arbitrator.py`)
-   **Package**: `tests` (New and existing tests)

## Phase 1: Core Implementation (`fustor_common`)

### Task 1.1: State & Initialization
Modify `LogicalClock.__init__` to include:
-   `self._session_buffers`: Dict to store circular buffers (deque) of skew samples per session.
-   `self._global_histogram`: Counter to track frequency of skew values globally.
-   `self._trust_window`: Float (1.0s).

### Task 1.2: Skew Calculation Logic
Implement `update(observed_mtime, agent_time, session_id)`:
1.  **Input Handling**: Support both legacy (`update(mtime)`) and new signatures.
2.  **Sampling**: Calculate `diff = agent_time - mtime`.
3.  **Buffer Management**: Add `diff` to session buffer; maintain `global_histogram` (add new, remove old).
4.  **Global Skew Election**: Find Mode of histogram. Use smallest Diff as tie-breaker.
5.  **Watermark Calculation**:
    -   `BaseLine = agent_time - G_Skew`
    -   `FastPath`: If `BaseLine < mtime <= BaseLine + TrustWindow`, use `mtime`.
    -   `Future Protection`: If `mtime > BaseLine + TrustWindow`, reject `mtime` (use `BaseLine`).

### Task 1.3: Safeguard & Fallback
Update `now()`:
-   Strictly return the calculated `_value`.
-   **Fallback**: Only if `_value == 0.0` (uninitialized), return `time.time()`.

### Task 1.4: Session Cleanup
Implement `remove_session(session_id)`:
-   Remove session's samples from `global_histogram`.
-   Delete session buffer.

## Phase 2: Integration (`view_fs`)

### Task 2.1: Event Processing Update
Modify `FSArbitrator.ingest_event` (or `process_event`):
-   Extract `agent_time` (from `event.index` or `event.timestamp`).
-   Extract `session_id`.
-   Pass these to `self.state.logical_clock.update()`.

## Phase 3: Verification & Testing

### Task 3.1: Unit Testing (`tests/unit/test_logical_clock.py`)
Create rigorous tests covering:
-   **Skew Convergence**: Verify clock aligns with Agent Time - Skew.
-   **Trust Window**: Verify small "future" mtimes are accepted (FastPath).
-   **Jump Protection**: Verify large "future" mtimes are rejected.
-   **Multi-Session**: Verify Mode logic with noisy neighbors.
-   **Fallback**: Verify behavior when empty.

### Task 3.2: Integration Testing
-   Run existing consistency tests.
-   Add specific test case for "Time Travel" (Future Timestamp file) to ensure no regression.

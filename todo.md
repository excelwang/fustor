# Fustor Todo List

## Fusion & Agent Stability (Prioritized)

- [ ] **Fusion Transient Session Management**
    - [ ] Analyze how `fusion` module distinguishes transient data source push sessions.
    - [ ] If distinguished, ensure that when all sessions for a transient data source become inactive (e.g., disconnected or timed out), the associated event queue, view data, and state are cleared/reset (as if the source was never monitored).

- [ ] **System-Wide Testing**
    - [ ] Execute all pytest tests (`uv run pytest`).
    - [ ] Fix any bugs encountered during the test run.
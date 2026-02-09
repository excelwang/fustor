# Fustor Pipe State Machine

The Fustor Pipe uses an `IntFlag` bitmask to represent its current state. This allows for composite states where multiple flags can be active simultaneously (e.g., `RUNNING | SNAPSHOT_SYNC`).

## State Flags (`fustor_core.pipe.PipeState`)

| Flag | Value | Description |
|------|-------|-------------|
| `STOPPED` | 0 | Pipe is idle and has no active tasks or sessions. |
| `INITIALIZING` | 1 | Pipe is setting up resources and handlers. |
| `RUNNING` | 2 | Pipe is currently active. Usually combined with a phase flag. |
| `PAUSED` | 4 | Pipe is temporarily suspended. |
| `ERROR` | 8 | Pipe encountered a fatal error and reached a terminal state. |
| `CONF_OUTDATED` | 16 | Configuration changed while pipe was running. |
| `SNAPSHOT_SYNC` | 32 | Currently executing full snapshot synchronization. |
| `MESSAGE_SYNC` | 64 | Currently executing realtime message synchronization. |
| `AUDIT_PHASE` | 128 | Currently executing periodic audit check. |
| `RECONNECTING` | 256 | Attempting to re-establish session after failure. |
| `DRAINING` | 512 | Processing remaining queued items before stopping. |
| `STOPPING` | 1024 | Gracefully shutting down tasks and closing session. |

## Common State Combinations

- **`INITIALIZING`**: Startup phase.
- **`RUNNING | SNAPSHOT_SYNC`**: Initial phase of all data.
- **`RUNNING | MESSAGE_SYNC`**: Processing live events.
- **`RUNNING | AUDIT_PHASE`**: Running background consistency check.
- **`RUNNING | RECONNECTING`**: Session lost, but still trying to recover without full restart.
- **`ERROR`**: Fatal failure, manual intervention or full restart required.

## State Transitions (Agent)

1. **Start**: `STOPPED` -> `INITIALIZING` -> `RUNNING`
2. **Pipe Flow**: `RUNNING` -> `RUNNING | SNAPSHOT_SYNC` -> `RUNNING | MESSAGE_SYNC`
3. **Audit**: `RUNNING | MESSAGE_SYNC` -> `RUNNING | MESSAGE_SYNC | AUDIT_PHASE` -> `RUNNING | MESSAGE_SYNC`
4. **Error Recovery**: `RUNNING | ...` -> `RUNNING | RECONNECTING` -> `RUNNING | ...` (if successful) or `ERROR` (if failed max retries)
5. **Stop**: `RUNNING | ...` -> `STOPPING` -> `DRAINING` -> `STOPPED`

## State Transitions (Fusion)

1. **Start**: `STOPPED` -> `INITIALIZING` -> `RUNNING`
2. **Session Active**: `RUNNING` (state doesn't change per session, but individual sessions track their own status)
3. **Stop**: `RUNNING` -> `STOPPING` -> `STOPPED`

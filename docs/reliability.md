# Fustor V2 Reliability and Error Recovery Guide

This document describes how Fustor V2 ensures reliable data transfer and handles various error scenarios in its distributed pipe architecture.

## 1. Overview

Fustor V2 uses a "Pipe" model where the Agent and Fusion coordinate through **Sessions**. Reliability is achieved through:
- **Session-based state tracking**: Ensures both sides are in sync about roles and progress.
- **Heartbeats**: Detects network failures or process crashes.
- **Exponential Backoff**: Prevents overwhelming the system during transient errors.
- **Role Assignment**: Ensures only one Leader is active for critical tasks (Snapshot/Audit).

## 2. Agent Reliability Mechanisms

The `AgentPipe` implements robust error recovery in its main control loop.

### 2.1 Exponential Backoff

When an error occurs during session creation or data sync, the pipe enters a retry loop with exponential backoff:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `error_retry_interval` | 5.0s | Initial delay after the first error. |
| `backoff_multiplier` | 2 | Value by which the delay is multiplied for each consecutive error. |
| `max_backoff_seconds` | 60s | Maximum delay cap. |
| `max_consecutive_errors` | 5 | Threshold after which the system may take more drastic recovery actions (logging/status change). |

### 2.2 Session Lifecycle

1.  **Creation**: Agent sends a POST to `/api/v1/pipe/sessions/`.
2.  **Maintenance**: Agent sends periodic heartbeats (POST `/heartbeat`).
3.  **Expiry**: If heartbeats stop, Fusion expires the session after `session_timeout_seconds`.
4.  **Reconnection**: If a session becomes obsolete (`SessionObsoletedError`), the Agent immediately clears its local session state and attempts to create a new one.

### 2.3 Heartbeat configuration

- **Interval**: Defaults to `heartbeat_interval_seconds` (10s) OR is suggested by the server in the session response.
- **Adaptive Heartbeat**: The Agent may skip a heartbeat if a data push was successfully acknowledged recently, as the ACK serves as a liveness signal.

## 3. Fusion Reliability Mechanisms

### 3.1 Session Monitoring

Fusion's `SessionManager` tracks `last_active_at` for every session. A background task cleans up sessions that have been inactive for longer than `session_timeout_seconds`.

### 3.2 View Consistency (Arbitration)

Reliability also means data correctness. Fustor V2 uses:
- **LogicalClock**: To ensure even if messages arrive out of order or from multiple Agents, the latest version (based on mtime and logical sequence) wins.
- **FSArbitrator**: Specifically handles NFS-related consistency challenges (Suspect list, Tombstone protection).

## 4. Configuration Reference

These properties can be set in your `agent-pipes-config/*.yaml`:

```yaml
id: my-pipe
# ...
session_timeout_seconds: 30
heartbeat_interval_sec: 10
audit_interval_sec: 600
sentinel_interval_sec: 120

# Advanced Reliability Settings
error_retry_interval: 5.0
max_consecutive_errors: 5
backoff_multiplier: 2
max_backoff_seconds: 60
```

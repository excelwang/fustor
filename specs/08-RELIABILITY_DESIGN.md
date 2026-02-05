# Reliability & Error Handling Design

> **Status**: Draft
> **Version**: 1.0
> **Scope**: Global (Agent & Fusion)

## 1. Core Philosophy: "Crash-Only Software" vs "Graceful Degradation"

Fustor adopts a hybrid approach:
1.  **Process Level**: **Crash-Only**. If the internal state is corrupted (e.g., MemoryError, logical inconsistency), the process should crash and restart (via Supervisord/Docker).
2.  **Request Level**: **Graceful Degradation**. If a single request fails (e.g., malformed event, timeout), drop the request, log the error, and continue processing others. **Never crash the pipeline for a bad data packet.**

## 2. Error Classifications

### 2.1 Transient Errors (Retryable)
- **Examples**: Network timeout, Fusion 503 Service Unavailable, FS `ETIMEDOUT`.
- **Strategy**: Exponential Backoff.
- **Component**: `AgentPipeline` Control Loop.

### 2.2 Permanent Errors (Non-Retryable)
- **Examples**: 401 Unauthorized, 400 Bad Request (Schema Mismatch), File Permission Denied (Persistent).
- **Strategy**:
    - **Agent**: Alert and Pause (or Skip if file-level).
    - **Fusion**: Return 4xx, record metric, drop event.

### 2.3 Critical State Errors (Fatal)
- **Examples**: Disk Full, Out of Memory, Corrupted Internal DB.
- **Strategy**: Fast Fail (Exit Process). Let orchestrator restart.

## 3. Component-Specific Strategies

### 3.1 Agent: Source Drivers (`fustor-source-fs`)
- **File Access**: If `os.stat` or `open` fails (e.g., Permission Denied, File Vanished):
    - **Action**: Log warning, emit `audit_skipped` or ignore event. **Do not crash scanner thread.**
- **Watcher Overflow**: If inotify buffer overflows:
    - **Action**: Log error, trigger `Snapshot Sync` (fallback to polling consistency).

### 3.2 Agent: Pipeline Engine
- **Session Loss**: If 419/404 from Fusion:
    - **Action**: Immediate Re-login.
- **Bus Lag**: If Consumer falls behind Producer by > 95%:
    - **Action**: Split EventBus (Auto-sharding).

### 3.3 Fusion: Event Processing
- **Malformed Event**:
    - **Action**: Log, increment `metric.event_drop_count`, continue processing batch.
- **View Logic Exception**:
    - **Action**: Catch generic `Exception` in `process_event`, log stack trace, return `success=False` (triggers Agent retry or drop depending on policy).

## 4. Resource Protection

### 4.1 Throttling
- **Agent**: `throttle_interval_sec` limits inotify noise.
- **Fusion**: `max_request_size_mb` limits memory consumption per request.

### 4.2 Timeouts
- **Network**: All HTTP/gRPC calls must have strict timeouts (default 30s).
- **Locks**: Internal locks (e.g., `FSViewProvider._global_semaphore`) must have acquisition timeouts to prevent deadlocks.

## 5. Observability
- **Metrics**: Error counts (`counter`), Latency (`histogram`).
- **Logs**: Structured JSON logs. Stack traces only for 5xx/Fatal errors.

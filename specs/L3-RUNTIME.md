---
layer: 3
id: RUNTIME
version: 1.0.0
requires:
  - ARCH.PIPELINE
  - ARCH.SESSION
  - ARCH.VIEW
  - ARCH.SOURCE
exports:
  - RUNTIME.PIPELINE_LIFECYCLE
  - RUNTIME.SESSION_FAILOVER
  - RUNTIME.AUDIT_CACHE
  - RUNTIME.SUSPECT_TTL
  - RUNTIME.CONCURRENCY
---

# L3-RUNTIME: Runtime Behavior Specifications

> **Purpose**: Define lifecycle, concurrency, and error handling for each component.

## 1. Pipeline Lifecycle (ARCH.PIPELINE)

### RUNTIME.PIPELINE_LIFECYCLE

**States**: `INIT → RUNNING → STOPPING → STOPPED`

| Trigger | Action |
|---------|--------|
| `start()` | Load config, initialize handlers, enter RUNNING |
| `stop()` | Drain queues, close connections, enter STOPPED |
| Crash | Logged, auto-restart by supervisor |

---

## 2. Session Failover (ARCH.SESSION)

### RUNTIME.SESSION_FAILOVER

**On Leader Promotion** (Follower → Leader):

| Action | Behavior |
|--------|----------|
| Pre-scan | **Skip** (realtime sync already running) |
| Snapshot | **Trigger** (catch up missed data) |
| Audit Cache | **Clear** (force full scan for new baseline) |
| Sentinel | **Start** (begin verification tasks) |

**Timeout Handling**:
- Session timeout: `2 * heartbeat_interval`
- On timeout: Release Leader lock, log warning.

---

## 3. Audit Cache (ARCH.SOURCE)

### RUNTIME.AUDIT_CACHE

**Storage**: In-memory Dict in AgentPipeline.
**Lifecycle**:
- Built: First audit after becoming Leader.
- Cleared: Session rebuild, Role change, Pipeline restart.

**Effect**:
- First audit: Full scan (high IO).
- Subsequent: Incremental scan (low IO).

---

## 4. Suspect TTL (ARCH.VIEW)

### RUNTIME.SUSPECT_TTL

**Background Task**: Runs every 0.5s.
**Algorithm**: Pop expired entries from min-heap, verify, process.

**Timer Source**: `time.monotonic()` (not affected by system clock changes).

---

## 5. Concurrency Control (ARCH.VIEW)

### RUNTIME.CONCURRENCY

**Two-Level Locking**:

| Lock | Scope | Use Case |
|------|-------|----------|
| Global Semaphore | Bounded concurrency | Limit `process_event` parallelism |
| Exclusive Lock | Mutual exclusion | `audit_start/end` requires exclusive access |

**Rule**: Audit lifecycle must drain event queue before acquiring exclusive lock.

---

## 6. Error Handling

### Agent Side
- FS exceptions (permission, vanished): Log, skip file, continue.
- Watcher overflow: Trigger fallback Snapshot Sync.

### Fusion Side
- Malformed event: Log, increment counter, skip.
- View exception: Catch, log stack, return `success=False`.

> See `31-RELIABILITY.md` for detailed error classification.

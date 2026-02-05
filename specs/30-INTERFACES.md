# Interfaces & Implementation

> **Purpose**: Defines the concrete APIs, configuration, and design patterns derived from the Core Algorithms.

## 1. System Interfaces (API)

### 1.1 Pipeline API (Agent -> Fusion)

**`POST /pipe/sessions/`**
- **Purpose**: Handshake to start a session.
- **Input**: `task_id`, `client_info`.
- **Output**: `session_id`, `role` (Leader/Follower), `timeout`.

**`POST /pipe/events/{session_id}`**
- **Purpose**: High-throughput event ingestion.
- **Input**: `EventBatch` (defined in `11-MODELS_AND_TERMS`).
- **Behavior**: Async dispatch to `View.process_event`.

**`POST /pipe/consistency/sentinel/feedback`**
- **Purpose**: Agent reporting back on Suspect file status.
- **Input**: List of `{path, mtime, exists}`.
- **Behavior**: Fusion updates Suspect Heap.

### 1.2 Consistency API (Agent -> Fusion)

**`POST /consistency/audit/start`**
- **Purpose**: Signal audit cycle beginning.
- **Behavior**: Fusion records `last_audit_start = time.time()`.

**`POST /consistency/audit/end`**
- **Purpose**: Signal audit cycle completion.
- **Behavior**: 
  1. Drain event queue (max 10s)
  2. Execute Tombstone TTL cleanup
  3. Execute Missing Item Detection
  4. Reset audit state

**`GET /consistency/sentinel/tasks`**
- **Purpose**: Fetch pending Sentinel verification tasks.
- **Output**: `{type: "suspect_check", paths: [...], source_id: N}`.

**`POST /consistency/sentinel/feedback`**
- **Purpose**: Submit Sentinel check results.
- **Input**: `{type: "suspect_update", updates: [{path, mtime, status}]}`.

### 1.3 Management API (User -> Fusion)

**`GET /views/{view_id}/tree`**
- **Purpose**: Fetch the unified file tree.
- **Filter**: Can filter out `integrity_suspect=True` files if strict consistency is needed.

**`GET /views/{view_id}/tree/stats`**
- **Purpose**: Get view statistics.
- **Output**: Includes `has_blind_spot: true/false`.

**`GET /views/{view_id}/tree/blind-spots`**
- **Purpose**: List all blind-spot files.
- **Output**: `{additions: [...], deletions: [...]}`.

### 1.4 Management CLI (Local)

**`fustor-agent|fusion config add --file <path>`**
- **Purpose**: Hot patch configuration.
- **Behavior**: Validates YAML, copies to config dir, reloads impacted components.

**`fustor-agent|fusion start`**
- **Purpose**: Global lifecycle control.
- **Behavior**: Loads all `enabled: true` configs and starts pipelines.

---

## 2. Configuration Reference

### 2.1 Agent (`agent-pipes.yaml`)
```yaml
audit_interval_sec: 600
sentinel_interval_sec: 120  # Max frequency, guided by Heap
```

### 2.2 Fusion (`fusion-pipes.yaml`)
```yaml
hot_file_threshold: 30.0    # Seconds
session_timeout_seconds: 30
tombstone_ttl_seconds: 3600 # 1 hour
```

---

## 3. Implementation Design Patterns

### 3.1 Event Processing Algorithm (Arbitrator)

For each incoming event `E` targeting path `P`:

1. **Tombstone Guard**
   - IF `P` exists in Tombstone List with logical timestamp `Ts_logical`:
     - IF `E.mtime > Ts_logical`: Remove tombstone (Reincarnation), ACCEPT
     - ELSE: DISCARD (Stale)
   - Note: Physical timestamp `Ts_physical` is used for TTL cleanup (§2.2).

2. **Mtime Arbitration**
   - IF `P` exists in Memory Tree with `node.mtime`:
     - IF `E.mtime ≤ node.mtime` AND NOT `E.audit_skipped`: DISCARD (Older/Duplicate)
     - ELSE: ACCEPT, Update node

3. **Parent Mtime Check** (For Snapshot/Audit only)
   - IF parent directory's `mtime > E.parent_mtime`: DISCARD (Parent updated after scan)
   - ELSE: ACCEPT as Blind-spot addition

4. **Suspect Judgment**
   - Compute `Age = Watermark - E.mtime`
   - IF `Age < hot_file_threshold`: Add to Suspect Heap

### 3.2 Stale Evidence Protection

Prevents audit from deleting nodes that received Realtime updates after audit started.

| Event Type | Action on `last_updated_at` |
|------------|------------------------------|
| **Realtime** | Set to current physical time |
| **Snapshot/Audit** | Preserve existing value |

**Audit-End Rule**: IF `node.last_updated_at > audit_start_time`, SKIP deletion.

### 3.3 Audit Skipped Protection

Directories marked `audit_skipped=True` (mtime unchanged, scan skipped) protect their children from Missing detection.

**Rule**: Only perform Missing detection on directories that were fully scanned during the current audit cycle.

### 3.4 Suspect Heap Optimization

Use a Min-Heap ordered by expiry time for O(log n) suspect management.

| Data Structure | Content | Purpose |
|----------------|---------|---------|
| `suspect_list` (Dict) | `path → (expiry, mtime)` | Quick lookup |
| `suspect_heap` (Heap) | `(expiry, path)` tuples | Ordered expiration |

**Cleanup Algorithm**: Pop expired entries from heap top, verify via dict, then process.

> This heap structure is also used by **Sentinel Sweep** (§20-CORE_ALGORITHMS §3.2) to schedule verification tasks.

---

## 4. Concurrency Control

### 4.1 Two-Level Locking (FSViewProvider)

| Lock Type | Scope | Use Case |
|-----------|-------|----------|
| **Global Semaphore** | Bounded concurrency | Limit concurrent `process_event` calls |
| **Global Exclusive Lock** | Mutual exclusion | `handle_audit_start/end` requires exclusive access |

**Rule**: Audit lifecycle operations must drain the event queue before acquiring exclusive lock.

---

## 5. ViewDriver Lifecycle Hooks

| Hook | Trigger | Purpose |
|------|---------|---------|
| `on_session_start` | New Session | Reset blind-spot list and audit buffers |
| `on_session_close` | Session ends | Cleanup (live views clear state) |
| `handle_audit_start` | Audit begins | Record `last_audit_start` timestamp |
| `handle_audit_end` | Audit ends | Execute Missing detection and Tombstone cleanup |
| `cleanup_expired_suspects` | Background (0.5s) | Heap-based TTL expiration |

---

## 6. Time Usage Reference

| Judgment Need | Time Source | Notes |
|---------------|-------------|-------|
| **Suspect Age** | Logical Time | `Watermark - mtime` |
| **Tombstone Reincarnation** | Logical Time | `mtime > tombstone_ts` |
| **Tombstone TTL Cleanup** | Physical Time | 1 hour default |
| **Stale Evidence Protection** | Physical Time | `last_updated_at > audit_start` |
| **Suspect TTL Expiration** | Monotonic Time | Immune to clock adjustments |
| **Event Index** | Physical Time | Millisecond-level capture timestamp |

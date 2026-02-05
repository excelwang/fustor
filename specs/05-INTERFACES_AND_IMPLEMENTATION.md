# Interfaces & Implementation

> **Purpose**: Defines the concrete APIs and implementation details (Pseudocode) derived from the Core Algorithms.

## 1. System Interfaces (API)

### 1.1 Pipeline API (Agent -> Fusion)

**`POST /pipe/sessions/`**
- **Purpose**: Handshake to start a session.
- **Input**: `task_id`, `client_info`.
- **Output**: `session_id`, `role` (Leader/Follower), `timeout`.

**`POST /pipe/events/{session_id}`**
- **Purpose**: High-throughput event ingestion.
- **Input**: `EventBatch` (defined in `04-MODELS_AND_TERMS.md`).
- **Behavior**: Async dispatch to `View.process_event`.

**`POST /pipe/consistency/sentinel/feedback`**
- **Purpose**: Agent reporting back on Suspect file status.
- **Input**: List of `{path, mtime, exists}`.
- **Behavior**: Fusion updates Suspect Heap (`Arbitrator.update_suspect`).

### 1.2 Management API (User -> Fusion)

**`GET /views/{view_id}/tree`**
- **Purpose**: Fetch the unified file tree.
- **Filter**: Can filter out `integrity_suspect=True` files if strict consistency is needed.

---

## 2. Algorithm Implementation (Pseudocode)

### 2.1 Arbitration Logic (`process_event`)

```python
def process_event(event):
    # 1. Update Logical Clock
    fusion.logical_clock.update(event.mtime)
    
    # 2. Tombstone Check
    if event.path in tombstone_list:
        if event.mtime > tombstone_list[event.path].logical_ts + EPSILON:
            clear_tombstone(event.path)
        else:
            return # Stale resurrection attempt

    # 3. Mtime Check
    existing = tree.get(event.path)
    if existing and event.mtime <= existing.mtime:
        return # Stale update

    # 4. Apply Update
    tree.update(event.path, event.data)
    
    # 5. Suspect Management
    age = logical_clock.watermark - event.mtime
    if age < THRESHOLD:
        mark_suspect(event.path)
        schedule_sentinel_check(event.path)
```

### 2.2 Sentinel Scheduling (Agent Side)

```python
def run_sentinel_loop():
    while True:
        # Priority Queue: Get earliest expiring suspect
        next_check_time, path = suspect_heap.peek()
        
        if now() < next_check_time:
            sleep(next_check_time - now())
            continue
            
        # IO Budget Check
        if io_bucket.try_consume(1):
            stat = os.stat(path)
            send_feedback(path, stat)
            suspect_heap.pop()
```

---

## 3. Configuration Reference

### 3.1 Agent (`agent-pipes-config/*.yaml`)
```yaml
id: pipeline-1
audit_interval_sec: 600
sentinel_interval_sec: 120  # Max frequency, guided by Heap
```

### 3.2 Fusion (`fusion-pipes-config/*.yaml`)
```yaml
hot_file_threshold: 30.0    # Seconds
session_timeout_seconds: 30
```

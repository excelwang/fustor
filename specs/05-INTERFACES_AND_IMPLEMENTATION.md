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

### 1.3 Management CLI (Local)

**`fustor-agent|fusion config add --file <path>`**
- **Purpose**: Hot patch configuration.
- **Behavior**: Validates YAML, copies to config dir, reloads impacted components.

**`fustor-agent|fusion start`**
- **Purpose**: Global lifecycle control.
- **Behavior**: Loads all `enabled: true` configs and starts pipelines.
---

## 3. Configuration Reference

### 3.1 Agent (`agent-pipes.yaml`)
```yaml
audit_interval_sec: 600
sentinel_interval_sec: 120  # Max frequency, guided by Heap
```

### 3.2 Fusion (`fusion-pipes.yaml`)
```yaml
hot_file_threshold: 30.0    # Seconds
session_timeout_seconds: 30
tombstone_ttl_seconds: 3600 # 1 hour
```

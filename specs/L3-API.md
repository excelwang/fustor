---
layer: 3
id: API
version: 1.0.0
requires:
  - ARCH.SESSION
  - ARCH.VIEW
  - ARCH.SENDER
  - ARCH.RECEIVER
exports:
  - API.SESSIONS
  - API.EVENTS
  - API.TREE
  - API.SENTINEL
  - API.AUDIT
---

# L3-API: Interface Specifications

> **Purpose**: Define all external APIs for each architectural component.

## 1. Pipeline API (Agent → Fusion)

### API.SESSIONS
**Component**: ARCH.SESSION

**`POST /pipe/sessions/`**
- **Purpose**: Handshake to start a session.
- **Request**: `{ task_id: str, client_info: dict }`
- **Response**: `{ session_id: str, role: "leader"|"follower", timeout: int }`

**`DELETE /pipe/sessions/{session_id}`**
- **Purpose**: Gracefully close session.

**`POST /pipe/sessions/{session_id}/heartbeat`**
- **Purpose**: Keep session alive.
- **Response**: `{ role: str, sentinel_tasks: [...] }`

---

### API.EVENTS
**Component**: ARCH.SENDER, ARCH.RECEIVER

**`POST /pipe/events/{session_id}`**
- **Purpose**: High-throughput event ingestion.
- **Request**: `EventBatch` (see L3-ALGO for processing rules)
- **Response**: `{ accepted: int, discarded: int }`

---

## 2. View API (User → Fusion)

### API.TREE
**Component**: ARCH.VIEW

**`GET /views/{view_id}/tree`**
- **Purpose**: Get unified file tree.
- **Query Params**:
  - `exclude_suspect: bool = false`
  - `depth: int = -1`
- **Response**: Nested tree structure with metadata.

**`GET /views/{view_id}/tree/stats`**
- **Purpose**: View statistics.
- **Response**: `{ has_blind_spot: bool, suspect_count: int, node_count: int }`

---

## 3. Consistency API (Agent → Fusion)

### API.AUDIT
**Component**: ARCH.VIEW

**`POST /consistency/audit/start`**
- **Behavior**: Record `last_audit_start = now()`.

**`POST /consistency/audit/end`**
- **Behavior**: Drain queue, execute Tombstone cleanup, Missing detection.

### API.SENTINEL
**Component**: ARCH.VIEW

**`GET /consistency/sentinel/tasks`**
- **Response**: `{ type: "suspect_check", paths: [...] }`

**`POST /consistency/sentinel/feedback`**
- **Request**: `{ updates: [{ path, mtime, status }] }`

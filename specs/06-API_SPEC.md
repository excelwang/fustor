# API Specification (V2)

> **Status**: Draft
> **Version**: 2.0

## 1. Overview

Fustor V2 API consists of two main categories:
1.  **Pipeline API**: Used by Agents to ingest data (high throughput).
2.  **Management API**: Used by users/dashboard to query views and status.

**Base URL**: `/api/v1`

## 2. Authentication

All requests must include the `X-API-Key` header.

```http
X-API-Key: fk_your_api_key_here
```

## 3. Pipeline API

### 3.1 Create Session

Start a new ingestion session.

- **Endpoint**: `POST /pipe/sessions/`
- **Request Body**:
    ```json
    {
      "task_id": "agent-host-1:pipeline-a",
      "client_info": {
        "hostname": "agent-host-1",
        "version": "2.0.0"
      },
      "session_timeout_seconds": 30
    }
    ```
- **Response (200 OK)**:
    ```json
    {
      "session_id": "sess_uuid_v4",
      "role": "leader",  // or "follower"
      "session_timeout_seconds": 30,
      "message": "Session created successfully"
    }
    ```

### 3.2 Send Heartbeat

Keep session alive and receive role updates.

- **Endpoint**: `POST /pipe/sessions/{session_id}/heartbeat`
- **Request Body**:
    ```json
    {
      "can_realtime": true  // If Agent is ready for realtime sync
    }
    ```
- **Response (200 OK)**:
    ```json
    {
      "status": "ok",
      "role": "leader"  // Current role (may change)
    }
    ```
- **Error (419)**: Session obsolete (reconnect required).

### 3.3 Ingest Events

Send a batch of events.

- **Endpoint**: `POST /pipe/events/{session_id}`
- **Request Body**:
    ```json
    {
      "source_type": "message",  // "message" | "snapshot" | "audit"
      "is_end": false,           // True if this is the last batch of a phase
      "events": [
        {
          "event_type": "UPDATE",
          "index": 1678888888000,
          "rows": [
            {
              "path": "/data/file.txt",
              "size": 1024,
              "modified_time": 1678888880.0,
              "is_dir": false
            }
          ]
        }
      ]
    }
    ```
- **Response (200 OK)**:
    ```json
    {
      "status": "ok",
      "count": 1
    }
    ```

### 3.4 Consistency Signals

#### Sentinel Task
- **Endpoint**: `GET /pipe/consistency/sentinel/tasks`
- **Response**:
    ```json
    {
      "type": "suspect_check",
      "paths": ["/data/hot_file.tmp"]
    }
    ```

#### Sentinel Feedback
- **Endpoint**: `POST /pipe/consistency/sentinel/feedback`
- **Request Body**:
    ```json
    {
      "type": "suspect_update",
      "updates": [
        {
          "path": "/data/hot_file.tmp",
          "status": "exists",
          "mtime": 1678889999.0
        }
      ]
    }
    ```

#### Audit Signals
- **Start**: `POST /pipe/consistency/audit/start`
- **End**: `POST /pipe/consistency/audit/end`

---

## 4. Management API

### 4.1 Get View Tree

Query the current consistent view.

- **Endpoint**: `GET /views/{view_id}/tree`
- **Query Params**:
    - `path`: Directory path (default: `/`)
    - `depth`: Recursion depth (default: 1)
- **Response**:
    ```json
    {
      "name": "root",
      "path": "/",
      "children": [ ... ]
    }
    ```

### 4.2 Get View Stats

- **Endpoint**: `GET /views/{view_id}/stats`
- **Response**:
    ```json
    {
      "file_count": 1000,
      "dir_count": 50,
      "total_size": 1048576,
      "blind_spots": 0,
      "suspects": 2
    }
    ```

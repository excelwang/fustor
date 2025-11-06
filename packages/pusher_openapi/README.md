# FuAgent OpenAPI Pusher Driver

This package provides the OpenAPI pusher driver for FuAgent. It allows `fuagent` to send data events from any source to a custom, user-defined HTTP/S endpoint.

---

### **Developer Guide: Implementing a FuAgent-Compatible OpenAPI Service**

#### **1. Introduction**

This guide explains how to build a web service that can act as a data consumer for `fuagent`. The `pusher_openapi` driver in `fuagent` is designed to push data events to a custom HTTP endpoint that you provide. It operates in a "push" model, sending batches of events via `POST` requests.

Your primary task is to implement an HTTP API that can accept these requests, process the data, and optionally control the synchronization flow (e.g., by requesting a full data snapshot).

#### **2. Endpoint Requirements**

Your service must expose at least one, and optionally two, HTTP endpoints. The URLs for these endpoints are defined in your service's OpenAPI specification file (e.g., `openapi.json`), which you provide to `fuagent` during the pusher configuration.

##### **2.1 Data Ingestion Endpoint**

This is the primary endpoint for receiving data.

*   **HTTP Method**: `POST`
*   **Path**: The path is configurable in `fuagent`. The default is `/ingest-batch`. You can change this in the pusher's `driver_params` with the `batch_endpoint` key.
*   **Headers**: `fuagent` will send the following headers:
    *   `Content-Type`: `application/json`
    *   `task-id`: The unique identifier for the sync task.
    *   `x-api-key`: If using API Key authentication, this header will contain your secret key.
    *   `Authorization`:
        *   If using API Key auth, this will be `Bearer <your_key>`.
        *   If using Basic auth, this will be `Basic <base64(user:password)>`.

##### **2.2 Checkpoint/Status Endpoint (Optional, for Resumable Syncs)**

To allow `fuagent` to resume a sync from where it left off, you can implement a status endpoint. This greatly enhances the robustness and efficiency of data synchronization.

*   **How to Define**: In your OpenAPI spec file, add a custom `x-fuagent-status-endpoint` field to your `servers` object.
    ```json
    "servers": [
      {
        "url": "https://api.your-service.com/v1",
        "x-fuagent-status-endpoint": "/fuagent-sync-status"
      }
    ],
    ```
*   **HTTP Method**: `GET`
*   **Query Parameters**: `fuagent` will call this endpoint with two query parameters:
    *   `task_id`: The unique identifier for the sync task.
    *   `agent_id`: The unique identifier for the `fuagent` instance.
    You should store the last processed event `index` on a per-`task_id`/`agent_id` basis.
*   **Expected Response**:
    *   **Checkpoint Found**: Return a `200 OK` with a JSON body containing the last processed index. The format can be a direct integer (`1678886400000`) or a JSON object (`{"index": 1678886400000}`).
    *   **No Checkpoint**: If no checkpoint exists for the given task, return a `404 Not Found`. `fuagent` will then start the sync from the beginning.
    *   **Authentication**: This endpoint must use the same authentication as your data ingestion endpoint.

#### **3. Request Payload: Data Ingestion**

The body of the `POST` request to your ingestion endpoint will be a JSON object with the following structure:

```json
{
  "agent_id": "agent-c7a8b9...",
  "task_id": "mysql-to-my-api-sync",
  "is_snapshot_end": false,
  "snapshot_sync_suggested": false,
  "events": [
    {
      "id": 101,
      "name": "Alice",
      "email": "alice@example.com",
      "index": 1678886400000
    },
    {
      "id": 102,
      "name": "Bob",
      "email": "bob@example.com",
      "index": 1678886400001
    }
  ]
}
```

**Field Descriptions:**

*   `agent_id` (string): The ID of the `fuagent` instance sending the data.
*   `task_id` (string): The ID of the specific sync task.
*   `is_snapshot_end` (boolean): This will be `true` for a special, final batch sent at the end of a full snapshot sync. This batch will have an empty `events` array.
*   `snapshot_sync_suggested` (boolean): `true` if `fuagent` lost its place in the data stream and recommends you trigger a new snapshot to ensure data consistency.
*   `events` (array of objects): An array of data records.
    *   **IMPORTANT**: The `events` array contains the raw data rows directly. Information about the event type (`insert`, `update`, `delete`) is **not included** in the payload. Your endpoint must be designed to handle this, typically by performing "upsert" (update or insert) logic.
    *   Each event object may contain an `index` field, which is the unique, incremental identifier for that event from the source. You should use the maximum `index` in a batch to save as your checkpoint.

#### **4. Response Body: Controlling the Sync**

Your ingestion endpoint's response can control `fuagent`'s behavior.

*   **On Success**: Return a `200 OK` or `204 No Content` status code.
*   **Triggering a Snapshot**: To tell `fuagent` that you need a full data refresh, return a `200 OK` with the following JSON body:
    ```json
    {
      "snapshot_needed": true
    }
    ```
    `fuagent` will then initiate a full snapshot sync for this task. If you don't need a snapshot, you can return an empty body, `{}`, or `{"snapshot_needed": false}`.
*   **On Failure**: Return a standard HTTP error code. `fuagent` will attempt to retry on `5xx` server errors.

#### **5. `fuagent` Configuration Example**

Here is how a user would configure `fuagent` to use your service:

```yaml
# in config.yaml
pushers:
  my-openapi-service:
    driver: openapi
    endpoint: "https://api.your-service.com/openapi.json" # URL to your OpenAPI spec
    credential:
      key: "your-secret-api-key" # For ApiKeyCredential
    driver_params:
      batch_endpoint: "/ingest-batch" # Optional: overrides the default path
```

#### **6. Quickstart Example (Python/FastAPI)**

Here is a minimal server that implements the required endpoints.

```python
# main.py
from fastapi import FastAPI, Request, HTTPException, Query
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional

app = FastAPI()

# In-memory database to store checkpoints
# In a real application, use a persistent database (Redis, Postgres, etc.)
CHECKPOINTS: Dict[str, int] = {}

class IngestPayload(BaseModel):
    agent_id: str
    task_id: str
    is_snapshot_end: bool
    snapshot_sync_suggested: bool
    events: List[Dict[str, Any]]

@app.post("/ingest-batch")
async def ingest_data(payload: IngestPayload, request: Request):
    """
    Receives a batch of events from fuagent.
    """
    print(f"Received {len(payload.events)} events for task '{payload.task_id}'.")
    
    # --- Your Business Logic Here ---
    # Process the events. For example, upsert them into your database.
    if payload.events:
        # Find the highest 'index' in the batch to save as a checkpoint
        last_index = max(event.get("index", 0) for event in payload.events)
        if last_index > 0:
            checkpoint_key = f"{payload.agent_id}:{payload.task_id}"
            CHECKPOINTS[checkpoint_key] = last_index
            print(f"Saved checkpoint for '{checkpoint_key}': {last_index}")

    # Example: If fuagent suggests a snapshot, accept it.
    if payload.snapshot_sync_suggested:
        print("Snapshot suggested by fuagent, requesting one.")
        return {"snapshot_needed": True}

    # Default response
    return {"snapshot_needed": False}

@app.get("/fuagent-sync-status")
async def get_sync_status(agent_id: str = Query(...), task_id: str = Query(...)):
    """
    Provides the last known event index for a given agent and task.
    """
    checkpoint_key = f"{agent_id}:{task_id}"
    index = CHECKPOINTS.get(checkpoint_key)

    if index is None:
        print(f"No checkpoint found for '{checkpoint_key}'.")
        raise HTTPException(status_code=404, detail="Checkpoint not found.")
    
    print(f"Providing checkpoint for '{checkpoint_key}': {index}")
    return {"index": index}

# To run this example:
# uvicorn main:app --reload
```
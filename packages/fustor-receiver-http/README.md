# fustor-receiver-http

HTTP Receiver for Fustor Fusion - implements the transport layer for receiving events from Agents.

## Overview

This package provides an HTTP-based implementation of the `Receiver` transport abstraction. It creates FastAPI routers that handle session management and event ingestion.

## Installation

```bash
pip install fustor-receiver-http
```

## Usage

```python
from fustor_receiver_http import HTTPReceiver, SessionInfo

# Create receiver
receiver = HTTPReceiver(
    receiver_id="main-receiver",
    config={"session_timeout_seconds": 30}
)

# Register API keys for pipelines
receiver.register_api_key("fk_abc123", "pipeline-1")

# Register callbacks
async def on_session_created(session_id, task_id, pipeline_id, client_info):
    # Handle session creation
    return SessionInfo(
        session_id=session_id,
        task_id=task_id,
        pipeline_id=pipeline_id,
        role="leader",
        created_at=time.time(),
        last_heartbeat=time.time()
    )

async def on_event_received(session_id, events, source_type, is_end):
    # Process events
    return True

receiver.register_callbacks(
    on_session_created=on_session_created,
    on_event_received=on_event_received,
)

# Mount routers in FastAPI app
app.include_router(receiver.get_session_router(), prefix="/api/v1/pipe/session")
app.include_router(receiver.get_ingestion_router(), prefix="/api/v1/pipe/ingest")
```

## API Endpoints

### Session Router

- `POST /` - Create a new session
- `POST /{session_id}/heartbeat` - Send heartbeat
- `DELETE /{session_id}` - Terminate session

### Ingestion Router

- `POST /{session_id}/events` - Ingest event batch

## Entry Points

This package registers itself as:
- `fustor.receivers:http` - Receiver registry

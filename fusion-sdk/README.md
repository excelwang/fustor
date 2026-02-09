# Fustor Fusion SDK

This package provides the Software Development Kit (SDK) for interacting with the Fustor Fusion service. It enables programmatic event ingestion, session management, and **Consistency Lifecycle Control**.

## Features

*   **FusionClient**: A high-level async client for both V1 and **V2 (Pipe)** APIs.
*   **Session Lifecycle**: Simplified session creation, heartbeats, and role-aware communication.
*   **Consistency API**: Direct support for signaling Audit Start/End and Sentinel feedback.
*   **Automatic Sanitization**: Transparent handling of surrogate characters in file system paths.

## Installation

```bash
uv sync --package fustor-fusion-sdk
```

## Usage

### 1. Reliable Event Ingestion (V2 Pipe API)

The V2 API uses the `/api/v1/pipe` endpoint and is the recommended way for Agents to interact with Fusion.

```python
import asyncio
from fustor_fusion_sdk.client import FusionClient

async def ingest_demo():
    async with FusionClient(base_url="http://fusion:8102", api_key="secret") as client:
        # Create a session (Leader/Follower role is assigned by Fusion)
        session = await client.create_session(task_id="agent-001")
        session_id = session["session_id"]
        
        # Ingest events
        await client.push_events(
            session_id=session_id,
            events=[...],
            source_type="message" 
        )
```

### 2. Consistency Management

For administrative tasks or custom reconciliation loops, use the consistency API:

```python
# Signal the start of a global audit cycle for a specific view
await client.signal_audit_start(view_id="fs-view-1")

# ... process all audit data ...

# Signal completion to trigger missing item detection
await client.signal_audit_end(view_id="fs-view-1")
```

## Advanced Topics

### Leader/Follower Roles
The `FusionClient` automatically extracts role updates from Fusion responses. You can check the current role using `client.current_role` (if the session bridge supports it).

### Custom Sanitizers
If you need custom path sanitization, you can pass a `sanitizer` function to the `FusionClient` constructor.

## Dependencies

*   `fustor-core`: Shared models and event definitions.
*   `httpx`: Asynchronous HTTP client.

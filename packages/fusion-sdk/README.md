# fustor-fusion-sdk

This package provides a Software Development Kit (SDK) for interacting with the Fustor Fusion service. It offers a specialized client and set of interfaces to facilitate programmatic access and integration with Fusion's ingestion and consistency management APIs.

## Features

*   **FusionClient**: A robust async Python client for making requests to the Fustor Fusion REST API.
*   **API Versioning**: Supports both "legacy" (v1) and "pipe" (v2) API paths for smooth migration.
*   **Automatic Sanitization**: Handles surrogate character cleanup for robust JSON serialization of file system paths.
*   **Interfaces**: Defines abstract interfaces for consistent interaction with Fusion components.

## Installation

This package is part of the Fustor monorepo and is typically installed in editable mode within the monorepo's development environment using `uv sync`.

## Usage

Developers can use the `FusionClient` to send event data to Fusion, maintain sessions via heartbeats, and signal consistency events like audit cycles.

### Example: Ingesting Data

```python
import asyncio
from fustor_fusion_sdk.client import FusionClient

async def main():
    # Initialize the client (v2 pipe API is recommended)
    async with FusionClient(
        base_url="http://localhost:8102", 
        api_key="your-secret-key",
        api_version="pipe"
    ) as client:
        
        # 1. Create a session for a task
        session = await client.create_session(task_id="research-pipeline")
        if not session:
            return
            
        session_id = session["session_id"]
        
        # 2. Push events
        events = [
            {"path": "/data/file1.txt", "modified_time": 1678886400, "rows": [{"action": "update"}]}
        ]
        success = await client.push_events(
            session_id=session_id,
            events=events,
            source_type="message"
        )
        
        if success:
            print("Successfully pushed events")
            
        # 3. Maintain session
        await client.send_heartbeat(session_id)

if __name__ == "__main__":
    asyncio.run(main())
```

## Dependencies

*   `fustor-common`: Provides shared enums and base models.
*   `httpx`: For efficient asynchronous HTTP requests.

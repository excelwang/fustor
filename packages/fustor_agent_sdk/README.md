# fustor-agent-sdk

This package provides a Software Development Kit (SDK) for interacting with the Fustor Agent service. It offers a set of interfaces, data models, and utility functions to facilitate programmatic access and integration with the Agent's functionalities.

## Features

*   **Interfaces**: Defines abstract interfaces for various components of the Fustor Agent, allowing for consistent interaction patterns.
*   **Models**: Provides Pydantic data models for requests, responses, and other data structures used by the Fustor Agent API.
*   **Utilities**: Includes helper functions and classes to simplify common tasks when working with the Fustor Agent.

## Installation

This package is part of the Fustor monorepo and is typically installed in editable mode within the monorepo's development environment using `uv sync`.

## Usage

Developers can use this SDK to build custom applications or integrations that need to communicate with the Fustor Agent service. It abstracts away the underlying HTTP calls and data serialization, providing a more Pythonic way to interact with the Agent.

Example (conceptual):

```python
from fustor_agent_sdk.interfaces import AgentClient
from fustor_agent_sdk.models import SyncTaskConfig

# Assuming AgentClient is implemented and configured
client = AgentClient(...)

# Example: Create a new sync task
new_task = SyncTaskConfig(
    task_id="my-new-task",
    source_id="my-source",
    pusher_id="my-pusher",
    field_mappings={"source_field": "target_field"}
)
response = client.create_sync_task(new_task)
print(response)
```

## Dependencies

*   `fustor-core`: Provides foundational elements and shared components.
*   `fustor-registry-client`: (If applicable) Used for interacting with the Fustor Registry service, which might be a dependency for Agent configuration.

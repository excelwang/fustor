# Fustor Agent SDK

This package provides the Software Development Kit (SDK) for extending and interacting with the Fustor Agent service. It focuses on the **V2 Pipe-centric architecture**, enabling developers to build custom data sources and delivery mechanisms.

## Conceptual Overview (V2 Architecture)

In the V2 architecture, the Fustor Agent operates through **Pipes**. A pipe coordinates the flow of data from a **Source** to a **Sender**.

```mermaid
graph LR
    Source[Source Driver] -- Events --> Bus[Event Bus]
    Bus -- Polled Events --> Pipe[Agent Pipe]
    Pipe -- Batched Events --> Sender[Sender Handler]
    Sender -- HTTP/gRPC --> Fusion[Fustor Fusion]
```

## Features

*   **Pipe Interfaces**: Standardized abstract classes for `AgentPipe`, `SourceDriver`, and `SenderHandler`.
*   **Driver Framework**: Tools and base classes to implement custom source drivers (e.g., MySQL, OSS, custom APIs).
*   **Sender Framework**: Robust handlers for delivering data to various destinations, supporting retry logic and role management (Leader/Follower).
*   **Configuration SDK**: Specialized Pydantic models and services to manage dynamic pipe configurations.

## Installation

```bash
uv sync --package fustor-agent-sdk
```

## Usage

### 1. Implementing a Custom Source Driver

To add a new data source, implement the `SourceDriver` interface:

```python
from fustor_core.drivers import SourceDriver
from fustor_core.event import EventBase

class MyCustomSource(SourceDriver):
    def get_event_iterator(self, **kwargs):
        # Your logic to fetch changes from the source
        for change in self._fetch_external_changes():
             yield EventBase(...)
```

### 2. Programmatic Pipe Management

```python
from fustor_agent_sdk.interfaces import PipeConfigServiceInterface
from fustor_core.models.config import PipeConfig

async def register_pipe(service: PipeConfigServiceInterface):
    config = PipeConfig(
        source_uri="custom://my-stream",
        sender_id="fusion-main",
        enabled=True
    )
    await service.add_config(id="stream-1", config=config)
```

## Extensibility

The SDK is designed to be modular. You can replace the default `EventBus` or `Persistence` layer by implementing the corresponding interfaces provided in `fustor_agent_sdk.interfaces`.

## Dependencies

*   `fustor-core`: Foundational models and core synchronization logic.
*   `fustor-common`: Shared utilities and constants.

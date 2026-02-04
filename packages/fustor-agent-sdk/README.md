# Fustor Agent SDK

This package provides the Software Development Kit (SDK) for extending and interacting with the Fustor Agent service. It focuses on the **V2 Pipeline-centric architecture**, enabling developers to build custom data sources and delivery mechanisms.

## Conceptual Overview (V2 Architecture)

In the V2 architecture, the Fustor Agent operates through **Pipelines**. A pipeline coordinates the flow of data from a **Source** to a **Sender**.

```mermaid
graph LR
    Source[Source Driver] -- Events --> Bus[Event Bus]
    Bus -- Polled Events --> Pipeline[Agent Pipeline]
    Pipeline -- Batched Events --> Sender[Sender Handler]
    Sender -- HTTP/gRPC --> Fusion[Fustor Fusion]
```

## Features

*   **Pipeline Interfaces**: Standardized abstract classes for `AgentPipeline`, `SourceDriver`, and `SenderHandler`.
*   **Driver Framework**: Tools and base classes to implement custom source drivers (e.g., MySQL, OSS, custom APIs).
*   **Sender Framework**: Robust handlers for delivering data to various destinations, supporting retry logic and role management (Leader/Follower).
*   **Configuration SDK**: Specialized Pydantic models and services to manage dynamic pipeline configurations.

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

### 2. Programmatic Pipeline Management

```python
from fustor_agent_sdk.interfaces import PipelineConfigServiceInterface
from fustor_core.models.config import PipelineConfig

async def register_pipeline(service: PipelineConfigServiceInterface):
    config = PipelineConfig(
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

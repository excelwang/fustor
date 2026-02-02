# fustor-agent-sdk

This package provides a Software Development Kit (SDK) for interacting with the Fustor Agent service. It offers a set of interfaces, data models, and utility functions to facilitate programmatic access and integration with the Agent's functionalities.

## Features

*   **Interfaces**: Defines abstract interfaces for various components of the Fustor Agent (Configs, Drivers, Instances), allowing for consistent interaction patterns across different implementations.
*   **Models**: Provides Pydantic data models for configuration and state management, building upon `fustor-core`.
*   **Utilities**: Includes helper functions and classes to simplify common tasks when working with the Fustor Agent runtime.

## Installation

This package is part of the Fustor monorepo and is typically installed in editable mode within the monorepo's development environment using `uv sync`.

## Usage

Developers can use this SDK to build custom applications, new drivers, or integrations that need to communicate with or extend the Fustor Agent service. It abstracts away the complex internal logic of event bus management and data serialization.

### Example: Working with Pipeline Configurations

```python
from fustor_core.models.config import PipelineConfig, FieldMapping
from fustor_agent_sdk.interfaces import PipelineConfigServiceInterface

# Assume service is initialized in your application
async def setup_pipeline(service: PipelineConfigServiceInterface):
    # Define a new pipeline configuration
    config = PipelineConfig(
        source="research-db",
        sender="fusion-http",
        disabled=False,
        fields_mapping=[
            FieldMapping(source=["uuid"], to="id", required=True),
            FieldMapping(source=["content"], to="data")
        ]
    )
    
    # Register the pipeline
    await service.add_config(id="research-pipeline", config=config)
    print("Pipeline registered successfully")
```

## Dependencies

*   `fustor-core`: Provides foundational elements, common models, and shared components.

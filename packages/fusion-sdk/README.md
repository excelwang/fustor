# fustor-fusion-sdk

This package provides a Software Development Kit (SDK) for interacting with the Fustor Fusion service. It offers a client and interfaces to facilitate programmatic access and integration with the Fusion service's functionalities, such as data ingestion and processing.

## Features

*   **Client**: A Python client for making requests to the Fustor Fusion API.
*   **Interfaces**: Defines abstract interfaces for various components of the Fustor Fusion service, allowing for consistent interaction patterns.

## Installation

This package is part of the Fustor monorepo and is typically installed in editable mode within the monorepo's development environment using `uv sync`.

## Usage

Developers can use this SDK to build custom applications or integrations that need to communicate with the Fustor Fusion service. It simplifies the process of sending data to Fusion and interacting with its processing capabilities.

Example (conceptual):

```python
from fustor_fusion_sdk.client import FusionClient
from fustor_fusion_sdk.models import IngestDataRequest

# Assuming FusionClient is initialized with the Fusion service URL
client = FusionClient(base_url="http://localhost:8102")

# Example: Ingest data
data_to_ingest = IngestDataRequest(
    session_id="some-session-id",
    events=[{"key": "value", "timestamp": 1678886400}]
)
response = client.ingest_data(data_to_ingest)
print(response)
```

## Dependencies

*   `fustor-common`: Provides foundational elements and shared components.

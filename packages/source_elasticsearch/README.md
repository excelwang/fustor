# fustor-source-elasticsearch

This package provides a `SourceDriver` implementation for the Fustor Agent service, enabling it to extract data from Elasticsearch. It supports both snapshot (historical) and message (real-time) data synchronization, leveraging Elasticsearch's Point-In-Time (PIT) and search APIs.

## Features

*   **Snapshot Synchronization**: Extracts historical data from an Elasticsearch index using Point-In-Time (PIT) for consistent snapshots.
*   **Real-time Message Synchronization**: Continuously polls for new or updated documents based on a timestamp field, providing a real-time data stream.
*   **Connection Management**: Handles connection to Elasticsearch using various authentication methods (Basic Auth, API Key).
*   **Privilege Checking**: Verifies user privileges for accessing the specified Elasticsearch index.
*   **Field Discovery**: Dynamically discovers available fields from the Elasticsearch index mapping.
*   **Shared Instance Model**: Optimizes resource usage by sharing Elasticsearch client instances for identical configurations.
*   **Wizard Definition**: Provides a comprehensive configuration wizard for UI integration, guiding users through connection and index setup.

## Installation

This package is part of the Fustor monorepo and is typically installed in editable mode within the monorepo's development environment using `uv sync`. It is registered as a `fustor_agent.drivers.sources` entry point.

## Usage

To use the `fustor-source-elasticsearch` driver, configure a Source in your Fustor Agent setup with the driver type `elasticsearch`. You will need to provide the Elasticsearch URI, credentials, and the target index name along with a timestamp field for real-time tracking.

Example (conceptual configuration in Fustor Agent):

```yaml
# ~/.fustor/config.yaml
sources:
  my-es-source:
    driver_type: elasticsearch
    uri: https://your-elasticsearch-host:9200
    credential:
      type: api_key
      key: YOUR_ELASTICSEARCH_API_KEY
    driver_params:
      index_name: my_data_index
      timestamp_field: "@timestamp" # Field used for real-time tracking
```

## Dependencies

*   `elasticsearch`: The official Python client for Elasticsearch.
*   `fustor-core`: Provides the `SourceDriver` abstract base class and other core components.
*   `fustor-event-model`: Provides `EventBase` for event data structures.
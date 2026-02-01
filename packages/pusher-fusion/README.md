# fustor-pusher-fusion

This package provides a `PusherDriver` implementation for the Fustor Agent service, enabling it to push data to the Fustor Fusion service. It leverages the `fustor-fusion-sdk` to interact with the Fusion API, handling session management, event pushing, and heartbeats.

## Features

*   **Fusion Integration**: Seamlessly pushes events from Fustor Agent to Fustor Fusion.
*   **Session Management**: Manages the session lifecycle with the Fusion service, including session creation and periodic heartbeats.
*   **Event Pushing**: Batches and sends event data to the Fusion service's ingestion endpoint.
*   **Leverages `fustor-fusion-sdk`**: Utilizes the official SDK for robust and consistent communication with Fusion.

## Installation

This package is part of the Fustor monorepo and is typically installed in editable mode within the monorepo's development environment using `uv sync`. It is registered as a `fustor_agent.drivers.pushers` entry point.

## Usage

To use the `fustor-pusher-fusion` driver, configure a Pusher in your Fustor Agent setup with the driver type `fusion`. You will need to provide the Fusion service endpoint and an API key for authentication.

Example (conceptual configuration in Fustor Agent):

```yaml
# Fustor 主目录下的 agent-config.yaml
pushers:
  my-fusion-pusher:
    driver_type: fusion
    endpoint: http://localhost:8102/ingestor-api/v1/
    credential:
      type: api_key
      key: YOUR_FUSION_API_KEY
```

## Dependencies

*   `fustor-core`: Provides the `PusherDriver` abstract base class and other core components.
*   `fustor-fusion-sdk`: The SDK for interacting with the Fustor Fusion service.
*   `fustor-event-model`: Provides `EventBase` for event data structures.
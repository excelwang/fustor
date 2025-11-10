# fustor-pusher-echo

This package provides an "echo" pusher driver for the Fustor Agent service. It serves as a basic example and debugging tool for `PusherDriver` implementations. Instead of pushing data to an external system, it simply logs all received events and control flags to the Fustor Agent's log output.

## Features

*   **Echo Functionality**: Logs all incoming events, including `realtime` and `snapshot` data, to the console/log.
*   **Control Flag Visibility**: Displays control flags like `is_snapshot_end` and `snapshot_sync_suggested` for debugging data flow.
*   **Session Management**: Implements `create_session` and `heartbeat` to demonstrate session lifecycle.
*   **No Configuration Needed**: The `get_needed_fields` method returns an empty schema, indicating it accepts all fields without specific requirements.
*   **Wizard Definition**: Provides a simple wizard step for UI integration.

## Installation

This package is part of the Fustor monorepo and is typically installed in editable mode within the monorepo's development environment using `uv sync`. It is registered as a `fustor_agent.drivers.pushers` entry point.

## Usage

To use the `fustor-pusher-echo` driver, configure a Pusher in your Fustor Agent setup with the driver type `echo`. When a sync task is configured to use this pusher, all data processed by the Agent will be logged by this driver.

This driver is particularly useful for:
*   **Debugging**: Understanding the exact data and control signals being sent by the Fustor Agent.
*   **Development**: As a template for creating new `PusherDriver` implementations.
*   **Testing**: Verifying that the Fustor Agent's data pipeline is correctly delivering events.

Example (conceptual configuration in Fustor Agent):

```yaml
# ~/.fustor/config.yaml
pushers:
  my-echo-pusher:
    driver_type: echo
    # No specific configuration parameters needed for the echo driver
```

## Dependencies

*   `fustor-core`: Provides the `PusherDriver` abstract base class and other core components.
*   `fustor-event-model`: Provides `EventBase` for event data structures.
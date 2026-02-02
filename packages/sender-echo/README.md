# fustor-sender-echo

This package provides an "echo" sender driver for the Fustor Agent service. It serves as a basic example and debugging tool for `SenderDriver` implementations. Instead of sending data to an external system, it simply logs all received events and control flags to the Fustor Agent's log output.

## Features

*   **Echo Functionality**: Logs all incoming events, including `realtime` and `snapshot` data, to the console/log.
*   **Control Flag Visibility**: Displays control flags like `is_snapshot_end` and `snapshot_sync_suggested` for debugging data flow.
*   **Session Management**: Implements `create_session` and `heartbeat` to demonstrate session lifecycle.
*   **No Configuration Needed**: The `get_needed_fields` method returns an empty schema, indicating it accepts all fields without specific requirements.
*   **Wizard Definition**: Provides a simple wizard step for UI integration.

## Installation

This package is part of the Fustor monorepo and is typically installed in editable mode within the monorepo's development environment using `uv sync`. It is registered as a `fustor_agent.drivers.senders` entry point.

## Usage

To use the `fustor-sender-echo` driver, configure a Sender in your Fustor Agent setup with the driver type `echo`. When a pipeline involves this sender, all data processed by the Agent will be logged by this driver.

This driver is particularly useful for:
*   **Debugging**: Understanding the exact data and control signals being sent by the Fustor Agent.
*   **Development**: As a template for creating new `SenderDriver` implementations.
*   **Testing**: Verifying that the Fustor Agent's data pipeline is correctly delivering events.

Example (conceptual configuration in Fustor Agent):

```yaml
# Fustor 主目录下的 agent-config.yaml
senders:
  my-echo-sender:
    driver_type: echo
    # No specific configuration parameters needed for the echo driver
```

## Dependencies

*   `fustor-core`: Provides the `SenderDriver` abstract base class and other core components.
*   `fustor-event-model`: Provides `EventBase` for event data structures.
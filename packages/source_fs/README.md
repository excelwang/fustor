# fustor-source-fs

This package provides a `SourceDriver` implementation for the Fustor Agent service, enabling it to monitor and extract data from local file systems. It employs a "Smart Dynamic Monitoring" strategy to efficiently handle large directory structures, supporting both snapshot (initial scan) and real-time (event-driven) synchronization of file changes.

## Features

*   **File System Monitoring**: Utilizes `watchdog` to detect file creation, modification, deletion, and movement events in real-time.
*   **Smart Dynamic Monitoring**: Implements a sophisticated strategy to monitor large directory trees efficiently, including:
    *   **Capacity-aware scheduling**: Prioritizes monitoring of "hot" (recently modified) directories.
    *   **LRU eviction**: Automatically evicts least recently used directory watches to stay within system limits.
    *   **Adaptive limits**: Adjusts monitoring limits dynamically based on system feedback.
*   **Snapshot Synchronization**: Performs an initial scan of the configured directory to capture existing files as a snapshot.
*   **Real-time Message Synchronization**: Delivers file system events (create, update, delete) as they occur.
*   **Shared Instance Model**: Optimizes resource usage by sharing `_WatchManager` instances for identical configurations.
*   **Connection & Privilege Checking**: Verifies path existence, readability, and execution permissions.
*   **Wizard Definition**: Provides a configuration wizard for UI integration, guiding users through path setup and monitoring parameters.
*   **Transient Source**: Identified as a transient source, meaning events are lost if not processed immediately, leading to specific back-pressure handling.

## Installation

This package is part of the Fustor monorepo and is typically installed in editable mode within the monorepo's development environment using `uv sync`. It is registered as a `fustor_agent.drivers.sources` entry point.

## Usage

To use the `fustor-source-fs` driver, configure a Source in your Fustor Agent setup with the driver type `fs`. You will need to provide the absolute path to the directory you wish to monitor.

Example (conceptual configuration in Fustor Agent):

```yaml
# Fustor 主目录下的 agent-config.yaml
sources:
  my-fs-source:
    driver_type: fs
    uri: /path/to/your/monitored/directory
    driver_params:
      min_monitoring_window_days: 7 # Ensure directories are monitored for at least 7 days
      max_sync_delay_seconds: 0.5 # Max delay for real-time events
```

## Dependencies

*   `watchdog`: Python library to monitor file system events.
*   `fustor-core`: Provides the `SourceDriver` abstract base class and other core components.
*   `fustor-event-model`: Provides `EventBase` for event data structures.

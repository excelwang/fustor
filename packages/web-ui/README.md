# fustor-web-ui

This package provides the web-based user interface (UI) for the Fustor Agent service. It offers a graphical way to configure, manage, and monitor data synchronization tasks, making the Fustor Agent more accessible and user-friendly.

## Features

*   **Graphical Configuration**: Intuitive forms and wizards for setting up data sources, pushers, and sync tasks.
*   **Real-time Monitoring**: Dashboard and logs for observing the status and performance of running sync tasks.
*   **Management**: Start, stop, and manage sync task instances and configurations.
*   **Integrated with Fustor Agent**: Designed to work seamlessly with the Fustor Agent's API.

## Installation

This package is part of the Fustor monorepo and is typically installed in editable mode within the monorepo's development environment using `uv sync`.

## Usage

The `fustor-web-ui` is usually served by the Fustor Agent service itself. Once the Fustor Agent is running, you can access the web UI through your web browser at the Agent's configured address (e.g., `http://localhost:8103`).

This UI provides:
*   A **Dashboard** for an overview of system health and running tasks.
*   **Configuration pages** for Sources, Pushers, and Sync Tasks.
*   A **Logs** viewer for detailed system logs.

## Dependencies

*   `fustor-core`: Provides foundational elements and shared components.
# fustor-source-mysql

This package provides a `SourceDriver` implementation for the Fustor Agent service, enabling it to extract data from MySQL databases. It supports both consistent snapshot (historical) and real-time change data capture (CDC) via MySQL's binary log.

## Features

*   **Consistent Snapshot Synchronization**: Performs a consistent snapshot of tables using `START TRANSACTION WITH CONSISTENT SNAPSHOT` to capture historical data.
*   **Real-time Change Data Capture (CDC)**: Streams real-time data changes (INSERT, UPDATE, DELETE) from MySQL's binary log using `pymysqlreplication`.
*   **Binlog Position Tracking**: Manages and checks binlog positions for resuming streams and determining data availability.
*   **Connection Management**: Handles connection to MySQL using username/password credentials.
*   **Runtime Parameter Validation**: Checks essential MySQL global variables like `log_bin` and `binlog_format` to ensure proper CDC setup.
*   **Agent User Management**: Provides functionality to create a dedicated agent user with necessary replication and select privileges.
*   **Privilege Checking**: Verifies that the agent user has the required permissions.
*   **Field Discovery**: Dynamically discovers available fields (columns) from MySQL schemas.
*   **Shared Instance Model**: Optimizes resource usage by sharing MySQL client instances for identical configurations.
*   **Wizard Definition**: Provides a comprehensive configuration wizard for UI integration, guiding users through connection, runtime checks, and agent user setup.

## Installation

This package is part of the Fustor monorepo and is typically installed in editable mode within the monorepo's development environment using `uv sync`. It is registered as a `fustor_agent.drivers.sources` entry point.

## Usage

To use the `fustor-source-mysql` driver, configure a Source in your Fustor Agent setup with the driver type `mysql`. You will need to provide the MySQL URI (host:port) and credentials for both an administrative user (for setup and checks) and a dedicated agent user (for data extraction).

Example (conceptual configuration in Fustor Agent):

```yaml
# Fustor 主目录下的 agent-config.yaml
sources:
  my-mysql-source:
    driver_type: mysql
    uri: localhost:3306
    admin_creds: # Used for initial setup and checks, not saved
      user: admin_user
      passwd: admin_password
    credential: # Dedicated agent user for data extraction, saved
      user: fustor_agent_user
      passwd: agent_password
```

## Dependencies

*   `aiomysql`: Asynchronous MySQL client for Python.
*   `mysql-replication`: Library for reading MySQL binary logs.
*   `fustor-core`: Provides the `SourceDriver` abstract base class and other core components.
*   `fustor-event-model`: Provides `EventBase` for event data structures.
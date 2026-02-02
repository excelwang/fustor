# Fustor Documentation

Welcome to the Fustor documentation. Fustor is a high-reliability, consistency-aware data synchronization platform designed for NFS, S3, MySQL CDC, and other data sources. It can sync data from multiple sources to a central Fusion service, which then serves as a single source of truth for the data, using various views like dir tree and graph

## Core Concepts

- **Pipeline**: The fundamental unit of synchronization, connecting a **Source** to a **Sender**.
- **Source**: A data origin (e.g., local FS, MySQL CDC, S3).
- **Sender**: A data destination (e.g., Fusion API, S3, HTTP Hook).
- **View**: A materialized state of the data in the destination system (Fusion).
- **Agent**: The process that runs pipelines and captures local events.
- **Fusion**: The central state manager that performs arbitration and serves the data.

## Documentation Index

### Architecture & Design
- [V2 Architecture Overview](../specifications/01-ARCHITECTURE.md): The core design principles of Fustor V2.
- [Consistency Design](../specifications/02-CONSISTENCY_DESIGN.md): Details on Smart Merge, Tombstones, and Audit cycles.
- [Logical Clock Design](../specifications/03-LOGICAL_CLOCK_DESIGN.md): How we handle event ordering and skew in distributed environments.

### Developer Guides
- [Migration Guide (V1 to V2)](./migration-guide.md): Steps to upgrade your configuration and code.
- [Reliability & Error Handling](./reliability.md): Configuring retry policies, backoffs, and timeouts.
- [3rd-Party Developers Guide](./3rd-party-developers-guide/README.md): How to build your own Sources and Senders.

### Internals
- [Internal Architectures](./internals/README.md): Deep dives into specific subsystem implementations.

## Getting Started

To get started with development, please ensure you have `uv` installed and run:

```bash
uv sync --all-packages
```

Refer to the root `README.md` for environmental setup and running tests.

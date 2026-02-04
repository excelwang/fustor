# Glossary

> **Supreme Authority**: This file defines the ubiquitous language for the project. All code, docs, and communication must align with these definitions.

## Terms

- **Agent**: The client-side component of Fustor responsible for collecting data.
- **Fusion**: The server-side component of Fustor responsible for receiving and processing data.
- **Source**: Layer 3 component in Agent; implementation for reading data (e.g., from FS, OSS).
- **Sender**: Layer 2 component in Agent; handles transport (e.g., HTTP, gRPC).
- **Pipeline**:
    - **Agent Pipeline**: Binds a Source to a Sender.
    - **Fusion Pipeline**: Binds a Receiver to one or more Views.
- **View**: Layer 3 component in Fusion; implementation for processing events and updating the view.
- **Receiver**: Layer 2 component in Fusion; handles receiving data via transport protocols.
- **Session**: A logical connection between an Agent Pipeline and a Fusion Pipeline.
- **EventBus**: High-throughput, low-latency message synchronization mechanism within Agent Pipeline.
- **LogicalClock**: Universal time arbitration mechanism for consistency.
- **Schema**: Layer 0 component defining the contract/model of data (e.g., `fustor-schema-fs`).

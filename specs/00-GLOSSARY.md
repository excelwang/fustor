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

## Consistency Terms

- **Audit**: Periodic scan by the Leader Agent to detect changes missed by real-time monitoring (e.g., blind spots).
- **Audit Sync**: The phase where the Agent sends audit events.
- **Snapshot Sync**: Initial full scan performed by the Agent on startup or role promotion.
- **Realtime Sync**: Continuous event streaming triggered by file system watchers (inotify).
- **Leader**: The primary Agent responsible for heavy consistency tasks (Snapshot, Audit, Sentinel).
- **Follower**: Secondary Agents that only stream Realtime events to minimize IO load.
- **Blind-spot**: A file/directory change that occurred on a node without an Agent, detected only via Audit.
- **Suspect**: A file ("Hot File") whose mtime is close to the Logical Clock watermark, indicating potential instability or cache lag.
- **Tombstone**: A marker for a deleted file to prevent it from "reincarnating" due to stale Snapshot/Audit data.
- **Sentinel**: A lightweight background task that verifying the stability of Suspect files.

## Workflow Terms

- **Workstream**: A long-running effort (epic) containing multiple tickets.
- **Ticket**: A specific unit of work (Task/Story/Bug) with defined requirements.
- **Status**: The current state of a Ticket (Backlog, Active, Done).

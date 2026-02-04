# Design Goals

> **Mission**: To build a decoupled, symmetric, and scalable distributed synchronization system.

## Core Principles

1.  **Total Decoupling**:
    - Agent and Fusion must be completely independent.
    - Third parties can use either side in isolation.

2.  **Symmetry**:
    - Every concept in Agent has a corresponding concept in Fusion.
    - Source <-> View
    - Sender <-> Receiver
    - Pipeline <-> Pipeline

3.  **Clear Layering**:
    - Strict separation of concerns (Layer 0 to Layer 5).
    - Reference Netty architecture.

4.  **Extensibility**:
    - Support multiple protocols (HTTP, gRPC).
    - Support multiple Schemas (FS, Database, etc.).

## Non-Functional Requirements

- **Performance**: High throughput using EventBus.
- **Consistency**: Strong consistency via LogicalClock and arbitration.
- **Reliability**: Session management, heartbeats, and fault tolerance.

# Design Goals & Project Scope

> **Mission**: To provide a **Real-time, Consistent, and Unified View** of distributed file systems (like NFS) by bridging the gap between distributed storage and client-side activity.

## 1. User Value & Scope

### 1.1 The Problem
In distributed storage environments (e.g., NFS shared across 100+ servers), users face:
- **Visibility Latency**: "I wrote a file on Server A, but Server B doesn't see it yet."
- **Inconsistency**: "Is this file actually deleted, or is it just a cache ghost?"
- **Monitoring Blind Spots**: "Who deleted this file? Was it a node without monitoring?"

### 1.2 Our Solution (The "Happy User" State)
Fustor solves this by deploying lightweight **Agents** on client nodes that stream file events to a central **Fusion** server.

**Target User Experience:**
1.  **"It Just Works"**: Install Agent, point to NFS, and see real-time updates in the dashboard.
2.  **Trustworthy Data**: The system explicitly handles consistency conflicts. If Fustor says a file exists, it exists and is complete.
    - **Integrity Guarantee**: Files appearing in the unified file tree must be integral (complete).
    - **Suspect Identification**: Files currently being written or in an unstable state must be explicitly marked as **Suspect** (integrity_suspect=True).
3.  **Resilience**: If a node crashes or network fails, Fustor self-heals without operator intervention.

### 1.3 Scope Boundaries
- **In Scope**:
    - Real-time metadata synchronization (file existence, size, mtime).
    - Consistency arbitration between multiple clients.
    - Discovery of **structural changes** in Blind-spots (Add/Delete/Rename).
- **Out of Scope**:
    - Data content synchronization (we sync metadata, not file contents).
    - **Content Updates in Blind-spots**: Modifications to existing files in unmonitored areas are NOT guaranteed to be detected immediately.
    - Distributed locking or transaction management.
    - Replacement for the underlying storage.

---

## 2. Technical Design Goals

### 2.1 Core Principles

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

5.  **Configuration Simplicity (No-DB)**:
    - **File-Based**: Source, Receiver, Sender, View, and Pipeline configurations must be stored in independent files (YAML), editable by users. No internal database.
    - **Hot Patching**: Support `add-config` / `del-config` commands to patch configuration at runtime without full restart.
    - **Enabled-Driven**: Runtime execution is strictly controlled by the `enabled` flag in the configuration. `start/stop` commands control the global lifecycle.

### 2.2 Non-Functional Requirements

- **Performance**: High throughput using EventBus.
- **Consistency**: Strong consistency via LogicalClock and arbitration.
- **Reliability**: Session management, heartbeats, and fault tolerance.
- **Efficiency**: Determine file integrity as efficiently as possible.
- **Coverage**: **Structural Consistency**. Ensure the file tree structure matches the filesystem (100% Add/Delete detection). Content freshness (modifications) in blind-spots is **Best-Effort** and may be sacrificed for efficiency.

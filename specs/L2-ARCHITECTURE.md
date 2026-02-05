---
layer: 2
id: ARCH
version: 1.0.0
requires:
  - CONTRACTS.TOMBSTONE_PROTECTION
  - CONTRACTS.MTIME_ARBITRATION
  - CONTRACTS.LEADER_UNIQUENESS
  - CONTRACTS.EVENTUAL_CONSISTENCY
exports:
  - ARCH.CORE
  - ARCH.PIPELINE
  - ARCH.SESSION
  - ARCH.VIEW
  - ARCH.ARBITRATOR
  - ARCH.LOGICAL_CLOCK
  - ARCH.SOURCE
  - ARCH.SENDER
  - ARCH.RECEIVER
components:
  - Core
  - Pipeline
  - Session
  - View
  - Arbitrator
  - LogicalClock
  - Source
  - Sender
  - Receiver
---

# L2: Architecture (Component Topology)

> **Purpose**: Define system components, their responsibilities, and data flow.

## 1. Design Principles

Per VISION requirements:
- **VISION.DECOUPLING**: Agent and Fusion are independent packages.
- **VISION.SYMMETRY**: Agent(Source→Sender) ↔ Fusion(Receiver→View).
- **VISION.LAYERING**: 6 layers (L0 Schema → L5 Application).

---

## 2. Component Registry

### ARCH.CORE
**Layer**: 1 (Core Abstractions)
**Packages**: `fustor-core`
**Responsibility**: Event models, Pipeline ABC, Transport ABC, LogicalClock.

### ARCH.PIPELINE
**Layer**: 4 (Pipeline Engine)
**Packages**: `fustor-agent`, `fustor-fusion`
**Responsibility**: Lifecycle management, Source→Sender/Receiver→View binding.
**L3 Coverage**:
- API: Pipeline lifecycle endpoints
- Algo: Batch processing, throttling
- Runtime: Start/Stop, Crash recovery

### ARCH.SESSION
**Layer**: 4 (Pipeline Engine)
**Packages**: `fustor-fusion`
**Responsibility**: Agent-Fusion binding, Leader election, Heartbeat.
**Supports**: CONTRACTS.LEADER_UNIQUENESS
**L3 Coverage**:
- API: `/pipe/sessions/`
- Algo: FCFS election, Fastest Follower promotion
- Runtime: Failover, Timeout handling

### ARCH.VIEW
**Layer**: 3 (Handler)
**Packages**: `fustor-view-fs`
**Responsibility**: Unified file tree, Event processing, Consistency state.
**Supports**: CONTRACTS.EVENTUAL_CONSISTENCY
**L3 Coverage**:
- API: `/views/{id}/tree`
- Algo: Smart Merge
- Runtime: Audit lifecycle hooks

### ARCH.ARBITRATOR
**Layer**: 3 (Handler)
**Packages**: `fustor-view-fs`
**Responsibility**: Event acceptance/rejection based on mtime, tombstone.
**Supports**: CONTRACTS.TOMBSTONE_PROTECTION, CONTRACTS.MTIME_ARBITRATION
**L3 Coverage**:
- Algo: Tombstone guard, Mtime arbitration, Parent mtime check

### ARCH.LOGICAL_CLOCK
**Layer**: 1 (Core)
**Packages**: `fustor-core`
**Responsibility**: Watermark calculation, Skew correction.
**L3 Coverage**:
- Algo: Mode Skew, Watermark formula
- Runtime: Skew sample collection

### ARCH.SOURCE
**Layer**: 3 (Handler)
**Packages**: `fustor-source-fs`
**Responsibility**: FS monitoring, Event generation (inotify + audit).
**L3 Coverage**:
- Algo: Hot Watch Set, True Silence
- Runtime: Watcher lifecycle, Audit scheduling

### ARCH.SENDER / ARCH.RECEIVER
**Layer**: 2 (Transport)
**Packages**: `fustor-sender-http`, `fustor-receiver-http`
**Responsibility**: HTTP transport, Batching, Retry.
**L3 Coverage**:
- API: POST /pipe/events
- Runtime: Backoff, Connection pooling

---

## 3. Data Flow

```
Agent Side                          Fusion Side
───────────                         ───────────
[Source]                            [Receiver]
  │ events                               │
  ▼                                      ▼
[Pipeline] ──batch──> [Sender]══════>[Receiver] ──> [Pipeline]
                                                        │
                                                        ▼
                                                   [Session]
                                                        │
                                                        ▼
                                                     [View]
                                                        │
                                                        ▼
                                                  [Arbitrator]
```

---

## 4. Package Diagram

```
fustor-core          (L1: Abstractions)
   ├── fustor-schema-fs     (L0: Data Models)
   │
   ├── fustor-source-fs     (L3: Agent Handler)
   ├── fustor-sender-http   (L2: Agent Transport)
   │
   ├── fustor-receiver-http (L2: Fusion Transport)
   └── fustor-view-fs       (L3: Fusion Handler)
         └── Arbitrator
         └── LogicalClock (impl)
```

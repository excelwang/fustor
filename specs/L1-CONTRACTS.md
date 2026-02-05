---
layer: 1
id: CONTRACTS
version: 1.0.0
requires:
  - VISION.INTEGRITY
  - VISION.STRUCTURAL_CONSISTENCY
  - VISION.RESILIENCE
exports:
  - CONTRACTS.TOMBSTONE_PROTECTION
  - CONTRACTS.MTIME_ARBITRATION
  - CONTRACTS.LEADER_UNIQUENESS
  - CONTRACTS.EVENTUAL_CONSISTENCY
invariants:
  - id: INV_NO_RESURRECTION
    statement: "A deleted file with mtime <= tombstone_ts must not reappear."
  - id: INV_LEADER_UNIQUE
    statement: "At most one Leader session per View at any time."
  - id: INV_EVENTUAL_SYNC
    statement: "After quiescence, Fusion view matches NFS filesystem."
---

# L1: Contracts (Acceptance Behaviors)

> **Purpose**: Define user-observable black-box behaviors. These are the acceptance criteria.

## 1. Consistency Contracts

### CONTRACTS.TOMBSTONE_PROTECTION
**Scenario**: Deleted file must not resurrect from stale audit.

```gherkin
Given a file "/data/x.txt" was deleted at logical_time=100
When an audit reports "/data/x.txt" with mtime=90
Then the event is DISCARDED
And the file remains deleted in the unified view
```

### CONTRACTS.MTIME_ARBITRATION
**Scenario**: Newer data always wins.

```gherkin
Given file "/data/y.txt" exists with mtime=200
When an event arrives for "/data/y.txt" with mtime=150
Then the event is DISCARDED (stale)

When an event arrives for "/data/y.txt" with mtime=250
Then the event is ACCEPTED
And the file's mtime is updated to 250
```

---

## 2. Reliability Contracts

### CONTRACTS.LEADER_UNIQUENESS
**Scenario**: Only one Leader per View.

```gherkin
Given View "fs-view-1" has Agent A as Leader
When Agent B connects to "fs-view-1"
Then Agent B becomes Follower
And there is exactly 1 Leader for "fs-view-1"
```

### CONTRACTS.EVENTUAL_CONSISTENCY
**Scenario**: System converges after quiescence.

```gherkin
Given multiple Agents report events with varying delays
When the system is quiescent for 30 seconds
Then the Fusion view tree is isomorphic to the NFS filesystem
```

---

## 3. Corner Cases

### Case: Future Timestamp (Clock Skew)
```gherkin
Given a file has mtime in the year 2050 (clock skew)
When the Logical Clock calculates Age
Then the Age is based on Mode Skew correction, not raw mtime
And the file is correctly categorized as Suspect or Stable
```

### Case: Split Brain Recovery
```gherkin
Given network partition causes 2 Agents to both believe they are Leader
When partition heals
Then Fusion demotes one to Follower within 2*heartbeat_interval
And data consistency is restored via arbitration
```

# Core Algorithms: Consistency & Time

> **Purpose**: This document defines the "Brain" of Fustor—the logic used to maintain consistency across distributed, uncoordinated agents.

## 1. Core Algorithm: Smart Merge (Consistency)

The core arbitration logic determines which events are accepted into the global view.

### 1.1 The "Truth" Hierarchy
1.  **Realtime Events** (Highest Priority): Inotify events from Agents are considered the most current truth.
2.  **Snapshot/Audit Events** (Lower Priority): Used to fill gaps (blind spots) but must never overwrite Realtime data unless proven newer.

### 1.2 Leader Election (Fusion-Side)
*Who is allowed to perform heavy tasks (Snapshot/Audit/Sentinel)?*

- **Strategy**: **First-Come-First-Served (FCFS)** with **Fastest Follower Promotion**.
- **Logic**:
    1.  The first Session to connect to a View acquires the **Leader Lock**.
    2.  Subsequent Sessions become **Followers**.
    3.  If Leader disconnects:
        - The lock is released.
        - The Follower with the **fastest response** is promoted to Leader.
        - Agent receives `role: leader` in next Heartbeat response.

### 1.3 Arbitration Rules

When Fusion receives an event `E` for path `P`:

#### Rule 1: Tombstone Protection
*Prevent "Zombie Resurrection" (deleted files reappearing due to stale snapshots).*
- **Logic**:
    - If `P` exists in **Tombstone List** with logical timestamp `Ts_tomb`:
    - If `E.mtime > Ts_tomb + epsilon`:
        - **Action**: Accept `E`, Remove Tombstone (Reincarnation).
    - Else:
        - **Action**: Discard `E` (Stale).

#### Rule 2: Mtime Arbitration
*Prevent overwriting newer data with older data.*
- **Logic**:
    - If `P` exists in **Memory Tree** with `node.mtime`:
    - If `E.mtime <= node.mtime` (and not `E.audit_skipped`):
        - **Action**: Discard `E` (Stale or Duplicate).
    - Else:
        - **Action**: Accept `E`, Update Node.

#### Rule 3: Parent Mtime Check (Blind-spot Discovery)
*Verify if a "new" file found by Audit is actually new.*
- **Context**: Audit finds `file.txt` in `/dir`, but Fusion doesn't have it.
- **Logic**:
- **Logic**:
    - If `/dir` in Memory Tree has `mtime > E.parent_mtime` (The parent mtime observed by Audit):
        - **Interpretation**: The directory has changed *after* the Audit scan observed this file. A Realtime event (e.g., deletion) has likely superseded this finding.
        - **Action**: Discard `E` (Stale).
    - Else:
        - **Action**: Accept `E` as **Blind-spot Addition**.

### 1.3 Audit Logic (Efficiency Mode)

To balance **Efficiency** and **Coverage**:

1.  **Structural Audit (Fast/Default)**:
    - Relies on directory `mtime` to skip recursion (`True Silence`).
    - **Accepts**: "Content-Only Update Blindness" (modifications that don't update dir mtime are missed).
    - **Goal**: Minimize IOPS.

2.  **Deep Audit (Slow)**:
    - Full recursion. Used for deep consistency checks.

---

## 2. Core Algorithm: Dual-Track Time (Logical Clock)

Fustor uses a hybrid clock system to order events across uncoordinated nodes.

### 2.1 The Problem
- **Clock Skew**: Agent clocks vary by seconds/minutes.
- **Unreliable Mtime**: `touch -d` can set mtime to 2050 or 1970.

### 2.2 Solution: Simplified Logical Clock

Fusion maintains a **Logical Watermark** that advances linearly with Fusion's physical clock, corrected for the dominant skew.

#### A. Skew Correction (Fusion-Side)
- **Concept**: Calculate `offset = Fusion_Physical_Time - Event_Mtime`.
- **Mechanism**:
    - Collect skew samples from all Realtime events.
    - Compute the **Mode** (most frequent) skew.

#### B. Watermark Definition
- **Formula**:
  `Watermark = Fusion_Physical_Time - Mode_Skew`
  where `Mode_Skew` is the most frequent skew between Fusion and Agents. when cold start, Mode_Skew = 0.

- **Advantages**:
    - **Stability**: Prevents "Time Travel" caused by rogue clients with future clocks (e.g., `touch -d 2050`).
    - **Predictability**: The watermark always flows forward at the speed of real-time, regardless of bursty event traffic.
    - **Simplicity**: No complex "FastForward" or "Trust Window" logic required.

#### C. Derived Logical Time
- Used for **Suspect Age** calculation.
- `Age = Watermark - File.mtime`.

---

## 3. Consistency States (The "Suspect" Model)

Files transition through states to ensure integrity.

### 3.1 States
1.  **Suspect (Hot)**: Recently modified. Might be incomplete (NFS cache, active write).
    - **Indicator**: `Age < Threshold (65s)`.
2.  **Stable**: Age > Threshold AND mtime hasn't changed.
3.  **Tombstone**: Deleted recently.

### 3.2 Sentinel Sweep (Verification)
- **Goal**: Verify Suspects efficiently.
- **Algorithm**: **Priority Queue Scheduling**.
    - Don't poll every 2s.
    - Schedule poll for `Expected_Expiry_Time`.
    - If `os.stat()` confirms mtime stable -> Promote to Stable.
    - If mtime changed -> Renew Suspect lease.

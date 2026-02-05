# Core Algorithms: Consistency & Time

> **Purpose**: This document defines the "Brain" of Fustor—the logic used to maintain consistency across distributed, uncoordinated agents.

## 1. Core Algorithm: Smart Merge (Consistency)

The core arbitration logic determines which events are accepted into the global view.

### 1.1 The "Truth" Hierarchy
1.  **Realtime Events** (Highest Priority): Inotify events from Agents are considered the most current truth.
2.  **Snapshot/Audit Events** (Lower Priority): Used to fill gaps (blind spots) but must never overwrite Realtime data unless proven newer.

### 1.2 Arbitration Rules

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
    - If `/dir` in Memory Tree has `mtime > E.parent_mtime`:
        - **Interpretation**: The directory has changed *after* the Audit scan started. The file might have been deleted in the interim.
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

### 2.2 Solution: Robust Logical Clock

Fusion maintains a **Logical Watermark** that advances monotonically.

#### A. Skew Correction (Fusion-Side)
- **Concept**: Calculate `offset = Fusion_Physical_Time - Event_Mtime`.
- **Mechanism**:
    - Collect skew samples from all Realtime events.
    - Compute the **Mode** (most frequent) skew.
    - **Result**: A global `BaseLine` time = `Fusion_Physical_Time - Mode_Skew`.

#### B. Watermark Advancement
- **Logic**:
    - `BaseLine` advances naturally with physical time.
    - `FastForward`: If `Event.mtime > Watermark` (within a Trust Window), jump Watermark forward.
    - **Trust Window**: Prevents a single "2050 file" from dragging the whole system forward.

#### C. Derived Logical Time
- Used for **Suspect Age** calculation.
- `Age = Watermark - File.mtime`.

---

## 3. Consistency States (The "Suspect" Model)

Files transition through states to ensure integrity.

### 3.1 States
1.  **Suspect (Hot)**: Recently modified. Might be incomplete (NFS cache, active write).
    - **Indicator**: `Age < Threshold (30s)`.
2.  **Stable**: Age > Threshold AND mtime hasn't changed.
3.  **Tombstone**: Deleted recently.

### 3.2 Sentinel Sweep (Verification)
- **Goal**: Verify Suspects efficiently.
- **Algorithm**: **Priority Queue Scheduling**.
    - Don't poll every 2s.
    - Schedule poll for `Expected_Expiry_Time`.
    - If `os.stat()` confirms mtime stable -> Promote to Stable.
    - If mtime changed -> Renew Suspect lease.

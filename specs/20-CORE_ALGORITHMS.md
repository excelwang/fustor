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
    - Tombstone stores dual timestamps: `(Ts_logical, Ts_physical)` per §11-MODELS_AND_TERMS.
    - If `P` exists in **Tombstone List** with logical timestamp `Ts_logical`:
    - If `E.mtime > Ts_logical`:
        - **Action**: Accept `E`, Remove Tombstone (Reincarnation).
    - Else:
        - **Action**: Discard `E` (Stale).
    - `Ts_physical` is used for TTL cleanup (see §30-INTERFACES).

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
    - If `/dir` in Memory Tree has `mtime > E.parent_mtime` (The parent mtime observed by Audit):
        - **Interpretation**: The directory has changed *after* the Audit scan observed this file. A Realtime event (e.g., deletion) has likely superseded this finding.
        - **Action**: Discard `E` (Stale).
    - Else:
        - **Action**: Accept `E` as **Blind-spot Addition**.

### 1.4 Audit Logic (Efficiency Mode)

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

### 3.3 Suspect Stability Model (Stability-based TTL)

The Suspect state uses a **Stability-based Model** rather than pure age:

| Condition | Action |
|-----------|--------|
| **Realtime Update/Delete** | Immediately remove from Suspect List |
| **TTL Expired + mtime unchanged** | Promote to Stable (clear suspect flag) |
| **TTL Expired + mtime changed** | Renew TTL for another full cycle |

**Key Design**:
- Stability check compares `current_mtime` vs `recorded_mtime` at entry time
- Does NOT re-check logical age (allows future-timestamp files to stabilize)
- Uses `time.monotonic()` for TTL to avoid system clock adjustments

### 3.4 Blind-spot Lifecycle

Blind-spot records track files discovered/deleted outside Agent monitoring.

| Subset | Content | Lifecycle |
|--------|---------|-----------|
| `blind_spot_additions` | Files found by Audit but not by Realtime | Persists until Realtime confirms |
| `blind_spot_deletions` | Files missing in Audit but present in Tree | Persists until Realtime confirms |

**Clear Conditions**:
1. **Realtime Confirmation**: A Realtime event for the same path clears the blind-spot record
2. **Session Reset**: `on_session_start` clears all blind-spot records (new baseline)

**Persistence Rule**: Blind-spots do NOT use TTL auto-expiration (prevents valid data loss).

---

## 4. Audit Quick-Scan Algorithm (Agent-Side)

Leverages POSIX semantics: creating/deleting files only updates the **direct parent directory's** mtime.

### 4.1 True Silence Optimization

| Directory State | Action |
|-----------------|--------|
| `mtime == cached_mtime` | Skip file enumeration, send `audit_skipped=True`, recurse subdirs |
| `mtime != cached_mtime` | Full scan: enumerate all files, update cache |

**Benefit**: O(dirs) instead of O(files) for unchanged directories.

### 4.2 Audit Message Fields

| Field | Purpose |
|-------|---------|
| `parent_path` | Parent directory path (for Parent Mtime Check) |
| `parent_mtime` | Parent mtime at scan time |
| `audit_skipped` | `true` if directory was skipped due to unchanged mtime |
| `index` | Physical capture timestamp (ms) |

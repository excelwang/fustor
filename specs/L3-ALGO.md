---
layer: 3
id: ALGO
version: 1.0.0
requires:
  - ARCH.ARBITRATOR
  - ARCH.LOGICAL_CLOCK
  - ARCH.SESSION
  - ARCH.SOURCE
exports:
  - ALGO.SMART_MERGE
  - ALGO.TOMBSTONE_GUARD
  - ALGO.MTIME_ARBITRATION
  - ALGO.WATERMARK
  - ALGO.LEADER_ELECTION
  - ALGO.TRUE_SILENCE
  - ALGO.SUSPECT_MODEL
invariants:
  - id: INV_TOMB_PROTECT
    statement: "event.mtime <= tomb_ts → DISCARD"
  - id: INV_MTIME_MONO
    statement: "event.mtime <= node.mtime → DISCARD (unless audit_skipped)"
  - id: INV_WATERMARK_STABLE
    statement: "Watermark advances monotonically with physical time"
---

# L3-ALGO: Algorithm Specifications

> **Purpose**: Define core logic for each architectural component.

## 1. Smart Merge (ARCH.ARBITRATOR)

### ALGO.TOMBSTONE_GUARD
For incoming event `E` on path `P`:

1. IF `P` in Tombstone List with `Ts_logical`:
   - IF `E.mtime > Ts_logical`: ACCEPT, remove tombstone (Reincarnation)
   - ELSE: DISCARD (Stale)

### ALGO.MTIME_ARBITRATION
2. IF `P` in Memory Tree with `node.mtime`:
   - IF `E.mtime <= node.mtime` AND NOT `E.audit_skipped`: DISCARD
   - ELSE: ACCEPT, update node

### ALGO.PARENT_CHECK
3. For Audit events:
   - IF `parent.mtime > E.parent_mtime`: DISCARD (Parent updated after scan)
   - ELSE: ACCEPT as Blind-spot addition

---

## 2. Logical Clock (ARCH.LOGICAL_CLOCK)

### ALGO.WATERMARK
**Formula**: `Watermark = Fusion_Physical_Time - Mode_Skew`

- `Mode_Skew`: Most frequent offset between Fusion clock and Agent events.
- Cold start: `Mode_Skew = 0`.

**Properties**:
- Monotonically advances with Fusion's physical clock.
- Immune to rogue future timestamps (e.g., `touch -d 2050`).

---

## 3. Leader Election (ARCH.SESSION)

### ALGO.LEADER_ELECTION
**Strategy**: First-Come-First-Served + Fastest Follower Promotion

1. First Session to connect acquires Leader Lock.
2. Subsequent Sessions become Followers.
3. On Leader disconnect:
   - Release lock.
   - Promote Follower with fastest heartbeat response.

---

## 4. Audit Optimization (ARCH.SOURCE)

### ALGO.TRUE_SILENCE
Leverages POSIX: file creation/deletion updates parent dir mtime.

| Directory State | Action |
|-----------------|--------|
| `mtime == cached` | Skip scan, send `audit_skipped=True` |
| `mtime != cached` | Full scan, update cache |

**Benefit**: O(dirs) instead of O(files) for unchanged directories.

---

## 5. Suspect Model (ARCH.VIEW)

### ALGO.SUSPECT_MODEL
**Entry Condition**: `Age = Watermark - mtime < hot_file_threshold`

**State Machine**:
| Condition | Action |
|-----------|--------|
| Realtime Update/Delete | Remove from Suspect List |
| TTL Expired + mtime unchanged | Promote to Stable |
| TTL Expired + mtime changed | Renew TTL |

**TTL Timer**: Uses `time.monotonic()` (immune to clock adjustments).

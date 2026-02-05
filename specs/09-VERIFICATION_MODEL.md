# Verification Model (Invariants)

> **Status**: Draft
> **Version**: 1.0
> **Purpose**: Defines the "Truth" for Integration Testing.

## 1. System Invariants

An invariant is a condition that must always be true during stable system operation (eventual consistency).

### 1.1 Data Consistency Invariant
> "After time T of quiescence (no new writes), the Fusion View Tree must be isomorphic to the NFS File System."

- **Verification**: `DeepDiff(Fusion.get_view_tree(), NFS.scandir()) == {}`
- **Relaxation**: Exclude `atime` (access time). Allow `mtime` tolerance of ±1s (due to float precision).

### 1.2 Tombstone Liveness Invariant
> "A Tombstone must not persist beyond its TTL."

- **Verification**:
    1. Delete file X.
    2. Wait for `Tombstone TTL` + `Buffer`.
    3. Query internal state (White-box).
    4. Assert `X not in tombstone_list`.

### 1.3 Leader Uniqueness Invariant
> "For any given View ID, there must be exactly zero or one Active Leader Session."

- **Verification**:
    1. Query `GET /api/v1/views/{view_id}/stats`.
    2. Assert `leader_session` is unique across all Agents.

### 1.4 Event Monotonicity (per file)
> "For a specific file path, the `last_updated_at` (physical) and `mtime` (logical) must generally increase."

- **Verification**: Monitor Event stream. `E(t).mtime <= E(t+1).mtime`.
- **Exception**: Old Mtime Injection (e.g., `cp -p`).

## 2. White-box Verification APIs

To validate these invariants concisely without waiting for end-to-end black-box propagation, Fusion must expose internal state.

### 2.1 State Dump API
`GET /api/v1/debug/views/{view_id}/state`

**Response**:
```json
{
  "view_id": "fs-view-1",
  "leader_session_id": "sess_abc",
  "logical_clock": {
    "watermark": 1700000000.0,
    "skew": 0.05
  },
  "consistency_lists": {
    "suspects": ["/data/hot.tmp"],
    "tombstones": ["/data/deleted.txt"],
    "blind_spots": []
  },
  "tree_stats": {
    "node_count": 15000
  }
}
```

## 3. Failure Scenarios & Expected Outcomes

| Scenario | Invariant Checked | Expected Outcome |
|----------|-------------------|------------------|
| **Agent Crash** | Leader Uniqueness | New Leader elected within `2 * heartbeat_interval`. |
| **Network Partition** | Data Consistency | Fusion View freezes; Resumes sync after partition heals. |
| **Split Brain (2 Leaders)** | Leader Uniqueness | Fusion rejects Session B's write with `409 Conflict` or demotes Session B. |
| **Clock Skew (+1h)** | Data Consistency | Fusion detects skew, adjusts Logical Clock, mtime accepted correctly. |

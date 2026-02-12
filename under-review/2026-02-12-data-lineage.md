# Data Lineage Tracking Feature Review - 2026-02-12

## 1. Requirement
The user requested that Fusion processed data (events, file nodes) be tagged with lineage information to trace their origin. Specifically:
- **Which Agent** sent the data.
- **Which Source** (URI) the data came from.

This allows downstream consumers or the system itself to understand data provenance.

## 2. Implementation Summary

To support this without breaking changes, we introduced an optional metadata channel through the event pipeline.

### Core Changes (`fustor-core`)
- **Event Model**: Added `metadata: Optional[Dict[str, Any]]` to `EventBase`. This generic field allows passing any auxiliary context without changing the core schema.

### Fusion Logic (`fustor-fusion`)
- **Metadata Injection**: In `FusionPipe.process_events`, we now look up the `SessionInfo` for the incoming session.
- **Data Source**: We extract:
    - `agent_id` from the `task_id` (format: `agent_id:pipe_id`).
    - `source_uri` from the session's client info.
- **Pipeline**: This lineage data is injected into `event.metadata` before the event is dispatched to handlers.

### View Domain (`fustor-view-fs`)
- **Node Schema**: `DirectoryNode` and `FileNode` object definitions were updated to include `last_agent_id` and `source_uri` fields.
- **Arbitration**: `FSArbitrator` extracts lineage info from incoming `event.metadata` and passes it to `TreeManager`.
- **Tree Management**: `TreeManager.update_node` persists this lineage information when creating or updating nodes.

## 3. Verification
A standalone verification script (`tests/verify_lineage.py`) was created to simulate the entire flow:
1.  Constructing an event with manual metadata.
2.  Processing it via `FSArbitrator`.
3.  Asserting that the resulting in-memory nodes contain the correct lineage tags.

**Status**: Verified Successfully.

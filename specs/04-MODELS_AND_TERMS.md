# Data Models & Terminology

> **Purpose**: Defines the shared vocabulary and data structures used across Agent and Fusion.

## 1. Glossary

### Core Entities
- **Agent**: Client-side daemon (`fustor-agent`) that observes storage.
- **Fusion**: Server-side aggregator (`fustor-fusion`) that maintains the unified view.
- **Session**: A logical binding between an Agent Pipeline and a Fusion Pipeline.
- **Pipeline**: The engine driving data flow.
    - **Agent Pipeline**: Source -> Buffer -> Sender
    - **Fusion Pipeline**: Receiver -> Router -> View

### Consistency Concepts
- **Watermark**: The current Logical Time of the file system view.
- **Suspect**: A file that is "Hot" (recent mtime) or unstable.
- **Tombstone**: A record of a deleted file to prevent stale data resurrection.
- **Blind-spot**: A file change that happened outside of Agent monitoring (e.g., on a node without an agent).

---

## 2. Data Models (Schema)

These models define the contract for `fustor-schema-fs`.

### 2.1 Event Model (JSON/Pydantic)

The fundamental unit of data exchange.

```python
class FSEventRow(BaseModel):
    path: str
    modified_time: float
    size: int = 0
    is_dir: bool = False
    
    # Consistency Metadata
    parent_path: Optional[str] = None
    parent_mtime: Optional[float] = None
    audit_skipped: bool = False  # True if directory was skipped due to "True Silence"

class EventBatch(BaseModel):
    source_type: Literal["realtime", "snapshot", "audit"]
    event_type: Literal["INSERT", "UPDATE", "DELETE"]
    index: int  # Physical timestamp (ms)
    rows: List[FSEventRow]
```

### 2.2 View Node Model

The in-memory representation in Fusion.

```python
class FileNode:
    path: str
    name: str
    size: int
    modified_time: float
    
    # Internal Consistency State
    last_updated_at: float  # Physical time of last update
    integrity_suspect: bool # True if "Suspect"
    known_by_agent: bool    # True if seen by Realtime
```

### 2.3 Session Model

| Field | Type | Description |
|-------|------|-------------|
| `session_id` | `str` | Unique session identifier |
| `task_id` | `str` | Agent pipeline task ID |
| `role` | `leader/follower` | Current role |
| `created_at` | `float` | Session creation time |
| `last_heartbeat_at` | `float` | Last heartbeat timestamp |

### 2.4 DirectoryNode Extensions

| Field | Type | Description |
|-------|------|-------------|
| `audit_skipped` | `bool` | True if skipped due to unchanged mtime |

### 2.5 Internal State Structures

**Tombstone Entry**: `(LogicalTime, PhysicalTime)`
- `LogicalTime`: For reincarnation check (`mtime > logical_ts`)
- `PhysicalTime`: For TTL cleanup (1 hour default)

**Suspect Entry**: `(ExpiryMonotonic, RecordedMtime)`
- `ExpiryMonotonic`: TTL expiry via `time.monotonic()`
- `RecordedMtime`: mtime at entry time (for stability check)

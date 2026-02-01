# fustor-event-model

This package defines the standardized event models and schemas used across various Fustor services and components. It provides a consistent structure for data events, ensuring interoperability and clear communication between different parts of the Fustor ecosystem.

## Features

*   **Standardized Event Models**: Pydantic models for representing different types of data events.
*   **Data Validation**: Leverages Pydantic for automatic data validation and serialization/deserialization.
*   **Interoperability**: Ensures that all Fustor services handle event data in a consistent and predictable manner.

## Core Fields

### `message_source`

The `message_source` field is critical for Fusion's consistency arbitration:

| Value | Description | Priority |
|-------|-------------|----------|
| `realtime` | inotify events | Highest |
| `snapshot` | Initial full scan | - |
| `audit` | Periodic audit scan | - |

### Key Event Fields

| Field | Type | Description |
|-------|------|-------------|
| `event_type` | enum | INSERT / UPDATE / DELETE |
| `path` | string | File path |
| `mtime` | float | File modification time |
| `size` | int | File size |
| `parent_mtime` | float | Parent directory mtime (only needed for `audit`) |

## Installation

This package is part of the Fustor monorepo and is typically installed in editable mode:

```bash
uv sync --extra dev
```

## Usage

```python
from fustor_event_model.models import UpdateEvent, DeleteEvent

# Create a realtime update event (FS format)
update_event = UpdateEvent(
    event_schema="/data/research",  # Source root path
    table="files",
    fields=["file_path", "size", "modified_time", "created_time", "is_dir"],
    rows=[{
        "file_path": "/data/research/file.txt",
        "size": 10240,
        "modified_time": 1706000123.0,
        "created_time": 1706000100.0,
        "is_dir": False
    }],
    index=1706000123000  # Timestamp in milliseconds
)

# Create a delete event
delete_event = DeleteEvent(
    event_schema="/data/research",
    table="files",
    fields=["file_path"],
    rows=[{"file_path": "/data/research/deleted_file.txt"}],
    index=1706000200000
)

# For audit events, include parent_mtime in the row
audit_event = UpdateEvent(
    event_schema="/data/research",
    table="files",
    fields=["file_path", "size", "modified_time", "created_time", "is_dir", "parent_mtime"],
    rows=[{
        "file_path": "/data/research/blind_spot_file.txt",
        "size": 2048,
        "modified_time": 1706000300.0,
        "created_time": 1706000300.0,
        "is_dir": False,
        "parent_mtime": 1706000250.0  # Parent directory mtime for audit arbitration
    }],
    index=1706000300000
)
```

## Related Documentation

*   [Consistency Design](../../docs/CONSISTENCY_DESIGN.md): Full specification of consistency arbitration logic.


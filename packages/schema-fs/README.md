# fustor-schema-fs

File System Schema for Fustor - defines the data contract between `fustor-source-fs` (Agent) and `fustor-view-fs` (Fusion).

## Overview

This package contains only type definitions and validation logic, with **no runtime dependencies**. It serves as the single source of truth for the file system event format.

## Schema

### Event Row Format

```python
{
    "path": str,              # Absolute file path
    "file_name": str,         # Base file name
    "size": int,              # File size in bytes
    "modified_time": float,   # Last modification time (Unix timestamp)
    "is_directory": bool,     # True if this is a directory
    # Optional fields
    "created_time": float,    # Creation time
    "parent_path": str,       # Parent directory path
    "parent_mtime": float,    # Parent directory mtime (for audit)
}
```

## Usage

```python
from fustor_schema_fs import (
    SCHEMA_NAME,
    SCHEMA_VERSION,
    validate_row,
    get_path,
    get_mtime,
)

# Check schema version
print(f"Schema: {SCHEMA_NAME} v{SCHEMA_VERSION}")

# Validate event row
row = {"path": "/data/file.txt", "file_name": "file.txt", ...}
if validate_row(row):
    path = get_path(row)
    mtime = get_mtime(row)
```

## Versioning

This schema follows semantic versioning:
- **Major** version changes indicate breaking changes to required fields
- **Minor** version changes add optional fields
- **Patch** version changes are documentation-only

Current version: `1.0`

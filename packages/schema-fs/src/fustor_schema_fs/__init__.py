"""
Fustor Schema: File System (FS)

This package defines the schema contract for file system data synchronization.
It is used by both Agent-side source drivers and Fusion-side view handlers.

Schema Version: 1.0

Row Format:
    {
        "path": str,              # Absolute file path
        "file_path": str,         # Alias for path (deprecated)
        "file_name": str,         # Base file name
        "size": int,              # File size in bytes
        "modified_time": float,   # Last modification time (Unix timestamp)
        "created_time": float,    # Creation time (Unix timestamp)
        "is_directory": bool,     # True if this is a directory
        "parent_path": str,       # Parent directory path
        "parent_mtime": float,    # Parent directory mtime (for audit)
        "audit_skipped": bool,    # True if this was a heartbeat (skipped during audit)
    }
"""

SCHEMA_NAME = "fs"
SCHEMA_VERSION = "1.0"

# Required fields for INSERT/UPDATE events
REQUIRED_FIELDS = [
    "path",
    "file_name",
    "size",
    "modified_time",
    "is_directory",
]

# Optional fields
OPTIONAL_FIELDS = [
    "file_path",       # Deprecated alias for path
    "created_time",
    "parent_path",
    "parent_mtime",
    "audit_skipped",
]

# Fields required for DELETE events
DELETE_FIELDS = [
    "path",
]


def validate_row(row: dict, event_type: str = "update") -> bool:
    """
    Validate a row against the FS schema.
    
    Args:
        row: The row data to validate
        event_type: Type of event ("insert", "update", "delete")
        
    Returns:
        True if valid, False otherwise
    """
    if event_type == "delete":
        return "path" in row or "file_path" in row
    
    for field in REQUIRED_FIELDS:
        if field not in row:
            # Allow file_path as alias for path
            if field == "path" and "file_path" in row:
                continue
            return False
    return True


def get_path(row: dict) -> str:
    """Extract the normalized path from a row."""
    return row.get("path") or row.get("file_path") or ""


def get_mtime(row: dict) -> float:
    """Extract the modification time from a row."""
    return row.get("modified_time") or 0.0


# Import Pydantic models (optional, requires pydantic)
try:
    from .models import FSRow, FSDeleteRow
except ImportError:
    FSRow = None
    FSDeleteRow = None


__all__ = [
    # Constants
    "SCHEMA_NAME",
    "SCHEMA_VERSION",
    "REQUIRED_FIELDS",
    "OPTIONAL_FIELDS",
    "DELETE_FIELDS",
    # Validation functions
    "validate_row",
    "get_path",
    "get_mtime",
    # Pydantic models (optional)
    "FSRow",
    "FSDeleteRow",
]


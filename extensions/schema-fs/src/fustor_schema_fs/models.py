# fustor-schema-fs/src/fustor_schema_fs/models.py
"""
Pydantic models for FS schema rows.

These models provide type-safe validation for file system event data.
"""
from typing import Optional
from pydantic import BaseModel, Field, field_validator, model_validator


class FSSchemaFields:
    """Constants for FS schema field names."""
    PATH = "path"
    FILE_NAME = "file_name"
    SIZE = "size"
    MODIFIED_TIME = "modified_time"
    IS_DIRECTORY = "is_directory"
    CREATED_TIME = "created_time"
    PARENT_PATH = "parent_path"
    PARENT_MTIME = "parent_mtime"
    AUDIT_SKIPPED = "audit_skipped"
    
    # Deprecated
    FILE_PATH = "file_path"

class FSRow(BaseModel):
    """
    A file system row representing a file or directory.
    
    This is the standard data format for FS events exchanged between
    Agent source drivers and Fusion view handlers.
    """
    
    # Required fields
    path: str = Field(..., description="Absolute file path")
    file_name: str = Field(..., description="Base file name")
    size: int = Field(..., ge=0, description="File size in bytes")
    modified_time: float = Field(..., description="Last modification time (Unix timestamp)")
    is_directory: bool = Field(..., description="True if this is a directory")
    
    # Optional fields
    file_path: Optional[str] = Field(None, description="Deprecated alias for path")
    created_time: Optional[float] = Field(None, description="Creation time (Unix timestamp)")
    parent_path: Optional[str] = Field(None, description="Parent directory path")
    parent_mtime: Optional[float] = Field(None, description="Parent directory mtime (for audit)")
    audit_skipped: bool = Field(False, description="True if this was a heartbeat during audit")
    
    @field_validator('path', mode='before')
    @classmethod
    def normalize_path(cls, v: str, info) -> str:
        """Ensure path is set, falling back to file_path if provided."""
        if v:
            return v
        # Fallback handled in model_validator
        return v
    
    @model_validator(mode='before')
    @classmethod
    def handle_path_alias(cls, data):
        """Handle file_path as deprecated alias for path."""
        if isinstance(data, dict):
            if not data.get('path') and data.get('file_path'):
                data['path'] = data['file_path']
        return data
    
    def get_normalized_path(self) -> str:
        """Get the normalized path (prefers path over file_path)."""
        return self.path or self.file_path or ""


class FSDeleteRow(BaseModel):
    """
    A minimal row for DELETE events.
    
    DELETE events only require the path field.
    """
    path: str = Field(..., description="Path of the deleted file/directory")
    file_path: Optional[str] = Field(None, description="Deprecated alias for path")
    
    @model_validator(mode='before')
    @classmethod
    def handle_path_alias(cls, data):
        """Handle file_path as deprecated alias for path."""
        if isinstance(data, dict):
            if not data.get('path') and data.get('file_path'):
                data['path'] = data['file_path']
        return data
    
    
    def get_normalized_path(self) -> str:
        """Get the normalized path."""
        return self.path or self.file_path or ""


# --- Event Models ---

from typing import List, Union
from fustor_core.event import EventBase, EventType
from fustor_core.event.types import MessageSource

class FSEventBase(EventBase):
    """Base class for FS-specific events."""
    event_schema: str = Field("fs", description="Schema name is always 'fs'")
    table: str = Field("files", description="Default table name for FS events")
    
class FSInsertEvent(FSEventBase):
    """
    Event representing a file/directory creation.
    """
    event_type: EventType = EventType.INSERT
    rows: List[FSRow] = Field(..., description="List of created files/directories")
    fields: List[str] = Field(
        default=["path", "file_name", "size", "modified_time", "is_directory"],
        description="Fields present in the rows"
    )

class FSUpdateEvent(FSEventBase):
    """
    Event representing a file/directory modification.
    """
    event_type: EventType = EventType.UPDATE
    rows: List[FSRow] = Field(..., description="List of modified files/directories")
    fields: List[str] = Field(
        default=["path", "file_name", "size", "modified_time", "is_directory"],
        description="Fields present in the rows"
    )

class FSDeleteEvent(FSEventBase):
    """
    Event representing a file/directory deletion.
    """
    event_type: EventType = EventType.DELETE
    rows: List[FSDeleteRow] = Field(..., description="List of deleted files/directories")
    fields: List[str] = Field(
        default=["path"],
        description="Fields present in the rows"
    )


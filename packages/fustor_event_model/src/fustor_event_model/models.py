from enum import Enum
from typing import List, Any, Optional, Dict
from pydantic import BaseModel, Field


class EventType(Enum):
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"


class MessageSource(str, Enum):
    """Source type of the message, used for consistency arbitration."""
    REALTIME = "realtime"
    SNAPSHOT = "snapshot"
    AUDIT = "audit"


class EventBase(BaseModel):
    event_type: EventType = Field(..., description="Type of the event")
    fields: List[str] = Field(..., description="List of field names in the rows")
    rows: List[Any] = Field(..., description="List of event data rows")
    index: int = Field(-1, description="Index of the event, e.g., timestamp or sequence number")
    event_schema: str = Field(..., description="Schema name (e.g., database name or source ID)")
    table: str = Field(..., description="Table name (e.g., table name or file path)")
    message_source: MessageSource = Field(
        default=MessageSource.REALTIME,
        description="Source of the message: realtime, snapshot, audit"
    )


class InsertEvent(EventBase):
    event_type: EventType = EventType.INSERT


class UpdateEvent(EventBase):
    event_type: EventType = EventType.UPDATE


class DeleteEvent(EventBase):
    event_type: EventType = EventType.DELETE


class FileSystemPayload(BaseModel):
    """Explicit definition of file system event payload structure."""
    file_path: str
    size: int
    modified_time: float
    created_time: float = 0.0
    is_dir: bool = False
    parent_path: Optional[str] = None
    parent_mtime: Optional[float] = None
    audit_skipped: bool = False  # Explicitly defined for Audit Optimization
    
    class Config:
        extra = "allow"  # Allow checking other fields if present
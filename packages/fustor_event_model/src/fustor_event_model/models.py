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
    # Optional Session ID (injected by Ingestion API)
    session_id: Optional[str] = Field(None, description="Session ID of the source agent")


class InsertEvent(EventBase):
    event_type: EventType = EventType.INSERT


class UpdateEvent(EventBase):
    event_type: EventType = EventType.UPDATE


class DeleteEvent(EventBase):
    event_type: EventType = EventType.DELETE


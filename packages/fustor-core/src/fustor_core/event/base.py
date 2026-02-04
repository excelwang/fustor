"""
Base event model for Fustor.
Migrated from fustor_event_model.models
"""
from typing import List, Any
from pydantic import BaseModel, Field
from .types import EventType, MessageSource


class EventBase(BaseModel):
    """
    Base class for all Fustor events.
    
    Events represent data changes captured by Source drivers and processed by View drivers.
    The event model is schema-agnostic - specific schemas (e.g., FS) define their own
    row structure within the `rows` field.
    """
    event_type: EventType = Field(..., description="Type of the event (insert/update/delete)")
    fields: List[str] = Field(..., description="List of field names in the rows")
    rows: List[Any] = Field(..., description="List of event data rows (schema-specific)")
    index: int = Field(-1, description="Event index/sequence number for ordering")
    event_schema: str = Field(..., description="Schema name (e.g., 'fs', 'mysql')")
    table: str = Field(..., description="Table/collection name within the schema")
    message_source: MessageSource = Field(
        default=MessageSource.REALTIME,
        description="Source of the message: realtime, snapshot, audit"
    )


class InsertEvent(EventBase):
    """Event representing a new item creation."""
    event_type: EventType = EventType.INSERT


class UpdateEvent(EventBase):
    """Event representing an item modification."""
    event_type: EventType = EventType.UPDATE


class DeleteEvent(EventBase):
    """Event representing an item deletion."""
    event_type: EventType = EventType.DELETE

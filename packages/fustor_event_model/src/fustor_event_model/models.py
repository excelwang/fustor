from enum import Enum
from typing import List, Any
from pydantic import BaseModel, Field # Added BaseModel and Field for consistency

class EventType(Enum):
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"

class EventBase(BaseModel): # Changed to inherit from BaseModel
    event_type: EventType = Field(..., description="Type of the event")
    fields: List[str] = Field(..., description="List of field names in the rows")
    rows: List[Any] = Field(..., description="List of event data rows")
    index: int = Field(-1, description="Index of the event, e.g., timestamp or sequence number")
    event_schema: str = Field(..., description="Schema name (e.g., database name or source ID)") # Renamed from schema
    table: str = Field(..., description="Table name (e.g., table name or file path)")

class InsertEvent(EventBase):
    event_type: EventType = EventType.INSERT

class UpdateEvent(EventBase):
    event_type: EventType = EventType.UPDATE

class DeleteEvent(EventBase):
    event_type: EventType = EventType.DELETE
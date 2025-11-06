from enum import Enum
from typing import List, Any

class EventType(Enum):
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"

class EventBase:
    def __init__(self, fields: List[str], rows: List[Any], event_type: EventType = EventType.INSERT, index: int = -1):
        self.event_type = event_type
        self.fields = fields
        self.rows = rows
        self.index: int = index

class InsertEvent(EventBase):
    def __init__(self, schema: str, table: str, rows: List[Any], index: int = -1):
        fields = list(rows[0].keys()) if rows else []
        super().__init__(fields, rows, EventType.INSERT, index=index)
        self.schema = schema
        self.table = table

class UpdateEvent(EventBase):
    def __init__(self, schema: str, table: str, rows: List[Any], index: int = -1):
        fields = list(rows[0].keys()) if rows else []
        super().__init__(fields, rows, EventType.UPDATE, index=index)
        self.schema = schema
        self.table = table

class DeleteEvent(EventBase):
    def __init__(self, schema: str, table: str, rows: List[Any], index: int = -1):
        fields = list(rows[0].keys()) if rows else []
        super().__init__(fields, rows, EventType.DELETE, index=index)
        self.schema = schema
        self.table = table

from typing import Optional, Dict, Any
from pydantic import BaseModel, Field
from fustor_core.event import EventBase

# ResponseBase was from fustor_common.models, but we can define a simple base here
class ResponseBase(BaseModel):
    """Base response model."""
    pass

class EventCreate(EventBase):
    pass

class EventResponse(ResponseBase, EventBase):
    id: str
    metadata: Optional[Dict[str, Any]] = None
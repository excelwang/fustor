from pydantic import BaseModel, Field
from fustor_common.models import ResponseBase
from fustor_event_model.models import EventBase

class EventCreate(EventBase):
    pass

class EventResponse(ResponseBase, EventBase):
    id: str
from pydantic import BaseModel, Field
from fustor_core.models import ResponseBase

class EventBase(BaseModel):
    content: dict = Field(default_factory=dict, description="日志内容")

class EventCreate(EventBase):
    pass

class EventResponse(ResponseBase, EventBase):
    id: str
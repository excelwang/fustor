from pydantic import BaseModel, Field

class EventBase(BaseModel):
    content: dict = Field(default_factory=dict, description="日志内容")

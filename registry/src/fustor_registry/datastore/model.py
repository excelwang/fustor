from typing import Optional, Dict
from pydantic import BaseModel, Field
from ..schemas import ResponseBase # Corrected import

class DatastoreBase(BaseModel):
    name: str = Field(..., description="存储库名称")
    visible: bool = Field(False, description="是否对公众可见")
    meta: Optional[Dict] = Field(None, description="存储库描述")
    allow_concurrent_push: bool = Field(False, description="是否允许并发推送")
    session_timeout_seconds: int = Field(30, description="会话超时秒数")

class DatastoreCreate(DatastoreBase):
    pass

class DatastoreUpdate(DatastoreBase):
    pass

class DatastoreResponse(ResponseBase, DatastoreCreate):
    id: int
from pydantic import BaseModel, Field, ConfigDict
from enum import Enum

class ResponseBase(BaseModel):
    # Base model for API responses, can include common fields like status, message
    pass

class ApiKeyBase(BaseModel):
    name: str = Field(..., min_length=3, max_length=50)
    datastore_id: int = Field(..., description="关联的存储库ID")

class ApiKeyCreate(ApiKeyBase):
    pass

class ApiKeyResponse(ResponseBase, ApiKeyBase):
    id: int
    key: str
    model_config = ConfigDict(from_attributes=True)

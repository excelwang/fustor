from pydantic import BaseModel, ConfigDict
from typing import List, Optional

class InternalApiKeyResponse(BaseModel):
    key: str
    datastore_id: int
    model_config = ConfigDict(from_attributes=True)

class InternalDatastoreConfigResponse(BaseModel):
    datastore_id: int
    allow_concurrent_push: bool
    session_timeout_seconds: int
    model_config = ConfigDict(from_attributes=True)

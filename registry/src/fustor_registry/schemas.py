from pydantic import Field, ConfigDict
from fustor_common.models import ResponseBase, ApiKeyBase

class ApiKeyCreate(ApiKeyBase):
    pass

class ApiKeyResponse(ResponseBase, ApiKeyBase):
    id: int
    key: str
    model_config = ConfigDict(from_attributes=True)

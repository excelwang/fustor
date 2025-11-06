from pydantic import BaseModel, Field, ConfigDict, field_validator
from typing import Optional, Dict, List

class ResponseBase(BaseModel):
    # Base model for API responses, can include common fields like status, message
    pass

class ApiKeyBase(BaseModel):
    id: Optional[int] = None
    name: str = Field(..., min_length=3, max_length=50)
    datastore_id: int = Field(..., description="关联的存储库ID")

class MessageResponse(BaseModel):
    """A simple response model for messages."""
    message: str


class DatastoreBase(BaseModel):
    id: Optional[int] = None
    name: str = Field(..., description="存储库名称")
    visible: bool = Field(False, description="是否对公众可见")
    meta: Optional[Dict] = Field(None, description="存储库描述")
    allow_concurrent_push: bool = Field(False, description="是否允许并发推送")
    session_timeout_seconds: int = Field(30, description="会话超时秒数")

class TokenResponse(BaseModel):
    access_token: str
    token_type: str
    refresh_token: str | None = None

class LogoutResponse(BaseModel):
    detail: str = Field("注销成功", description="注销操作结果消息")

class Password(BaseModel):
    password: str = Field(
        ..., 
        min_length=8, 
        max_length=64,
        pattern=r'^[A-Za-z\d@$!%*#?&]+$',
        description="8-64位字符，必须包含至少1字母和1数字，允许特殊字符 @$!%*#?&"
    )

    @field_validator('password')
    @classmethod
    def validate_password_chars(cls, v: str) -> str:
        if not any(c.isalpha() for c in v):
            raise ValueError("必须包含至少一个字母")
        if not any(c.isdigit() for c in v):
            raise ValueError("必须包含至少一个数字")
        return v

class ValidationResponse(BaseModel):
    """A standard response model for validation actions."""
    success: bool
    message: str

class CleanupResponse(BaseModel):
    """A standard response model for cleanup actions."""
    message: str
    deleted_count: int
    deleted_ids: List[str]

class AdminCredentials(BaseModel):
    user: str
    passwd: str

class LoginRequest(BaseModel):
    username: str
    password: str

class DatastoreConfig(BaseModel):
    datastore_id: int
    allow_concurrent_push: bool
    session_timeout_seconds: int

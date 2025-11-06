from pydantic import BaseModel, Field, field_validator
from ..security import hash_password

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
        description="8-64位字符，必须包含至少1字母和1数字，允许特殊字符 @$!%*#?&",
        json_schema_extra={
            "min_length": {"message": "密码长度需在8-64个字符之间"},
            "max_length": {"message": "密码长度需在8-64个字符之间"}
        }
    )

    @field_validator('password')
    @classmethod
    def validate_password_chars(cls, v: str) -> str:
        if not any(c.isalpha() for c in v):
            raise ValueError("必须包含至少一个字母")
        if not any(c.isdigit() for c in v):
            raise ValueError("必须包含至少一个数字")
        return hash_password(v)
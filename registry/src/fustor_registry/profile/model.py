from pydantic import BaseModel, Field
from ..auth.model import Password
from ..models import UserLevel # Corrected
from ..schemas import ResponseBase # Corrected

class ProfileUpdate(BaseModel):
    name: str = Field(..., min_length=3, max_length=50, description="用户姓名")

class PasswordUpdate(Password):
    """密码更新模型"""
    old_password: str = Field(..., description="旧密码")

class ProfileResponse(ResponseBase, ProfileUpdate):
    """用户完整响应模型"""
    id: int
    email: str = Field(..., pattern=r'^\S+@\S+\.\S+$', description="有效邮箱地址")
    level: UserLevel = Field(default=UserLevel.NORMAL, description="用户等级")
    is_active: bool = Field(default=True, description="账户激活状态")
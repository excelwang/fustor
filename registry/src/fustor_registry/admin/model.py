from pydantic import Field
from ..profile.model import ProfileUpdate
from ..auth.model import Password

class UserCreate(ProfileUpdate, Password):
    """用户创建模型"""
    email: str = Field(..., pattern=r'^\S+@\S+\.\S+$', description="有效邮箱地址")
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.exceptions import RequestValidationError
from fastapi import Body

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from ..database import get_db
from ..models import UserModel
from ..security import get_current_user, verify_password, hash_password
from fustor_common.models import MessageResponse
from .model import PasswordUpdate, ProfileUpdate, ProfileResponse

router = APIRouter(prefix="/profile", tags=["个人信息管理"])
@router.get("/",
           response_model=ProfileResponse,
           status_code=status.HTTP_200_OK,
           summary="查看用户详情",
           responses={
               404: {"description": "用户不存在"}
           })
async def get_profile(
    current_user: UserModel = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """根据用户ID获取详细信息
    - 返回用户的完整档案
    """
    user = await db.get(UserModel, current_user.id)
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="用户不存在")
    return user

@router.put("/",
           response_model=ProfileResponse,
           status_code=status.HTTP_200_OK,
           summary="更新个人信息",
           description="更新个人信息，保留未提供字段的原始值",
           responses={
               400: {"description": "无效输入数据"},
               404: {"description": "用户不存在"}
           })
async def update_profile(
    profile_update: ProfileUpdate, 
    current_user: UserModel = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """更新用户资料
    """
    db_user = await db.get(UserModel, current_user.id)
    if not db_user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail="用户不存在"
        )
    # 更新字段
    for field, value in profile_update.model_dump(exclude_unset=True).items():
        setattr(db_user, field, value)
    
    await db.commit()
    await db.refresh(db_user)
    return db_user

@router.put("/password",
           response_model=MessageResponse,
           status_code=status.HTTP_200_OK,
           summary="修改密码",
           responses={
               400: {"description": "旧密码错误"},
               404: {"description": "用户不存在"}
           })
async def change_password(
    password_data: PasswordUpdate,
    current_user: UserModel = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """修改用户密码
    - 验证旧密码是否正确
    - 新密码需进行哈希存储
    """
    db_user = await db.get(UserModel, current_user.id)
    if not db_user:
        raise HTTPException(status_code=404, detail="用户不存在")

    if not verify_password(password_data.old_password, db_user.password):
        raise HTTPException(status_code=400, detail="旧密码不正确")
    
    db_user.password = hash_password(password_data.password)
    await db.commit()
    await db.refresh(db_user)
    return MessageResponse(message="密码更新成功")

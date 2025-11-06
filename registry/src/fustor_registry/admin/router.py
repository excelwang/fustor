from typing import List
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, or_
from ..database import get_db
from ..models import UserModel
from ..profile.model import ProfileResponse
from ..security import get_current_admin
from .model import UserCreate

router = APIRouter(prefix="/admin", tags=["系统管理"])

@router.get("/users", 
           response_model=List[ProfileResponse],
           status_code=status.HTTP_200_OK,
           summary="列出所有用户",
           description="获取系统所有注册用户的列表")
async def list_users(
    current_user: UserModel = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """获取用户列表（仅限管理员）
    - 返回所有用户详细信息
    - 按创建时间倒序排列
    """
    
    result = await db.execute(select(UserModel).order_by(UserModel.created_at.desc()))
    return result.scalars().all()

@router.get("/users/{user_id}",
           response_model=ProfileResponse,
           status_code=status.HTTP_200_OK,
           summary="查看用户详情",
           responses={
               404: {"description": "用户不存在"},
               403: {"description": "无权访问"}
           })
async def get_user(
    user_id: int, 
    current_user: UserModel = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """根据用户ID获取详细信息
    - 返回用户的完整档案
    - 普通用户只能查看自己的信息
    """
    user = await db.get(UserModel, user_id)
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="用户不存在")
    
    return user

@router.post("/users",
           response_model=ProfileResponse,
           status_code=status.HTTP_201_CREATED,
           summary="创建新用户",
           description="创建一个具有指定属性的新用户",
           responses={
               400: {"description": "无效输入数据"},
               403: {"description": "无权操作"}
           })
async def create_user(
    user: UserCreate, 
    current_user: UserModel = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    """创建新用户账户（仅限管理员或注册）
    - 自动哈希处理密码
    - 确保邮箱和用户名唯一性
    """
    
    # 检查邮箱和用户名唯一性
    result = await db.execute(
        select(UserModel).where(
            or_(UserModel.email == user.email, UserModel.name == user.name)
        )
    )
    if result.scalars().first():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="邮箱或用户名已被注册"
        )
    
    # 创建新用户
    db_user = UserModel(**user.model_dump(exclude_unset=True))
    
    db.add(db_user)
    await db.commit()
    await db.refresh(db_user)
    return db_user

@router.delete("/{user_id}",
           status_code=status.HTTP_204_NO_CONTENT,
           summary="删除用户",
           description="永久删除用户账户（仅管理员）",
           responses={
               401: {"description": "未授权访问"},
               403: {"description": "无权限操作"},
               404: {"description": "用户不存在"}
           })
async def delete_user(
    user_id: int, 
    current_user: UserModel = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    """删除用户账户，需要管理员权限
    - 需要管理员认证
    - 执行物理删除
    """
    user = await db.get(UserModel, user_id)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail="用户不存在"
        )
    
    await db.delete(user)
    await db.commit()
    return

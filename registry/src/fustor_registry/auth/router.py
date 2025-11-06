from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from ..models import UserModel
from ..database import get_db
from .model import TokenResponse, LogoutResponse
from ..security import (
    create_access_token,
    create_refresh_token,
    get_current_user,
    blacklist_token,
    oauth2_scheme,
    verify_password
)

router = APIRouter(prefix="/auth", tags=["身份认证"])

@router.post("/login", response_model=TokenResponse, status_code=status.HTTP_200_OK, summary="用户登录获取访问令牌")
async def login_for_access_token(
    form_data: OAuth2PasswordRequestForm = Depends(),
    db: AsyncSession = Depends(get_db)
):
    user = (await db.execute(
        select(UserModel).where(
            UserModel.email == form_data.username,
            UserModel.is_active == True
        )
    )).scalar_one_or_none()
    if not user or not verify_password(form_data.password, str(user.password)):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="邮箱或密码错误",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token = create_access_token(data={"sub": str(user.id)})
    refresh_token = create_refresh_token(data={"sub": str(user.id)})
    
    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "bearer"
    }

@router.post("/refresh", 
           response_model=TokenResponse,
           summary="刷新访问令牌",
           description="使用刷新令牌获取新的访问凭证")
async def refresh_access_token(
    current_user: UserModel = Depends(get_current_user, use_cache=False),
    db: AsyncSession = Depends(get_db)
):
    await blacklist_token(Depends(oauth2_scheme), db)
    
    new_access_token = create_access_token(data={"sub": str(current_user.id)})
    new_refresh_token = create_refresh_token(data={"sub": str(current_user.id)})
    
    return {
        "access_token": new_access_token,
        "refresh_token": new_refresh_token,
        "token_type": "bearer"
    }

@router.get("/logout", 
           response_model=LogoutResponse,
           summary="用户登出",
           description="使当前访问令牌失效并加入黑名单")
async def logout_user(
    current_user: UserModel = Depends(get_current_user),
    token: str = Depends(oauth2_scheme),
    db: AsyncSession = Depends(get_db)
):
    await blacklist_token(token, db)
    return LogoutResponse(detail="注销成功")

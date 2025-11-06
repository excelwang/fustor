from datetime import datetime, timedelta, timezone
from typing import Annotated
from jose import JWTError, jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
import secrets
import hashlib
from .database import get_db
from .models import UserModel, UserLevel, TokenBlacklistModel
from .config import register_config

SECRET_KEY = register_config.FUSTOR_CORE_SECRET_KEY
ALGORITHM = register_config.FUSTOR_CORE_JWT_ALGORITHM
ACCESS_TOKEN_EXPIRE_MINUTES = register_config.FUSTOR_CORE_JWT_ACCESS_TOKEN_EXPIRE_MINUTES
REFRESH_TOKEN_EXPIRE_DAYS = register_config.FUSTOR_CORE_JWT_REFRESH_TOKEN_EXPIRE_DAYS

oauth2_scheme = OAuth2PasswordBearer(tokenUrl=f"/v1/auth/login")
ALGORITHM = "HS256"

async def is_token_blacklisted(token: str, db: AsyncSession) -> bool:
    """检查token是否在黑名单中"""
    result = await db.execute(
        select(TokenBlacklistModel).where(TokenBlacklistModel.token == token)
    )
    return result.scalar_one_or_none() is not None

async def blacklist_token(token: str, db: AsyncSession) -> None:
    """将token加入黑名单"""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        expires_at = datetime.fromtimestamp(payload["exp"], timezone.utc)
        db.add(TokenBlacklistModel(token=token, expires_at=expires_at))
        await db.commit()
    except JWTError:
        pass

async def get_current_user(
    token: Annotated[str, Depends(oauth2_scheme)],
    db: AsyncSession = Depends(get_db)
) -> UserModel:
    auth_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="非法访问",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    if await is_token_blacklisted(token, db):
        raise auth_exception
        
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        userid = payload.get("sub")
        if not isinstance(userid, str):
            raise auth_exception
        stat = select(UserModel).where(
                UserModel.id == int(userid),
                UserModel.is_active == True
            )
        user = (await db.execute(stat)).scalar_one_or_none()
        if user is None:
            raise auth_exception
        return user
    except JWTError:
        raise auth_exception

async def get_current_admin(
    token: Annotated[str, Depends(oauth2_scheme)],
    db: AsyncSession = Depends(get_db)
) -> UserModel:
    user = await get_current_user(token, db)
    stat = select(UserModel).where(
            UserModel.id == user.id,
            UserModel.level == UserLevel.ADMIN
        )
    user = (await db.execute(stat)).scalar_one_or_none()
    if user is None:
        raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="无权访问",
        headers={"WWW-Authenticate": "Bearer"},
    )
    return user

def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def create_refresh_token(data: dict):
    expire = datetime.now(timezone.utc) + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
    data.update({"exp": expire})
    encoded_jwt = jwt.encode(data, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def hash_password(password: str, salt = secrets.token_hex(16)) -> str:
    """使用pbkdf2_hmac进行密码哈希"""
    return f"{salt}${hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000).hex()}"

def verify_password(password: str, hashed: str) -> bool:
    """验证密码与哈希是否匹配"""
    salt = hashed.split("$")[0]
    new_hash = hash_password(password, salt)
    return secrets.compare_digest(new_hash, hashed)
from fastapi import APIRouter, HTTPException, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
import secrets
from ..database import get_db
from ..models import UserModel, DatastoreModel, UserAPIKeyModel
from ..security import get_current_user
from ..schemas import ApiKeyCreate, ApiKeyResponse

router = APIRouter(prefix="/keys", tags=["API Key管理"])

@router.post("/", response_model=ApiKeyResponse, status_code=status.HTTP_201_CREATED, summary="创建新的API密钥")
async def create_api_key(
    key_data: ApiKeyCreate,
    current_user: UserModel = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    # datastore_id is now mandatory, so we always check its validity
    result = await db.execute(
        select(DatastoreModel).where(
            DatastoreModel.id == key_data.datastore_id,
            DatastoreModel.owner_id == current_user.id
        )
    )
    if not result.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="无效的存储库ID或无权访问"
        )

    # 检查名称唯一性
    result = await db.execute(
        select(UserAPIKeyModel).where(
            UserAPIKeyModel.name == key_data.name,
            UserAPIKeyModel.user_id == current_user.id
        )
    )
    if result.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="API Key名称已存在"
        )
    
    # 创建记录
    db_key = UserAPIKeyModel(
        name=key_data.name,
        key=secrets.token_urlsafe(32),
        user_id=current_user.id,
        datastore_id=key_data.datastore_id
    )
    
    db.add(db_key)
    await db.commit()
    await db.refresh(db_key)
    
    return db_key

@router.get("/", 
           response_model=list[ApiKeyResponse], 
           summary="获取API密钥列表",
           description="列出当前用户所有已创建的API密钥")
async def list_api_keys(
    current_user: UserModel = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    result = await db.execute(
        select(UserAPIKeyModel).where(UserAPIKeyModel.user_id == current_user.id)
    )
    return result.scalars().all()

@router.delete("/{key_id}", 
              status_code=status.HTTP_204_NO_CONTENT,
              summary="删除API密钥",
              description="永久删除指定API密钥记录")
async def delete_api_key(
    key_id: int,
    current_user: UserModel = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    result = await db.execute(
        select(UserAPIKeyModel).where(
            UserAPIKeyModel.id == key_id,
            UserAPIKeyModel.user_id == current_user.id
        )
    )
    key = result.scalar_one_or_none()
    if not key:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="API Key未找到")
    
    await db.delete(key)
    await db.commit()
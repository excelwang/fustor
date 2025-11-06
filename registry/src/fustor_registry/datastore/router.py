from typing import List, Optional, Dict
from fastapi import APIRouter, HTTPException, status, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from ..database import get_db
from ..models import UserModel, DatastoreModel
from ..security import get_current_user
from .model import DatastoreResponse, DatastoreCreate

router = APIRouter(prefix="/datastores", tags=["存储库管理"])


@router.post("/", response_model=DatastoreResponse, status_code=status.HTTP_201_CREATED, summary="创建新的数据存储库")
async def register_datastore(
    db_create: DatastoreCreate,
    current_user: UserModel = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    db_model = DatastoreModel(
        name=db_create.name,
        meta=db_create.meta,
        visible=db_create.visible,
        allow_concurrent_push=db_create.allow_concurrent_push,
        session_timeout_seconds=db_create.session_timeout_seconds,
        owner_id=current_user.id
    )

    db.add(db_model)
    await db.commit()
    await db.refresh(db_model)
    return db_model

@router.get("/", 
           response_model=List[DatastoreResponse],
           summary="列出存储库",
           description="获取用户所有已注册的数据存储库")
async def list_datastores(
    current_user: UserModel = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)):
    """
    供用户查询所有已录入的数据存储库
    """
    result = await db.execute(select(DatastoreModel).where(
            DatastoreModel.owner_id == current_user.id # Corrected
        ))
    datastores = result.scalars().all()
    return datastores

@router.get("/{datastore_id}", 
           response_model=DatastoreResponse,
           summary="查看存储库详情",
           description="获取指定数据存储库的配置信息")
async def get_datastore(
    datastore_id: int,
    current_user: UserModel = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    result = await db.execute(
        select(DatastoreModel).where(
            DatastoreModel.id == datastore_id,
            DatastoreModel.owner_id == current_user.id # Corrected
        )
    )
    db_record = result.scalar_one_or_none()
    if not db_record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Datastore not found")
    return db_record

@router.put("/{datastore_id}", response_model=DatastoreResponse, summary="更新数据存储库配置")
async def update_datastore(
    datastore_id: int,
    db_update: DatastoreCreate,
    current_user: UserModel = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    result = await db.execute(
        select(DatastoreModel).where(
            DatastoreModel.id == datastore_id,
            DatastoreModel.owner_id == current_user.id # Corrected
        )
    )
    db_record = result.scalar_one_or_none()
    if not db_record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Datastore not found")
    
    for field, value in db_update.model_dump(exclude_unset=True).items():
        setattr(db_record, field, value)

    await db.commit()
    await db.refresh(db_record)
    return db_record

@router.delete("/{datastore_id}", status_code=status.HTTP_204_NO_CONTENT, summary="删除指定数据存储库")
async def delete_datastore(
    datastore_id: int,
    current_user: UserModel = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    result = await db.execute(
        select(DatastoreModel).where(
            DatastoreModel.id == datastore_id,
            DatastoreModel.owner_id == current_user.id # Corrected
        )
    )
    db_record = result.scalar_one_or_none()
    if not db_record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Datastore not found")
    await db.delete(db_record)
    await db.commit()
    return
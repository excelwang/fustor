from typing import List, Optional

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from fustor_registry_client.models import InternalApiKeyResponse, InternalDatastoreConfigResponse

from fustor_registry.database import get_db
from fustor_registry.models import UserAPIKeyModel, DatastoreModel

internal_keys_router = APIRouter()

@internal_keys_router.get("/api-keys", response_model=List[InternalApiKeyResponse], summary="获取所有API密钥及其关联的存储库ID (内部接口)")
async def get_all_api_keys(
    db: AsyncSession = Depends(get_db)
):
    result = await db.execute(
        select(UserAPIKeyModel.key, UserAPIKeyModel.datastore_id)
        .where(UserAPIKeyModel.is_active == True)
    )
    return [InternalApiKeyResponse(key=k, datastore_id=d) for k, d in result.all()]

@internal_keys_router.get("/datastores-config", response_model=List[InternalDatastoreConfigResponse], summary="获取所有Datastore的配置 (内部接口)")
async def get_all_datastores_config(
    db: AsyncSession = Depends(get_db)
):
    result = await db.execute(
        select(
            DatastoreModel.id,
            DatastoreModel.allow_concurrent_push,
            DatastoreModel.session_timeout_seconds
        )
    )

    datastores_map = {}
    for d_id, acp, sts in result.all():
        datastores_map[d_id] = InternalDatastoreConfigResponse(
            datastore_id=d_id,
            allow_concurrent_push=acp,
            session_timeout_seconds=sts
        )
    return list(datastores_map.values())

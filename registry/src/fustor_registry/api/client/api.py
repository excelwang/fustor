from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, status, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from fustor_registry_client.models import ClientApiKeyResponse, ClientDatastoreConfigResponse

from fustor_registry.database import get_db
from fustor_registry.models import UserAPIKeyModel, DatastoreModel
from fustor_registry.config import register_config

# Initialize the HTTPBearer security scheme
bearer_scheme = HTTPBearer()

async def authenticate_client_api(credentials: HTTPAuthorizationCredentials = Security(bearer_scheme)):
    """
    Authenticates client API requests using the configured API token.
    """
    expected_token = register_config.FUSTOR_REGISTRY_CLIENT_TOKEN
    if not expected_token:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Server configuration error: no API token configured"
        )

    if credentials.credentials != expected_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API token"
        )

client_keys_router = APIRouter()

@client_keys_router.get("/api-keys", response_model=List[ClientApiKeyResponse], summary="获取所有API密钥及其关联的存储库ID (内部接口)")
async def get_all_api_keys(
    db: AsyncSession = Depends(get_db),
    _: None = Security(authenticate_client_api)
):
    result = await db.execute(
        select(UserAPIKeyModel.key, UserAPIKeyModel.datastore_id)
        .where(UserAPIKeyModel.is_active == True)
    )
    return [ClientApiKeyResponse(key=k, datastore_id=d) for k, d in result.all()]

@client_keys_router.get("/datastores-config", response_model=List[ClientDatastoreConfigResponse], summary="获取所有Datastore的配置 (内部接口)")
async def get_all_datastores_config(
    db: AsyncSession = Depends(get_db),
    _: None = Security(authenticate_client_api)
):
    result = await db.execute(
        select(
            DatastoreModel.id,
            DatastoreModel.allow_concurrent_push,
            DatastoreModel.session_timeout_seconds,
            DatastoreModel.meta
        )
    )

    datastores_map = {}
    for d_id, acp, sts, meta in result.all():
        datastores_map[d_id] = ClientDatastoreConfigResponse(
            datastore_id=d_id,
            allow_concurrent_push=acp,
            session_timeout_seconds=sts,
            meta=meta
        )
    return list(datastores_map.values())

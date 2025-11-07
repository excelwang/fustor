# src/fustor_agent/api/schemas.py

from pydantic import BaseModel, Field
from typing import Generic, TypeVar, Dict, Any, Optional

from fustor_common.models import MessageResponse, ValidationResponse, CleanupResponse, AdminCredentials
from fustor_common.schemas import ConfigCreateResponse

T = TypeVar('T')

class TestSourceConnectionRequest(BaseModel):
    uri: str
    admin_creds: AdminCredentials

class TestPusherConnectionRequest(BaseModel):
    endpoint: str
    credential: Optional[AdminCredentials] = None

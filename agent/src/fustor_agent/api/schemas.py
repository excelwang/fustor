# src/fustor_agent/api/schemas.py

from pydantic import BaseModel, Field
from typing import List, Generic, TypeVar, Dict, Any, Optional
from fustor_common.models import MessageResponse, ValidationResponse, CleanupResponse, AdminCredentials

T = TypeVar('T')

class ConfigCreateResponse(BaseModel, Generic[T]):
    """A standard response for successfully creating a new configuration."""
    id: str
    config: T

class TestSourceConnectionRequest(BaseModel):
    uri: str
    admin_creds: AdminCredentials

class TestPusherConnectionRequest(BaseModel):
    endpoint: str
    credential: Optional[AdminCredentials] = None

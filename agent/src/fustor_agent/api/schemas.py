# src/fustor_agent/api/schemas.py

from pydantic import BaseModel, Field
from typing import List, Generic, TypeVar, Dict, Any, Optional

T = TypeVar('T')

class ValidationResponse(BaseModel):
    """A standard response model for validation actions."""
    success: bool
    message: str

class CleanupResponse(BaseModel):
    """A standard response model for cleanup actions."""
    message: str
    deleted_count: int
    deleted_ids: List[str]

class ConfigCreateResponse(BaseModel, Generic[T]):
    """A standard response for successfully creating a new configuration."""
    id: str
    config: T

class MessageResponse(BaseModel):
    """A simple response model for messages."""
    message: str

class AdminCredentials(BaseModel):
    user: str
    passwd: str

class TestSourceConnectionRequest(BaseModel):
    uri: str
    admin_creds: AdminCredentials

class TestPusherConnectionRequest(BaseModel):
    endpoint: str
    credential: Optional[AdminCredentials] = None

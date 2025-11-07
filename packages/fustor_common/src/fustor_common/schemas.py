from pydantic import BaseModel
from typing import Generic, TypeVar

T = TypeVar('T')

class ConfigCreateResponse(BaseModel, Generic[T]):
    """A standard response for successfully creating a new configuration."""
    id: str
    config: T

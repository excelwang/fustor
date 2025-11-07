from typing import Dict, Optional, List, Any, Protocol
from dataclasses import dataclass
import asyncio

@dataclass
class SessionInfo:
    session_id: str
    datastore_id: int
    last_activity: float
    created_at: float
    task_id: Optional[str] = None
    allow_concurrent_push: Optional[bool] = None
    session_timeout_seconds: Optional[int] = None
    client_ip: Optional[str] = None
    cleanup_task: Optional[asyncio.Task] = None

class ApiKeyCacheInterface(Protocol):
    """
    Interface for managing API key cache.
    """
    def set_cache(self, api_keys_data: List[Dict[str, Any]]):
        ...

    def get_datastore_id(self, api_key: str) -> Optional[int]:
        ...

from fustor_common.models import DatastoreConfig
from fustor_registry_client.models import InternalDatastoreConfigResponse

class DatastoreConfigCacheInterface(Protocol):
    """
    Interface for managing datastore config cache.
    """
    def set_cache(self, datastore_configs_data: List[InternalDatastoreConfigResponse]):
        ...

    def get_datastore_config(self, datastore_id: int) -> Optional[DatastoreConfig]:
        ...

from fustor_registry_client.models import InternalDatastoreConfigResponse

class ParserProcessingTaskManagerInterface(Protocol):
    """
    Interface for managing datastore processing tasks.
    """
    async def start_processing_for_datastore(self, datastore_id: int):
        ...

    async def stop_processing_for_datastore(self, datastore_id: int):
        ...

    async def sync_tasks(self, latest_datastore_configs: List[InternalDatastoreConfigResponse]):
        ...

    async def shutdown(self):
        ...

class SessionManagerInterface(Protocol):
    """
    Interface for managing user sessions.
    """
    async def create_session_entry(self, datastore_id: int, session_id: str, 
                                 task_id: Optional[str] = None, 
                                 client_ip: Optional[str] = None,
                                 allow_concurrent_push: Optional[bool] = None,
                                 session_timeout_seconds: Optional[int] = None) -> SessionInfo:
        ...

    async def keep_session_alive(self, datastore_id: int, session_id: str, 
                               client_ip: Optional[str] = None) -> Optional[SessionInfo]:
        ...

    async def get_session_info(self, datastore_id: int, session_id: str) -> Optional[SessionInfo]:
        ...

    async def get_datastore_sessions(self, datastore_id: int) -> Dict[str, SessionInfo]:
        ...

    async def remove_session(self, datastore_id: int, session_id: str) -> bool:
        ...

    async def cleanup_expired_sessions(self):
        ...

    async def terminate_session(self, datastore_id: int, session_id: str) -> bool:
        ...

    async def start_periodic_cleanup(self, interval_seconds: int = 60):
        ...

    async def stop_periodic_cleanup(self):
        ...
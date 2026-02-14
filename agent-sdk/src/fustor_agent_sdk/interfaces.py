from typing import Protocol, Dict, Any, List, Optional, TypeVar, Tuple, Set
from pydantic import BaseModel # Import BaseModel

from fustor_core.models.config import SourceConfig, FieldMapping
from fustor_core.models.states import EventBusInstance, EventBusState

T = TypeVar('T', bound=BaseModel) # Use BaseModel here

class BaseConfigService(Protocol[T]):
    """
    Interface for common config management operations.
    """
    def list_configs(self) -> Dict[str, T]:
        ...

    def get_config(self, id: str) -> Optional[T]:
        ...

    async def add_config(self, id: str, config: T) -> T:
        ...

    async def update_config(self, id: str, updates: Dict[str, Any]) -> T:
        ...

    async def delete_config(self, id: str) -> T:
        ...

    async def enable(self, id: str) -> T:
        ...

    async def disable(self, id: str) -> T:
        ...

class SourceConfigServiceInterface(BaseConfigService[SourceConfig]):
    """
    Interface for managing SourceConfig objects.
    Defines the contract for CRUD operations and schema management for source configurations.
    """
    async def cleanup_obsolete_configs(self) -> List[str]:
        ...

    async def check_and_disable_missing_schema_sources(self) -> List[str]:
        ...

    async def discover_and_cache_fields(self, source_id: str, admin_user: str, admin_password: str):
        ...

from fustor_core.models.config import SenderConfig

class SenderConfigServiceInterface(BaseConfigService[SenderConfig]):
    """
    Interface for managing SenderConfig objects.
    
    Senders are responsible for transmitting events from Agent to Fusion.
    """
    async def cleanup_obsolete_configs(self) -> List[str]:
        ...



from fustor_core.models.config import PipeConfig

class PipeConfigServiceInterface(BaseConfigService[PipeConfig]):
    """
    Interface for managing PipeConfig objects.
    """
    async def enable(self, id: str):
        """Enables a Pipe configuration, ensuring its source and sender are also enabled."""
        ...

class BaseInstanceServiceInterface(Protocol):
    """
    Interface for common instance management operations.
    """
    def get_instance(self, id: str) -> Optional[Any]:
        ...

    def list_instances(self) -> List[Any]:
        ...

class EventBusServiceInterface(BaseInstanceServiceInterface):
    """
    Interface for managing EventBusService objects.
    """
    def set_dependencies(self, pipe_instance_service: "PipeInstanceService"):
        ...

    async def get_or_create_bus_for_subscriber(
        self, 
        source_id: str,
        source_config: SourceConfig, 
        pipe_id: str,
        required_position: int,
        fields_mapping: List[FieldMapping]
    ) -> Tuple[Any, bool]: # Use Any for EventBusInstanceRuntime to avoid circular import
        ...

    async def release_subscriber(self, bus_id: str, pipe_id: str):
        ...

    async def release_all_unused_buses(self):
        ...

    async def commit_and_handle_split(
        self, 
        bus_id: str, 
        pipe_id: str, 
        num_events: int, 
        last_consumed_position: int,
        fields_mapping: List[FieldMapping]
    ):
        ...

from fustor_core.models.config import SourceConfig

class SourceDriverServiceInterface(Protocol):
    """
    Interface for discovering and interacting with Source driver classes.
    """
    def list_available_drivers(self) -> List[str]:
        ...

    async def get_available_fields(self, driver_type: str, **kwargs) -> Dict[str, Any]:
        ...

    async def test_connection(self, driver_type: str, **kwargs) -> Tuple[bool, str]:
        ...

    async def check_params(self, driver_type: str, **kwargs) -> Tuple[bool, str]:
        ...

    async def create_agent_user(self, driver_type: str, **kwargs) -> Tuple[bool, str]:
        ...

    async def check_privileges(self, driver_type: str, **kwargs) -> Tuple[bool, str]:
        ...

class SenderDriverServiceInterface(Protocol):
    """
    Interface for discovering and interacting with Sender driver classes.
    
    Senders are responsible for transmitting events from Agent to Fusion.
    """
    def list_available_drivers(self) -> List[str]:
        ...


        ...

    async def check_privileges(self, driver_type: str, **kwargs) -> Tuple[bool, str]:
        ...

    async def get_needed_fields(self, driver_type: str, **kwargs) -> Dict[str, Any]:
        ...



from fustor_core.models.states import PipeState

class PipeInstanceServiceInterface(BaseInstanceServiceInterface):
    """
    Interface for managing PipeInstanceService objects.
    """
    async def start_one(self, id: str):
        ...

    async def stop_one(self, id: str, should_release_bus: bool = True):
        ...

    async def remap_pipe_to_new_bus(self, pipe_id: str, new_bus: Any, needed_position_lost: bool):
        ...

    async def mark_dependent_pipes_outdated(self, dependency_type: str, dependency_id: str, reason_info: str, updates: Optional[Dict[str, Any]] = None):
        ...

    async def start_all_enabled(self):
        ...

    async def restart_outdated_pipes(self) -> int:
        ...

    async def stop_all(self):
        ...


class CommandProcessorInterface(Protocol):
    """
    Interface for processing commands from Fusion (L3 Management).
    """
    async def process_commands(self, pipe: Any, commands: List[Dict[str, Any]]) -> None:
        """Process a list of management commands for a specific pipe."""
        ...

    async def initialize(self, pipe: Any) -> None:
        """Initialize the command processor for a specific pipe."""
        ...
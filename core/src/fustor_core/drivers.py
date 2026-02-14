"""
Abstract Base Classes for Fuagent Drivers.

This module defines the formal interface for Source and Sender drivers.
All drivers must inherit from the appropriate base class and implement its
abstract methods.
"""
from abc import ABC, abstractmethod
from typing import (
    Any,
    Dict,
    Iterator,
    List,
    Tuple,
    Optional,
)

# Use local imports to avoid circular dependency with deprecated fustor_event_model
from fustor_core.event.base import EventBase
from fustor_core.models.config import SourceConfig, SenderConfig


class ViewDriver(ABC):
    """
    Abstract Base Class for View Drivers.
    
    A ViewDriver consumes events and maintains a consistent, queryable view of data.
    This is the Fusion-side counterpart to SourceDriver (Agent-side).
    
    View drivers are discovered via the 'fustor.view_drivers' entry point group.
    """
    
    # The schema/table pattern this driver handles (e.g., "file_directory")
    # Used by ViewManager to route events to the appropriate driver.
    target_schema: str = ""
    
    def __init__(self, id: str, view_id: str, config: Dict[str, Any]):
        """
        Initialize the view driver.
        
        Args:
            id: Unique identifier for this view instance.
            view_id: identifier for the view group.
            config: Driver-specific configuration dictionary.
        """
        self.id = id
        self.view_id = view_id
        self.config = config

    async def initialize(self):
        """
        Optional: Perform asynchronous initialization.
        """
        pass
    

    
    @property
    def requires_full_reset_on_session_close(self) -> bool:
        """
        Indicates if the view should be fully reset when the last session closes.
        This often corresponds to 'Live' mode drivers where state is ephemeral to the session.
        """
        if self.config:
            # Check for standard 'live' config patterns
            return self.config.get("mode") == "live" or self.config.get("is_live") is True
        return False

    @abstractmethod
    async def process_event(self, event: EventBase) -> bool:
        """
        Process a single event and update internal state.
        
        Returns:
            True if the event was successfully processed, False otherwise.
        """
        raise NotImplementedError
    
    @abstractmethod
    async def get_data_view(self, **kwargs) -> Any:
        """
        Return the current consistent data view.
        
        The format of the returned data is driver-specific.
        """
        raise NotImplementedError

    async def on_session_start(self, **kwargs):
        """Called when a new Agent session starts."""
        pass
    
    async def on_session_close(self, **kwargs):
        """Called when an Agent session terminates."""
        pass

    async def handle_audit_start(self):
        """Called at the start of an audit cycle. Optional hook."""
        pass
    
    async def handle_audit_end(self):
        """Called at the end of an audit cycle. Optional hook for cleanup/reconciliation."""
        pass

    async def reset(self):
        """Clears all in-memory state for this driver. Optional hook."""
        pass
    
    async def cleanup_expired_suspects(self):
        """Periodic cleanup hook for time-sensitive data. Optional."""
        pass

    async def close(self):
        """
        Optional: Gracefully closes any open resources.
        """
        pass


    async def resolve_session_role(self, session_id: str, **kwargs) -> Dict[str, Any]:
        """
        Determine the role (leader/follower) for a newly created session.
        
        Args:
            session_id: The ID of the session being created
            kwargs: Additional context (e.g., pipe_id for ForestView scoping)
            
        Returns:
            Dict containing 'role' (leader/follower) and other metadata.
            Default implementation returns leader role (no election).
        """
        return {"role": "leader"}




class SenderDriver(ABC):
    """
    Abstract Base Class for all Sender drivers.

    Defines the contract for drivers that receive data from the Fuagent core
    and transmit it to a destination (e.g., Fusion, Object Storage).
    """

    def __init__(self, id: str, config: SenderConfig):
        """
        Initializes the driver with its specific configuration.
        """
        self.id = id
        self.config = config

    @abstractmethod
    async def send_events(
        self, 
        events: List[EventBase], 
        source_type: str = "message", 
        is_end: bool = False,
        **kwargs
    ) -> Dict:
        """
        Transmits a list of events to the destination.
        
        Args:
            events: List of events to send.
            source_type: Type of events ('message', 'snapshot', 'audit').
            is_end: Whether this is the final batch for the current session/phase.
            **kwargs: Implementation specific parameters.
        """
        raise NotImplementedError



    async def create_session(self, task_id: str) -> str:
        """
        Creates a new session with the destination.
        Returns the session ID string (or dict with session details).
        """
        raise NotImplementedError

    async def heartbeat(self, **kwargs) -> Dict:
        """
        Sends a heartbeat to maintain session state.
        """
        raise NotImplementedError



    async def signal_audit_start(self, source_id: Any) -> bool:
        """
        Optional: Signals the start of an audit cycle.
        """
        return False

    async def signal_audit_end(self, source_id: Any) -> bool:
        """
        Optional: Signals the end of an audit cycle.
        """
        return False

    async def get_sentinel_tasks(self, **kwargs) -> Optional[Dict[str, Any]]:
        """
        Optional: Queries for sentinel tasks.
        """
        return None

    async def submit_sentinel_results(self, results: Dict[str, Any], **kwargs) -> bool:
        """
        Optional: Submits sentinel task results.
        """
        return True

    async def close(self):
        """
        Optional: Gracefully closes any open resources, like network clients.
        """
        pass

    @classmethod
    @abstractmethod
    async def get_needed_fields(cls, **kwargs) -> Dict:
        """
        Declares the data fields required by this sender.
        Returns a JSON Schema dictionary. An empty dict means all fields are accepted.
        """
        raise NotImplementedError

    @classmethod
    async def test_connection(cls, **kwargs) -> Tuple[bool, str]:
        """
        Optional: Tests the connection to the source service.
        """
        return (True, "Connection test not implemented for this driver.")

    @classmethod
    async def check_privileges(cls, **kwargs) -> Tuple[bool, str]:
        """
        Optional: Checks if the provided credentials have sufficient privileges.
        """
        return (True, "Privilege check not implemented for this driver.")  
    



class SourceDriver(ABC):
    """
    Abstract Base Class for all Source drivers.

    Defines the contract for drivers that produce data for the Fuagent core.
    """

    def __init__(self, id: str, config: SourceConfig):
        """
        Initializes the driver with its specific configuration.
        """
        self.id = id
        self.config = config

    # Indicates whether this source driver requires a formal schema discovery process.
    # If False, the agent will skip the discovery step and consider the schema valid.
    require_schema_discovery: bool = True

    @property
    def is_transient(self) -> bool:
        """
        Indicates whether this source driver is transient.
        Transient sources lose events if not processed immediately.
        Defaults to False. Drivers that are transient should override this property.
        """
        return False

    @abstractmethod
    def get_snapshot_iterator(self, **kwargs) -> Iterator[EventBase]:
        """
        Performs a one-time, full snapshot of the source data.
        This method returns an iterator that yields new events.
        """
        raise NotImplementedError

    def is_position_available(self, position: int) -> bool:
        """
        Checks if the driver can resume from a specific position.
        For transient sources, this should return False since they don't keep historical events.
        Defaults to True for non-transient sources, but drivers should override this method
        to provide accurate information about position availability.
        """
        if position <= 0: #means from the latest snapshot
            return False
        return not self.is_transient

    @abstractmethod
    def get_message_iterator(self, start_position: int = -1, **kwargs) -> Iterator[EventBase]:
        """
        Performs incremental data capture (CDC).

        This method returns an iterator that yields new events.
        Optionally, a start_position can be provided to resume from a specific point.
        Use is_position_available() to check if a position can be resumed from.
        
        Args:
            start_position (int): The position to start from, or -1 for latest position
            **kwargs: Additional implementation-specific parameters

        Returns:
            Iterator[EventBase]: An iterator that yields new events.
        """
        raise NotImplementedError

    def get_audit_iterator(self, **kwargs) -> Iterator[EventBase]:
        """
        Optional: Performs a consistency audit of the source data.
        
        This method returns an iterator that yields events representing the current state,
        similar to snapshot but used for periodic consistency checks and 'blind-spot' detection.
        Unlike snapshot, audit events typically include message_source='audit'.

        Default implementation returns an empty iterator, meaning no audit is performed.
        """
        return iter([])

    def perform_sentinel_check(self, task_batch: Dict[str, Any]) -> Dict[str, Any]:
        """
        Optional: Performs a sentinel check based on the provided task batch.
        
        Args:
            task_batch (Dict[str, Any]): The tasks to verify, e.g., {'type': 'suspect_check', 'paths': [...]}.

        Returns:
            Dict[str, Any]: The verification results.
        
        Default implementation returns empty dict.
        """
        return {}

    async def close(self):
        """
        Optional: Gracefully closes any open resources, like database connections or file handles.
        """
        pass

    @classmethod
    @abstractmethod
    async def get_available_fields(cls, **kwargs) -> Dict:
        """
        Declares the data fields that this source can provide.
        Returns a JSON Schema dictionary.
        """
        raise NotImplementedError

    @classmethod
    async def test_connection(cls, **kwargs) -> Tuple[bool, str]:
        """
        Optional: Tests the connection to the source service.
        """
        return (True, "Connection test not implemented for this driver.")

    @classmethod
    async def check_privileges(cls, **kwargs) -> Tuple[bool, str]:
        """
        Optional: Checks if the provided credentials have sufficient privileges.
        """
        return (True, "Privilege check not implemented for this driver.")

    @classmethod
    async def check_runtime_params(cls, **kwargs) -> Tuple[bool, str]:
        """
        Optional: Checks if the runtime parameters of the underlying source system are edequate for generating events.
        """
        return (True, "Runtime parameter check not implemented for this driver.")

    @classmethod
    async def create_agent_user(cls, **kwargs) -> Tuple[bool, str]:
        """
        Optional: Creates a agent user for the source service.
        """
        return (True, "Agent user creation not implemented for this driver.")
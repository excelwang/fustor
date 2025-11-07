"""
Abstract Base Classes for Fuagent Drivers.

This module defines the formal interface for Source and Pusher drivers.
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
)

# Forward-referencing models to avoid circular imports if drivers.py is imported by models
from fustor_event_model.models import EventBase # Import EventBase from fustor_event_model
from fustor_core.models.config import SourceConfig, PusherConfig


class PusherDriver(ABC):
    """
    Abstract Base Class for all Pusher drivers.

    Defines the contract for drivers that receive data from the Fuagent core.
    These drivers are expected to be asynchronous.
    """

    def __init__(self, id: str, config: PusherConfig):
        """
        Initializes the driver with its specific configuration.
        """
        self.id = id
        self.config = config

    @abstractmethod
    async def push(self, events: List[EventBase], **kwargs) -> Dict:
        """
        Receives and processes a list of events. This is the primary data-writing method.
        """
        raise NotImplementedError
    
    async def get_latest_committed_index(self, **kwargs) -> int:
        """
        Optional: Gets the last successfully processed index from the pusher endpoint.
        Used for resumable syncs. A return value of -1 indicates starting from the beginning.
        """
        return -1
    
    @abstractmethod
    async def heartbeat(self, **kwargs) -> Dict:
        """
        Sends a heartbeat to maintain session state with the pusher endpoint.
        The `kwargs` will contain `agent_id`, `task_id`, and `session_id`.
        Returns a dictionary with status information.
        """
        raise NotImplementedError
    
    @abstractmethod
    async def create_session(self, task_id: str) -> str:
        """
        Creates a new session with the pusher endpoint.
        Returns the session ID string.
        """
        raise NotImplementedError

    async def close(self):
        """
        Optional: Gracefully closes any open resources, like network clients.
        """
        pass

    @classmethod
    @abstractmethod
    async def get_needed_fields(cls, **kwargs) -> Dict:
        """
        Declares the data fields required by this pusher.
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
    
    @classmethod
    async def get_wizard_steps(cls) -> Dict[str, Any]:
        """
        Optional: Provides configuration wizard steps for UI integration.
        Returns a dictionary defining the steps.
        """
        return {} 

class SourceDriver(ABC):
    """
    Abstract Base Class for all Source drivers.

    Defines the contract for drivers that produce data for the Fuagent core.
    Note the mix of synchronous and asynchronous methods, reflecting the current
    design of existing drivers (e.g., threading-based fs vs. async network drivers).
    """

    def __init__(self, id: str, config: SourceConfig):
        """
        Initializes the driver with its specific configuration.
        """
        self.id = id
        self.config = config

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
    
    @classmethod
    async def get_wizard_steps(cls) -> Dict[str, Any]:
        """
        Optional: Provides configuration wizard steps for UI integration.
        Returns a dictionary defining the steps.
        """
        return {}
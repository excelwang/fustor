"""
Fustor Acceptance Test Protocol

This module defines the abstract interface for testing Fustor's black-box behavior.
Both Mock and Real adapters implement this protocol, enabling test reuse.

Usage:
    pytest tests/acceptance/ --system=mock   # Fast, logic validation
    pytest tests/acceptance/ --system=real   # Slow, real system validation
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class EventAction(Enum):
    CREATE = "create"
    MODIFY = "modify"
    DELETE = "delete"


class ConsistencyState(Enum):
    STABLE = "stable"
    SUSPECT = "suspect"
    UNKNOWN = "unknown"


@dataclass
class DataItem:
    """
    A piece of data in the unified view.
    Abstract representation - could be file, S3 object, DB record, etc.
    """
    id: str                          # Unique identifier (path, key, URI)
    version: Any                     # Version indicator (mtime, etag, revision)
    metadata: dict = field(default_factory=dict)


@dataclass
class Event:
    """
    A change event from a data source.
    """
    source_id: str                   # Which source reported this
    item_id: str                     # Which item changed
    action: EventAction              # What happened
    version: Any                     # Version at change time
    is_audit: bool = False           # Was this from audit (vs realtime)?


@dataclass
class Observation:
    """
    What a user observes when querying the unified view.
    """
    exists: bool
    item: Optional[DataItem] = None
    consistency_state: ConsistencyState = ConsistencyState.UNKNOWN


@dataclass
class SubmitResult:
    """
    Result of submitting an event.
    """
    accepted: bool
    reason: Optional[str] = None


class UnifiedDataSystem(ABC):
    """
    Abstract interface for ANY unified data system.
    
    L1 Contracts tests depend ONLY on this interface.
    Does NOT assume: file system, events, tree structure, mtime, etc.
    """
    
    @abstractmethod
    def setup(self, initial_items: list[dict]) -> None:
        """
        Initialize the system with given items.
        
        Args:
            initial_items: List of {"id": str, "version": Any, ...}
        """
        pass
    
    @abstractmethod
    def submit_event(self, event: Event) -> SubmitResult:
        """
        Submit a change event to the system.
        
        Returns:
            SubmitResult indicating if event was accepted or discarded.
        """
        pass
    
    @abstractmethod
    def observe(self, item_id: str) -> Observation:
        """
        Observe an item from the unified view.
        
        Returns:
            Observation with existence, data, and consistency state.
        """
        pass
    
    @abstractmethod
    def list_items(self, prefix: Optional[str] = None) -> list[str]:
        """
        List all item IDs in the unified view.
        
        Args:
            prefix: Optional prefix filter.
        Returns:
            List of item IDs.
        """
        pass
    
    @abstractmethod
    def wait_for_consistency(self, timeout_sec: float = 10.0) -> bool:
        """
        Wait until system reaches eventual consistency.
        
        For mock: Immediate.
        For real: Poll until quiescent or timeout.
        
        Returns:
            True if consistent, False if timeout.
        """
        pass
    
    @abstractmethod
    def teardown(self) -> None:
        """Cleanup after test."""
        pass

"""
Integration layer for using in-memory queue instead of database for event ingestion.
This module provides high-throughput ingestion using an in-memory queue.
"""
import asyncio
import logging
from typing import Dict, List, Optional, Any
from .in_memory_queue import memory_event_queue
from fustor_event_model.models import EventBase


logger = logging.getLogger(__name__)


class QueueBasedIngestor:
    """
    Integration class that uses in-memory queue for high-throughput event ingestion.
    """
    
    async def initialize(self):
        """Initialize the queue-based ingestor."""
        # Load queue state from file if it exists
        await memory_event_queue.load_from_file()

        # Start periodic file persistence (every 15 minutes)
        await memory_event_queue.start_periodic_file_persistence(interval_seconds=900)

        logger.info("Queue-based ingestor initialized")

    async def add_event(self, datastore_id: int, event: EventBase, task_id: Optional[str] = None) -> str:
        """
        Add an event to the in-memory queue.
        
        Args:
            datastore_id: The ID of the datastore
            event: The EventBase object to add
            task_id: Optional task ID for position tracking
            
        Returns:
            Event ID if successful
        """
        return await memory_event_queue.add_event(datastore_id, event, task_id)

    async def add_events_batch(self, datastore_id: int, events: List[EventBase], task_id: Optional[str] = None) -> int:
        """
        Add a batch of events to the in-memory queue.
        
        Args:
            datastore_id: The ID of the datastore
            events: List of EventBase objects to add
            task_id: Optional task ID for position tracking
            
        Returns:
            Number of events successfully added
        """
        return await memory_event_queue.add_events_batch(datastore_id, events, task_id)

    async def process_next_batch(self, datastore_id: int, batch_size: int = 100) -> List[EventBase]:
        """
        Process the next batch of events from the queue.
        
        Args:
            datastore_id: The ID of the datastore to process
            batch_size: Maximum number of events to process
            
        Returns:
            List of EventBase objects that were processed
        """
        queued_events = await memory_event_queue.get_events_batch(datastore_id, batch_size)
        return [qe.event for qe in queued_events]

    async def get_position(self, datastore_id: int, task_id: str) -> Optional[int]:
        """
        Get the current position for a specific datastore and task.
        
        Args:
            datastore_id: The ID of the datastore
            task_id: The task ID
            
        Returns:
            The highest processed index, or None if no position exists
        """
        return await memory_event_queue.get_position(datastore_id, task_id)

    async def update_position(self, datastore_id: int, task_id: str, index: int) -> None:
        """
        Update the position for a specific datastore and task.
        
        Args:
            datastore_id: The ID of the datastore
            task_id: The task ID
            index: The index to set as the position
        """
        await memory_event_queue.update_position(datastore_id, task_id, index)

    async def get_queue_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the in-memory queue.
        
        Returns:
            Dictionary with queue statistics
        """
        return await memory_event_queue.get_stats()


# Global instance
queue_based_ingestor = QueueBasedIngestor()


# Functions for direct usage
async def add_event_to_queue(datastore_id: int, event: EventBase, task_id: Optional[str] = None) -> str:
    """
    Add an event to the in-memory queue.
    """
    return await queue_based_ingestor.add_event(datastore_id, content, task_id)


async def add_events_batch_to_queue(datastore_id: int, events: List[EventBase], task_id: Optional[str] = None) -> int:
    """
    Add a batch of events to the in-memory queue.
    """
    return await queue_based_ingestor.add_events_batch(datastore_id, events, task_id)


async def get_events_from_queue(datastore_id: int, batch_size: int = 100) -> List[EventBase]:
    """
    Get a batch of events from the in-memory queue.
    """
    return await queue_based_ingestor.process_next_batch(datastore_id, batch_size)


async def get_position_from_queue(datastore_id: int, task_id: str) -> Optional[int]:
    """
    Get the current position for a specific datastore and task.
    """
    return await queue_based_ingestor.get_position(datastore_id, task_id)


async def update_position_in_queue(datastore_id: int, task_id: str, index: int) -> None:
    """
    Update the position for a specific datastore and task.
    """
    await queue_based_ingestor.update_position(datastore_id, task_id, index)
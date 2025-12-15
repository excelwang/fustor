"""
Pure Python memory queue implementation for high-throughput event ingestion.
Provides an alternative to database-based event storage using asyncio queues.
"""
import asyncio
import json
import logging
import time
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from collections import defaultdict
from uuid import uuid4
from fustor_event_model.models import EventBase


logger = logging.getLogger(__name__)


@dataclass
class QueuedEvent:
    """Represents an event in the queue."""
    id: str
    datastore_id: int
    event: EventBase # Changed content to event, type is EventBase
    timestamp: float = field(default_factory=time.time)
    processing_attempts: int = 0


class InMemoryEventQueue:
    """
    Pure Python in-memory event queue for high-throughput ingestion.
    Uses asyncio queues for each datastore.
    """

    def __init__(self):
        # Dictionary of asyncio queues, one per datastore_id
        self._queues: Dict[int, asyncio.Queue] = defaultdict(asyncio.Queue)
        # Lock for thread-safe operations
        self._lock = asyncio.Lock()
        # Statistics
        self._stats = {
            "total_events_sent": 0,
            "total_events_received": 0,
        }
        # Positions to track the highest processed index for each (datastore_id, task_id) combination
        self._positions: Dict[Tuple[int, str], int] = {} # task_id can be a unique string

    async def add_event(self, datastore_id: int, event: EventBase, task_id: Optional[str] = None) -> str:
        """
        Add an event to the queue for a specific datastore.

        Args:
            datastore_id: The ID of the datastore
            event: The EventBase object to add
            task_id: Optional task ID for position tracking

        Returns:
            Event ID if successful
        """
        event_id = str(uuid4())
        queued_event = QueuedEvent(
            id=event_id,
            datastore_id=datastore_id,
            event=event
        )

        queue_key = datastore_id
        queue = self._queues[queue_key]
        await queue.put(queued_event)

        async with self._lock:
            self._stats["total_events_sent"] += 1

            # Update position if task_id is provided and event has index
            if task_id and event.index != -1:
                current_position = self._positions.get((datastore_id, task_id), 0)
                new_index = event.index
                if new_index > current_position:
                    self._positions[(datastore_id, task_id)] = new_index

        logger.debug(f"Added event {event_id} to datastore {datastore_id} queue")
        return event_id

    async def add_events_batch(self, datastore_id: int, events: List[EventBase], task_id: Optional[str] = None) -> int:
        """
        Add a batch of events for a specific datastore.

        Args:
            datastore_id: The ID of the datastore
            events: List of EventBase objects to add
            task_id: Optional task ID for position tracking

        Returns:
            Number of events successfully added
        """
        added_count = 0
        max_index = 0

        for event in events:
            try:
                await self.add_event(datastore_id, event, task_id)
                added_count += 1

                # Track the maximum index for position update
                if task_id and event.index != -1:
                    index_value = event.index
                    if index_value > max_index:
                        max_index = index_value
            except Exception as e:
                logger.error(f"Failed to add event to datastore {datastore_id}: {e}")

        # Update position with the maximum index from the batch
        if task_id and max_index > 0:
            async with self._lock:
                current_position = self._positions.get((datastore_id, task_id), 0)
                if max_index > current_position:
                    self._positions[(datastore_id, task_id)] = max_index

        return added_count

    async def get_event(self, datastore_id: int, timeout: Optional[float] = None) -> Optional[QueuedEvent]:
        """
        Get a single event from the queue for a specific datastore.

        Args:
            datastore_id: The ID of the datastore
            timeout: Optional timeout in seconds

        Returns:
            QueuedEvent if available, None if timeout or exception
        """
        queue_key = datastore_id
        queue = self._queues.get(queue_key)
        if queue is None or queue.empty():
            return None

        try:
            # Get event from queue with optional timeout
            if timeout:
                try:
                    queued_event = await asyncio.wait_for(queue.get(), timeout=timeout)
                except asyncio.TimeoutError:
                    return None
            else:
                queued_event = await queue.get()

            async with self._lock:
                self._stats["total_events_received"] += 1

            logger.debug(f"Retrieved event {queued_event.id} from datastore {datastore_id} queue")
            return queued_event
        except Exception as e:
            logger.error(f"Error getting event from queue: {e}")
            return None

    async def get_events_batch(
        self,
        datastore_id: int,
        batch_size: int = 100,
        timeout: Optional[float] = 1.0
    ) -> List[QueuedEvent]:
        """
        Get a batch of events from the queue for a specific datastore.

        Args:
            datastore_id: The ID of the datastore
            batch_size: Maximum number of events to get
            timeout: Timeout for waiting for first event

        Returns:
            List of QueuedEvents
        """
        events = []

        # Get the first event with timeout
        first_event = await self.get_event(datastore_id, timeout)
        if first_event:
            events.append(first_event)
        else:
            return events  # Return empty if no initial event

        # Try to get more events without blocking
        queue_key = datastore_id
        queue = self._queues.get(queue_key)
        if queue:
            for _ in range(batch_size - 1):
                if queue.empty():
                    break
                try:
                    event = queue.get_nowait()
                    events.append(event)
                    async with self._lock:
                        self._stats["total_events_received"] += 1
                except asyncio.QueueEmpty:
                    break  # No more events available

        logger.debug(f"Retrieved batch of {len(events)} events for datastore {datastore_id}")
        return events

    async def get_position(self, datastore_id: int, task_id: str) -> Optional[int]:
        """
        Get the current position (highest processed index) for a specific datastore and task.

        Args:
            datastore_id: The ID of the datastore
            task_id: The task ID

        Returns:
            The highest processed index, or None if no position exists
        """
        async with self._lock:
            return self._positions.get((datastore_id, task_id))

    async def update_position(self, datastore_id: int, task_id: str, index: int) -> None:
        """
        Update the position for a specific datastore and task.

        Args:
            datastore_id: The ID of the datastore
            task_id: The task ID
            index: The index to set as the position
        """
        async with self._lock:
            current_position = self._positions.get((datastore_id, task_id), 0)
            if index > current_position:
                self._positions[(datastore_id, task_id)] = index

    async def get_stats(self) -> Dict[str, Any]:
        """
        Get queue statistics.
        """
        async with self._lock:
            stats = self._stats.copy()

        # Add queue-specific stats
        for ds_id, queue in self._queues.items():
            stats[f'datastore_{ds_id}_queue_size'] = queue.qsize()

        return stats

    def get_queue_size(self, datastore_id: int) -> int:
        """
        Get the current size of a specific datastore queue.
        """
        queue_key = datastore_id
        queue = self._queues.get(queue_key)
        return queue.qsize() if queue else 0

    async def clear_datastore_data(self, datastore_id: int):
        """
        Clears the event queue and all position tracking for a given datastore.
        """
        async with self._lock:
            # Clear the queue for the datastore
            if datastore_id in self._queues:
                # Draining the queue to prevent dangling references (though not strictly necessary for deletion)
                while not self._queues[datastore_id].empty():
                    self._queues[datastore_id].get_nowait()
                del self._queues[datastore_id]
                logger.info(f"Cleared event queue for datastore {datastore_id}.")
            
            # Clear all positions associated with this datastore
            keys_to_delete = [key for key in self._positions if key[0] == datastore_id]
            for key in keys_to_delete:
                del self._positions[key]
            if keys_to_delete:
                logger.info(f"Cleared {len(keys_to_delete)} position entries for datastore {datastore_id}.")


# Global instance
memory_event_queue = InMemoryEventQueue()
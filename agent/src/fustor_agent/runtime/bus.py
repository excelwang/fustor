import asyncio
import logging
from collections import deque
from typing import Dict, List, Optional, Any, Set

from fustor_core.models.config import FieldMapping
from fustor_core.event import EventBase
from fustor_core.exceptions import TransientSourceBufferFullError

logger = logging.getLogger("fustor_agent")

# A type alias for clarity in subscriber state tracking
SubscriberState = Dict[str, Any]  # last_consumed_index: int, last_seen_position: int

class EventBusFailedError(Exception):
    """Custom exception raised when operating on a failed event bus."""
    pass

class MemoryEventBus:
    """
    An in-memory, multi-consumer event bus with automatic garbage collection
    and support for dynamic consumer splitting.
    """
    def __init__(self, bus_id: str, capacity: int, start_position: int):
        self.id = bus_id
        self.capacity = capacity
        self.buffer: deque[EventBase] = deque()
        self.subscribers: Dict[str, SubscriberState] = {}
        self.lock = asyncio.Lock()
        self._producer_can_put = asyncio.Event()
        self._producer_can_put.set()
        self._consumer_can_get = asyncio.Event()
        self.failed: bool = False
        self.error_message: Optional[str] = None
        self.buffer_start_position: int = start_position
        self.low_watermark: int = start_position
        self._next_event_index: int = start_position
        self.subscriber_field_map: Dict[str, Set[str]] = {}
        self.required_fields: Set[str] = set()

    def _recalculate_required_fields(self):
        if not self.subscriber_field_map: # If no subscribers, no fields are required
            self.required_fields = set()
            log_fields = set()
        else:
            all_fields: Optional[Set[str]] = None # Assume all fields needed if any subscriber needs all
            for fields_set in self.subscriber_field_map.values():
                if fields_set is None: # If any subscriber needs all fields, then the bus needs all fields
                    all_fields = None
                    break
                if all_fields is None: # Initialize if not already None
                    all_fields = set()
                all_fields.update(fields_set)
            self.required_fields = all_fields
            log_fields = "ALL_FIELDS" if all_fields is None else all_fields
        logger.debug(f"Bus '{self.id}': Recalculated required fields: {log_fields}")

    async def put(self, event: EventBase, is_transient: bool = False):
        if self.failed:
            raise EventBusFailedError(f"Bus '{self.id}' is in a failed state.")
        while True:
            async with self.lock:
                if len(self.buffer) < self.capacity:
                    break
                if is_transient:
                    raise TransientSourceBufferFullError(
                        f"Transient source's event buffer is filled up with no extra space left! (Capacity: {self.capacity})."
                        f"Consider doubling its size in the agent config, specifically at the max_queue_size parameter in the source config."
                    )
                self._producer_can_put.clear()
                logger.debug(f"Bus '{self.id}': Buffer full, producer is waiting.")
            await self._producer_can_put.wait()
            if self.failed:
                raise EventBusFailedError(f"Bus '{self.id}' failed while producer was waiting.")
        async with self.lock:
            last_index = self.buffer[-1].index if self.buffer else self.buffer_start_position - 1
            if event.index != -1:
                if event.index < last_index:
                    logger.warning(f"Bus '{self.id}': Discarding out-of-order event. Event index {event.index} < last index in buffer {last_index}.")
                    if len(self.buffer) < self.capacity:
                        self._producer_can_put.set()
                    return
                self._next_event_index = event.index + 1
            else:
                event.index = self._next_event_index
                self._next_event_index += 1
            self.buffer.append(event)
            logger.debug(f"Bus '{self.id}': Put event with index {event.index}. Buffer size: {len(self.buffer)}/{self.capacity}")
            if len(self.buffer) < self.capacity:
                self._producer_can_put.set()
            self._consumer_can_get.set()

    async def get_events_for(self, pipe_id: str, batch_size: int, timeout: float) -> List[EventBase]:
        if self.failed:
            raise EventBusFailedError(f"Bus '{self.id}' failed: {self.error_message}")
        async with self.lock:
            state = self.subscribers.get(pipe_id)
            if not state: return []
            last_consumed_index = state.get('last_consumed_index', self.buffer_start_position - 1)
            events = []
            for event in self.buffer:
                if event.index > last_consumed_index:
                    events.append(event)
                    if len(events) >= batch_size:
                        break
            if events:
                return events
            self._consumer_can_get.clear()
        try:
            await asyncio.wait_for(self._consumer_can_get.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            return []
        async with self.lock:
            state = self.subscribers.get(pipe_id)
            if not state: return []
            last_consumed_index = state.get('last_consumed_index', self.buffer_start_position - 1)
            events = []
            for event in self.buffer:
                if event.index > last_consumed_index:
                    events.append(event)
                    if len(events) >= batch_size:
                        break
            return events

    async def commit(self, pipe_id: str, num_events_consumed: int, last_consumed_position: int) -> Optional[str]:
        async with self.lock:
            state = self.subscribers.get(pipe_id)
            if not state:
                logger.warning(f"Commit called for non-existent subscriber '{pipe_id}' on bus '{self.id}'.")
                return None
            state['last_consumed_index'] = last_consumed_position
            state['last_seen_position'] = last_consumed_position
            self._update_low_watermark()
            task_to_split = self._check_for_split(committed_subscriber_id=pipe_id)
            self._trim_buffer()
            return task_to_split

    def mark_as_failed(self, error: str):
        logger.error(f"Marking Bus '{self.id}' as FAILED. Reason: {error}")
        self.failed = True
        self.error_message = error
        self._producer_can_put.set()

    def recover(self):
        """Recover the bus from a failed state."""
        logger.info(f"Recovering Bus '{self.id}' from failed state.")
        self.failed = False
        self.error_message = None
        if len(self.buffer) < self.capacity:
            self._producer_can_put.set()

    async def subscribe(self, pipe_id: str, initial_position: int, fields_mapping: List[FieldMapping]):
        async with self.lock:
            if pipe_id in self.subscribers:
                return
            self.subscribers[pipe_id] = {
                'last_seen_position': initial_position - 1,
                'last_consumed_index': initial_position - 1
            }
            required_source_fields: Optional[Set[str]] = None # Use None to signify "all fields"
            if fields_mapping: # Only process fields_mapping if it's not empty
                required_source_fields = set()
                for fm in fields_mapping:
                    for source_str in fm.source:
                        field_name = source_str.split(':')[0]
                        required_source_fields.add(field_name)
            
            # Store the required fields. If None, it means all fields are required.
            self.subscriber_field_map[pipe_id] = required_source_fields
            self._recalculate_required_fields()
            
            log_fields = "ALL_FIELDS" if required_source_fields is None else required_source_fields
            logger.debug(f"Pipe '{pipe_id}' subscribed to Bus '{self.id}' at event index {initial_position - 1}. Required fields: {log_fields}")
            self._update_low_watermark()

    async def unsubscribe(self, pipe_id: str):
        async with self.lock:
            if pipe_id in self.subscribers:
                del self.subscribers[pipe_id]
                if pipe_id in self.subscriber_field_map:
                    del self.subscriber_field_map[pipe_id]
                self._recalculate_required_fields()
                logger.debug(f"Pipe '{pipe_id}' unsubscribed from Bus '{self.id}'.")
                self._update_low_watermark()
                
    def _update_low_watermark(self):
        if not self.subscribers:
            if self.buffer:
                self.low_watermark = self.buffer[-1].index
            return
        positions = [
            s.get('last_seen_position', self.buffer_start_position - 1)
            for s in self.subscribers.values()
        ]
        self.low_watermark = min(positions)

    def _trim_buffer(self):
        events_to_remove = 0
        for event in self.buffer:
            if event.index <= self.low_watermark:
                events_to_remove += 1
            else:
                break
        if events_to_remove > 0:
            logger.debug(f"Bus '{self.id}': trimming {events_to_remove} events older than or equal to watermark {self.low_watermark}.")
            for _ in range(events_to_remove):
                self.buffer.popleft()
            if self.buffer:
                self.buffer_start_position = self.buffer[0].index
        if len(self.buffer) < self.capacity:
            self._producer_can_put.set()
        
    def _check_for_split(self, committed_subscriber_id: str) -> Optional[str]:
        if len(self.subscribers) < 2:
            return None
        indices = {sid: s['last_consumed_index'] for sid, s in self.subscribers.items()}
        fastest_id = max(indices, key=lambda sid: indices[sid])
        if fastest_id != committed_subscriber_id:
            return None
        slowest_id = min(indices, key=lambda sid: indices[sid])
        slowest_consumer_index = indices[slowest_id]
        slowest_event_deque_position = -1
        for i, event in enumerate(self.buffer):
            if event.index == slowest_consumer_index:
                slowest_event_deque_position = i
                break
        if slowest_event_deque_position == -1:
            return None
        backlogged_events_count = (len(self.buffer) - 1) - slowest_event_deque_position
        if backlogged_events_count >= int(self.capacity * 0.95):
            logger.warning(
                f"Bus '{self.id}' split condition met! "
                f"Fastest task '{fastest_id}' is ahead of slowest task '{slowest_id}' "
                f"by approximately {backlogged_events_count} buffered events."
            )
            return fastest_id
        return None

    def can_subscribe(self, required_position: int) -> bool:
        if not self.buffer:
            return self.buffer_start_position == required_position
        return self.buffer_start_position <= required_position <= self.buffer[-1].index

    def get_subscriber_count(self) -> int:
        return len(self.subscribers)

    async def update_subscriber_position(self, pipe_id: str, new_position: int):
        async with self.lock:
            state = self.subscribers.get(pipe_id)
            if state:
                if new_position > state['last_consumed_index']:
                    state['last_consumed_index'] = new_position
                    state['last_seen_position'] = new_position
                    logger.debug(f"Bus '{self.id}': Subscriber '{pipe_id}' position updated to {new_position}.")
                    self._update_low_watermark()
            else:
                logger.warning(f"Bus '{self.id}': Attempted to update position for non-existent subscriber '{pipe_id}'.")

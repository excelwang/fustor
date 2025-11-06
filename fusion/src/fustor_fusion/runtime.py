import asyncio
from typing import Dict

class DatastoreEventManager:
    def __init__(self):
        self._events: Dict[int, asyncio.Event] = {} # Keyed by datastore_id only
        self._locks: Dict[int, asyncio.Lock] = {} # Keyed by datastore_id only
        self._lock = asyncio.Lock()

    async def _get_or_create_event(self, datastore_id: int) -> asyncio.Event:
        async with self._lock:
            if datastore_id not in self._events:
                self._events[datastore_id] = asyncio.Event()
            return self._events[datastore_id]

    async def _get_or_create_lock(self, datastore_id: int) -> asyncio.Lock:
        async with self._lock:
            if datastore_id not in self._locks:
                self._locks[datastore_id] = asyncio.Lock()
            return self._locks[datastore_id]

    async def notify(self, datastore_id: int):
        event = await self._get_or_create_event(datastore_id)
        event.set()

    async def wait_for_event(self, datastore_id: int):
        event = await self._get_or_create_event(datastore_id)
        await event.wait()
        event.clear()

datastore_event_manager = DatastoreEventManager()

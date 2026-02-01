import asyncio
from typing import Dict

class DatastoreEventManager:
    def __init__(self):
        self._events: Dict[str, asyncio.Event] = {} # Keyed by datastore_id only
        self._locks: Dict[str, asyncio.Lock] = {} # Keyed by datastore_id only
        self._lock = asyncio.Lock()

    async def _get_or_create_event(self, datastore_id: str) -> asyncio.Event:
        ds_id_str = str(datastore_id)
        async with self._lock:
            if ds_id_str not in self._events:
                self._events[ds_id_str] = asyncio.Event()
            return self._events[ds_id_str]

    async def _get_or_create_lock(self, datastore_id: str) -> asyncio.Lock:
        ds_id_str = str(datastore_id)
        async with self._lock:
            if ds_id_str not in self._locks:
                self._locks[ds_id_str] = asyncio.Lock()
            return self._locks[ds_id_str]

    async def notify(self, datastore_id: str):
        event = await self._get_or_create_event(datastore_id)
        event.set()

    async def wait_for_event(self, datastore_id: str):
        event = await self._get_or_create_event(datastore_id)
        await event.wait()
        event.clear()

datastore_event_manager = DatastoreEventManager()

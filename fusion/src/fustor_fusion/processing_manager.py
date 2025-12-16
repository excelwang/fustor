import asyncio
import logging
from typing import Dict, Set, Callable, Coroutine, Any, List
from fustor_registry.api.client.api import ClientDatastoreConfigResponse
from fustor_fusion_sdk.interfaces import ParserProcessingTaskManagerInterface # Import the interface

logger = logging.getLogger(__name__)

class ParserProcessingTaskManager(ParserProcessingTaskManagerInterface): # Inherit from the interface
    """
    Starts, stops, and tracks the per-datastore event processing tasks.
    """
    def __init__(self, processing_coro: Callable[[int], Coroutine[Any, Any, None]]):
        self._running_tasks: Dict[int, asyncio.Task] = {} # Keyed by datastore_id
        self._lock = asyncio.Lock()
        self._processing_coro = processing_coro

    async def start_processing_for_datastore(self, datastore_id: int):
        """Starts a new processing task for a datastore if not already running."""
        engine_key = datastore_id
        async with self._lock:
            if engine_key in self._running_tasks:
                logger.warning(f"Task for datastore {datastore_id} is already running.")
                return

            logger.info(f"Starting event processing task for datastore {datastore_id}.")
            task = asyncio.create_task(self._processing_coro(datastore_id))
            self._running_tasks[engine_key] = task

    async def stop_processing_for_datastore(self, datastore_id: int):
        """Stops a running processing task for a datastore."""
        engine_key = datastore_id
        async with self._lock:
            task = self._running_tasks.pop(engine_key, None)
            if task:
                logger.info(f"Stopping event processing task for datastore {datastore_id}.")
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    logger.info(f"Task for datastore {datastore_id} cancelled successfully.")
            else:
                logger.warning(f"No running task found for datastore {datastore_id} to stop.")

    async def sync_tasks(self, latest_datastore_configs: List[ClientDatastoreConfigResponse]):
        """
        Compares the latest set of datastore configurations with the currently running tasks
        and starts/stops tasks accordingly.
        """
        async with self._lock:
            current_engine_keys = set(self._running_tasks.keys())
            new_engine_keys = set()

            for datastore_config in latest_datastore_configs:
                new_engine_keys.add(datastore_config.datastore_id)

            to_start_keys = new_engine_keys - current_engine_keys
            to_stop_keys = current_engine_keys - new_engine_keys

        for datastore_id in to_start_keys:
            await self.start_processing_for_datastore(datastore_id)

        for datastore_id in to_stop_keys:
            await self.stop_processing_for_datastore(datastore_id)

    async def shutdown(self):
        """
        Stops all running tasks during application shutdown.
        """
        logger.info("Shutting down all datastore processing tasks...")
        async with self._lock:
            all_tasks = list(self._running_tasks.values())
            self._running_tasks.clear()

        for task in all_tasks:
            task.cancel()

        await asyncio.gather(*all_tasks, return_exceptions=True)
        logger.info("All datastore processing tasks have been shut down.")
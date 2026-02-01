import asyncio
from typing import TYPE_CHECKING, Dict, Any, Optional, Union
import logging 

from .base import BaseInstanceService
from fustor_core.models.states import SyncState
from fustor_agent.runtime import AgentPipeline, PipelineBridge
from fustor_core.exceptions import NotFoundError
from fustor_agent_sdk.interfaces import SyncInstanceServiceInterface # Import the interface

# SyncRuntime is now always AgentPipeline
SyncRuntime = AgentPipeline



if TYPE_CHECKING:
    from fustor_agent.services.configs.sync import SyncConfigService
    from fustor_agent.services.configs.source import SourceConfigService
    from fustor_agent.services.configs.sender import SenderConfigService
    from fustor_agent.services.instances.bus import EventBusService, EventBusInstanceRuntime
    from fustor_agent.services.drivers.sender_driver import SenderDriverService
    from fustor_agent.services.drivers.source_driver import SourceDriverService

logger = logging.getLogger("fustor_agent")

class SyncInstanceService(BaseInstanceService, SyncInstanceServiceInterface): # Inherit from the interface
    def __init__(
        self, 
        sync_config_service: "SyncConfigService",
        source_config_service: "SourceConfigService",
        sender_config_service: "SenderConfigService",
        bus_service: "EventBusService",
        sender_driver_service: "SenderDriverService",
        source_driver_service: "SourceDriverService",
        agent_id: str  # Added agent_id
    ):
        super().__init__()
        self.sync_config_service = sync_config_service
        self.source_config_service = source_config_service
        self.sender_config_service = sender_config_service
        self.bus_service = bus_service
        self.sender_driver_service = sender_driver_service
        self.source_driver_service = source_driver_service
        self.agent_id = agent_id  # Store agent_id
        self.logger = logging.getLogger("fustor_agent") 

    async def start_one(self, id: str):
        self.logger.debug(f"Enter start_one for sync_id: {id}")

        if self.get_instance(id):
            self.logger.warning(f"Sync instance '{id}' is already running or being managed.")
            return

        sync_config = self.sync_config_service.get_config(id)
        if not sync_config:
            raise NotFoundError(f"Sync config '{id}' not found.")

        if sync_config.disabled:
            self.logger.info(f"Sync instance '{id}' will not be started because its configuration is disabled.")
            return

        source_config = self.source_config_service.get_config(sync_config.source)
        if not source_config:
            raise NotFoundError(f"Source config '{sync_config.source}' not found for sync '{id}'.")
        
        sender_config = self.sender_config_service.get_config(sync_config.sender)
        if not sender_config:
            raise NotFoundError(f"Required Sender config '{sync_config.sender}' not found.")
        
        self.logger.info(f"Attempting to start sync instance '{id}'...")
        try:
            # Always use new Pipeline architecture
            self.logger.info(f"Using AgentPipeline for '{id}'")
            
            bridge = PipelineBridge(
                sender_driver_service=self.sender_driver_service,
                source_driver_service=self.source_driver_service
            )
            
            pipeline = bridge.create_pipeline(
                pipeline_id=id,
                agent_id=self.agent_id,
                sync_config=sync_config,
                source_config=source_config,
                sender_config=sender_config,
                event_bus=None  # EventBus integration TBD
            )
            
            self.pool[id] = pipeline
            await pipeline.start()
            
            self.logger.info(f"Sync instance '{id}' start initiated successfully.")

        except Exception as e:
            self.logger.error(f"Failed to start sync instance '{id}': {e}", exc_info=True)
            if self.get_instance(id):
                self.pool.pop(id)
            # Re-raise to be caught by the API layer
            raise



    async def stop_one(self, id: str, should_release_bus: bool = True):
        instance = self.get_instance(id)
        if not instance:
            return

        self.logger.info(f"Attempting to stop {instance}...")
        try:
            # The instance's bus might not exist if stopped during snapshot phase
            bus_id = instance.bus.id if instance.bus else None
            
            await instance.stop()
            self.pool.pop(id, None)
            self.logger.info(f"{instance} stopped and removed from pool.")
            
            if should_release_bus and bus_id:
                await self.bus_service.release_subscriber(bus_id, id)
        except Exception as e:
            self.logger.error(f"Failed to cleanly stop {instance}: {e}", exc_info=True)

    async def remap_sync_to_new_bus(self, sync_id: str, new_bus: "EventBusInstanceRuntime", needed_position_lost: bool):
        sync_instance = self.get_instance(sync_id)
        if not sync_instance:
            self.logger.warning(f"Sync task '{sync_id}' not found during bus remapping.")
            return

        old_bus_id = sync_instance.bus.id if sync_instance.bus else None
        self.logger.info(f"Remapping sync task '{sync_id}' to new bus '{new_bus.id}'...")
        
        # Call the instance's remap method, which also handles the signal
        await sync_instance.remap_to_new_bus(new_bus, needed_position_lost)
        
        if old_bus_id:
            await self.bus_service.release_subscriber(old_bus_id, sync_id)
        
        self.logger.info(f"Sync task '{sync_id}' remapped to bus '{new_bus.id}' successfully.")

    async def mark_dependent_syncs_outdated(self, dependency_type: str, dependency_id: str, reason_info: str, updates: Optional[Dict[str, Any]] = None):
        from fustor_core.pipeline import PipelineState
        affected_syncs = []
        for inst in self.list_instances():
            sync_config = self.sync_config_service.get_config(inst.id)
            if not sync_config:
                continue
            
            if (dependency_type == "source" and sync_config.source == dependency_id) or \
               (dependency_type == "sender" and sync_config.sender == dependency_id):
                affected_syncs.append(inst)

        logger.info(f"Marking syncs dependent on {dependency_type} '{dependency_id}' as outdated.")
        for sync_instance in affected_syncs:
            # Use PipelineState which is what AgentPipeline uses internally
            sync_instance.state |= PipelineState.CONF_OUTDATED
            sync_instance.info = reason_info # Update info directly
        self.logger.info(f"Marked {len(affected_syncs)} syncs as outdated.")


    async def start_all_enabled(self):
        all_sync_configs = self.sync_config_service.list_configs()
        if not all_sync_configs:
            return

        self.logger.info(f"Attempting to auto-start all enabled sync tasks...")
        start_tasks = [
            self.start_one(id)
            for id, cfg in all_sync_configs.items() if not cfg.disabled
        ]
        if start_tasks:
            # We must run tasks sequentially to avoid race conditions if they share resources,
            # but for now, gather is acceptable for starting independent tasks.
            await asyncio.gather(*start_tasks, return_exceptions=True)
            
    async def restart_outdated_syncs(self) -> int:
        from fustor_core.pipeline import PipelineState
        outdated_syncs = [
            inst for inst in self.list_instances() 
            if inst.state & PipelineState.CONF_OUTDATED
        ]

        
        if not outdated_syncs:
            return 0
            
        self.logger.info(f"Found {len(outdated_syncs)} outdated sync tasks to restart.")
        
        for sync in outdated_syncs:
            await self.stop_one(sync.id) 
            await asyncio.sleep(1) # Give time for graceful shutdown
            await self.start_one(sync.id)
        
        return len(outdated_syncs)

    async def stop_all(self):
        self.logger.info("Stopping all sync instances...")
        keys_to_stop = list(self.pool.keys())
        stop_tasks = [self.stop_one(key, should_release_bus=False) for key in keys_to_stop]
        
        if stop_tasks:
            await asyncio.gather(*stop_tasks)

        await self.bus_service.release_all_unused_buses()

    async def trigger_audit(self, id: str):
        instance = self.get_instance(id)
        if not instance:
            raise NotFoundError(f"Sync instance '{id}' not found.")
        await instance.trigger_audit()

    async def trigger_sentinel(self, id: str):
        instance = self.get_instance(id)
        if not instance:
            raise NotFoundError(f"Sync instance '{id}' not found.")
        await instance.trigger_sentinel()
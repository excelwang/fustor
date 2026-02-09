import asyncio
from typing import TYPE_CHECKING, Dict, Any, Optional, Union
import logging 

from .base import BaseInstanceService
from fustor_core.models.states import PipelineState
from fustor_agent.runtime import AgentPipeline
from fustor_agent.runtime.source_handler_adapter import SourceHandlerAdapter
from fustor_agent.runtime.sender_handler_adapter import SenderHandlerAdapter
from fustor_core.exceptions import NotFoundError
from fustor_agent_sdk.interfaces import PipelineInstanceServiceInterface # Import the interface

# PipelineRuntime is now always AgentPipeline
PipelineRuntime = AgentPipeline





if TYPE_CHECKING:
    from fustor_agent.services.configs.pipeline import PipelineConfigService
    from fustor_agent.services.configs.source import SourceConfigService
    from fustor_agent.services.configs.sender import SenderConfigService
    from fustor_agent.services.instances.bus import EventBusService, EventBusInstanceRuntime
    from fustor_agent.services.drivers.sender_driver import SenderDriverService
    from fustor_agent.services.drivers.source_driver import SourceDriverService

logger = logging.getLogger("fustor_agent")

class PipelineInstanceService(BaseInstanceService, PipelineInstanceServiceInterface): # Inherit from the interface
    def __init__(
        self, 
        pipeline_config_service: "PipelineConfigService",
        source_config_service: "SourceConfigService",
        sender_config_service: "SenderConfigService",
        bus_service: "EventBusService",
        sender_driver_service: "SenderDriverService",
        source_driver_service: "SourceDriverService",
        agent_id: str  # Added agent_id
    ):
        super().__init__()
        self.pipeline_config_service = pipeline_config_service
        self.source_config_service = source_config_service
        self.sender_config_service = sender_config_service
        self.bus_service = bus_service
        self.sender_driver_service = sender_driver_service
        self.source_driver_service = source_driver_service
        self.agent_id = agent_id  # Store agent_id
        self.logger = logging.getLogger("fustor_agent") 

    async def start_one(self, id: str):
        self.logger.debug(f"Enter start_one for pipeline_id: {id}")

        if self.get_instance(id):
            self.logger.warning(f"Pipeline instance '{id}' is already running or being managed.")
            return

        pipeline_config = self.pipeline_config_service.get_config(id)
        if not pipeline_config:
            self.logger.error(f"Pipeline config '{id}' not found.")
            raise NotFoundError(f"Pipeline config '{id}' not found.")
        self.logger.debug(f"Found pipeline config for {id}")

        if pipeline_config.disabled:
            self.logger.info(f"Pipeline instance '{id}' will not be started because its configuration is disabled.")
            return

        source_config = self.source_config_service.get_config(pipeline_config.source)
        if not source_config:
            self.logger.error(f"Source config for {id} not found")
            raise NotFoundError(f"Source config '{pipeline_config.source}' not found for pipeline '{id}'.")
        self.logger.debug(f"Found source config for {id}")
        
        sender_config = self.sender_config_service.get_config(pipeline_config.sender)
        if not sender_config:
            self.logger.error(f"Sender config for {id} not found")
            raise NotFoundError(f"Required Sender config '{pipeline_config.sender}' not found.")
        self.logger.debug(f"Found sender config for {id}")
        
        self.logger.info(f"Attempting to start pipeline instance '{id}'...")
        try:
            # Obtain EventBus mandatory for all pipelines (unifying architecture)
            task_id = f"{self.agent_id}:{id}"
            field_mappings = getattr(pipeline_config, "fields_mapping", [])
            
            # We assume start from position 0 for new pipelines
            event_bus, needed_position_lost = await self.bus_service.get_or_create_bus_for_subscriber(
                source_id=pipeline_config.source,
                source_config=source_config,
                pipeline_id=task_id,
                required_position=0, 
                fields_mapping=field_mappings
            )
            self.logger.info(f"Subscribed to EventBus {event_bus.id} for pipeline '{task_id}'")

            # Create Handlers (Inlined from PipelineBridge to simplify)
            source_driver_class = self.source_driver_service._get_driver_by_type(source_config.driver)
            source_driver = source_driver_class(id=pipeline_config.source, config=source_config)
            source_handler = SourceHandlerAdapter(source_driver, config=source_config)

            sender_driver_class = self.sender_driver_service._get_driver_by_type(sender_config.driver)
            
            # Extract sender config and credentials
            sender_credentials = {}
            if hasattr(sender_config.credential, "model_dump"):
                sender_credentials = sender_config.credential.model_dump()
            elif hasattr(sender_config.credential, "dict"):
                sender_credentials = sender_config.credential.dict()
            elif sender_config.credential:
                sender_credentials = dict(sender_config.credential)

            sender_driver_config = {
                "batch_size": sender_config.batch_size,
                "timeout_sec": sender_config.timeout_sec,
                "api_version": getattr(sender_config, "api_version", "v2"),
                **sender_config.driver_params
            }

            sender_driver = sender_driver_class(
                sender_id=pipeline_config.sender,
                endpoint=sender_config.uri,
                credential=sender_credentials,
                config=sender_driver_config
            )
            sender_handler = SenderHandlerAdapter(sender_driver, config=sender_config)

            # Build runtime config
            runtime_config = {
                "batch_size": getattr(pipeline_config, 'batch_size', 100),
                "heartbeat_interval_sec": getattr(pipeline_config, 'heartbeat_interval_sec', 10),
                "audit_interval_sec": getattr(pipeline_config, 'audit_interval_sec', 600),
                "sentinel_interval_sec": getattr(pipeline_config, 'sentinel_interval_sec', 120),
                "session_timeout_seconds": None,
                "fields_mapping": field_mappings,
            }

            pipeline = AgentPipeline(
                pipeline_id=id,
                task_id=task_id,
                config=runtime_config,
                source_handler=source_handler,
                sender_handler=sender_handler,
                event_bus=event_bus,
                bus_service=self.bus_service
            )
            
            self.pool[id] = pipeline
            await pipeline.start()
            
            self.logger.info(f"Pipeline instance '{id}' start initiated successfully.")

        except Exception as e:
            self.logger.error(f"Failed to start pipeline instance '{id}': {e}", exc_info=True)
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
            # The instance's bus might not exist if stopped during snapshot sync phase
            bus_id = instance.bus.id if instance.bus else None
            
            await instance.stop()
            self.pool.pop(id, None)
            self.logger.info(f"{instance} stopped and removed from pool.")
            
            if should_release_bus and bus_id:
                # Use task_id for bus subscription release
                await self.bus_service.release_subscriber(bus_id, instance.task_id)
        except Exception as e:
            self.logger.error(f"Failed to cleanly stop {instance}: {e}", exc_info=True)

    async def remap_pipeline_to_new_bus(self, pipeline_id: str, new_bus: "EventBusInstanceRuntime", needed_position_lost: bool):
        # Search by short id or task_id (bus uses task_id)
        pipeline_instance = self.get_instance(pipeline_id)
        if not pipeline_instance:
            # Search by task_id in pool
            for inst in self.pool.values():
                if getattr(inst, 'task_id', None) == pipeline_id:
                    pipeline_instance = inst
                    break
        
        if not pipeline_instance:
            self.logger.warning(f"Pipeline task '{pipeline_id}' not found in pool during bus remapping.")
            return

        old_bus_id = pipeline_instance.bus.id if pipeline_instance.bus else None
        self.logger.info(f"Remapping sync task '{pipeline_instance.id}' (task_id={pipeline_instance.task_id}) to new bus '{new_bus.id}'...")
        
        # Call the instance's remap method, which also handles the signal
        await pipeline_instance.remap_to_new_bus(new_bus, needed_position_lost)
        
        if old_bus_id:
            await self.bus_service.release_subscriber(old_bus_id, pipeline_instance.task_id)
        
        self.logger.info(f"Pipeline task '{pipeline_instance.id}' remapped to bus '{new_bus.id}' successfully.")



    async def mark_dependent_pipelines_outdated(self, dependency_type: str, dependency_id: str, reason_info: str, updates: Optional[Dict[str, Any]] = None):
        from fustor_core.pipeline import PipelineState
        affected_pipelines = []
        for inst in self.list_instances():
            pipeline_config = self.pipeline_config_service.get_config(inst.id)
            if not pipeline_config:
                continue
            
            if (dependency_type == "source" and pipeline_config.source == dependency_id) or \
               (dependency_type == "sender" and pipeline_config.sender == dependency_id):
                affected_pipelines.append(inst)

        logger.info(f"Marking pipelines dependent on {dependency_type} '{dependency_id}' as outdated.")
        for pipeline_instance in affected_pipelines:
            # Use PipelineState which is what AgentPipeline uses internally
            pipeline_instance.state |= PipelineState.CONF_OUTDATED
            pipeline_instance.info = reason_info # Update info directly
        self.logger.info(f"Marked {len(affected_pipelines)} pipelines as outdated.")




    async def start_all_enabled(self):
        all_pipeline_configs = self.pipeline_config_service.list_configs()
        if not all_pipeline_configs:
            return

        self.logger.info(f"Attempting to auto-start all enabled sync tasks...")
        start_tasks = [
            self.start_one(id)
            for id, cfg in all_pipeline_configs.items() if not cfg.disabled
        ]
        if start_tasks:
            # We must run tasks sequentially to avoid race conditions if they share resources,
            # but for now, gather is acceptable for starting independent tasks.
            await asyncio.gather(*start_tasks, return_exceptions=True)
            
    async def restart_outdated_pipelines(self) -> int:
        from fustor_core.pipeline import PipelineState
        outdated_pipelines = [
            inst for inst in self.list_instances() 
            if inst.state & PipelineState.CONF_OUTDATED
        ]

        
        if not outdated_pipelines:
            return 0
            
        self.logger.info(f"Found {len(outdated_pipelines)} outdated sync tasks to restart.")
        
        for pipeline in outdated_pipelines:
            await self.stop_one(pipeline.id) 
            await asyncio.sleep(1) # Give time for graceful shutdown
            await self.start_one(pipeline.id)
        
        return len(outdated_pipelines)



    async def stop_all(self):
        self.logger.info("Stopping all pipeline instances...")
        keys_to_stop = list(self.pool.keys())
        stop_tasks = [self.stop_one(key, should_release_bus=False) for key in keys_to_stop]
        
        if stop_tasks:
            await asyncio.gather(*stop_tasks)

        await self.bus_service.release_all_unused_buses()

    async def trigger_audit(self, id: str):
        instance = self.get_instance(id)
        if not instance:
            raise NotFoundError(f"Pipeline instance '{id}' not found.")
        await instance.trigger_audit()

    async def trigger_sentinel(self, id: str):
        instance = self.get_instance(id)
        if not instance:
            raise NotFoundError(f"Pipeline instance '{id}' not found.")
        await instance.trigger_sentinel()


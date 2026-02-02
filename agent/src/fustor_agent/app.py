# src/fustor_agent/app.py

import asyncio
import json
import logging
import os
import shutil
import uuid  # Import uuid module
from typing import Dict, Any
import fustor_agent

from . import get_app_config, STATE_FILE_PATH
from fustor_agent_sdk.utils import get_or_generate_agent_id # Import the utility function

# Import existing config and instance services
from .services.configs.source import SourceConfigService
from .services.configs.sender import SenderConfigService
from .services.configs.pipeline import PipelineConfigService
from .services.instances.bus import EventBusService, EventBusInstanceRuntime
from .services.instances.pipeline import PipelineInstanceService

# --- NEW: Import the new driver services ---
from .services.drivers.source_driver import SourceDriverService
from .services.drivers.sender_driver import SenderDriverService

from fustor_core.models.states import SyncState, EventBusState

class App:
    """
    The main application orchestrator.
    It is responsible for initializing all services on startup and
    gracefully saving state on shutdown.
    """
    def __init__(self, config_dir: str):
        if config_dir:
            fustor_agent.CONFIG_DIR = config_dir

        # Logging is now set up in cli.py, so we just get the logger here.
        self.logger = logging.getLogger("fustor_agent")
        
        self.logger.info("Initializing application...")

        # --- Agent ID Loading/Generation ---
        self.agent_id = get_or_generate_agent_id(fustor_agent.CONFIG_DIR, self.logger)
        
        # 1. Load static configuration
        self._app_config = get_app_config()

        # 2. Initialize all services
        # Config services
        self.source_config_service = SourceConfigService(self._app_config)
        self.sender_config_service = SenderConfigService(self._app_config)
        self.pipeline_config_service = PipelineConfigService(
            self._app_config,
            self.source_config_service,
            self.sender_config_service
        )
        
        
        # --- NEW: Instantiate the new driver services ---
        self.source_driver_service = SourceDriverService()
        self.sender_driver_service = SenderDriverService()

        # Instance services
        # [MODIFIED] Correctly inject SourceDriverService into EventBusService
        self.event_bus_service = EventBusService(
            self.source_config_service.list_configs(),
            self.source_driver_service
        )
        self.pipeline_instance_service = PipelineInstanceService(
            self.pipeline_config_service,
            self.source_config_service, # Pass the source config service
            self.sender_config_service,
            self.event_bus_service, # Corrected: use self.event_bus_service
            self.sender_driver_service,
            self.source_driver_service, # Added missing argument
            self.agent_id # Pass the agent_id to the service
        )
        
        # 3. Inject cross-service dependencies to resolve circular references
        self.source_config_service.set_dependencies(self.pipeline_instance_service)
        self.sender_config_service.set_dependencies(self.pipeline_instance_service)
        self.pipeline_config_service.set_dependencies(self.pipeline_instance_service)
        self.event_bus_service.set_dependencies(self.pipeline_instance_service)
        
        self.logger.info("All services initialized and dependencies injected.")

    async def startup(self):
        """
        Starts the application, including recovering persisted runtime state
        and launching tasks.
        """
        await self._load_and_recover_states()
        
        # NEW: Check for and disable sources with missing schema cache
        disabled_sources = await self.source_config_service.check_and_disable_missing_schema_sources()
        if disabled_sources:
            self.logger.warning(
                f"The following sources were disabled due to missing schema cache: {', '.join(disabled_sources)}. "
                "Please run 'fustor_agent discover-schema --source-id <id> --admin-user <user> --admin-password <password>' for each to re-enable."
            )

        self.logger.info("Attempting to automatically start enabled pipeline tasks...")
        await self.pipeline_instance_service.start_all_enabled()

    async def _load_and_recover_states(self):
        """
        Loads and recovers runtime instances from the state file.
        """
        self.logger.info(f"Loading runtime state snapshot from '{STATE_FILE_PATH}'...")
        backup_path = STATE_FILE_PATH + ".bak"

        try:
            with open(STATE_FILE_PATH, 'r', encoding='utf-8') as f:
                saved_states = json.load(f)
            
            # If the main state file was loaded successfully, the backup is no longer needed.
            if os.path.exists(backup_path):
                try:
                    os.remove(backup_path)
                    self.logger.info(f"Successfully loaded state. Removed obsolete backup file: {backup_path}")
                except OSError as e:
                    self.logger.warning(f"Could not remove backup state file '{backup_path}': {e}")

        except (FileNotFoundError, json.JSONDecodeError):
            self.logger.warning("State snapshot file not found or invalid. Starting with a clean state.")
            return

        bus_states = saved_states.get("event_buses", {})
        pipeline_states = saved_states.get("sync_tasks", saved_states.get("pipeline_tasks", {}))

        # 1. Recover EventBusInstances first
        recovered_buses: Dict[str, EventBusInstanceRuntime] = {}
        for bus_id, bus_state_data in bus_states.items():
            source_id = bus_state_data.get("source_name")
            source_config = self.source_config_service.get_config(source_id)
            if not source_config:
                self.logger.warning(f"Source config '{source_id}' for bus '{bus_id}' not found. Skipping bus recovery.")
                continue
            
            # Recreate EventBusInstanceRuntime
            # Need to generate source_signature for recovery
            source_signature = (source_config.driver, source_config.uri, source_config.credential)
            bus_runtime = EventBusInstanceRuntime(
                source_id, 
                source_config, 
                source_signature, 
                self.source_driver_service # Inject service during recovery
            )
            bus_runtime.id = bus_id # Ensure ID is preserved
            bus_runtime.state = EventBusState[bus_state_data.get("state", "IDLE").split('.')[-1]]
            bus_runtime.info = bus_state_data.get("info", "")
            bus_runtime.statistics = bus_state_data.get("statistics", {})
            
            # Re-add to pool
            self.event_bus_service.pool[bus_id] = bus_runtime
            self.event_bus_service.bus_by_signature[source_signature] = bus_runtime
            recovered_buses[bus_id] = bus_runtime
            self.logger.info(f"Recovered EventBus '{bus_id}' for source '{source_id}' with state {bus_runtime.state.name}.")

        # 2. Recover AgentPipelines
        if not pipeline_states:
            return

        self.logger.info(f"Found {len(pipeline_states)} pipeline tasks to recover.")
        recovery_tasks = []
        for pipeline_id, pipeline_state_data in pipeline_states.items():
            pipeline_conf = self.pipeline_config_service.get_config(pipeline_id)
            if not pipeline_conf :
                self.logger.warning(f"Pipeline configuration '{pipeline_id}' not found in current config. Skipping recovery.")
                continue
            
            state_str = pipeline_state_data.get("state", "STOPPED")
            state_parts = [part.strip() for part in state_str.split('|')]
            state = SyncState(0)
            for part in state_parts:
                try:
                    state |= SyncState[part.split('.')[-1]]
                except KeyError:
                    self.logger.warning(f"Unknown state part: {part}")
            persisted_bus_id = pipeline_state_data.get("bus_id")

            if state in {SyncState.MESSAGE_SYNC,SyncState.RUNNING_CONF_OUTDATE, SyncState.STOPPING}:
                self.logger.warning(f"Pipeline task '{pipeline_id}' was active on last shutdown. Recovering...")
                
                # Define async closure to capture pipeline_id
                async def recover_task(current_pipeline_id, current_persisted_bus_id):
                    try:
                        # Ensure the bus is started if it was producing
                        if current_persisted_bus_id and current_persisted_bus_id in recovered_buses:
                            bus_runtime = recovered_buses[current_persisted_bus_id]
                            if bus_runtime.state == EventBusState.PRODUCING:
                                await bus_runtime.start_producer() # Ensure producer is running
                            await bus_runtime.internal_bus.subscribe(current_pipeline_id, 0, []) # Re-subscribe with empty mapping, it will be updated
                        
                        await self.pipeline_instance_service.start_one(current_pipeline_id)
                    except Exception as e:
                        self.logger.error(f"Failed to recover and restart pipeline '{current_pipeline_id}': {e}", exc_info=True)

                recovery_tasks.append(recover_task(pipeline_id, persisted_bus_id))

        if recovery_tasks:
            await asyncio.gather(*recovery_tasks)
            self.logger.info("Recovery process for active tasks completed.")

    async def apply_configuration_changes(self):
        """
        Applies all pending configuration changes by restarting affected tasks.
        """
        self.logger.info("Applying pending configuration changes...")
        restarted_count = await self.pipeline_instance_service.restart_outdated_pipelines()
        self.logger.info(f"Successfully applied changes by restarting {restarted_count} pipeline tasks.")

    import shutil # Add this import at the top of the file

    async def shutdown(self):
        """
        Gracefully shuts down the application, stopping all tasks and
        persisting the final state.
        """
        self.logger.info("Shutting down application...")
        
        await self.pipeline_instance_service.stop_all()

        self.logger.info(f"Saving final runtime state to '{STATE_FILE_PATH}'...")
        
        bus_dtos = [bus.get_dto().model_dump() for bus in self.event_bus_service.list_instances()]
        
        pipelines_raw = self.pipeline_instance_service.list_instances()
        pipeline_dtos = [p.get_dto().model_dump() for p in pipelines_raw] 

        current_states = {
            "event_buses": {bus['id']: bus for bus in bus_dtos},
            "pipeline_tasks": {p['id']: p for p in pipeline_dtos},
            "sync_tasks": {p['id']: p for p in pipeline_dtos}, # Compatibility
        }
        
        temp_file_path = None
        try:
            # Create a backup of the existing state file before overwriting
            if os.path.exists(STATE_FILE_PATH):
                backup_path = STATE_FILE_PATH + ".bak"
                shutil.copyfile(STATE_FILE_PATH, backup_path)
                self.logger.info(f"Created backup of state file at {backup_path}")

            temp_file_path = STATE_FILE_PATH + ".tmp"
            with open(temp_file_path, 'w', encoding='utf-8') as f:
                json.dump(current_states, f, indent=2, default=str)
            os.replace(temp_file_path, STATE_FILE_PATH)
            self.logger.info("Final runtime state saved successfully.")
        except Exception as e:
            self.logger.error(f"Failed to save state file: {e}", exc_info=True)
        finally:
            if temp_file_path and os.path.exists(temp_file_path):
                os.remove(temp_file_path)
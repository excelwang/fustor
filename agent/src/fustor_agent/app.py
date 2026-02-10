# src/fustor_agent/app.py
"""
Agent Application - Main orchestrator using new unified pipe config V2.
"""
import asyncio
import json
import logging
import os
import shutil
from typing import Dict, Any, Optional, List

from fustor_core.common import get_fustor_home_dir
from fustor_agent_sdk.utils import get_or_generate_agent_id

from .config.unified import agent_config, AgentPipeConfig

# Import driver and instance services
from .services.drivers.source_driver import SourceDriverService
from .services.drivers.sender_driver import SenderDriverService
from .services.instances.bus import EventBusService
from .services.instances.pipe import PipeInstanceService
from .runtime.sender_handler_adapter import SenderHandlerAdapter
from .runtime.source_handler_adapter import SourceHandlerAdapter

# State file path
HOME_FUSTOR_DIR = get_fustor_home_dir()
STATE_FILE_PATH = os.path.join(HOME_FUSTOR_DIR, "agent-state.json")


class App:
    """
    Main application orchestrator.
    
    Refactored to use unified AgentConfigLoader V2 (Dict-based).
    """
    
    def __init__(self, config_list: Optional[List[str]] = None):
        """
        Initialize the application.
        
        Args:
            config_list: List of pipe IDs or filenames (e.g., 'set2.yaml') to start.
                        If None, loads 'default.yaml'.
        """
        self.logger = logging.getLogger("fustor_agent")
        self.logger.info("Initializing application...")
        
        # Agent ID
        self.agent_id = get_or_generate_agent_id(HOME_FUSTOR_DIR, self.logger)
        
        # Load Config
        agent_config.reload()
        
        # Determine which pipes to start based on config_list
        self._target_pipe_ids = self._resolve_target_pipes(config_list)
        
        # Driver services
        self.source_driver_service = SourceDriverService()
        self.sender_driver_service = SenderDriverService()
        
        # Instance services
        self.event_bus_service = EventBusService(
            {},  # Will be populated dynamically
            self.source_driver_service
        )
        
        self.pipe_runtime: Dict[str, Any] = {}
        
        self.logger.info(f"Target pipes: {self._target_pipe_ids}")
    
    def _resolve_target_pipes(self, config_list: Optional[List[str]]) -> List[str]:
        """Resolve which pipe IDs to start."""
        if config_list is None:
            # Default behavior: all pipes from all yaml files
            return list(agent_config.get_all_pipes().keys())
        
        targets = []
        for item in config_list:
            if item.endswith('.yaml') or item.endswith('.yml'):
                # Load all pipes from specific file
                pipes = agent_config.get_pipes_from_file(item)
                if not pipes:
                    self.logger.warning(f"No pipes found in file '{item}'")
                targets.extend(pipes.keys())
            else:
                # Assume it's a pipe ID
                if agent_config.get_pipe(item):
                    targets.append(item)
                else:
                    self.logger.error(f"Pipe ID '{item}' not found in any loaded config")
        return targets
    
    async def startup(self):
        """Start the application and target pipes."""
        self.logger.info("Starting application...")
        
        for pipe_id in self._target_pipe_ids:
            try:
                await self._start_pipe(pipe_id)
            except Exception as e:
                self.logger.error(f"Failed to start pipe '{pipe_id}': {e}", exc_info=True)
    
    async def _start_pipe(self, pipe_id: str):
        """Start a single pipe using resolved configuration."""
        self.logger.info(f"Starting pipe: {pipe_id}")
        
        resolved = agent_config.resolve_pipe_refs(pipe_id)
        if not resolved:
            raise ValueError(f"Could not resolve configuration for pipe '{pipe_id}'")
        
        pipe_cfg = resolved['pipe']
        source_cfg = resolved['source']
        sender_cfg = resolved['sender']
        
        if pipe_cfg.disabled:
            self.logger.info(f"Pipe '{pipe_id}' is disabled, skipping")
            return

        # 1. Get or create event bus for source
        # Use source ID from config for sharing
        source_id = pipe_cfg.source
        
        bus_runtime, _ = await self.event_bus_service.get_or_create_bus_for_subscriber(
            source_id=source_id,
            source_config=source_cfg,
            pipe_id=pipe_id,
            required_position=0,
            fields_mapping=pipe_cfg.fields_mapping
        )
        
        # 2. Create sender driver
        # SenderDriverService expects a Config object, unified config provides Pydantic SenderConfig
        sender = self.sender_driver_service.create_driver(pipe_cfg.sender, sender_cfg)
        
        # 3. Create pipe instance
        from .runtime.agent_pipe import AgentPipe
        
        # Adapt unified AgentPipeConfig to what AgentPipe expects (dict-like or object)
        # AgentPipe usually takes a config dict or object. 
        # We'll pass the unified config object directly if AgentPipe supports it, 
        # or convert relevant fields.
        # Wrap drivers in adapters for AgentPipe
        # AgentPipe requires SourceHandler and SenderHandler
        
        # Source Handler: Adapt from the bus's source driver
        source_handler = SourceHandlerAdapter(bus_runtime.source_driver_instance)
        
        # Sender Handler: Adapt from the sender driver
        sender_handler = SenderHandlerAdapter(sender)
        
        # Helper to get dict from pydantic model (v1/v2 compatible)
        pipe_config_dict = pipe_cfg.model_dump() if hasattr(pipe_cfg, "model_dump") else pipe_cfg.dict()

        pipe = AgentPipe(
            pipe_id=pipe_id,
            task_id=f"{self.agent_id}:{pipe_id}",
            config=pipe_config_dict,
            source_handler=source_handler,
            sender_handler=sender_handler,
            event_bus=bus_runtime,
            bus_service=self.event_bus_service
        )
        
        # 4. Start pipe
        await pipe.start()
        self.pipe_runtime[pipe_id] = pipe
        self.logger.info(f"Pipe '{pipe_id}' started successfully")

    async def _stop_pipe(self, pipe_id: str):
        """Stop a single pipe and release its resources."""
        pipe = self.pipe_runtime.get(pipe_id)
        if not pipe:
            return
        
        self.logger.info(f"Stopping pipe: {pipe_id}")
        await pipe.stop()
        
        # Release subscriber from bus
        await self.event_bus_service.release_subscriber(pipe.bus.id, pipe_id)
        
        del self.pipe_runtime[pipe_id]
        self.logger.info(f"Pipe '{pipe_id}' stopped and resources released")
    
    async def shutdown(self):
        """Gracefully shutdown all pipes."""
        self.logger.info("Shutting down application...")
        
        for pipe_id in list(self.pipe_runtime.keys()):
            try:
                await self._stop_pipe(pipe_id)
            except Exception as e:
                self.logger.error(f"Error stopping pipe '{pipe_id}': {e}")
        
        await self.event_bus_service.release_all_unused_buses()
        await self._save_state()
        self.logger.info("Application shutdown complete")
    
    async def _save_state(self):
        """Save runtime state to file."""
        try:
            state = {
                "pipes": {
                    pid: {"state": str(p.state)} 
                    for pid, p in self.pipe_runtime.items()
                }
            }
            if os.path.exists(STATE_FILE_PATH):
                shutil.copyfile(STATE_FILE_PATH, STATE_FILE_PATH + ".bak")
            
            with open(STATE_FILE_PATH, 'w') as f:
                json.dump(state, f, indent=2, default=str)
        except Exception as e:
            self.logger.error(f"Failed to save state: {e}")
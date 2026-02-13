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
# ID generation is removed per config-only requirement

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
        
        # Load Config first to check for agent_id
        agent_config.reload()
        self.config_loader = agent_config
        
        # Agent ID Priority: Config > File/Auto (IP-based)
        # Environment variable support removed per user request ("Only keep one: configuration file")
        
        config_id = agent_config.agent_id
        
        if config_id and config_id != "unknown-agent":
             self.logger.info(f"Using Agent ID: {config_id}")
             self.agent_id = config_id
        else:
             import socket
             try:
                 self.agent_id = socket.gethostname()
             except:
                 self.agent_id = "unknown-agent"
             
             # Sync back to config for other components
             agent_config.agent_id = self.agent_id
             self.logger.info(f"Using Agent ID (auto): {self.agent_id}")
        
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
            # Default behavior: only pipes from default.yaml
            all_default = agent_config.get_default_pipes()
            targets = []
            for pid, pcfg in all_default.items():
                source = agent_config.get_source(pcfg.source)
                if source and not source.disabled:
                    targets.append(pid)
            return targets
        
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
        
        # Runtime validation for redundancy
        self._validate_runtime_uniqueness(self._target_pipe_ids)
        
        for pipe_id in self._target_pipe_ids:
            try:
                await self._start_pipe(pipe_id)
            except Exception as e:
                self.logger.error(f"Failed to start pipe '{pipe_id}': {e}", exc_info=True)

    def _validate_runtime_uniqueness(self, pipe_ids: List[str]):
        """Ensure no two pipes in the list share the same source and sender."""
        seen_pairs = {} # (source, sender) -> pipe_id
        for pid in pipe_ids:
            resolved = agent_config.resolve_pipe_refs(pid)
            if not resolved:
                continue
            p_cfg = resolved['pipe']
            pair = (p_cfg.source, p_cfg.sender)
            if pair in seen_pairs:
                error_msg = (
                    f"CRITICAL: Redundant configuration detected in runtime. "
                    f"Pipe '{pid}' and Pipe '{seen_pairs[pair]}' both use "
                    f"Source '{pair[0]}' and Sender '{pair[1]}'. "
                    f"Agent will not start conflicting pipes to prevent data corruption."
                )
                self.logger.error(error_msg)
                raise ValueError(error_msg)
            seen_pairs[pair] = pid
    
    async def _start_pipe(self, pipe_id: str):
        """Start a single pipe using resolved configuration."""
        self.logger.info(f"Starting pipe: {pipe_id}")
        
        resolved = agent_config.resolve_pipe_refs(pipe_id)
        if not resolved:
            raise ValueError(f"Could not resolve configuration for pipe '{pipe_id}'")
        
        pipe_cfg = resolved['pipe']
        source_cfg = resolved['source']
        sender_cfg = resolved['sender']
        
        # 1. Get or create event bus for source
        # Use source ID from config for sharing
        source_id = pipe_cfg.source
        task_id = f"{self.agent_id}:{pipe_id}"
        
        # Convert fields_mapping to FieldMapping objects for EventBus
        from fustor_core.models.config import FieldMapping
        field_mappings = [
            FieldMapping(**m) if isinstance(m, dict) else m 
            for m in pipe_cfg.fields_mapping
        ]

        bus_runtime, _ = await self.event_bus_service.get_or_create_bus_for_subscriber(
            source_id=source_id,
            source_config=source_cfg,
            pipe_id=task_id,
            required_position=0,
            fields_mapping=field_mappings
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
        await self.event_bus_service.release_subscriber(pipe.bus.id, pipe.task_id)
        
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

    async def reload_config(self):
        """
        Reload configuration from disk and synchronize running pipes.
        Triggered by SIGHUP.
        """
        self.logger.info("Reloading configuration...")
        agent_config.reload()
        
        # Get diff between currently running pipes and new enabled pipes
        # Note: We only auto-reload pipes that were either in the original startup list 
        # or are in default.yaml if no list was provided.
        # For simplicity, we diff against ALL enabled pipes now, 
        # but a production implementation might restrict this to the original 'namespace'.
        
        current_running_ids = set(self.pipe_runtime.keys())
        diff = agent_config.get_diff(current_running_ids)
        
        added = diff["added"]
        removed = diff["removed"]
        
        if not added and not removed:
            self.logger.info("No configuration changes affecting pipes.")
            return
            
        self.logger.info(f"Config change detected: added={added}, removed={removed}")
        
        # 0. Runtime validation for combined set (existing minus removed plus added)
        remaining = current_running_ids - removed
        to_validate = list(remaining | added)
        try:
            self._validate_runtime_uniqueness(to_validate)
        except ValueError as e:
            self.logger.error(f"Reload aborted: {e}")
            return

        # 1. Stop removed pipes
        for pid in removed:
            try:
                await self._stop_pipe(pid)
            except Exception as e:
                self.logger.error(f"Error stopping pipe '{pid}' during reload: {e}")
                
        # 2. Start added pipes
        for pid in added:
            try:
                await self._start_pipe(pid)
            except Exception as e:
                self.logger.error(f"Failed to start added pipe '{pid}' during reload: {e}")
        
        self.logger.info("Configuration reload complete.")

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
# agent/src/fustor_agent/runtime/pipeline_bridge.py
"""
Bridge between legacy SyncInstance and new AgentPipeline.

This module provides utilities to gradually migrate from SyncInstance
to AgentPipeline without breaking existing functionality.

Migration Strategy:
1. SyncInstance remains the production code path
2. AgentPipeline can be used in parallel for testing
3. Feature flags control which path is active
4. Eventually SyncInstance is deprecated
"""
import logging
from typing import Any, Dict, Optional, TYPE_CHECKING

from fustor_core.models.config import SyncConfig, SenderConfig, SourceConfig

from .agent_pipeline import AgentPipeline
from .source_handler_adapter import SourceHandlerAdapter
from .sender_handler_adapter import SenderHandlerAdapter

if TYPE_CHECKING:
    from fustor_agent.services.drivers.sender_driver import SenderDriverService
    from fustor_agent.services.drivers.source_driver import SourceDriverService
    from fustor_agent.services.instances.bus import EventBusInstanceRuntime

logger = logging.getLogger("fustor_agent")


class PipelineBridge:
    """
    Factory for creating AgentPipeline from legacy configuration.
    
    This bridges the gap between the old SyncInstance configuration style
    and the new Pipeline architecture.
    
    Example:
        bridge = PipelineBridge(
            sender_driver_service=sender_service,
            source_driver_service=source_service
        )
        
        pipeline = bridge.create_pipeline(
            pipeline_id="my-sync",
            agent_id="agent-1",
            sync_config=sync_config,
            source_config=source_config,
            sender_config=sender_config
        )
        
        await pipeline.start()
    """
    
    def __init__(
        self,
        sender_driver_service: "SenderDriverService",
        source_driver_service: "SourceDriverService"
    ):
        """
        Initialize the bridge.
        
        Args:
            sender_driver_service: Service for creating sender drivers
            source_driver_service: Service for creating source drivers
        """
        self._sender_driver_service = sender_driver_service
        self._source_driver_service = source_driver_service
    
    def create_pipeline(
        self,
        pipeline_id: str,
        agent_id: str,
        sync_config: SyncConfig,
        source_config: SourceConfig,
        sender_config: SenderConfig,
        event_bus: Optional["EventBusInstanceRuntime"] = None,
        initial_statistics: Optional[Dict[str, Any]] = None
    ) -> AgentPipeline:
        """
        Create an AgentPipeline from legacy configuration.
        
        This method translates the old-style configuration into the new
        Pipeline architecture, creating the appropriate handlers.
        
        Args:
            pipeline_id: Unique identifier for this pipeline
            agent_id: The agent's ID
            sync_config: Sync configuration
            source_config: Source configuration
            sender_config: Sender configuration  
            event_bus: Optional event bus
            initial_statistics: Optional initial statistics
            
        Returns:
            Configured AgentPipeline instance
        """
        task_id = f"{agent_id}:{pipeline_id}"
        
        # Create source handler via adapter
        source_driver_class = self._source_driver_service._get_driver_by_type(
            source_config.driver
        )
        source_driver = source_driver_class(
            id=sync_config.source,
            config=source_config
        )
        source_handler = SourceHandlerAdapter(source_driver)
        
        # Create sender handler via adapter
        sender_driver_class = self._sender_driver_service._get_driver_by_type(
            sender_config.driver
        )
        sender_driver = sender_driver_class(
            sender_id=sync_config.pusher,
            endpoint=sender_config.endpoint,
            credential=self._extract_credential(sender_config),
            config=self._extract_sender_config(sender_config)
        )
        sender_handler = SenderHandlerAdapter(sender_driver)
        
        # Build pipeline config from sync config
        pipeline_config = self._build_pipeline_config(sync_config)
        if initial_statistics:
            # Pass through initial stats
            pass
        
        # Create pipeline
        pipeline = AgentPipeline(
            pipeline_id=pipeline_id,
            task_id=task_id,
            config=pipeline_config,
            source_handler=source_handler,
            sender_handler=sender_handler,
            event_bus=event_bus
        )
        
        if initial_statistics:
            pipeline.statistics.update(initial_statistics)
        
        return pipeline
    
    def _extract_credential(self, sender_config: SenderConfig) -> Dict[str, Any]:
        """Extract credential dict from sender config."""
        if hasattr(sender_config, 'credential') and sender_config.credential:
            cred = sender_config.credential
            return {
                "api_key": getattr(cred, 'api_key', ''),
                "secret": getattr(cred, 'secret', '')
            }
        return {}
    
    def _extract_sender_config(self, sender_config: SenderConfig) -> Dict[str, Any]:
        """Extract config dict from sender config."""
        config = {}
        if hasattr(sender_config, 'datastore_id'):
            config["datastore_id"] = sender_config.datastore_id
        if hasattr(sender_config, 'endpoint'):
            config["endpoint"] = sender_config.endpoint
        return config
    
    def _build_pipeline_config(self, sync_config: SyncConfig) -> Dict[str, Any]:
        """Build pipeline config from sync config."""
        config = {
            "batch_size": getattr(sync_config, 'batch_size', 100),
            "heartbeat_interval_sec": getattr(sync_config, 'heartbeat_interval_sec', 10),
            "audit_interval_sec": getattr(sync_config, 'audit_interval_sec', 600),
            "sentinel_interval_sec": getattr(sync_config, 'sentinel_interval_sec', 120),
            "session_timeout_seconds": getattr(sync_config, 'session_timeout_seconds', 30),
        }
        return config


def create_pipeline_from_sync_config(
    pipeline_id: str,
    agent_id: str,
    sync_config: SyncConfig,
    source_config: SourceConfig,
    sender_config: SenderConfig,
    sender_driver_service: "SenderDriverService",
    source_driver_service: "SourceDriverService",
    event_bus: Optional["EventBusInstanceRuntime"] = None
) -> AgentPipeline:
    """
    Convenience function to create an AgentPipeline from sync configuration.
    
    Args:
        pipeline_id: Pipeline ID
        agent_id: Agent ID
        sync_config: Sync configuration
        source_config: Source configuration
        sender_config: Sender configuration
        sender_driver_service: Sender driver service
        source_driver_service: Source driver service
        event_bus: Optional event bus
        
    Returns:
        Configured AgentPipeline
    """
    bridge = PipelineBridge(
        sender_driver_service=sender_driver_service,
        source_driver_service=source_driver_service
    )
    
    return bridge.create_pipeline(
        pipeline_id=pipeline_id,
        agent_id=agent_id,
        sync_config=sync_config,
        source_config=source_config,
        sender_config=sender_config,
        event_bus=event_bus
    )


# Feature flag for migration
USE_AGENT_PIPELINE = False  # Set to True to use new pipeline

def should_use_pipeline() -> bool:
    """Check if we should use AgentPipeline instead of SyncInstance."""
    import os
    env_flag = os.environ.get("FUSTOR_USE_PIPELINE", "").lower()
    return env_flag in ("1", "true", "yes") or USE_AGENT_PIPELINE

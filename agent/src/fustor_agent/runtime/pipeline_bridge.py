"""
Pipeline factory and configuration bridge.
"""
import logging
from typing import Any, Dict, Optional, TYPE_CHECKING

from fustor_core.models.config import PipelineConfig, SenderConfig, SourceConfig

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
    Factory for creating AgentPipeline from configuration.
    
    Example:
        factory = PipelineBridge(
            sender_driver_service=sender_service,
            source_driver_service=source_service
        )
        
        pipeline = factory.create_pipeline(
            pipeline_id="my-pipeline",
            agent_id="agent-1",
            pipeline_config=pipeline_config,
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
        pipeline_config: PipelineConfig,
        source_config: SourceConfig,
        sender_config: SenderConfig,
        event_bus: Optional["EventBusInstanceRuntime"] = None,
        bus_service: Any = None,
        initial_statistics: Optional[Dict[str, Any]] = None
    ) -> AgentPipeline:
        """
        Create an AgentPipeline from legacy configuration.
        
        This method translates the old-style configuration into the new
        Pipeline architecture, creating the appropriate handlers.
        
        Args:
            pipeline_id: Unique identifier for this pipeline
            agent_id: The agent's ID
            sync_config: Pipeline configuration
            source_config: Source configuration
            sender_config: Sender configuration  
            event_bus: Optional event bus
            bus_service: Optional bus service for the pipeline
            initial_statistics: Optional initial statistics
            
        Returns:
            Configured AgentPipeline instance
        """
        task_id = f"{agent_id}:{pipeline_id}"
        
        # Validation
        if not pipeline_config:
            raise ValueError("pipeline_config cannot be None")
        if not source_config:
            raise ValueError("source_config cannot be None")
        if not sender_config:
            raise ValueError("sender_config cannot be None")
            
        if not getattr(pipeline_config, 'source', None):
            raise ValueError("pipeline_config.source cannot be empty")
        if not getattr(pipeline_config, 'sender', None):
            raise ValueError("pipeline_config.sender cannot be empty")
            
        # Create source handler via adapter
        source_driver_class = self._source_driver_service._get_driver_by_type(
            source_config.driver
        )
        source_driver = source_driver_class(
            id=pipeline_config.source,
            config=source_config
        )
        source_handler = SourceHandlerAdapter(source_driver, config=source_config)
        
        # Create sender handler via adapter
        sender_driver_class = self._sender_driver_service._get_driver_by_type(
            sender_config.driver
        )
        sender_driver = sender_driver_class(
            sender_id=pipeline_config.sender,
            endpoint=sender_config.uri,
            credential=self._extract_credential(sender_config),
            config=self._extract_sender_config(sender_config)
        )


        sender_handler = SenderHandlerAdapter(sender_driver, config=sender_config)
        
        # Build runtime config
        runtime_config = self._build_runtime_config(pipeline_config)
        
        # Create pipeline
        pipeline = AgentPipeline(
            pipeline_id=pipeline_id,
            task_id=task_id,
            config=runtime_config,
            source_handler=source_handler,
            sender_handler=sender_handler,
            event_bus=event_bus,
            bus_service=bus_service
        )
        
        if initial_statistics:
            pipeline.statistics.update(initial_statistics)
        
        return pipeline
    
    def _extract_credential(self, sender_config: SenderConfig) -> Dict[str, Any]:
        """Extract credential dict from sender config."""
        if hasattr(sender_config.credential, "model_dump"):
            return sender_config.credential.model_dump()
        elif hasattr(sender_config.credential, "dict"):
            return sender_config.credential.dict()
        return dict(sender_config.credential) if sender_config.credential else {}
    
    def _extract_sender_config(self, sender_config: SenderConfig) -> Dict[str, Any]:
        """Extract config dict from sender config."""
        return {
            "batch_size": sender_config.batch_size,
            "timeout_sec": sender_config.timeout_sec,
            **sender_config.driver_params
        }

    
    def _build_runtime_config(self, pipeline_config: PipelineConfig) -> Dict[str, Any]:
        """Build runtime config from pipeline config."""
        config = {
            "batch_size": getattr(pipeline_config, 'batch_size', 100),
            "heartbeat_interval_sec": getattr(pipeline_config, 'heartbeat_interval_sec', 10),
            "audit_interval_sec": getattr(pipeline_config, 'audit_interval_sec', 600),
            "sentinel_interval_sec": getattr(pipeline_config, 'sentinel_interval_sec', 120),
            "session_timeout_seconds": getattr(pipeline_config, 'session_timeout_seconds', 30),
            "fields_mapping": getattr(pipeline_config, 'fields_mapping', []),
        }
        return config


def create_pipeline_from_config(
    pipeline_id: str,
    agent_id: str,
    pipeline_config: PipelineConfig,
    source_config: SourceConfig,
    sender_config: SenderConfig,
    sender_driver_service: "SenderDriverService",
    source_driver_service: "SourceDriverService",
    event_bus: Optional["EventBusInstanceRuntime"] = None
) -> AgentPipeline:
    """
    Convenience function to create an AgentPipeline from pipeline configuration.
    
    Args:
        pipeline_id: Pipeline ID
        agent_id: Agent ID
        sync_config: Pipeline configuration
        source_config: Source configuration
        sender_config: Sender configuration
        sender_driver_service: Sender driver service
        source_driver_service: Source driver service
        event_bus: Optional event bus
        
    Returns:
        Configured AgentPipeline
    """
    factory = PipelineBridge(
        sender_driver_service=sender_driver_service,
        source_driver_service=source_driver_service
    )
    
    return factory.create_pipeline(
        pipeline_id=pipeline_id,
        agent_id=agent_id,
        pipeline_config=pipeline_config,
        source_config=source_config,
        sender_config=sender_config,
        event_bus=event_bus
    )





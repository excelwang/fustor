# fusion/src/fustor_fusion/runtime/pipeline_manager.py
import asyncio
import logging
import time
from typing import Dict, List, Optional, Any

from fustor_core.transport import Receiver
from fustor_core.event import EventBase
from fustor_receiver_http import HTTPReceiver, SessionInfo
from .fusion_pipeline import FusionPipeline
from .session_bridge import create_session_bridge
from .view_handler_adapter import create_view_handler_from_manager
from ..config import receivers_config, fusion_pipelines_config
from ..view_manager.manager import get_cached_view_manager

logger = logging.getLogger(__name__)

class PipelineManager:
    """
    Manages the lifecycle of FusionPipelines and their associated Receivers.
    
    This is the central coordinator for V2 architecture on the Fusion side.
    """
    
    def __init__(self):
        self._pipelines: Dict[str, FusionPipeline] = {}
        self._receivers: Dict[str, Receiver] = {}
        self._bridges: Dict[str, Any] = {}
        self._session_to_pipeline: Dict[str, str] = {}
        self._lock = asyncio.Lock()
    
    def load_receivers(self):
        """Load receiver configurations and instantiate receivers (Synchronous)."""
        # 1. Load configurations
        receivers_cfg = receivers_config.reload()
        
        # 2. Setup Receivers
        for r_id, r_cfg in receivers_cfg.items():
            if r_cfg.driver == "http":
                receiver = HTTPReceiver(
                    receiver_id=r_id,
                    bind_host=r_cfg.bind_host,
                    port=r_cfg.port,
                    config={"session_timeout_seconds": r_cfg.session_timeout_seconds}
                )
                
                # Register API keys
                for ak in r_cfg.api_keys:
                    receiver.register_api_key(ak.key, ak.pipeline_id)
                
                self._receivers[r_id] = receiver
                logger.info(f"Initialized HTTP Receiver: {r_id}")

    async def initialize_pipelines(self):
        """Initialize pipelines and receivers based on configuration."""
        # Ensure receivers are loaded first (sync)
        if not self._receivers:
            self.load_receivers()
            
        async with self._lock:
            pipes_cfg = fusion_pipelines_config.reload()
            for p_id, p_cfg in pipes_cfg.items():
                if not p_cfg.enabled:
                    continue
                
                # Load ViewHandlers
                view_handlers = []
                # ViewHandlers are associated with a view_id
                for v_id in p_cfg.views:
                    try:
                        # We use view_id from extra or default to pipeline_id
                        view_id = p_cfg.extra.get("view_id", p_cfg.extra.get("datastore_id", p_id))
                        vm = await get_cached_view_manager(view_id)
                        handler = create_view_handler_from_manager(vm)
                        view_handlers.append(handler)
                    except Exception as e:
                        logger.error(f"Failed to load view {v_id} for pipeline {p_id}: {e}")
                
                pipeline = FusionPipeline(
                    pipeline_id=p_id,
                    config={
                        "view_id": p_cfg.extra.get("view_id", p_cfg.extra.get("datastore_id", p_id)),
                        "allow_concurrent_push": p_cfg.allow_concurrent_push,
                        "session_timeout_seconds": p_cfg.session_timeout_seconds
                    },
                    view_handlers=view_handlers
                )
                
                self._pipelines[p_id] = pipeline
                
                # Setup Bridge to legacy SessionManager
                bridge = create_session_bridge(pipeline)
                self._bridges[p_id] = bridge
                
                logger.info(f"Initialized Fusion Pipeline: {p_id} with {len(view_handlers)} views")
            
            # 4. Link Receivers to Pipelines via callbacks
            for r_id, receiver in self._receivers.items():
                if isinstance(receiver, HTTPReceiver):
                    receiver.register_callbacks(
                        on_session_created=self._on_session_created,
                        on_event_received=self._on_event_received,
                        on_heartbeat=self._on_heartbeat,
                        on_session_closed=self._on_session_closed
                    )
    
    async def start(self):
        """Start all pipelines and receivers."""
        async with self._lock:
            for p_id, pipeline in self._pipelines.items():
                await pipeline.start()
            for r_id, receiver in self._receivers.items():
                await receiver.start()
            logger.info("All Fusion V2 components started")
    
    async def stop(self):
        """Stop all pipelines and receivers."""
        async with self._lock:
            for r_id, receiver in self._receivers.items():
                await receiver.stop()
            for p_id, pipeline in self._pipelines.items():
                await pipeline.stop()
            logger.info("All Fusion V2 components stopped")

    def get_receiver(self, receiver_id: str) -> Optional[Receiver]:
        """Get receiver by ID."""
        return self._receivers.get(receiver_id)

    def get_pipeline(self, pipeline_id: str) -> Optional[FusionPipeline]:
        """Get pipeline by ID."""
        return self._pipelines.get(pipeline_id)

    def get_pipelines(self) -> Dict[str, FusionPipeline]:
        """Get all pipelines."""
        return self._pipelines.copy()

    # --- Receiver Callbacks ---

    async def _on_session_created(
        self, session_id: str, task_id: str, pipeline_id: str, client_info: Dict[str, Any]
    ) -> SessionInfo:
        async with self._lock:
            pipeline = self._pipelines.get(pipeline_id)
            if not pipeline:
                raise ValueError(f"Pipeline {pipeline_id} not found")
            
            bridge = self._bridges.get(pipeline_id)
            if not bridge:
                logger.warning(f"Bridge missing for pipeline {pipeline_id} in create_session, creating on-the-fly")
                bridge = create_session_bridge(pipeline)
                self._bridges[pipeline_id] = bridge

            # Use bridge to sync with legacy SessionManager
            result = await bridge.create_session(
                task_id=task_id,
                client_ip=client_info.get("client_ip"),
                session_id=session_id
            )
            role = result["role"]
            
            # Register mapping
            self._session_to_pipeline[session_id] = pipeline_id
            
            return SessionInfo(
                session_id=session_id,
                task_id=task_id,
                view_id=pipeline_id,
                role=role,
                created_at=time.time(),
                last_heartbeat=time.time()
            )

    async def _on_event_received(
        self, session_id: str, events: List[EventBase], source_type: str, is_end: bool
    ) -> bool:
        pipeline_id = self._session_to_pipeline.get(session_id)
        if pipeline_id:
            pipeline = self._pipelines.get(pipeline_id)
            if pipeline:
                result = await pipeline.process_events(
                    events, session_id, source_type, is_end=is_end
                )
                return result.get("success", False)
        
        logger.warning(f"Session {session_id} not found in any pipeline mapping")
        return False

    async def _on_heartbeat(self, session_id: str) -> Dict[str, Any]:
        pipeline_id = self._session_to_pipeline.get(session_id)
        if pipeline_id:
            bridge = self._bridges.get(pipeline_id)
            if bridge:
                return await bridge.keep_alive(session_id)
            
            # Fallback (though ideally bridge is always present now)
            pipeline = self._pipelines.get(pipeline_id)
            if pipeline:
                await pipeline.keep_session_alive(session_id)
                return {
                    "role": await pipeline.get_session_role(session_id),
                    "status": "ok"
                }
        return {"status": "error", "message": "Session not found"}

    async def _on_session_closed(self, session_id: str):
        async with self._lock:
            pipeline_id = self._session_to_pipeline.pop(session_id, None)
            if pipeline_id:
                bridge = self._bridges.get(pipeline_id)
                if not bridge:
                     pipeline = self._pipelines.get(pipeline_id)
                     if pipeline:
                         logger.warning(f"Bridge missing for pipeline {pipeline_id} in close_session, creating on-the-fly")
                         bridge = create_session_bridge(pipeline)
                         self._bridges[pipeline_id] = bridge
                
                if bridge:
                    await bridge.close_session(session_id)

# Global singleton
pipeline_manager = PipelineManager()

# fusion/src/fustor_fusion/runtime/pipe_manager.py
import asyncio
import logging
import time
import os
from typing import Dict, List, Optional, Any, Tuple

from fustor_core.transport import Receiver
from fustor_core.event import EventBase
from fustor_receiver_http import HTTPReceiver, SessionInfo
from .fusion_pipe import FusionPipe
from .session_bridge import create_session_bridge
from .view_handler_adapter import create_view_handler_from_manager
from ..config.unified import fusion_config, FusionPipeConfig
from ..view_manager.manager import get_cached_view_manager

logger = logging.getLogger(__name__)

class PipeManager:
    """
    Manages the lifecycle of FusionPipes and their associated Receivers.
    
    Refactored to use unified FusionConfigLoader V2.
    """
    
    def __init__(self):
        self._pipes: Dict[str, FusionPipe] = {}
        self._receivers: Dict[str, Receiver] = {} # Keyed by signature (driver, port)
        self._bridges: Dict[str, Any] = {}
        self._session_to_pipe: Dict[str, str] = {}
        self._lock = asyncio.Lock()
        self._target_pipe_ids: List[str] = []
    
    async def initialize_pipes(self, config_list: Optional[List[str]] = None):
        """
        Initialize pipes and receivers based on configuration.
        """
        logger.info(f"PipeManager.initialize_pipes called with config_list={config_list}")
        
        # 1. Load configs
        fusion_config.reload()
        
        # 2. Resolve target pipe IDs
        self._target_pipe_ids = self._resolve_target_pipes(config_list)
        
        initialized_count = 0
        async with self._lock:
            for p_id in self._target_pipe_ids:
                try:
                    resolved = fusion_config.resolve_pipe_refs(p_id)
                    if not resolved:
                        logger.error(f"Could not resolve configuration for pipe '{p_id}'")
                        continue

                    p_cfg = resolved['pipe']
                    r_cfg = resolved['receiver']
                    
                    if not p_cfg.enabled:
                        continue

                    # 1. Initialize/Get Receiver (Shared by port for HTTP)
                    r_sig = (r_cfg.driver, r_cfg.port)
                    
                    if r_sig not in self._receivers:
                        r_id = f"recv_{r_cfg.driver}_{r_cfg.port}"
                        if r_cfg.driver == "http":
                            receiver = HTTPReceiver(
                                receiver_id=r_id,
                                bind_host=r_cfg.bind_host,
                                port=r_cfg.port,
                                config={"session_timeout_seconds": r_cfg.session_timeout_seconds}
                            )
                            receiver.register_callbacks(
                                on_session_created=self._on_session_created,
                                on_event_received=self._on_event_received,
                                on_heartbeat=self._on_heartbeat,
                                on_session_closed=self._on_session_closed,
                                on_scan_complete=self._on_scan_complete
                            )
                            self._receivers[r_sig] = receiver
                            logger.info(f"Initialized shared HTTP Receiver on port {r_cfg.port}")
                    
                    receiver = self._receivers[r_sig]
                    # Register API keys for this specific pipe on the shared receiver
                    if isinstance(receiver, HTTPReceiver):
                        for ak in r_cfg.api_keys:
                            # Only register keys relevant to this pipe
                            if ak.pipe_id == p_id:
                                receiver.register_api_key(ak.key, ak.pipe_id)

                    # 2. Initialize Views
                    view_handlers = []
                    # resolved['views'] is a dict {view_id: ViewConfig}
                    for v_id, v_cfg in resolved['views'].items():
                        try:
                            # The view manager handles the actual FS/View logic based on group_id (view_id)
                            # We might need to pass v_cfg details to view manager if it's dynamic
                            # For now, assuming view manager loads its own config or we use existing pattern
                            vm = await get_cached_view_manager(v_id)
                            handler = create_view_handler_from_manager(vm)
                            view_handlers.append(handler)
                        except Exception as e:
                            logger.error(f"Failed to load view group {v_id} for pipe {p_id}: {e}")

                    if not view_handlers:
                        logger.warning(f"Pipe {p_id} has no valid views, skipping")
                        continue

                    # 3. Create Pipe
                    # Determine primary view ID for session leadership
                    primary_view_id = p_id 
                    if p_cfg.views:
                         primary_view_id = p_cfg.views[0]

                    pipe = FusionPipe(
                        pipe_id=p_id,
                        config={
                            "view_id": primary_view_id,
                            "allow_concurrent_push": p_cfg.allow_concurrent_push,
                            "session_timeout_seconds": p_cfg.session_timeout_seconds
                        },
                        view_handlers=view_handlers
                    )
                    
                    self._pipes[p_id] = pipe
                    self._bridges[p_id] = create_session_bridge(pipe)
                    
                    logger.info(f"Initialized Fusion Pipe: {p_id} with {len(view_handlers)} views")
                    initialized_count += 1
                    
                except Exception as e:
                    logger.error(f"Failed to initialize pipe {p_id}: {e}", exc_info=True)
            
        return {"initialized": initialized_count}

    def _resolve_target_pipes(self, config_list: Optional[List[str]]) -> List[str]:
        if config_list is None:
            return list(fusion_config.get_default_pipes().keys())
        
        targets = []
        for item in config_list:
            if item.endswith('.yaml') or item.endswith('.yml'):
                 pipes = fusion_config.get_pipes_from_file(item)
                 targets.extend(pipes.keys())
            else:
                if fusion_config.get_pipe(item):
                    targets.append(item)
                else:
                    logger.error(f"Pipe ID '{item}' not found in any loaded config")
        return targets

    async def start(self):
        async with self._lock:
            for p_id, pipe in self._pipes.items():
                await pipe.start()
            for r_sig, receiver in self._receivers.items():
                await receiver.start()
            logger.info("Fusion components started")
    
    async def stop(self):
        async with self._lock:
            for r_sig, receiver in self._receivers.items():
                await receiver.stop()
            for p_id, pipe in self._pipes.items():
                await pipe.stop()
            logger.info("Fusion components stopped")

    def get_pipes(self) -> Dict[str, FusionPipe]:
        return self._pipes.copy()

    def get_receiver(self, receiver_id: str) -> Optional[Receiver]:
        """
        Get receiver by ID (e.g. 'http-main') or internal signature ID.
        Unified config maps IDs (like 'http-main') to configs.
        The runtime keyed them by signature (driver, port).
        We need to match the config ID to the runtime instance.
        """
        # 1. Check if receiver_id is a config ID
        config = fusion_config.get_receiver(receiver_id)
        if config:
            sig = (config.driver, config.port)
            return self._receivers.get(sig)
        
        # 2. Check if it's an internal ID (e.g. recv_http_8102)
        for r in self._receivers.values():
            if r.receiver_id == receiver_id:
                return r
        
        return None

    # --- Receiver Callbacks (UNCHANGED) ---
    async def _on_session_created(self, session_id, task_id, pipe_id, client_info, session_timeout_seconds):
        async with self._lock:
            pipe = self._pipes.get(pipe_id)
            if not pipe: raise ValueError(f"Pipe {pipe_id} not found")
            bridge = self._bridges.get(pipe_id)
            # Create session via bridge (legacy compat)
            source_uri = client_info.get("source_uri") if client_info else None
            result = await bridge.create_session(
                task_id=task_id, 
                client_ip=client_info.get("client_ip") if client_info else None, 
                session_id=session_id, 
                session_timeout_seconds=session_timeout_seconds,
                source_uri=source_uri
            )
            self._session_to_pipe[session_id] = pipe_id
            # Return updated SessionInfo with source_uri
            info = SessionInfo(session_id, task_id, pipe_id, result["role"], time.time(), time.time())
            info.source_uri = source_uri
            return info

    async def _on_event_received(self, session_id, events, source_type, is_end):
        pipe_id = self._session_to_pipe.get(session_id)
        if pipe_id:
            pipe = self._pipes.get(pipe_id)
            if pipe:
                res = await pipe.process_events(events, session_id, source_type, is_end=is_end)
                return res.get("success", False)
        return False

    async def _on_heartbeat(self, session_id, can_realtime=False):
        pipe_id = self._session_to_pipe.get(session_id)
        if pipe_id:
            bridge = self._bridges.get(pipe_id)
            if bridge: return await bridge.keep_alive(session_id, can_realtime=can_realtime)
        return {"status": "error"}

    async def _on_session_closed(self, session_id):
        async with self._lock:
            pipe_id = self._session_to_pipe.pop(session_id, None)
            if pipe_id:
                bridge = self._bridges.get(pipe_id)
                if bridge: await bridge.close_session(session_id)

    async def _on_scan_complete(self, session_id: str, scan_path: str):
        """Handle scan completion notification from Agent."""
        from .session_bridge import session_manager
        pipe_id = self._session_to_pipe.get(session_id)
        if pipe_id:
            pipe = self._pipes.get(pipe_id)
            if pipe:
                view_id = pipe.view_id
                await session_manager.complete_scan(view_id, session_id, scan_path)
                logger.debug(f"Scan complete for path {scan_path} on session {session_id}")

# Global singleton
pipe_manager = PipeManager()

# fusion/src/fustor_fusion/runtime/pipe_manager.py
import asyncio
import logging
import time
import os
from typing import Dict, List, Optional, Any, Tuple
from fastapi import APIRouter

from fustor_core.transport.receiver import Receiver, ReceiverRegistry
from fustor_receiver_http import SessionInfo # Kept for registration of 'http' driver and type info
from .fusion_pipe import FusionPipe
from ..config.unified import fusion_config
from ..view_manager.manager import get_cached_view_manager
from ..api.consistency import consistency_router

logger = logging.getLogger(__name__)

class FusionPipeManager:
    """
    Manages the lifecycle of FusionPipes and their associated Receivers.
    
    Refactored to use unified FusionConfigLoader V2.
    """
    
    def __init__(self):
        self._pipes: Dict[str, FusionPipe] = {}
        self._receivers: Dict[str, Receiver] = {} # Keyed by signature (driver, port)
        self._bridges: Dict[str, Any] = {}
        self._session_to_pipe: Dict[str, str] = {}
        self._init_lock = asyncio.Lock()  # Only for initialization/start/stop
        self._pipe_locks: Dict[str, asyncio.Lock] = {}  # Per-pipe locks for session ops
        self._target_pipe_ids: List[str] = []
    
    def _get_pipe_lock(self, pipe_id: str) -> asyncio.Lock:
        """获取 per-pipe 锁（惰性创建）。"""
        return self._pipe_locks.setdefault(pipe_id, asyncio.Lock())
    
    async def initialize_pipes(self, config_list: Optional[List[str]] = None):
        """
        Public API: Initialize pipes and receivers based on configuration.
        Acquires _init_lock before proceeding.
        """
        async with self._init_lock:
            return await self._initialize_pipes_internal(config_list)

    async def _initialize_pipes_internal(self, config_list: Optional[List[str]] = None):
        """
        Internal initialization logic (no locking).
        """
        logger.info(f"FusionPipeManager._initialize_pipes_internal called with config_list={config_list}")
        
        # 1. Load configs
        fusion_config.reload()
        
        # 2. Resolve target pipe IDs
        self._target_pipe_ids = self._resolve_target_pipes(config_list)
        
        initialized_count = 0
        # No lock here, expected to be called with _init_lock held
        for p_id in self._target_pipe_ids:
            try:
                resolved = fusion_config.resolve_pipe_refs(p_id)
                if not resolved:
                    logger.error(f"Could not resolve configuration for pipe '{p_id}'")
                    continue

                p_cfg = resolved['pipe']
                r_cfg = resolved['receiver']
                
                if r_cfg.disabled:
                    logger.warning(f"FusionPipe '{p_id}' skipped because receiver '{p_cfg.receiver}' is disabled")
                    continue

                # 1. Initialize/Get Receiver (Shared by driver + port)
                r_sig = (r_cfg.driver, r_cfg.port)
                
                if r_sig not in self._receivers:
                    r_id = f"recv_{r_cfg.driver}_{r_cfg.port}"
                    
                    # Use Registry to create the concrete receiver instance (driver-agnostic)
                    receiver = ReceiverRegistry.create(
                        r_cfg.driver,
                        receiver_id=r_id,
                        bind_host=r_cfg.bind_host,
                        port=r_cfg.port,
                        config={
                            "session_timeout_seconds": p_cfg.session_timeout_seconds,
                        }
                    )
                    
                    receiver.register_callbacks(
                        on_session_created=self._on_session_created,
                        on_event_received=self._on_event_received,
                        on_heartbeat=self._on_heartbeat,
                        on_session_closed=self._on_session_closed,
                        on_scan_complete=self._on_scan_complete
                    )
                    
                    # Fix: Mount consistency router here immediately upon receiver creation
                    consistency_api = APIRouter(prefix="/api/v1/pipe")
                    consistency_api.include_router(consistency_router)
                    receiver.mount_router(consistency_api)
                    
                    self._receivers[r_sig] = receiver
                    logger.info(f"Initialized shared {r_cfg.driver.upper()} Receiver on port {r_cfg.port} with consistency routes")
                
                receiver = self._receivers[r_sig]
                
                # 2. Register API keys for this specific pipe on the shared receiver
                for ak in r_cfg.api_keys:
                    # Only register keys relevant to this pipe
                    if ak.pipe_id == p_id:
                        receiver.register_api_key(ak.key, ak.pipe_id)

                # 2. Initialize Views
                view_handlers = []
                # resolved['views'] is a dict {view_id: ViewConfig}
                for v_id, v_cfg in resolved['views'].items():
                    if v_cfg.disabled:
                        logger.warning(f"View '{v_id}' for pipe '{p_id}' is disabled, skipping")
                        continue
                    try:
                        # The view manager handles the actual FS/View logic based on group_id (view_id)
                        # We might need to pass v_cfg details to view manager if it's dynamic
                        # For now, assuming view manager loads its own config or we use existing pattern
                        vm = await get_cached_view_manager(v_id)
                        from .view_handler_adapter import create_view_handler_from_manager
                        handler = create_view_handler_from_manager(vm)
                        view_handlers.append(handler)
                    except Exception as e:
                        logger.error(f"Failed to load view group {v_id} for pipe {p_id}: {e}")

                if not view_handlers:
                    logger.warning(f"FusionPipe {p_id} has no valid views, skipping")
                    continue

                # 3. Create FusionPipe
                # Determine primary view ID for session leadership
                primary_view_id = p_id 
                if p_cfg.views:
                     primary_view_id = p_cfg.views[0]

                fusion_pipe = FusionPipe(
                    pipe_id=p_id,
                    config={
                        "view_id": primary_view_id,
                        "allow_concurrent_push": p_cfg.allow_concurrent_push,
                        "session_timeout_seconds": p_cfg.session_timeout_seconds
                    },
                    view_handlers=view_handlers
                )
                
                self._pipes[p_id] = fusion_pipe
                from .session_bridge import create_session_bridge
                bridge = create_session_bridge(fusion_pipe)
                self._bridges[p_id] = bridge
                
                # Link bridge to pipe for GAP-4 session isolation
                fusion_pipe.session_bridge = bridge
                logger.info(f"Initialized FusionPipe: {p_id} with {len(view_handlers)} views")
                initialized_count += 1
                
            except Exception as e:
                logger.error(f"Failed to initialize pipe {p_id}: {e}", exc_info=True)
            
        return {"initialized": initialized_count}

    def _resolve_target_pipes(self, config_list: Optional[List[str]]) -> List[str]:
        if config_list is None:
            # Default behavior: only enabled pipes from default.yaml
            return list(fusion_config.get_enabled_pipes().keys())
        
        targets = []
        for item in config_list:
            if item.endswith('.yaml') or item.endswith('.yml'):
                 pipes = fusion_config.get_pipes_from_file(item)
                 targets.extend(pipes.keys())
            else:
                if fusion_config.get_pipe(item):
                    targets.append(item)
                else:
                    logger.error(f"FusionPipe ID '{item}' not found in any loaded config")
        return targets

    async def start(self):
        async with self._init_lock:
            for p_id, fusion_pipe in self._pipes.items():
                await fusion_pipe.start()
            for r_sig, receiver in self._receivers.items():
                await receiver.start()
            logger.info("Fusion components started")
    
    async def stop(self):
        async with self._init_lock:
            for r_sig, receiver in self._receivers.items():
                await receiver.stop()
            for p_id, fusion_pipe in self._pipes.items():
                await fusion_pipe.stop()
            logger.info("Fusion components stopped")

    async def reload(self):
        """
        Reload configuration and synchronize running pipes.
        Triggered by SIGHUP or management API.
        """
        logger.info("Reloading Fusion configuration...")
        fusion_config.reload()
        
        async with self._init_lock:
            current_pipe_ids = set(self._pipes.keys())
            
            # Detect changed pipes by comparing their resolved config (simplified)
            # A more robust way is to hash the resolved config, but for now we can 
            # just treat all existing pipes as potentially modified and restart them
            # to be safe, or compare their resolved config dicts.
            modified = set()
            for p_id in current_pipe_ids:
                new_resolved = fusion_config.resolve_pipe_refs(p_id)
                if not new_resolved:
                    continue # Will be handled by removed
                
                # Compare critical parts: receiver ID and view IDs
                # Note: This is an optimization. In a real system we'd compare the whole config.
                p_cfg = new_resolved['pipe']
                r_cfg = new_resolved['receiver']
                
                # Check if p_cfg or r_cfg or views changed
                # For simplicity, we can just restart any pipe that is still in config
                # because we don't track the 'old_resolved' currently.
                # However, to avoid excessive restarts, let's at least check if it's still enabled.
                modified.add(p_id)

            diff = fusion_config.get_diff(current_pipe_ids)
            added = diff["added"]
            removed = diff["removed"]
            
            if not added and not removed and not modified:
                logger.info("No configuration changes affecting pipes.")
                return
                
            logger.info(f"Config change detected: added={added}, removed={removed}, modified={len(modified)}")
            
            # 1. Stop removed AND modified pipes
            for p_id in (removed | modified):
                fusion_pipe = self._pipes.pop(p_id, None)
                if fusion_pipe:
                    await fusion_pipe.stop()
                    self._bridges.pop(p_id, None)
                    logger.info(f"FusionPipe '{p_id}' stopped for reload/update")
            
            # 2. Re-initialize and start added AND modified pipes
            to_start = added | modified
            if to_start:
                await self._initialize_pipes_internal(list(to_start))
                for p_id in to_start:
                    if p_id in self._pipes:
                        await self._pipes[p_id].start()
            
            # 3. Clean up unused receivers
            active_receiver_sigs = set()
            for p_id in self._pipes:
                resolved = fusion_config.resolve_pipe_refs(p_id)
                if resolved:
                    r_cfg = resolved['receiver']
                    active_receiver_sigs.add((r_cfg.driver, r_cfg.port))
            
            for sig in list(self._receivers.keys()):
                if sig not in active_receiver_sigs:
                    receiver = self._receivers.pop(sig)
                    await receiver.stop()
                    logger.info(f"Stopped and removed unused receiver: {sig}")
            
            # Start any new receivers that were just initialized but not started
            for sig, receiver in self._receivers.items():
                # We don't have a reliable 'is_running' on Receiver yet, 
                # but calling start() again should be idempotent or we can track it.
                # For now, just start all.
                await receiver.start()

        logger.info("Fusion configuration reload complete.")

    def get_pipes(self) -> Dict[str, FusionPipe]:
        return self._pipes.copy()

    def get_pipe(self, pipe_id: str) -> Optional[FusionPipe]:
        """Get a specific pipe instance by ID."""
        return self._pipes.get(pipe_id)

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
            if r.id == receiver_id:
                return r
        
        return None

    # --- Receiver Callbacks (UNCHANGED) ---
    async def _on_session_created(self, session_id, task_id, p_id, client_info, session_timeout_seconds):
        # Lock-free lookup (pipes dict is stable after init)
        fusion_pipe = self._pipes.get(p_id)
        if not fusion_pipe: raise ValueError(f"FusionPipe {p_id} not found")
        bridge = self._bridges.get(p_id)
        
        # Per-pipe lock for session creation
        async with self._get_pipe_lock(p_id):
            source_uri = client_info.get("source_uri") if client_info else None
            result = await bridge.create_session(
                task_id=task_id, 
                client_ip=client_info.get("client_ip") if client_info else None, 
                session_id=session_id, 
                session_timeout_seconds=session_timeout_seconds,
                source_uri=source_uri
            )
            self._session_to_pipe[session_id] = p_id
            
            # Fetch sync policy from config
            p_cfg = fusion_config.get_pipe(p_id)
            audit_interval = p_cfg.audit_interval_sec if p_cfg else None
            sentinel_interval = p_cfg.sentinel_interval_sec if p_cfg else None
            
            info = SessionInfo(session_id, task_id, p_id, result["role"], time.time(), time.time())
            info.source_uri = source_uri
            info.audit_interval_sec = audit_interval
            info.sentinel_interval_sec = sentinel_interval
            return info

    async def _on_event_received(self, session_id, events, source_type, is_end, metadata=None):
        p_id = self._session_to_pipe.get(session_id)
        if p_id:
            fusion_pipe = self._pipes.get(p_id)
            if fusion_pipe:
                # metadata is passed as part of kwargs
                res = await fusion_pipe.process_events(events, session_id, source_type, is_end=is_end, metadata=metadata)
                return res.get("success", False)
        # Session not found or pipe gone
        raise ValueError(f"Session {session_id} not found or expired")

    async def _on_heartbeat(self, session_id, can_realtime=bool, agent_status=None):
        pipe_id = self._session_to_pipe.get(session_id)
        if pipe_id:
            bridge = self._bridges.get(pipe_id)
            if bridge: return await bridge.keep_alive(session_id, can_realtime=can_realtime, agent_status=agent_status)
        return {"status": "error"}

    async def _on_session_closed(self, session_id):
        pipe_id = self._session_to_pipe.pop(session_id, None)
        if pipe_id:
            async with self._get_pipe_lock(pipe_id):
                bridge = self._bridges.get(pipe_id)
                if bridge: await bridge.close_session(session_id)

    async def _on_scan_complete(self, session_id: str, scan_path: str, job_id: Optional[str] = None):
        """Handle technical scan completion notification (maps to business find)."""
        from ..core.session_manager import session_manager
        pipe_id = self._session_to_pipe.get(session_id)
        if pipe_id:
            fusion_pipe = self._pipes.get(pipe_id)
            if fusion_pipe:
                view_id = fusion_pipe.view_id
                await session_manager.complete_agent_job(view_id, session_id, scan_path, job_id)
                logger.debug(f"Scan complete (job_id={job_id}) for path {scan_path} on session {session_id}")

# Alias for compatibility
PipeManager = FusionPipeManager

# Global singleton
pipe_manager = FusionPipeManager()
fusion_pipe_manager = pipe_manager


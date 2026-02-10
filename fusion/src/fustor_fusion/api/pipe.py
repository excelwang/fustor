# fusion/src/fustor_fusion/api/pipe.py
"""
Unified Pipe API Router.

This module provides the new /api/v1/pipe endpoint that will eventually
replace /api/v1/ingest. For now, it reuses existing session and ingestion
routers while providing a cleaner structure.

API Structure:
- /api/v1/pipe/session - Session management
- /api/v1/pipe/ingest - Event ingestion
- /api/v1/pipe/consistency - Consistency checks (signals)
"""
import logging
from fastapi import APIRouter, HTTPException
from .. import runtime_objects
from ..config.unified import fusion_config

logger = logging.getLogger(__name__)

# Re-export existing routers with new paths
from .session import session_router
from .consistency import consistency_router

# Create unified pipe router
pipe_router = APIRouter(tags=["Pipe"])

def setup_pipe_routers():
    """
    Setup pipe management routers on the main Fusion app.
    
    Note: Session and Ingestion routers are no longer mounted here.
    They are served by each HTTPReceiver on its own independent port.
    
    This is called during application lifespan startup.
    """
    # 1. Clear existing routes to avoid duplicates if called multiple times (e.g. during tests)
    pipe_router.routes = []
    
    if runtime_objects.pipe_manager is None:
        logger.error("setup_pipe_routers called before pipe_manager initialized!")
        return False
    
    # Consistency router handles its own delegation
    pipe_router.include_router(consistency_router)
    
    # 2. Add Management endpoints for debugging/monitoring
    @pipe_router.get("/pipes", tags=["Pipe Management"])
    async def list_pipes():
        """List all managed pipes."""
        pipes = runtime_objects.pipe_manager.get_pipes()
        return {pid: await p.get_dto() for pid, p in pipes.items()}

    @pipe_router.get("/pipes/{pipe_id}", tags=["Pipe Management"])
    async def get_pipe_info(pipe_id: str):
        """Get detailed information about a specific pipe."""
        pipe = runtime_objects.pipe_manager.get_pipe(pipe_id)
        if not pipe:
            raise HTTPException(status_code=404, detail="Pipe not found")
        return await pipe.get_dto()
    
    @pipe_router.get("/session/", tags=["Pipe"])
    async def list_active_sessions():
        """
        List all active sessions across all view_ids.
        Mainly for integration tests and monitoring.
        """
        from ..core.session_manager import session_manager
        from ..view_state_manager import view_state_manager
        
        all_sessions = []
        sessions_by_view = await session_manager.get_all_active_sessions()
        for view_id, sessions in sessions_by_view.items():
            for sid, si in sessions.items():
                is_leader = await view_state_manager.is_leader(view_id, sid)
                all_sessions.append({
                    "session_id": sid,
                    "task_id": si.task_id,
                    "agent_id": si.task_id, # IT tests expect agent_id
                    "source_uri": si.source_uri,
                    "view_id": view_id,
                    "role": "leader" if is_leader else "follower",
                    "can_snapshot": is_leader,
                    "can_audit": is_leader,
                    "can_realtime": si.can_realtime,
                    "can_send": True
                })
        
        return {"active_sessions": all_sessions, "count": len(all_sessions)}

    return True

# NOTE: We no longer mount routers here at module level.
# They are mounted via setup_pipe_routers() in main.py lifespan.


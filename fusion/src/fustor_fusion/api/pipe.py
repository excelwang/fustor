# fusion/src/fustor_fusion/api/pipe.py
"""
Unified Pipeline API Router.

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

logger = logging.getLogger(__name__)

# Re-export existing routers with new paths
from .session import session_router
from .consistency import consistency_router

# Create unified pipe router
pipe_router = APIRouter(tags=["Pipeline"])

def setup_pipe_routers():
    """
    Mount routers from HTTPReceiver into the pipe_router, 
    falling back to legacy routers if configured.
    
    This is called during application lifespan startup.
    """
    # 1. Clear existing routes to avoid duplicates if called multiple times (e.g. during tests)
    pipe_router.routes = []
    
    if runtime_objects.pipeline_manager is None:
        logger.error("setup_pipe_routers called before pipeline_manager initialized!")
        return False
        
    success = False
    
    # Get the default HTTP receiver (e.g., 'http-main')
    receiver = runtime_objects.pipeline_manager.get_receiver("http-main")
    if receiver and hasattr(receiver, "get_session_router"):
        logger.info("Mounting Session and Ingestion routers from HTTPReceiver")
        pipe_router.include_router(receiver.get_session_router(), prefix="/session")
        pipe_router.include_router(receiver.get_ingestion_router(), prefix="/ingest")
        success = True
            
    if not success:
        logger.warning("HTTPReceiver not found, falling back to legacy routers")
        # Fallback to legacy
        pipe_router.include_router(session_router, prefix="/session")
        # pipe_router.include_router(ingestion_router, prefix="/ingest") # Legacy removed
    
    # Consistency router is common for both or handles its own delegation
    pipe_router.include_router(consistency_router)
    
    # 2. Add Management endpoints for debugging/monitoring
    @pipe_router.get("/pipelines", tags=["Pipeline Management"])
    async def list_pipelines():
        """List all managed pipelines."""
        pipelines = runtime_objects.pipeline_manager.get_pipelines()
        return {pid: await p.get_dto() for pid, p in pipelines.items()}

    @pipe_router.get("/pipelines/{pipeline_id}", tags=["Pipeline Management"])
    async def get_pipeline_info(pipeline_id: str):
        """Get detailed information about a specific pipeline."""
        pipeline = runtime_objects.pipeline_manager.get_pipeline(pipeline_id)
        if not pipeline:
            raise HTTPException(status_code=404, detail="Pipeline not found")
        return await pipeline.get_dto()
    
    @pipe_router.get("/session/", tags=["Pipeline"])
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
                    "view_id": view_id,
                    "role": "leader" if is_leader else "follower"
                })
        
        return {"active_sessions": all_sessions, "count": len(all_sessions)}

    return success

# NOTE: We no longer mount routers here at module level.
# They are mounted via setup_pipe_routers() in main.py lifespan.


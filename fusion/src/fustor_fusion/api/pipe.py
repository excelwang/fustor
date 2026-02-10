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
from fastapi import APIRouter, HTTPException, Depends
from .. import runtime_objects
from ..config.unified import fusion_config
from ..auth.dependencies import get_view_id_from_api_key

logger = logging.getLogger(__name__)

# Re-export existing routers with new paths
from .session import session_router
from .consistency import consistency_router

# Create unified pipe router
pipe_router = APIRouter(tags=["Pipe"])

# Consistency router handles its own delegation
pipe_router.include_router(consistency_router)

# 2. Add Management endpoints for debuging/monitoring
@pipe_router.get("/pipes", tags=["Pipe Management"])
async def list_pipes():
    """List all managed pipes."""
    if runtime_objects.pipe_manager is None:
        raise HTTPException(status_code=503, detail="Pipe Manager not initialized")
    pipes = runtime_objects.pipe_manager.get_pipes()
    return {pid: await p.get_dto() for pid, p in pipes.items()}

@pipe_router.get("/pipes/{pipe_id}", tags=["Pipe Management"])
async def get_pipe_info(pipe_id: str):
    """Get detailed information about a specific pipe."""
    if runtime_objects.pipe_manager is None:
        raise HTTPException(status_code=503, detail="Pipe Manager not initialized")
    pipe = runtime_objects.pipe_manager.get_pipe(pipe_id)
    if not pipe:
        raise HTTPException(status_code=404, detail="Pipe not found")
    return await pipe.get_dto()

@pipe_router.get("/stats", tags=["Pipe Management"])
async def get_global_stats(
    view_id: str = Depends(get_view_id_from_api_key)
):
    """Get synchronization process metrics for the authorized pipe."""
    if runtime_objects.pipe_manager is None:
        raise HTTPException(status_code=503, detail="Pipe Manager not initialized")
    
    pipe = runtime_objects.pipe_manager.get_pipe(view_id)
    if not pipe:
        return {"events_received": 0, "events_processed": 0, "active_sessions": 0}
    
    return {
        "events_received": pipe.statistics.get("events_received", 0),
        "events_processed": pipe.statistics.get("events_processed", 0),
        "errors": pipe.statistics.get("errors", 0)
    }

def setup_pipe_routers():
    """
    Legacy setup function. 
    Now that routes are defined at module level strategies, this is a no-op 
    or just a verification step.
    """
    if runtime_objects.pipe_manager is None:
        logger.error("setup_pipe_routers called before pipe_manager initialized!")
        return False
    return True

# NOTE: We no longer mount routers here at module level.
# They are mounted via setup_pipe_routers() in main.py lifespan.


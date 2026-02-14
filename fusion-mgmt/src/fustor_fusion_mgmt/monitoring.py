import logging
from fastapi import APIRouter, HTTPException, Depends
from fustor_fusion import runtime_objects
from fustor_fusion.auth.dependencies import get_view_id_from_api_key

logger = logging.getLogger("fustor_fusion_mgmt.monitoring")
monitoring_router = APIRouter()

@monitoring_router.get("/pipes", tags=["Management"])
async def list_pipes():
    """List all managed pipes."""
    if runtime_objects.pipe_manager is None:
        raise HTTPException(status_code=503, detail="Pipe Manager not initialized")
    pipes = runtime_objects.pipe_manager.get_pipes()
    return {pid: await p.get_dto() for pid, p in pipes.items()}

@monitoring_router.get("/pipes/{pipe_id}", tags=["Management"])
async def get_pipe_info(pipe_id: str):
    """Get detailed information about a specific pipe."""
    if runtime_objects.pipe_manager is None:
        raise HTTPException(status_code=503, detail="Pipe Manager not initialized")
    pipe = runtime_objects.pipe_manager.get_pipe(pipe_id)
    if not pipe:
        raise HTTPException(status_code=404, detail="Pipe not found")
    return await pipe.get_dto()

@monitoring_router.get("/stats", tags=["Management"])
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

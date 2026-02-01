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
from fastapi import APIRouter

# Re-export existing routers with new paths
from .session import session_router
from .ingestion import ingestion_router
from .consistency import consistency_router

# Create unified pipe router
pipe_router = APIRouter(tags=["Pipeline"])

# Mount sub-routers dynamically
def setup_pipe_v2_routers():
    """
    Mount V2 routers from HTTPReceiver into the pipe_router.
    This should be called during application startup.
    """
    from .. import runtime_objects
    
    if runtime_objects.pipeline_manager:
        # Get the default HTTP receiver (e.g., 'http-main')
        # In a multi-receiver setup, we might want to mount all of them or a specific one
        receiver = runtime_objects.pipeline_manager.get_receiver("http-main")
        if receiver and hasattr(receiver, "get_session_router"):
            logger.info("Mounting V2 Session and Ingestion routers from HTTPReceiver")
            pipe_router.include_router(receiver.get_session_router(), prefix="/session")
            pipe_router.include_router(receiver.get_ingestion_router(), prefix="/ingest")
            return True
            
    logger.warning("V2 HTTPReceiver not found, falling back to legacy routers")
    # Fallback to legacy
    pipe_router.include_router(session_router, prefix="/session")
    pipe_router.include_router(ingestion_router, prefix="/ingest")
    return False

# Initial mount (will likely be fallback unless called later)
# We also include consistency_router which is still unified
pipe_router.include_router(consistency_router)

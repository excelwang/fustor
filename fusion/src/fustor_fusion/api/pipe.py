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
from fastapi import APIRouter

logger = logging.getLogger(__name__)

# Re-export existing routers with new paths
from .session import session_router
from .ingestion import ingestion_router
from .consistency import consistency_router

# Create unified pipe router
pipe_router = APIRouter(tags=["Pipeline"])

def setup_pipe_v2_routers():
    """
    Mount V2 routers from HTTPReceiver into the pipe_router, 
    falling back to legacy routers if V2 is not available.
    
    This is called during application lifespan startup.
    """
    from .. import runtime_objects
    
    # 1. Clear existing routes to avoid duplicates if called multiple times (e.g. during tests)
    pipe_router.routes = []
    
    v2_success = False
    if runtime_objects.pipeline_manager:
        # Get the default HTTP receiver (e.g., 'http-main')
        receiver = runtime_objects.pipeline_manager.get_receiver("http-main")
        if receiver and hasattr(receiver, "get_session_router"):
            logger.info("Mounting V2 Session and Ingestion routers from HTTPReceiver")
            pipe_router.include_router(receiver.get_session_router(), prefix="/session")
            pipe_router.include_router(receiver.get_ingestion_router(), prefix="/ingest")
            v2_success = True
            
    if not v2_success:
        logger.warning("V2 HTTPReceiver not found, falling back to legacy routers")
        # Fallback to legacy
        pipe_router.include_router(session_router, prefix="/session")
        pipe_router.include_router(ingestion_router, prefix="/ingest")
    
    # Consistency router is common for both or handles its own delegation
    pipe_router.include_router(consistency_router)
    
    return v2_success

# NOTE: We no longer mount routers here at module level.
# They are mounted via setup_pipe_v2_routers() in main.py lifespan.


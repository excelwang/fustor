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

# Mount sub-routers
pipe_router.include_router(session_router, prefix="/session")
pipe_router.include_router(ingestion_router, prefix="/ingest") 
pipe_router.include_router(consistency_router)  # /consistency prefix already in router

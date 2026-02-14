# fusion/src/fustor_fusion/api/pipe.py
"""
Unified Pipe API Router.

API Structure:
- /api/v1/pipe/session - Session management
- /api/v1/pipe/{session_id}/events - Event ingestion
- /api/v1/pipe/consistency - Consistency checks (signals)
- /api/v1/pipe/pipes - Pipe management
- /api/v1/pipe/stats - Global stats
"""
import logging
from fastapi import APIRouter, HTTPException, Depends, Request
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from .. import runtime_objects
from ..config.unified import fusion_config
from ..auth.dependencies import get_view_id_from_api_key

logger = logging.getLogger(__name__)

from .session import session_router
from .consistency import consistency_router

# Create unified pipe router
pipe_router = APIRouter(tags=["Pipe"])

# Consistency router handles its own delegation
pipe_router.include_router(consistency_router)

# Session management
pipe_router.include_router(session_router, prefix="/session")


# --- Event Ingestion ---

class IngestPayload(BaseModel):
    events: List[Dict[str, Any]]
    source_type: Optional[str] = "message"
    is_end: Optional[bool] = False


@pipe_router.post("/{session_id}/events", summary="Ingest events into a pipe session", tags=["Event Ingestion"])
async def ingest_events(
    session_id: str,
    payload: IngestPayload,
    request: Request,
):
    """Ingest a batch of events into an active pipe session."""
    if runtime_objects.pipe_manager is None:
        raise HTTPException(status_code=503, detail="Pipe Manager not initialized")

    try:
        success = await runtime_objects.pipe_manager._on_event_received(
            session_id=session_id,
            events=payload.events,
            source_type=payload.source_type,
            is_end=payload.is_end
        )
        return {"success": success}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Ingestion failed for session {session_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# --- Pipe Management ---

def setup_pipe_routers():
    """Verification step to ensure pipe_manager is initialized."""
    if runtime_objects.pipe_manager is None:
        logger.error("setup_pipe_routers called before pipe_manager initialized!")
        return False
    return True

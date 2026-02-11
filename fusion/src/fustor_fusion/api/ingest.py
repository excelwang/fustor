# fusion/src/fustor_fusion/api/ingest.py
"""
Core ingestion API for handling events via the main management port.
"""
from fastapi import APIRouter, Depends, status, HTTPException, Request
from pydantic import BaseModel
import logging
from typing import List, Dict, Any, Optional

from ..auth.dependencies import get_view_id_from_api_key
from .. import runtime_objects

logger = logging.getLogger(__name__)
ingest_router = APIRouter(tags=["Ingestion"])

class IngestPayload(BaseModel):
    events: List[Dict[str, Any]]
    source_type: Optional[str] = "message"
    is_end: Optional[bool] = False

@ingest_router.post("/{session_id}/events", summary="Ingest events into a pipe session")
async def ingest_events(
    session_id: str,
    payload: IngestPayload,
    request: Request,
):
    """
    Ingest a batch of events into an active pipe session.
    Delegates to PipeManager.
    """
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

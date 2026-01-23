from fastapi import APIRouter, Depends, status, HTTPException
import logging

from ..auth.dependencies import get_datastore_id_from_api_key
from ..parsers.manager import get_cached_parser_manager

logger = logging.getLogger(__name__)

consistency_router = APIRouter(tags=["Consistency Management"], prefix="/consistency")

@consistency_router.post("/audit/start", summary="Signal start of an audit cycle")
async def signal_audit_start(
    datastore_id: int = Depends(get_datastore_id_from_api_key)
):
    """
    Explicitly signal the start of an audit cycle.
    This clears blind-spot lists and prepares the system for full reconciliation.
    """
    parser_manager = await get_cached_parser_manager(datastore_id)
    parser = await parser_manager.get_file_directory_parser()
    if not parser:
        raise HTTPException(status_code=404, detail="Parser not found")
        
    await parser.handle_audit_start()
    return {"status": "audit_started"}

@consistency_router.post("/audit/end", summary="Signal end of an audit cycle")
async def signal_audit_end(
    datastore_id: int = Depends(get_datastore_id_from_api_key)
):
    """
    Signal the completion of an audit cycle.
    This triggers missing file detection and cleanup logic.
    """
    parser_manager = await get_cached_parser_manager(datastore_id)
    parser = await parser_manager.get_file_directory_parser()
    if not parser:
        raise HTTPException(status_code=404, detail="Parser not found")
        
    await parser.handle_audit_end()
    return {"status": "audit_ended"}

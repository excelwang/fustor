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

from typing import Dict, Any, List

@consistency_router.get("/sentinel/tasks", summary="Get sentinel check tasks")
async def get_sentinel_tasks(
    datastore_id: int = Depends(get_datastore_id_from_api_key)
) -> Dict[str, Any]:
    """
    Get generic sentinel check tasks.
    Currently maps FS Suspect List to 'suspect_check' tasks.
    """
    parser_manager = await get_cached_parser_manager(datastore_id)
    parser = await parser_manager.get_file_directory_parser()
    if not parser:
        return {} 
        
    suspects = await parser.get_suspect_list()
    
    if suspects:
        paths = list(suspects.keys())
        # Return a single task batch
        return {
             'type': 'suspect_check', 
             'paths': paths,
             'source_id': datastore_id
        }
    return {}

@consistency_router.post("/sentinel/feedback", summary="Submit sentinel check feedback")
async def submit_sentinel_feedback(
    feedback: Dict[str, Any],
    datastore_id: int = Depends(get_datastore_id_from_api_key)
):
    """
    Submit feedback from sentinel checks.
    Expects payload: {"type": "suspect_update", "updates": [...]}
    """
    parser_manager = await get_cached_parser_manager(datastore_id)
    parser = await parser_manager.get_file_directory_parser()
    if not parser:
         raise HTTPException(status_code=404, detail="Parser not found")

    # Handle generic feedback
    # The payload is the whole feedback dict from Agent/Pusher
    res_type = feedback.get('type')
    if res_type == 'suspect_update':
        updates = feedback.get('updates', [])
        # updates is list of dicts: {'path': xx, 'mtime': yy, ...}
        for item in updates:
            path = item.get('path')
            mtime = item.get('mtime')
            if path and mtime is not None:
                await parser.update_suspect(path, float(mtime))
                
        return {"status": "processed", "count": len(updates)}
    
    return {"status": "ignored", "reason": "unknown_type"}

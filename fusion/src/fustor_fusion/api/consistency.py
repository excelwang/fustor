from fastapi import APIRouter, Depends, status, HTTPException
import logging
import asyncio
from typing import Dict, Any

from ..auth.dependencies import get_datastore_id_from_api_key
from ..view_manager.manager import get_cached_view_manager
from ..in_memory_queue import memory_event_queue
from ..processing_manager import processing_manager

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
    view_manager = await get_cached_view_manager(datastore_id)
    provider = await view_manager.get_file_directory_provider()
    if not provider:
        raise HTTPException(status_code=404, detail="View Provider not found")
        
    await provider.handle_audit_start()
    return {"status": "audit_started"}

@consistency_router.post("/audit/end", summary="Signal end of an audit cycle")
async def signal_audit_end(
    datastore_id: int = Depends(get_datastore_id_from_api_key)
):
    """
    Signal the completion of an audit cycle.
    This triggers missing file detection and cleanup logic.
    """
    # Wait for queue to drain (with timeout)
    max_wait = 5.0  # seconds
    wait_interval = 0.2
    elapsed = 0.0
    
    # Wait at least 1 second upfront to give HTTP pipeline time to complete
    await asyncio.sleep(1.0)
    
    while elapsed < max_wait:
        queue_size = memory_event_queue.get_queue_size(datastore_id)
        inflight = processing_manager.get_inflight_count(datastore_id)
        if queue_size == 0 and inflight == 0:
            break
        await asyncio.sleep(wait_interval)
        elapsed += wait_interval
    
    if elapsed >= max_wait:
        logger.warning(f"Audit end signal timeout waiting for queue drain: queue={queue_size}, inflight={inflight}")
    else:
        logger.info(f"Queue drained for audit end (waited {1.0 + elapsed:.1f}s), proceeding with missing file detection")
        # Additional delay to ensure any in-progress operations complete
        await asyncio.sleep(0.2)

    view_manager = await get_cached_view_manager(datastore_id)
    provider = await view_manager.get_file_directory_provider()
    if not provider:
        raise HTTPException(status_code=404, detail="View Provider not found")
        
    await provider.handle_audit_end()
    return {"status": "audit_ended"}


@consistency_router.get("/sentinel/tasks", summary="Get sentinel check tasks")
async def get_sentinel_tasks(
    datastore_id: int = Depends(get_datastore_id_from_api_key)
) -> Dict[str, Any]:
    """
    Get generic sentinel check tasks.
    Currently maps FS Suspect List to 'suspect_check' tasks.
    """
    view_manager = await get_cached_view_manager(datastore_id)
    provider = await view_manager.get_file_directory_provider()
    if not provider:
        return {} 
        
    suspects = await provider.get_suspect_list()
    
    if suspects:
        paths = list(suspects.keys())
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
    view_manager = await get_cached_view_manager(datastore_id)
    provider = await view_manager.get_file_directory_provider()
    if not provider:
         raise HTTPException(status_code=404, detail="View Provider not found")

    res_type = feedback.get('type')
    if res_type == 'suspect_update':
        updates = feedback.get('updates', [])
        for item in updates:
            path = item.get('path')
            mtime = item.get('mtime')
            if path and mtime is not None:
                await provider.update_suspect(path, float(mtime))
                
        return {"status": "processed", "count": len(updates)}
    
    return {"status": "ignored", "reason": "unknown_type"}

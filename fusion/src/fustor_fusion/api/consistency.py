from fastapi import APIRouter, Depends, status, HTTPException
import logging
import asyncio

from ..auth.dependencies import get_datastore_id_from_api_key
from ..parsers.manager import get_cached_parser_manager
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
    
    IMPORTANT: We wait for the event queue to drain before processing
    to ensure all audit events (including audit_skipped flags) are applied.
    """
    # Wait for queue to drain (with timeout)
    # We need a substantial minimum wait because:
    # 1. The Agent calls signal_audit_end AFTER pushing events
    # 2. But events are pushed via HTTP and go through the queue
    # 3. HTTP requests may still be in transit when this endpoint is called
    max_wait = 5.0  # seconds
    wait_interval = 0.2
    elapsed = 0.0
    
    # Wait at least 1 second upfront to give HTTP pipeline time to complete
    await asyncio.sleep(1.0)
    
    while elapsed < max_wait:
        queue_size = memory_event_queue.get_queue_size(datastore_id)
        inflight = processing_manager.get_inflight_count(datastore_id)
        logger.debug(f"Audit end queue check: queue={queue_size}, inflight={inflight}")
        if queue_size == 0 and inflight == 0:
            break
        await asyncio.sleep(wait_interval)
        elapsed += wait_interval
    
    if elapsed >= max_wait:
        logger.warning(f"Audit end signal timeout waiting for queue drain: queue={queue_size}, inflight={inflight}")
    else:
        logger.info(f"Queue drained for audit end (waited {1.0 + elapsed:.1f}s), proceeding with missing file detection")
        # Additional delay to ensure any in-progress parsing operations complete
        await asyncio.sleep(0.2)

    
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

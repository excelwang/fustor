# fusion/src/fustor_fusion/api/consistency.py
"""
Consistency management API for audit cycles and sentinel checks.
"""
from fastapi import APIRouter, Depends, status, HTTPException
import logging
import asyncio
import time
from typing import Dict, Any, List

from ..auth.dependencies import get_view_id_from_api_key
from ..view_manager.manager import get_cached_view_manager
from .. import runtime_objects

logger = logging.getLogger(__name__)

consistency_router = APIRouter(tags=["Consistency Management"], prefix="/consistency")


@consistency_router.post("/audit/start", summary="Signal start of an audit cycle")
async def signal_audit_start(
    view_id: str = Depends(get_view_id_from_api_key)
):
    """
    Explicitly signal the start of an audit cycle.
    This clears blind-spot lists and prepares the system for full reconciliation.
    Broadcasts to all view driver instances that support audit handling.
    """
    view_manager = await get_cached_view_manager(view_id)
    driver_ids = view_manager.get_available_driver_ids()
    
    handled_count = 0
    for driver_id in driver_ids:
        driver_instance = view_manager.get_driver_instance(driver_id)
        if hasattr(driver_instance, 'handle_audit_start'):
            try:
                await driver_instance.handle_audit_start()
                handled_count += 1
            except Exception as e:
                logger.error(f"Failed to handle audit start for driver {driver_id}: {e}")

    if handled_count == 0 and driver_ids:
        logger.debug(f"No driver instances handled audit start (available: {driver_ids})")
    
    if not driver_ids:
        logger.warning(f"Audit start signal received but no view driver instances are loaded for view {view_id}")

    return {"status": "audit_started", "drivers_handled": handled_count}


@consistency_router.post("/audit/end", summary="Signal end of an audit cycle")
async def signal_audit_end(
    view_id: str = Depends(get_view_id_from_api_key)
):
    """
    Signal the completion of an audit cycle.
    This triggers missing item detection and cleanup logic.
    Broadcasts to all view driver instances that support audit handling.
    """
    max_wait = 10.0
    wait_interval = 0.1
    elapsed = 0.0
    

    # Replace busy-wait with Event-based wait
    pm = runtime_objects.pipe_manager
    if pm:
        wait_tasks = []
        pipes = pm.get_pipes()
        for p in pipes.values():
                if hasattr(p, 'view_id') and p.view_id == view_id:
                    if hasattr(p, 'wait_for_drain'):
                        wait_tasks.append(p.wait_for_drain(timeout=max_wait))
        
        if wait_tasks:
            logger.info(f"Waiting for {len(wait_tasks)} pipes to drain before audit end...")
            # Wait for all relevant pipes to drain
            start_time = time.time()
            results = await asyncio.gather(*wait_tasks, return_exceptions=True)
            elapsed = time.time() - start_time
    
    if elapsed >= max_wait:
        logger.warning(f"Audit end signal timeout waiting for pipe drain (waited {max_wait}s)")
    else:
        logger.info(f"Pipes drained for audit end (waited {elapsed:.2f}s), proceeding with missing item detection")

    view_manager = await get_cached_view_manager(view_id)
    driver_ids = view_manager.get_available_driver_ids()
    
    handled_count = 0
    for driver_id in driver_ids:
        driver_instance = view_manager.get_driver_instance(driver_id)
        if hasattr(driver_instance, 'handle_audit_end'):
            try:
                await driver_instance.handle_audit_end()
                handled_count += 1
            except Exception as e:
                logger.error(f"Failed to handle audit end for driver {driver_id}: {e}")

    return {"status": "audit_ended", "drivers_handled": handled_count}


@consistency_router.get("/sentinel/tasks", summary="Get sentinel check tasks")
async def get_sentinel_tasks(
    view_id: str = Depends(get_view_id_from_api_key)
) -> Dict[str, Any]:
    """
    Get generic sentinel check tasks.
    Aggregates tasks from all view driver instances.
    Currently maps Suspect Lists to 'suspect_check' tasks.
    """
    view_manager = await get_cached_view_manager(view_id)
    driver_ids = view_manager.get_available_driver_ids()
    
    all_suspects_paths = []
    
    for driver_id in driver_ids:
        driver_instance = view_manager.get_driver_instance(driver_id)
        if hasattr(driver_instance, 'get_suspect_list'):
            try:
                suspects = await driver_instance.get_suspect_list()
                if suspects:
                    all_suspects_paths.extend(list(suspects.keys()))
            except Exception as e:
                logger.error(f"Failed to get suspect list from driver {driver_id}: {e}")
    
    if all_suspects_paths:
        unique_paths = list(set(all_suspects_paths))
        return {
            'type': 'suspect_check', 
            'paths': unique_paths,
            'view_id': view_id
        }
        
    return {}


@consistency_router.post("/sentinel/feedback", summary="Submit sentinel check feedback")
async def submit_sentinel_feedback(
    feedback: Dict[str, Any],
    view_id: str = Depends(get_view_id_from_api_key)
):
    """
    Submit feedback from sentinel checks.
    Expects payload: {"type": "suspect_update", "updates": [...]}
    Broadcasts feedback to all view driver instances.
    """
    view_manager = await get_cached_view_manager(view_id)
    driver_ids = view_manager.get_available_driver_ids()

    res_type = feedback.get('type')
    processed_count = 0

    if res_type == 'suspect_update':
        updates = feedback.get('updates', [])
        
        for driver_id in driver_ids:
            driver_instance = view_manager.get_driver_instance(driver_id)
            if hasattr(driver_instance, 'update_suspect'):
                try:
                    count_for_driver = 0
                    for item in updates:
                        path = item.get('path')
                        mtime = item.get('mtime')
                        size = item.get('size')
                        if path and mtime is not None:
                            await driver_instance.update_suspect(path, float(mtime), size=size)
                            count_for_driver += 1
                    if count_for_driver > 0:
                        processed_count += 1
                except Exception as e:
                    logger.error(f"Failed to update suspects in driver {driver_id}: {e}")

        return {"status": "processed", "drivers_updated": processed_count}
    
    return {"status": "ignored", "reason": "unknown_type"}


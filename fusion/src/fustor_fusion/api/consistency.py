# fusion/src/fustor_fusion/api/consistency.py
"""
Consistency management API for audit cycles and sentinel checks.
"""
from fastapi import APIRouter, Depends, status, HTTPException
import logging
import asyncio
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
    Broadcasts to all view providers that support audit handling.
    """
    view_manager = await get_cached_view_manager(view_id)
    provider_names = view_manager.get_available_providers()
    
    handled_count = 0
    for name in provider_names:
        provider = view_manager.get_provider(name)
        if hasattr(provider, 'handle_audit_start'):
            try:
                await provider.handle_audit_start()
                handled_count += 1
            except Exception as e:
                logger.error(f"Failed to handle audit start for provider {name}: {e}")

    if handled_count == 0 and provider_names:
        logger.debug(f"No providers handled audit start (available: {provider_names})")
    
    if not provider_names:
        logger.warning(f"Audit start signal received but no view providers are loaded for view {view_id}")

    return {"status": "audit_started", "providers_handled": handled_count}


@consistency_router.post("/audit/end", summary="Signal end of an audit cycle")
async def signal_audit_end(
    view_id: str = Depends(get_view_id_from_api_key)
):
    """
    Signal the completion of an audit cycle.
    This triggers missing item detection and cleanup logic.
    Broadcasts to all view providers that support audit handling.
    """
    max_wait = 10.0
    wait_interval = 0.1
    elapsed = 0.0
    

    while elapsed < max_wait:
        queue_size = 0
        pm = runtime_objects.pipeline_manager
        if pm:
            pipelines = pm.get_pipelines()
            for p in pipelines.values():
                 if hasattr(p, 'view_id') and p.view_id == view_id:
                     dto = await p.get_dto()
                     queue_size += dto.get('queue_size', 0)
        
        if queue_size == 0:
            await asyncio.sleep(wait_interval)
            # Check again to ensure stability
            recheck_queue = 0
            if pm:
                pipelines = pm.get_pipelines()
                for p in pipelines.values():
                     if hasattr(p, 'view_id') and p.view_id == view_id:
                         dto = await p.get_dto()
                         recheck_queue += dto.get('queue_size', 0)
            
            if recheck_queue == 0:
                break
        
        await asyncio.sleep(wait_interval)
        elapsed += wait_interval
    
    if elapsed >= max_wait:
        logger.warning(f"Audit end signal timeout waiting for queue drain: queue={queue_size}")
    else:
        logger.info(f"Queue drained for audit end (waited {elapsed:.2f}s), proceeding with missing item detection")

    view_manager = await get_cached_view_manager(view_id)
    provider_names = view_manager.get_available_providers()
    
    handled_count = 0
    for name in provider_names:
        provider = view_manager.get_provider(name)
        if hasattr(provider, 'handle_audit_end'):
            try:
                await provider.handle_audit_end()
                handled_count += 1
            except Exception as e:
                logger.error(f"Failed to handle audit end for provider {name}: {e}")

    return {"status": "audit_ended", "providers_handled": handled_count}


@consistency_router.get("/sentinel/tasks", summary="Get sentinel check tasks")
async def get_sentinel_tasks(
    view_id: str = Depends(get_view_id_from_api_key)
) -> Dict[str, Any]:
    """
    Get generic sentinel check tasks.
    Aggregates tasks from all view providers.
    Currently maps Suspect Lists to 'suspect_check' tasks.
    """
    view_manager = await get_cached_view_manager(view_id)
    provider_names = view_manager.get_available_providers()
    
    all_suspects_paths = []
    
    for name in provider_names:
        provider = view_manager.get_provider(name)
        if hasattr(provider, 'get_suspect_list'):
            try:
                suspects = await provider.get_suspect_list()
                if suspects:
                    all_suspects_paths.extend(list(suspects.keys()))
            except Exception as e:
                logger.error(f"Failed to get suspect list from provider {name}: {e}")
    
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
    Broadcasts feedback to all view providers.
    """
    view_manager = await get_cached_view_manager(view_id)
    provider_names = view_manager.get_available_providers()

    res_type = feedback.get('type')
    processed_count = 0

    if res_type == 'suspect_update':
        updates = feedback.get('updates', [])
        
        for name in provider_names:
            provider = view_manager.get_provider(name)
            if hasattr(provider, 'update_suspect'):
                try:
                    count_for_provider = 0
                    for item in updates:
                        path = item.get('path')
                        mtime = item.get('mtime')
                        if path and mtime is not None:
                            await provider.update_suspect(path, float(mtime))
                            count_for_provider += 1
                    if count_for_provider > 0:
                        processed_count += 1
                except Exception as e:
                    logger.error(f"Failed to update suspects in provider {name}: {e}")

        return {"status": "processed", "providers_updated": processed_count}
    
    return {"status": "ignored", "reason": "unknown_type"}

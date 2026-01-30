from fastapi import APIRouter, Depends, status, HTTPException
import logging
import asyncio
from typing import Dict, Any, List

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
    Broadcasts to all view providers that support audit handling.
    """
    view_manager = await get_cached_view_manager(datastore_id)
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
        # If providers exist but none support audit, that might be fine, but worth logging
        logger.debug(f"No providers handled audit start (available: {provider_names})")
    
    # If NO providers exist at all, that's probably a configuration issue? 
    # But strictly speaking, it's not a 404 resource not found, just an empty system.
    if not provider_names:
         logger.warning(f"Audit start signal received but no view providers are loaded for datastore {datastore_id}")

    return {"status": "audit_started", "providers_handled": handled_count}

@consistency_router.post("/audit/end", summary="Signal end of an audit cycle")
async def signal_audit_end(
    datastore_id: int = Depends(get_datastore_id_from_api_key)
):
    """
    Signal the completion of an audit cycle.
    This triggers missing item detection and cleanup logic.
    Broadcasts to all view providers that support audit handling.
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
        logger.info(f"Queue drained for audit end (waited {1.0 + elapsed:.1f}s), proceeding with missing item detection")
        # Additional delay to ensure any in-progress operations complete
        await asyncio.sleep(0.2)

    view_manager = await get_cached_view_manager(datastore_id)
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
    datastore_id: int = Depends(get_datastore_id_from_api_key)
) -> Dict[str, Any]:
    """
    Get generic sentinel check tasks.
    Aggregates tasks from all view providers.
    Currently maps Suspect Lists to 'suspect_check' tasks.
    """
    view_manager = await get_cached_view_manager(datastore_id)
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
    
    # Deduplicate paths if multiple providers track same items (unlikely but safe)
    if all_suspects_paths:
        unique_paths = list(set(all_suspects_paths))
        return {
             'type': 'suspect_check', 
             'paths': unique_paths,
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
    Broadcasts feedback to all view providers.
    """
    view_manager = await get_cached_view_manager(datastore_id)
    provider_names = view_manager.get_available_providers()

    res_type = feedback.get('type')
    processed_count = 0

    if res_type == 'suspect_update':
        updates = feedback.get('updates', [])
        
        for name in provider_names:
            provider = view_manager.get_provider(name)
            if hasattr(provider, 'update_suspect'):
                # Pass updates to each provider
                # Optimization: In future we might filter updates by provider responsibility
                try:
                    count_for_provider = 0
                    for item in updates:
                        path = item.get('path')
                        mtime = item.get('mtime')
                        if path and mtime is not None:
                            await provider.update_suspect(path, float(mtime))
                            count_for_provider += 1
                    if count_for_provider > 0:
                        processed_count += 1 # Count providers that processed something
                except Exception as e:
                    logger.error(f"Failed to update suspects in provider {name}: {e}")

        return {"status": "processed", "providers_updated": processed_count}
    
    return {"status": "ignored", "reason": "unknown_type"}

# fusion/src/fustor_fusion/api/ingestion.py
"""
Event ingestion API for receiving batches of events from Agent.
"""
from fastapi import APIRouter, Depends, status, HTTPException, Query, Request
from pydantic import BaseModel
import logging
import asyncio
from typing import List, Dict, Any
import time

from ..auth.dependencies import get_view_id_from_api_key
from ..runtime import datastore_event_manager
from ..core.session_manager import session_manager
from ..config import receivers_config
from ..datastore_state_manager import datastore_state_manager
from ..processing_manager import processing_manager

from ..queue_integration import queue_based_ingestor, add_events_batch_to_queue, get_position_from_queue, update_position_in_queue
from ..in_memory_queue import memory_event_queue

from fustor_core.event import EventBase, EventType, MessageSource

from ..view_manager.manager import get_cached_view_manager
from datetime import datetime

logger = logging.getLogger(__name__)
ingestion_router = APIRouter(tags=["Ingestion"])


def _get_pipeline_config(pipeline_id: str) -> Dict[str, Any]:
    """Get pipeline configuration."""
    pipelines = receivers_config.get_active_pipelines()
    if pipeline_id in pipelines:
        return pipelines[pipeline_id]
    return {"allow_concurrent_push": False}


@ingestion_router.get("/stats", summary="Get global ingestion statistics", dependencies=[Depends(get_view_id_from_api_key)])
async def get_global_stats():
    """
    Get aggregated statistics across all active pipelines for the monitoring dashboard.
    """
    active_pipelines = receivers_config.get_active_pipelines()
    
    sources_map = {}
    total_volume = 0
    max_latency_ms = 0
    oldest_item_info = {"path": "N/A", "age_days": 0}
    max_staleness_seconds = -1

    for pipeline_id, cfg in active_pipelines.items():
        # V2 uses string IDs directly
        view_id = pipeline_id
        
        ds_sessions = await session_manager.get_datastore_sessions(view_id)
        for s_id, s_info in ds_sessions.items():
            task_id = s_info.task_id or f"Task-{s_id[:6]}"
            
            display_id = task_id
            if ":" in task_id:
                parts = task_id.split(":", 1)
                uuid_part = parts[0]
                name_part = parts[1]
                if len(uuid_part) > 8:
                    display_id = f"{uuid_part[:8]}...:{name_part}"
            elif len(task_id) > 20:
                display_id = f"{task_id[:8]}..."

            if task_id not in sources_map:
                sources_map[task_id] = {
                    "id": display_id,
                    "type": "Agent"
                }

        try:
            view_manager = await get_cached_view_manager(view_id)
            stats = await view_manager.get_aggregated_stats()
            
            total_volume += stats.get("total_volume", 0)

            ds_latency = stats.get("max_latency_ms", 0)
            if ds_latency > max_latency_ms:
                max_latency_ms = ds_latency

            stale = stats.get("max_staleness_seconds", 0)
            if stale > max_staleness_seconds:
                max_staleness_seconds = stale
                info = stats.get("oldest_item_info")
                if info:
                    oldest_item_info = {
                        "path": f"[{view_id}] {info.get('path', 'unknown')}",
                        "age_days": int(stale / 86400)
                    }
        except Exception as e:
            logger.error(f"Failed to get stats for view {view_id}: {e}")

    return {
        "sources": list(sources_map.values()),
        "metrics": {
            "total_volume": total_volume,
            "latency_ms": int(max_latency_ms),
            "oldest_item": oldest_item_info
        }
    }


@ingestion_router.get("/position", summary="Get latest checkpoint position")
async def get_position(
    session_id: str = Query(..., description="Sync source unique ID"),
    view_id=Depends(get_view_id_from_api_key),
):
    si = await session_manager.get_session_info(view_id, session_id)
    if not si:
        raise HTTPException(status_code=404, detail="Session not found")
    
    position_index = await get_position_from_queue(view_id, si.task_id)
    
    if position_index is not None:
        return {"index": position_index}
    else:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Checkpoint not found, suggest triggering snapshot sync")


class BatchIngestPayload(BaseModel):
    """Defines the generic payload for receiving a batch of events from any client."""
    session_id: str
    events: List[Dict[str, Any]]
    source_type: str  # 'message' or 'snapshot'
    is_snapshot_end: bool = False


@ingestion_router.post(
    "/",
    summary="Receive batch events",
    description="This endpoint receives batch events from clients."
)
async def ingest_event_batch(
    payload: BatchIngestPayload,
    request: Request,
    view_id=Depends(get_view_id_from_api_key),
):
    si = await session_manager.get_session_info(view_id, payload.session_id)
    if not si:
        raise HTTPException(status_code=404, detail="Session not found")
    await session_manager.keep_session_alive(
        view_id, 
        payload.session_id,
        client_ip=request.client.host
    )

    # Check for outdated snapshot pushes
    pipeline_config = _get_pipeline_config(str(view_id))
    allow_concurrent_push = pipeline_config.get("allow_concurrent_push", False)
    
    if allow_concurrent_push and payload.source_type == 'snapshot':
        is_authoritative = await datastore_state_manager.is_authoritative_session(view_id, payload.session_id)
        if not is_authoritative:
            logger.warning(f"Received snapshot push from outdated session '{payload.session_id}' for view {view_id}. Rejecting with 419.")
            raise HTTPException(status_code=419, detail="A newer sync session has been started. This snapshot task is now obsolete and should stop.")

    # Handle snapshot end signal
    if payload.is_snapshot_end and payload.source_type != 'audit':
        logger.info(f"Received end signal for source_type={payload.source_type} for view {view_id}")
        await datastore_state_manager.set_snapshot_complete(view_id, payload.session_id)

    try:
        if payload.events:
            latest_index = 0
            event_objects_to_add: List[EventBase] = []
            for event_dict in payload.events:
                event_type = EventType(event_dict.get("event_type", EventType.UPDATE.value))
                event_schema = event_dict.get("event_schema", "default_schema")
                table = event_dict.get("table", "default_table")
                index = event_dict.get("index", -1)
                rows = event_dict.get("rows", [])
                fields = event_dict.get("fields", list(rows[0].keys()) if rows else [])

                msg_source = event_dict.get("message_source")
                if not msg_source and payload.source_type == 'audit':
                    msg_source = MessageSource.AUDIT
                elif not msg_source and payload.source_type == 'snapshot':
                    msg_source = MessageSource.SNAPSHOT
                elif not msg_source:
                    msg_source = MessageSource.REALTIME
                
                event_obj = EventBase(
                    event_type=event_type,
                    event_schema=event_schema,
                    table=table,
                    index=index,
                    rows=rows,
                    fields=fields,
                    message_source=msg_source
                )
                
                event_objects_to_add.append(event_obj)

                if isinstance(index, int):
                    latest_index = max(latest_index, index)

            if latest_index > 0:
                await update_position_in_queue(view_id, si.task_id, latest_index)

            await add_events_batch_to_queue(view_id, event_objects_to_add, si.task_id)
            
            await processing_manager.ensure_processor(view_id)
            
        try:
            await datastore_event_manager.notify(view_id)
        except Exception as e:
            logger.error(f"Failed to notify event manager for view {view_id}: {e}", exc_info=True)

        # NEW: Return role info to reduce heartbeat overhead
        is_leader = await datastore_state_manager.is_leader(view_id, payload.session_id)
        return {
            "status": "ok", 
            "role": "leader" if is_leader else "follower",
            "is_leader": is_leader
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to process batch events (task: {si.task_id}): {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to push batch events: {str(e)}")

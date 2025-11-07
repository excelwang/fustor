from fastapi import APIRouter, Depends, status, HTTPException, Query, Request
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
import logging
from typing import List, Dict, Any
import time

from ..auth.dependencies import get_datastore_id_from_api_key
from ..runtime import datastore_event_manager
from ..core.session_manager import session_manager
from ..auth.datastore_cache import datastore_config_cache
from ..datastore_state_manager import datastore_state_manager

# Import the queue-based ingestion
from ..queue_integration import queue_based_ingestor, add_events_batch_to_queue, get_position_from_queue, update_position_in_queue
from fustor_event_model.models import EventBase, EventType # Import EventBase and EventType

# Import the parsers module
from ..parsers.manager import ParserManager # CORRECTED

logger = logging.getLogger(__name__)
ingestion_router = APIRouter(tags=["Ingestion"])


@ingestion_router.get("/position", summary="获取同步源的最新检查点位置")
async def get_position(
    session_id: str = Query(..., description="同步源的唯一 ID"),
    datastore_id=Depends(get_datastore_id_from_api_key),
):
    si = await session_manager.get_session_info(datastore_id, session_id)
    if not si:
        raise HTTPException(status_code=404, detail="Session not found")
    
    # Get position from memory queue
    position_index = await get_position_from_queue(datastore_id, si.task_id)
    
    if position_index is not None:
        return {"index": position_index}
    else:
        # No position found
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="该同步源的检查点未找到，建议触发快照同步")

# --- Pydantic Models for Ingestion ---
class BatchIngestPayload(BaseModel):
    """
    Defines the generic payload for receiving a batch of events from any client.
    """
    session_id: str
    events: List[Dict[str, Any]] # Events are received as dicts
    source_type: str # 'message' or 'snapshot'
# --- End Ingestion Models ---


@ingestion_router.post(
    "/",
    summary="接收批量事件",
    description="此端点用于从客户端接收批量事件。",
    status_code=status.HTTP_204_NO_CONTENT
)
async def ingest_event_batch(
    payload: BatchIngestPayload,
    request: Request,
    datastore_id=Depends(get_datastore_id_from_api_key),
):
    si = await session_manager.get_session_info(datastore_id, payload.session_id)
    if not si:
        raise HTTPException(status_code=404, detail="Session not found")
    await session_manager.keep_session_alive(
        datastore_id, 
        payload.session_id,
        client_ip=request.client.host # Assuming request is available here
    )

    # NEW: Check for outdated snapshot pushes
    datastore_config = datastore_config_cache.get_datastore_config(datastore_id)
    if datastore_config and datastore_config.allow_concurrent_push and payload.source_type == 'snapshot':
        is_authoritative = await datastore_state_manager.is_authoritative_session(datastore_id, payload.session_id)
        if not is_authoritative:
            logger.warning(f"Received snapshot push from outdated session '{payload.session_id}' for datastore {datastore_id}. Rejecting with 419.")
            raise HTTPException(status_code=419, detail="A newer sync session has been started. This snapshot task is now obsolete and should stop.")

    try:
        if payload.events:
            latest_index = 0
            event_objects_to_add: List[EventBase] = []
            for event_dict in payload.events:
                # Infer event_type, schema, table, index, and fields from the dict
                # Default to UPDATE if not specified, as it's the most common for generic data
                event_type = EventType(event_dict.get("event_type", EventType.UPDATE.value))
                event_schema = event_dict.get("event_schema", "default_schema") # Use event_schema
                table = event_dict.get("table", "default_table")
                index = event_dict.get("index", -1)
                rows = event_dict.get("rows", [])
                fields = event_dict.get("fields", list(rows[0].keys()) if rows else [])

                # Create EventBase object
                event_obj = EventBase(
                    event_type=event_type,
                    event_schema=event_schema, # Use event_schema
                    table=table,
                    index=index,
                    rows=rows,
                    fields=fields
                )
                event_objects_to_add.append(event_obj)

                if isinstance(index, int):
                    latest_index = max(latest_index, index)

            # Update position in memory queue
            if latest_index > 0:
                await update_position_in_queue(datastore_id, si.task_id, latest_index)

            # Add events to the in-memory queue for high-throughput ingestion
            # Pass task_id for position tracking
            total_events_added = await add_events_batch_to_queue(datastore_id, event_objects_to_add, si.task_id)
            
        # Notify the background task that there are new events
        try:
            await datastore_event_manager.notify(datastore_id)
        except Exception as e:
            logger.error(f"Failed to notify event manager for datastore {datastore_id}: {e}", exc_info=True)

    except Exception as e:
        logger.error(f"处理批量事件失败 (task: {si.task_id}): {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"推送批量事件失败: {str(e)}")
import asyncio
import logging
import threading
from typing import Any, List, Optional, Tuple, TYPE_CHECKING, Iterator

from fustor_core.pipeline import PipelineState
from fustor_core.exceptions import SessionObsoletedError

if TYPE_CHECKING:
    from ..agent_pipeline import AgentPipeline

logger = logging.getLogger("fustor_agent.pipeline.phases")

async def run_snapshot_sync(pipeline: "AgentPipeline") -> None:
    """Execute snapshot synchronization for the given pipeline."""
    logger.info(f"Pipeline {pipeline.id}: Starting snapshot sync")
    
    try:
        # Get snapshot iterator from source
        snapshot_iter = pipeline.source_handler.get_snapshot_iterator()
        
        batch = []
        # Support both sync and async iterators
        if not hasattr(snapshot_iter, "__aiter__"):
            snapshot_iter = pipeline._aiter_sync(snapshot_iter)

        async for event in snapshot_iter:
            if not pipeline.is_running() and not (pipeline.state & PipelineState.RECONNECTING):
                break
                
            batch.append(event)
            if len(batch) >= pipeline.batch_size:
                batch = pipeline.map_batch(batch)
                success, response = await pipeline.sender_handler.send_batch(
                    pipeline.session_id, batch, {"phase": "snapshot"}
                )
                if success:
                    pipeline._update_role_from_response(response)
                    pipeline.statistics["events_pushed"] += len(batch)
                    batch = []
                else:
                    raise Exception("Snapshot batch send failed")
                
        # Send remaining events in batch
        if batch and pipeline.has_active_session():
            batch = pipeline.map_batch(batch)
            success, response = await pipeline.sender_handler.send_batch(
                pipeline.session_id, batch, {"phase": "snapshot", "is_final": True}
            )
            if success:
                pipeline._update_role_from_response(response)
                pipeline.statistics["events_pushed"] += len(batch)
                
        logger.info(f"Pipeline {pipeline.id}: Snapshot sync complete")
    except asyncio.CancelledError:
        logger.info(f"Snapshot sync for {pipeline.id} cancelled")
        raise
    except Exception as e:
        logger.error(f"Pipeline {pipeline.id} snapshot sync error: {e}", exc_info=True)
        raise

async def run_driver_message_sync(pipeline: "AgentPipeline") -> None:
    """Execute message sync directly from driver."""


    # Pass a stop event if possible for better cleanup
    stop_event = threading.Event()
    msg_iter = pipeline.source_handler.get_message_iterator(
        start_position=-1, 
        stop_event=stop_event
    )
    
    try:
        batch = []
        if not hasattr(msg_iter, "__aiter__"):
            msg_iter = pipeline._aiter_sync(msg_iter)

        async for event in msg_iter:
            if not pipeline.is_running() and not (pipeline.state & PipelineState.RECONNECTING):
                break
                
            batch.append(event)
            if len(batch) >= pipeline.batch_size:
                batch = pipeline.map_batch(batch)
                success, response = await pipeline.sender_handler.send_batch(
                    pipeline.session_id, batch, {"phase": "realtime"}
                )
                if success:
                    pipeline._update_role_from_response(response)
                    pipeline.statistics["events_pushed"] += len(batch)
                    batch = []
                else:
                    raise Exception("Realtime batch send failed")
    except asyncio.CancelledError:
        logger.info(f"Driver message sync for {pipeline.id} cancelled")
        raise
    finally:
        # Send remaining events in batch
        if batch and pipeline.has_active_session():
            try:
                batch = pipeline.map_batch(batch)
                success, response = await pipeline.sender_handler.send_batch(
                    pipeline.session_id, batch, {"phase": "realtime", "is_final": True}
                )
                if success:
                    pipeline._update_role_from_response(response)
                    pipeline.statistics["events_pushed"] += len(batch)
            except Exception as e:
                logger.warning(f"Failed to push final message batch: {e}")
        
        # Signal stop to the underlying sync iterator
        stop_event.set()

async def run_bus_message_sync(pipeline: "AgentPipeline") -> None:
    """Execute message sync reading from an internal event bus."""
    if not pipeline._bus:
        logger.error(f"Pipeline {pipeline.id}: Cannot run bus sync without a bus")
        return
        
    logger.info(f"Pipeline {pipeline.id}: Starting bus message sync from bus '{pipeline._bus.id}'")
    
    try:
        while pipeline.is_running() or (pipeline.state & PipelineState.RECONNECTING):
            # 1. Fetch from bus
            events = await pipeline._bus.internal_bus.get_events_for(
                pipeline.id, 
                pipeline.batch_size, 
                timeout=0.2  # 200ms poll timeout, matching Master version for low latency
            )
            
            if not events:
                continue
                
            # 2. Sync send to fusion
            events = pipeline.map_batch(events)
            success, response = await pipeline.sender_handler.send_batch(
                pipeline.session_id, events, {"phase": "realtime"}
            )
            
            if success:
                pipeline._update_role_from_response(response)
                pipeline.statistics["events_pushed"] += len(events)
                # Commit to bus
                if pipeline._bus_service:
                    await pipeline._bus_service.commit_and_handle_split(
                        pipeline._bus.id, 
                        pipeline.id, 
                        len(events), 
                        events[-1].index,
                        pipeline.config.get("fields_mapping", [])
                    )
                else:
                    await pipeline._bus.internal_bus.commit(pipeline.id, len(events), events[-1].index)
            else:
                logger.warning(f"Pipeline {pipeline.id}: Failed to send bus events")
                await asyncio.sleep(1.0) # Wait before retry
                
    except asyncio.CancelledError:
        logger.info(f"Bus message sync for {pipeline.id} cancelled")
        raise
    except Exception as e:
        logger.error(f"Pipeline {pipeline.id} bus sync error: {e}", exc_info=True)
        raise

async def run_audit_sync(pipeline: "AgentPipeline") -> None:
    """Execute a full audit synchronization."""
    logger.info(f"Pipeline {pipeline.id}: Starting audit sync")
    old_state = pipeline.state
    pipeline._set_state(old_state | PipelineState.AUDIT_PHASE)
    
    try:
        # Signal start (happens automatically in handler adapter)
        audit_iter = pipeline.source_handler.get_audit_iterator()
        
        batch = []
        if not hasattr(audit_iter, "__aiter__"):
            audit_iter = pipeline._aiter_sync(audit_iter)
            
        async for item in audit_iter:
            if not pipeline.is_running():
                break
            
            # FSDriver.get_audit_iterator returns Tuple[Optional[EventBase], Dict[str, float]]
            # Unpack the tuple: event can be None (for silent dirs), mtime_cache_update is a dict
            if isinstance(item, tuple) and len(item) == 2:
                event, mtime_cache_update = item
                if event is None:
                    continue  # Skip None events (used for mtime cache updates only)
            else:
                # Handle case where iterator yields plain events (for other drivers)
                event = item
            
            batch.append(event)
            if len(batch) >= pipeline.batch_size:
                batch = pipeline.map_batch(batch)
                await pipeline.sender_handler.send_batch(
                    pipeline.session_id, batch, {"phase": "audit"}
                )
                batch = []
        
        if batch:
            batch = pipeline.map_batch(batch)
            await pipeline.sender_handler.send_batch(
                pipeline.session_id, batch, {"phase": "audit"}
            )
    finally:
        # Always send audit end signal to ensure Fusion calls handle_audit_end()
        if pipeline.has_active_session():
            try:
                await pipeline.sender_handler.send_batch(
                    pipeline.session_id, [], {"phase": "audit", "is_final": True}
                )
            except Exception as e:
                logger.warning(f"Failed to send audit end signal: {e}")
        # Clear audit phase flag
        pipeline._set_state(old_state & ~PipelineState.AUDIT_PHASE)

async def run_sentinel_check(pipeline: "AgentPipeline") -> None:
    """Execute a sentinel check cycle."""
    logger.debug(f"Pipeline {pipeline.id}: Running sentinel check")
    
    try:
        # 1. Fetch tasks from Fusion
        task_batch = await pipeline.sender_handler.get_sentinel_tasks()
        
        if not task_batch or not task_batch.get("paths"):
            logger.debug(f"Pipeline {pipeline.id}: No sentinel tasks available")
            return
        
        logger.info(f"Pipeline {pipeline.id}: Received {len(task_batch.get('paths', []))} sentinel tasks")
        
        # 2. Perform check via Source handler
        results = pipeline.source_handler.perform_sentinel_check(task_batch)
        
        if results:
            # 3. Submit results back to Fusion
            success = await pipeline.sender_handler.submit_sentinel_results(results)
            if success:
                logger.info(f"Pipeline {pipeline.id}: Submitted sentinel results for {len(results.get('updates', []))} items")
            else:
                logger.warning(f"Pipeline {pipeline.id}: Failed to submit sentinel results")
                
    except Exception as e:
        logger.error(f"Pipeline {pipeline.id}: Error during sentinel check: {e}", exc_info=True)

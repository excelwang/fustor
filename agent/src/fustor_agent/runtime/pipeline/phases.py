import asyncio
import logging
import threading
from typing import Any, List, Optional, Tuple, TYPE_CHECKING, Iterator

from fustor_core.pipeline import PipelineState
from fustor_core.exceptions import SessionObsoletedError
from fustor_core.common.metrics import get_metrics

if TYPE_CHECKING:
    from ..agent_pipeline import AgentPipeline

logger = logging.getLogger("fustor_agent.pipeline.phases")

async def run_snapshot_sync(pipeline: "AgentPipeline") -> None:
    """Execute snapshot sync phase for the given pipeline."""
    logger.info(f"Pipeline {pipeline.id}: Starting snapshot sync phase")
    
    try:
        # Get snapshot iterator from source
        snapshot_iter = pipeline.source_handler.get_snapshot_iterator()
        
        batch = []
        # Support both sync and async iterators
        if not hasattr(snapshot_iter, "__aiter__"):
            snapshot_iter = pipeline._aiter_sync_phase(snapshot_iter)

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
                    await pipeline._update_role_from_response(response)
                    pipeline.statistics["events_pushed"] += len(batch)
                    get_metrics().counter("fustor.agent.events_pushed", len(batch), {"pipeline": pipeline.id, "phase": "snapshot"})
                    batch = []
                else:
                    raise Exception("Snapshot batch send failed")
                
        # Send remaining events and signal completion
        if pipeline.has_active_session():
            batch = pipeline.map_batch(batch)
            success, response = await pipeline.sender_handler.send_batch(
                pipeline.session_id, batch, {"phase": "snapshot", "is_final": True}
            )
            if success:
                await pipeline._update_role_from_response(response)
                pipeline.statistics["events_pushed"] += len(batch)
                
        logger.info(f"Pipeline {pipeline.id}: Snapshot sync phase complete")
    except asyncio.CancelledError:
        logger.info(f"Snapshot sync phase for {pipeline.id} cancelled")
        raise
    except Exception as e:
        logger.error(f"Pipeline {pipeline.id} snapshot sync phase error: {e}", exc_info=True)
        raise



async def run_bus_message_sync(pipeline: "AgentPipeline") -> None:
    """Execute message sync phase reading from an internal event bus."""
    if not pipeline._bus:
        logger.error(f"Pipeline {pipeline.id}: Cannot run bus message sync phase without a bus")
        return
        
    logger.info(f"Pipeline {pipeline.id}: Starting bus message sync phase from bus '{pipeline._bus.id}'")
    
    try:
        while pipeline.is_running() or (pipeline.state & PipelineState.RECONNECTING):
            # 1. Fetch from bus
            # Use task_id (agent_id:pipeline_id) for bus operations
            events = await pipeline._bus.internal_bus.get_events_for(
                pipeline.task_id, 
                pipeline.batch_size, 
                timeout=0.2  # 200ms poll timeout
            )
            
            if not events:
                # Even if no events, we mark ready after first successful poll
                pipeline.is_realtime_ready = True
                # Safety sleep to prevent busy loop if get_events_for returns immediately
                await asyncio.sleep(0.1)
                continue
            
            pipeline.is_realtime_ready = True
                
            # 2. Send to fusion
            events = pipeline.map_batch(events)
            
            success, response = await pipeline.sender_handler.send_batch(
                pipeline.session_id, events, {"phase": "realtime"}
            )
            
            if success:
                await pipeline._update_role_from_response(response)
                pipeline.statistics["events_pushed"] += len(events)
                get_metrics().counter("fustor.agent.events_pushed", len(events), {"pipeline": pipeline.id, "phase": "realtime_bus"})
                
                # Commit to bus using task_id
                if pipeline._bus_service:
                    await pipeline._bus_service.commit_and_handle_split(
                        pipeline._bus.id, 
                        pipeline.task_id, 
                        len(events), 
                        events[-1].index,
                        pipeline.config.get("fields_mapping", [])
                    )
                else:
                    await pipeline._bus.internal_bus.commit(pipeline.task_id, len(events), events[-1].index)
            else:
                logger.warning(f"Pipeline {pipeline.id}: Failed to send bus events")
                await asyncio.sleep(1.0) # Wait before retry

                
    except asyncio.CancelledError:
        logger.info(f"Bus message sync phase for {pipeline.id} cancelled")
        raise
    except Exception as e:
        logger.error(f"Pipeline {pipeline.id} bus phase error: {e}", exc_info=True)
        raise

async def run_audit_sync(pipeline: "AgentPipeline") -> None:
    """Execute a full audit synchronization."""
    logger.info(f"Pipeline {pipeline.id}: Starting audit phase")
    old_state = pipeline.state
    pipeline._set_state(old_state | PipelineState.AUDIT_PHASE)
    
    try:
        # Signal start (happens automatically in handler adapter)
        audit_iter = pipeline.source_handler.get_audit_iterator(mtime_cache=pipeline.audit_context)
        
        batch = []
        if not hasattr(audit_iter, "__aiter__"):
            audit_iter = pipeline._aiter_sync_phase(audit_iter)
            
        async for item in audit_iter:
            if not pipeline.is_running():
                break
            
            # FSDriver.get_audit_iterator returns Tuple[Optional[EventBase], Dict[str, float]]
            # Unpack the tuple: event can be None (for silent dirs), mtime_cache_update is a dict
            if isinstance(item, tuple) and len(item) == 2:
                event, mtime_cache_update = item
                # Update cache immediately (D-05/U-02)
                if mtime_cache_update:
                    pipeline.audit_context.update(mtime_cache_update)
                    
                if event is None:
                    continue  # Skip None events (silent dirs)
            else:
                # Handle case where iterator yields plain events (for other drivers)
                event = item
            
            batch.append(event)
            if len(batch) >= pipeline.batch_size:
                batch = pipeline.map_batch(batch)
                await pipeline.sender_handler.send_batch(
                    pipeline.session_id, batch, {"phase": "audit"}
                )
                get_metrics().counter("fustor.agent.events_pushed", len(batch), {"pipeline": pipeline.id, "phase": "audit"})
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
                get_metrics().counter("fustor.agent.sentinel_checks", 1, {"pipeline": pipeline.id})
            else:
                logger.warning(f"Pipeline {pipeline.id}: Failed to submit sentinel results")
                
    except Exception as e:
        logger.error(f"Pipeline {pipeline.id}: Error during sentinel check: {e}", exc_info=True)

import asyncio
import logging
import threading
from typing import Any, List, Optional, Tuple, TYPE_CHECKING, Iterator

from fustor_core.pipe import PipeState
from fustor_core.exceptions import SessionObsoletedError
from fustor_core.common.metrics import get_metrics

if TYPE_CHECKING:
    from ..agent_pipe import AgentPipe

logger = logging.getLogger("fustor_agent.pipe.phases")

async def run_snapshot_sync(pipe: "AgentPipe") -> None:
    """Execute snapshot sync phase for the given pipe."""
    logger.info(f"Pipe {pipe.id}: Starting snapshot sync phase")
    
    try:
        # Get snapshot iterator from source
        snapshot_iter = pipe.source_handler.get_snapshot_iterator()
        
        batch = []
        # Support both sync and async iterators
        if not hasattr(snapshot_iter, "__aiter__"):
            snapshot_iter = pipe._aiter_sync_phase(snapshot_iter)

        async for event in snapshot_iter:
            if not pipe.is_running() and not (pipe.state & PipeState.RECONNECTING):
                break
                
            batch.append(event)
            if len(batch) >= pipe.batch_size:
                batch = pipe.map_batch(batch)
                success, response = await pipe.sender_handler.send_batch(
                    pipe.session_id, batch, {"phase": "snapshot"}
                )
                if success:
                    await pipe._update_role_from_response(response)
                    pipe.statistics["events_pushed"] += len(batch)
                    get_metrics().counter("fustor.agent.events_pushed", len(batch), {"pipe": pipe.id, "phase": "snapshot"})
                    batch = []
                else:
                    raise Exception("Snapshot batch send failed")
                
        # Send remaining events and signal completion
        if pipe.has_active_session():
            batch = pipe.map_batch(batch)
            success, response = await pipe.sender_handler.send_batch(
                pipe.session_id, batch, {"phase": "snapshot", "is_final": True}
            )
            if success:
                await pipe._update_role_from_response(response)
                pipe.statistics["events_pushed"] += len(batch)
                
        logger.info(f"Pipe {pipe.id}: Snapshot sync phase complete")
    except asyncio.CancelledError:
        logger.info(f"Snapshot sync phase for {pipe.id} cancelled")
        raise
    except Exception as e:
        logger.error(f"Pipe {pipe.id} snapshot sync phase error: {e}", exc_info=True)
        raise



async def run_bus_message_sync(pipe: "AgentPipe") -> None:
    """Execute message sync phase reading from an internal event bus."""
    if not pipe._bus:
        logger.error(f"Pipe {pipe.id}: Cannot run bus message sync phase without a bus")
        return
        
    logger.info(f"Pipe {pipe.id}: Starting bus message sync phase from bus '{pipe._bus.id}'")
    
    try:
        while pipe.is_running() or (pipe.state & PipeState.RECONNECTING):
            # 1. Fetch from bus
            # Use pipe.task_id for bus operations to match subscription ID
            events = await pipe._bus.internal_bus.get_events_for(
                pipe.task_id, 
                pipe.batch_size, 
                timeout=0.2  # 200ms poll timeout
            )
            
            if not events:
                # Even if no events, we mark ready after first successful poll
                pipe.is_realtime_ready = True
                # Safety sleep to prevent busy loop if get_events_for returns immediately
                await asyncio.sleep(0.1)
                continue
            
            pipe.is_realtime_ready = True
                
            # 2. Send to fusion
            events = pipe.map_batch(events)
            
            success, response = await pipe.sender_handler.send_batch(
                pipe.session_id, events, {"phase": "realtime"}
            )
            
            if success:
                await pipe._update_role_from_response(response)
                pipe.statistics["events_pushed"] += len(events)
                get_metrics().counter("fustor.agent.events_pushed", len(events), {"pipe": pipe.id, "phase": "realtime_bus"})
                
                # Commit to bus using pipe.task_id
                if pipe._bus_service:
                    await pipe._bus_service.commit_and_handle_split(
                        pipe._bus.id, 
                        pipe.task_id, 
                        len(events), 
                        events[-1].index,
                        pipe.config.get("fields_mapping", [])
                    )
                else:
                    await pipe._bus.internal_bus.commit(pipe.task_id, len(events), events[-1].index)
            else:
                logger.warning(f"Pipe {pipe.id}: Failed to send bus events")
                await asyncio.sleep(1.0) # Wait before retry

                
    except asyncio.CancelledError:
        logger.info(f"Bus message sync phase for {pipe.id} cancelled")
        raise
    except Exception as e:
        logger.error(f"Pipe {pipe.id} bus phase error: {e}", exc_info=True)
        raise

async def run_audit_sync(pipe: "AgentPipe") -> None:
    """Execute a full audit synchronization."""
    logger.info(f"Pipe {pipe.id}: Starting audit phase")
    old_state = pipe.state
    pipe._set_state(old_state | PipeState.AUDIT_PHASE)
    
    try:
        # Signal start (happens automatically in handler adapter)
        audit_iter = pipe.source_handler.get_audit_iterator(mtime_cache=pipe.audit_context)
        
        batch = []
        if not hasattr(audit_iter, "__aiter__"):
            audit_iter = pipe._aiter_sync_phase(audit_iter)
            
        async for item in audit_iter:
            if not pipe.is_running():
                break
            
            # FSDriver.get_audit_iterator returns Tuple[Optional[EventBase], Dict[str, float]]
            # Unpack the tuple: event can be None (for silent dirs), mtime_cache_update is a dict
            if isinstance(item, tuple) and len(item) == 2:
                event, mtime_cache_update = item
                # Update cache immediately (D-05/U-02)
                if mtime_cache_update:
                    pipe.audit_context.update(mtime_cache_update)
                    
                if event is None:
                    continue  # Skip None events (silent dirs)
            else:
                # Handle case where iterator yields plain events (for other drivers)
                event = item
            
            batch.append(event)
            if len(batch) >= pipe.batch_size:
                batch = pipe.map_batch(batch)
                await pipe.sender_handler.send_batch(
                    pipe.session_id, batch, {"phase": "audit"}
                )
                get_metrics().counter("fustor.agent.events_pushed", len(batch), {"pipe": pipe.id, "phase": "audit"})
                batch = []
        
        if batch:
            batch = pipe.map_batch(batch)
            await pipe.sender_handler.send_batch(
                pipe.session_id, batch, {"phase": "audit"}
            )
    finally:
        # Always send audit end signal to ensure Fusion calls handle_audit_end()
        if pipe.has_active_session():
            try:
                await pipe.sender_handler.send_batch(
                    pipe.session_id, [], {"phase": "audit", "is_final": True}
                )
            except Exception as e:
                logger.warning(f"Failed to send audit end signal: {e}")
        # Clear audit phase flag
        pipe._set_state(old_state & ~PipeState.AUDIT_PHASE)

async def run_sentinel_check(pipe: "AgentPipe") -> None:
    """Execute a sentinel check cycle."""
    logger.debug(f"Pipe {pipe.id}: Running sentinel check")
    
    try:
        # 1. Fetch tasks from Fusion
        task_batch = await pipe.sender_handler.get_sentinel_tasks()
        
        if not task_batch or not task_batch.get("paths"):
            logger.debug(f"Pipe {pipe.id}: No sentinel tasks available")
            return
        
        logger.info(f"Pipe {pipe.id}: Received {len(task_batch.get('paths', []))} sentinel tasks")
        
        # 2. Perform check via Source handler
        results = pipe.source_handler.perform_sentinel_check(task_batch)
        
        if results:
            # 3. Submit results back to Fusion
            success = await pipe.sender_handler.submit_sentinel_results(results)
            if success:
                logger.info(f"Pipe {pipe.id}: Submitted sentinel results for {len(results.get('updates', []))} items")
                get_metrics().counter("fustor.agent.sentinel_checks", 1, {"pipe": pipe.id})
            else:
                logger.warning(f"Pipe {pipe.id}: Failed to submit sentinel results")
                
    except Exception as e:
        logger.error(f"Pipe {pipe.id}: Error during sentinel check: {e}", exc_info=True)

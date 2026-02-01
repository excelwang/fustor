# agent/src/fustor_agent/runtime/agent_pipeline.py
"""
Agent Pipeline implementation.

AgentPipeline orchestrates the flow: Source -> Sender
This is the V2 architecture replacement for SyncInstance.
"""
import asyncio
import logging
import threading
from typing import Optional, Any, Dict, TYPE_CHECKING, Iterator

from fustor_core.pipeline import Pipeline, PipelineState
from fustor_core.pipeline.handler import SourceHandler
from fustor_core.pipeline.sender import SenderHandler
from fustor_core.models.states import SyncState, SyncInstanceDTO

if TYPE_CHECKING:
    from fustor_core.pipeline import PipelineContext
    from fustor_agent.services.instances.bus import EventBusInstanceRuntime
    from fustor_core.models.states import SyncInstanceDTO

logger = logging.getLogger("fustor_agent")


class AgentPipeline(Pipeline):
    """
    Agent-side Pipeline implementation.
    
    Orchestrates the data flow from Source to Sender:
    1. Session lifecycle management with Fusion
    2. Snapshot sync phase
    3. Realtime message sync phase
    4. Periodic audit and sentinel checks
    5. Heartbeat and role management (Leader/Follower)
    
    This is designed to eventually replace SyncInstance.
    """
    
    # Class-level timing constants
    CONTROL_LOOP_INTERVAL = 1.0
    FOLLOWER_STANDBY_INTERVAL = 1.0
    ROLE_CHECK_INTERVAL = 1.0
    ERROR_RETRY_INTERVAL = 5.0
    MAX_CONSECUTIVE_ERRORS = 5
    BACKOFF_MULTIPLIER = 2
    MAX_BACKOFF_SECONDS = 60
    
    def __init__(
        self,
        pipeline_id: str,
        task_id: str,
        config: Dict[str, Any],
        source_handler: SourceHandler,
        sender_handler: SenderHandler,
        event_bus: Optional["EventBusInstanceRuntime"] = None,
        bus_service: Any = None,
        context: Optional["PipelineContext"] = None
    ):
        """
        Initialize the AgentPipeline.
        
        Args:
            pipeline_id: Unique identifier for this pipeline (sync_id)
            task_id: Full task identifier (agent_id:sync_id)
            config: Pipeline configuration
            source_handler: Handler for reading source data
            sender_handler: Handler for sending data to Fusion
            event_bus: Optional event bus for inter-component messaging
            bus_service: Optional service for bus management
            context: Optional shared context
        """
        super().__init__(pipeline_id, config, context)
        
        self.task_id = task_id
        self.source_handler = source_handler
        self.sender_handler = sender_handler
        self._bus = event_bus  # Private attribute for bus
        self._bus_service = bus_service
        
        # Role tracking (from heartbeat response)
        self.current_role: Optional[str] = None  # "leader" or "follower"
        
        # Task handles
        self._main_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._snapshot_task: Optional[asyncio.Task] = None
        self._message_sync_task: Optional[asyncio.Task] = None
        self._audit_task: Optional[asyncio.Task] = None
        self._sentinel_task: Optional[asyncio.Task] = None
        
        # Configuration
        self.heartbeat_interval_sec = config.get("heartbeat_interval_sec", 10)
        self.audit_interval_sec = config.get("audit_interval_sec", 600)
        self.sentinel_interval_sec = config.get("sentinel_interval_sec", 120)
        self.batch_size = config.get("batch_size", 100)
        

        
        # Statistics
        self.statistics: Dict[str, Any] = {
            "events_pushed": 0,
            "last_pushed_event_id": None
        }
        self._consecutive_errors = 0
    
    async def start(self) -> None:
        """
        Start the pipeline.
        
        This initializes states and starts the main control loop.
        """
        if self.is_running():
            logger.warning(f"Pipeline {self.id} is already running")
            return
        
        self._set_state(PipelineState.INITIALIZING, "Starting pipeline...")
        
        # Initialize handlers
        try:
            await self.source_handler.initialize()
            await self.sender_handler.initialize()
        except Exception as e:
            self._set_state(PipelineState.ERROR, f"Initialization failed: {e}")
            logger.error(f"Pipeline {self.id} initialization failed: {e}")
            return

        # Start main control loop - it will handle session creation
        self._main_task = asyncio.create_task(self._run_control_loop())
    
    async def stop(self) -> None:
        """
        Stop the pipeline gracefully.
        """
        if self.state == PipelineState.STOPPED:
            logger.debug(f"Pipeline {self.id} is already stopped")
            return
        
        self._set_state(PipelineState.STOPPED, "Stopping...")
        
        # Cancel all tasks
        tasks_to_cancel = [
            self._main_task,
            self._heartbeat_task,
            self._snapshot_task,
            self._message_sync_task,
            self._audit_task,
            self._sentinel_task
        ]
        
        for task in tasks_to_cancel:
            if task and not task.done():
                task.cancel()
        
        # Wait for tasks to finish
        active_tasks = [t for t in tasks_to_cancel if t]
        if active_tasks:
            await asyncio.gather(*active_tasks, return_exceptions=True)
        
        # Close session
        if self.session_id:
            try:
                await self.sender_handler.close_session(self.session_id)
            except Exception as e:
                logger.warning(f"Error closing session: {e}")
            await self.on_session_closed(self.session_id)
        
        # Close handlers
        try:
            await self.source_handler.close()
            await self.sender_handler.close()
        except Exception as e:
            logger.warning(f"Error closing handlers: {e}")

        self._set_state(PipelineState.STOPPED, "Stopped")
    
    async def on_session_created(self, session_id: str, **kwargs) -> None:
        """Handle session creation."""
        self.session_id = session_id
        self.current_role = kwargs.get("role", "follower")
        
        logger.info(f"Pipeline {self.id}: Session {session_id} created, role={self.current_role}")
        
        # Start heartbeat loop
        self._heartbeat_task = asyncio.create_task(self._run_heartbeat_loop())
    
    async def on_session_closed(self, session_id: str) -> None:
        """Handle session closure."""
        logger.info(f"Pipeline {self.id}: Session {session_id} closed")
        self.session_id = None
        self.current_role = None
    
    async def _run_control_loop(self) -> None:
        """
        Main control loop that manages pipeline state transitions.
        
        State machine:
        1. Wait for LEADER role
        2. Run snapshot sync (SNAPSHOT_PHASE)
        3. Start message sync (MESSAGE_PHASE)
        4. Handle role changes
        """
        self._set_state(PipelineState.RUNNING, "Waiting for role assignment...")
        
        # In V2, we keep trying unless explicitly stopped
        while self.state != PipelineState.STOPPED:
            try:
                # Wait until we have a session (in case of disconnection)
                if not self.session_id:
                    self._set_state(PipelineState.RUNNING | PipelineState.RECONNECTING, "Attempting to create session...")
                    try:
                        session_id, metadata = await self.sender_handler.create_session(
                            task_id=self.task_id,
                            source_type=self.source_handler.schema_name,
                            session_timeout_seconds=self.session_timeout_seconds
                        )
                        await self.on_session_created(session_id, **metadata)
                    except Exception as e:
                        raise RuntimeError(f"Session creation failed: {e}")

                # Wait until we have a role
                if not self.current_role:
                    await asyncio.sleep(self.ROLE_CHECK_INTERVAL)
                    continue
                
                if self.current_role == "leader":
                    await self._run_leader_sequence()
                    # If leader sequence finishes (e.g. source exhausted), 
                    # wait a bit before checking again to avoid busy loop
                    await asyncio.sleep(self.CONTROL_LOOP_INTERVAL)
                else:
                    # Follower: just wait and maintain heartbeat
                    self._set_state(PipelineState.PAUSED, "Follower mode - standby")
                    await asyncio.sleep(self.FOLLOWER_STANDBY_INTERVAL)
                
                # Reset error counter on successful iteration or state achievement
                if self._consecutive_errors > 0:
                    logger.info(f"Pipeline {self.id} recovered after {self._consecutive_errors} errors")
                    self._consecutive_errors = 0
                
            except asyncio.CancelledError:
                logger.info(f"Pipeline {self.id} control loop cancelled")
                break
            except Exception as e:
                self._consecutive_errors += 1
                
                # Exponential backoff
                backoff = min(
                    self.ERROR_RETRY_INTERVAL * (self.BACKOFF_MULTIPLIER ** (self._consecutive_errors - 1)),
                    self.MAX_BACKOFF_SECONDS
                )
                
                self._set_state(PipelineState.ERROR | PipelineState.RECONNECTING, 
                                f"Error (retry {self._consecutive_errors}, backoff {backoff}s): {e}")
                
                logger.error(f"Pipeline {self.id} control loop error: {e}. Retrying in {backoff}s...", exc_info=True)
                
                # If session failed, ensure it's cleared so we try to recreate it
                if self.session_id:
                    try:
                        # Clear tasks that might be hanging
                        for task in [self._audit_task, self._sentinel_task, self._message_sync_task, self._snapshot_task]:
                            if task and not task.done():
                                task.cancel()
                        await self.on_session_closed(self.session_id)
                    except:
                        pass

                await asyncio.sleep(backoff)

    
    async def _run_leader_sequence(self) -> None:
        """
        Run the leader sequence: Snapshot -> Message Sync + Audit/Sentinel loops.
        """
        # Phase 1: Snapshot sync
        self._set_state(PipelineState.RUNNING | PipelineState.SNAPSHOT_PHASE, "Starting snapshot sync...")
        
        try:
            await self._run_snapshot_sync()
        except Exception as e:
            self._set_state(PipelineState.ERROR, f"Snapshot sync failed: {e}")
            return
        
        # Phase 2: Message sync + background tasks
        self._set_state(PipelineState.RUNNING | PipelineState.MESSAGE_PHASE, "Starting message sync...")
        
        # Start audit and sentinel loops
        if self.audit_interval_sec > 0:
            self._audit_task = asyncio.create_task(self._run_audit_loop())
        if self.sentinel_interval_sec > 0:
            self._sentinel_task = asyncio.create_task(self._run_sentinel_loop())
        
        # Run message sync (this blocks until we lose leader role)
        try:
            await self._run_message_sync()
        except asyncio.CancelledError:
            pass
    
    async def _run_heartbeat_loop(self) -> None:
        """Maintain session through periodic heartbeats."""
        while self.session_id:
            try:
                response = await self.sender_handler.send_heartbeat(self.session_id)
                
                new_role = response.get("role", self.current_role)
                if new_role != self.current_role:
                    await self._handle_role_change(new_role)
                
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")
                # Don't fail the pipeline on heartbeat errors
            
            await asyncio.sleep(self.heartbeat_interval_sec)
    
    async def _handle_role_change(self, new_role: str) -> None:
        """Handle role change from heartbeat response."""
        old_role = self.current_role
        self.current_role = new_role
        
        logger.info(f"Pipeline {self.id}: Role changed {old_role} -> {new_role}")
        
        if new_role == "follower" and old_role == "leader":
            # Lost leadership - cancel leader tasks
            for task in [self._audit_task, self._sentinel_task, self._message_sync_task]:
                if task and not task.done():
                    task.cancel()
    
    async def _aiter_sync(self, sync_iter: Iterator[Any], queue_size: int = 1000):
        """
        Safely and efficiently wrap a synchronous iterator into an async generator.
        
        This implementation runs the synchronous iterator in a dedicated background
        thread and communicates items back via an asyncio.Queue, avoiding the overhead
        of creating a new thread/Future for every single item.
        """
        queue = asyncio.Queue(maxsize=queue_size)
        loop = asyncio.get_event_loop()
        stop_event = threading.Event()
        
        def _producer():
            try:
                for item in sync_iter:
                    if stop_event.is_set():
                        break
                    # Blocking put via run_coroutine_threadsafe to respect backpressure
                    future = asyncio.run_coroutine_threadsafe(queue.put(item), loop)
                    future.result() # Wait for the queue to have space
            except Exception as e:
                asyncio.run_coroutine_threadsafe(queue.put(e), loop)
            finally:
                asyncio.run_coroutine_threadsafe(queue.put(StopAsyncIteration), loop)

        # Start producer thread
        thread = threading.Thread(target=_producer, name=f"PipelineSource-Producer-{self.id}", daemon=True)
        thread.start()

        try:
            while True:
                item = await queue.get()
                if item is StopAsyncIteration:
                    break
                if isinstance(item, Exception):
                    raise item
                yield item
                queue.task_done()
        finally:
            stop_event.set()
            # We don't block on thread join here to keep the async side responsive,
            # but the thread will exit shortly after seeing the stop_event or finding the queue closed.

    async def _run_snapshot_sync(self) -> None:
        """Execute snapshot synchronization."""
        logger.info(f"Pipeline {self.id}: Starting snapshot sync")
        
        # Get snapshot iterator from source
        snapshot_iter = self.source_handler.get_snapshot_iterator()
        
        batch = []
        # Support both sync and async iterators
        if not hasattr(snapshot_iter, "__aiter__"):
            snapshot_iter = self._aiter_sync(snapshot_iter)

        async for event in snapshot_iter:
            batch.append(event)
            if len(batch) >= self.batch_size:
                success, _ = await self.sender_handler.send_batch(
                    self.session_id, batch, {"phase": "snapshot"}
                )
                if not success: 
                    raise RuntimeError("Snapshot batch send failed")
                self.statistics["events_pushed"] += len(batch)
                batch = []
        
        # Send remaining events
        if batch:
            success, _ = await self.sender_handler.send_batch(
                self.session_id, batch, {"phase": "snapshot", "is_final": True}
            )
            if not success:
                raise RuntimeError("Final snapshot batch send failed")
            self.statistics["events_pushed"] += len(batch)
        
        logger.info(f"Pipeline {self.id}: Snapshot sync complete")

    async def _run_message_sync(self) -> None:
        """Execute realtime message synchronization."""
        logger.info(f"Pipeline {self.id}: Starting message sync")
        
        if self._bus:
            await self._run_bus_message_sync()
        else:
            await self._run_driver_message_sync()

    async def _run_bus_message_sync(self) -> None:
        """Execute message sync using EventBus."""
        logger.info(f"Pipeline {self.id}: Reading from EventBus {self._bus.id}")
        
        try:
            while self.is_running():
                # Get events from bus
                events = await self._bus.internal_bus.get_events_for(
                    self.id, batch_size=self.batch_size, timeout=1.0
                )
                
                if not events:
                    continue
                
                # Send to Fusion
                success, _ = await self.sender_handler.send_batch(
                    self.session_id, events, {"phase": "realtime"}
                )
                
                if success:
                    self.statistics["events_pushed"] += len(events)
                    # Commit to bus
                    if self._bus_service:
                        await self._bus_service.commit_and_handle_split(
                            self._bus.id, 
                            self.id, 
                            len(events), 
                            events[-1].index,
                            self.config.get("fields_mapping", [])
                        )
                    else:
                        await self._bus.internal_bus.commit(self.id, len(events), events[-1].index)
                else:
                    logger.warning(f"Pipeline {self.id}: Failed to send bus events")
                    await asyncio.sleep(1.0) # Wait before retry
                    
        except asyncio.CancelledError:
            logger.info(f"Bus message sync for {self.id} cancelled")
            raise
        except Exception as e:
            logger.error(f"Pipeline {self.id} bus sync error: {e}", exc_info=True)
            raise

    async def _run_driver_message_sync(self) -> None:
        """Execute message sync directly from driver."""
        # Pass a stop event if possible for better cleanup
        stop_event = threading.Event()
        msg_iter = self.source_handler.get_message_iterator(
            start_position=-1, 
            stop_event=stop_event
        )
        
        try:
            batch = []
            if not hasattr(msg_iter, "__aiter__"):
                msg_iter = self._aiter_sync(msg_iter)

            async for event in msg_iter:
                if not self.is_running() and not (self.state & PipelineState.RECONNECTING):
                    break
                    
                batch.append(event)
                if len(batch) >= self.batch_size:
                    success, _ = await self.sender_handler.send_batch(
                        self.session_id, batch, {"phase": "realtime"}
                    )
                    if success:
                        self.statistics["events_pushed"] += len(batch)
                    batch = []
        except asyncio.CancelledError:
            logger.info(f"Driver message sync for {self.id} cancelled")
            raise
        finally:
            # Send remaining events in batch
            if batch and self.session_id:
                try:
                    success, _ = await self.sender_handler.send_batch(
                        self.session_id, batch, {"phase": "realtime", "is_final": True}
                    )
                    if success:
                        self.statistics["events_pushed"] += len(batch)
                except Exception as e:
                    logger.warning(f"Failed to push final message batch: {e}")
            
            # Signal stop to the underlying sync iterator
            stop_event.set()

    async def _run_audit_loop(self) -> None:
        """Periodically run audit sync."""
        while self.current_role == "leader" and self.is_running():
            try:
                await asyncio.sleep(self.audit_interval_sec)
                
                if self.current_role != "leader" or not self.is_running():
                    break
                
                await self._run_audit_sync()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Audit loop error: {e}")
                await asyncio.sleep(10) # Wait before retry
    
    async def _run_audit_sync(self) -> None:
        """Execute audit synchronization."""
        logger.debug(f"Pipeline {self.id}: Running audit sync")
        
        old_state = self.state
        self._set_state(self.state | PipelineState.AUDIT_PHASE)
        
        try:
            # Signal audit start
            await self.sender_handler.send_batch(
                self.session_id, [], {"phase": "audit", "is_start": True}
            )
            
            audit_iter = self.source_handler.get_audit_iterator()
            if not hasattr(audit_iter, "__aiter__"):
                audit_iter = self._aiter_sync(audit_iter)
            
            batch = []
            async for event in audit_iter:
                batch.append(event)
                
                if len(batch) >= self.batch_size:
                    await self.sender_handler.send_batch(
                        self.session_id, batch, {"phase": "audit"}
                    )
                    batch = []
            
            if batch:
                await self.sender_handler.send_batch(
                    self.session_id, batch, {"phase": "audit"}
                )
        finally:
            # Always send audit end signal to ensure Fusion calls handle_audit_end()
            if self.session_id:
                try:
                    await self.sender_handler.send_batch(
                        self.session_id, [], {"phase": "audit", "is_final": True}
                    )
                except Exception as e:
                    logger.warning(f"Failed to send audit end signal: {e}")
            # Clear audit phase flag
            self._set_state(old_state & ~PipelineState.AUDIT_PHASE)
    
    async def _run_sentinel_loop(self) -> None:
        """Periodically run sentinel checks."""
        while self.current_role == "leader":
            await asyncio.sleep(self.sentinel_interval_sec)
            
            if self.current_role != "leader":
                break
            
            try:
                await self._run_sentinel_check()
            except Exception as e:
                logger.error(f"Sentinel check error: {e}")
    
    async def _run_sentinel_check(self) -> None:
        """Execute sentinel check."""
        logger.debug(f"Pipeline {self.id}: Running sentinel check")
        
        # Get task batch from Fusion (would come from heartbeat response)
        # For now, this is a placeholder
        task_batch = {}
        
        if task_batch:
            result = self.source_handler.perform_sentinel_check(task_batch)
            if result:
                await self.sender_handler.send_batch(
                    self.session_id, [result], {"phase": "sentinel"}
                )

    async def trigger_audit(self) -> None:

        """Manually trigger an audit cycle."""
        if self.current_role == "leader":
            asyncio.create_task(self._run_audit_sync())
        else:
            logger.warning(f"Pipeline {self.id}: Cannot trigger audit, not a leader")

    async def trigger_sentinel(self) -> None:
        """Manually trigger a sentinel check."""
        if self.current_role == "leader":
            asyncio.create_task(self._run_sentinel_check())
        else:
            logger.warning(f"Pipeline {self.id}: Cannot trigger sentinel, not a leader")

    @property
    def bus(self) -> Optional["EventBusInstanceRuntime"]:
        """Legacy access to event bus."""
        return self._bus

    def get_dto(self) -> SyncInstanceDTO:
        """Get pipeline data transfer object compatible with SyncInstanceDTO."""
        # Map PipelineState to SyncState
        
        state = SyncState.STOPPED
        if self.state & PipelineState.SNAPSHOT_PHASE:
            state |= SyncState.SNAPSHOT_SYNC
        elif self.state & PipelineState.MESSAGE_PHASE:
            state |= SyncState.MESSAGE_SYNC
        elif self.state & PipelineState.AUDIT_PHASE:
            state |= SyncState.AUDIT_SYNC
        
        if self.state & PipelineState.CONF_OUTDATED:
            state |= SyncState.RUNNING_CONF_OUTDATE
        
        if self.state & PipelineState.ERROR:
            state |= SyncState.ERROR
        
        if self.state & PipelineState.RECONNECTING:
            state |= SyncState.RECONNECTING

        # If it's running but no specific phase is set, mark it as starting or broadly running
        if self.state & PipelineState.RUNNING and state == SyncState.STOPPED:
            state = SyncState.STARTING

        return SyncInstanceDTO(
            id=self.id,
            state=state,
            info=self.info or "",
            statistics=self.statistics.copy(),
            bus_id=self._bus.id if self._bus else None,
            task_id=self.task_id,
            current_role=self.current_role,
        )

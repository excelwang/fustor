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
from fustor_core.exceptions import SessionObsoletedError

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
        
        # Timing and backoff configurations
        self.control_loop_interval = config.get("control_loop_interval", 1.0)
        self.follower_standby_interval = config.get("follower_standby_interval", 1.0)
        self.role_check_interval = config.get("role_check_interval", 1.0)
        self.error_retry_interval = config.get("error_retry_interval", 5.0)
        self.max_consecutive_errors = config.get("max_consecutive_errors", 5)
        self.backoff_multiplier = config.get("backoff_multiplier", 2)
        self.max_backoff_seconds = config.get("max_backoff_seconds", 60)
        

        
        # Statistics
        self.statistics: Dict[str, Any] = {
            "events_pushed": 0,
            "last_pushed_event_id": None
        }
        self._consecutive_errors = 0
        self._last_heartbeat_at = 0.0  # Time of last successful role update (monotonic)
    
    def _update_role_from_response(self, response: Dict[str, Any]) -> None:
        """Update role and heartbeat timer based on server response."""
        new_role = response.get("role")
        if new_role:
            # This is a bit tricky since it's async, but role change handled elsewhere
            # We use a synchronous partial update here, role change logic remains in specialized handlers
            asyncio.create_task(self._handle_role_change(new_role))
        
        self._last_heartbeat_at = asyncio.get_event_loop().time()
    
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
        if self.has_active_session():
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
        
        # Use suggested heartbeat interval from server if available
        suggested_interval = kwargs.get("suggested_heartbeat_interval_seconds")
        if suggested_interval:
            self.heartbeat_interval_sec = suggested_interval
            logger.info(f"Pipeline {self.id}: Using server-suggested heartbeat interval: {suggested_interval}s")
        
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
                if not self.has_active_session():
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
                    await asyncio.sleep(self.role_check_interval)
                    continue
                
                if self.current_role == "leader":
                    await self._run_leader_sequence()
                    # If leader sequence finishes (e.g. source exhausted), 
                    # wait a bit before checking again to avoid busy loop
                    if self.has_active_session():
                        await asyncio.sleep(self.control_loop_interval)
                else:
                    # Follower: just wait and maintain heartbeat
                    self._set_state(PipelineState.PAUSED, "Follower mode - standby")
                    await asyncio.sleep(self.follower_standby_interval)
                
                # Reset error counter on successful iteration or state achievement
                if self._consecutive_errors > 0:
                    logger.info(f"Pipeline {self.id} recovered after {self._consecutive_errors} errors")
                    self._consecutive_errors = 0
                
            except asyncio.CancelledError:
                logger.info(f"Pipeline {self.id} control loop cancelled")
                break
            except SessionObsoletedError as e:
                logger.warning(f"Pipeline {self.id} session is obsolete: {e}. Reconnecting immediately.")
                # Clear session so we recreate it in the next iteration
                if self.has_active_session():
                    await self._cleanup_leader_tasks()
                    await self.on_session_closed(self.session_id)
                
                # No backoff for obsolete session, just restart the loop
                continue

            except Exception as e:
                self._consecutive_errors += 1
                
                # Exponential backoff
                backoff = min(
                    self.error_retry_interval * (self.backoff_multiplier ** (self._consecutive_errors - 1)),
                    self.max_backoff_seconds
                )
                
                self._set_state(PipelineState.ERROR | PipelineState.RECONNECTING, 
                                f"Error (retry {self._consecutive_errors}, backoff {backoff}s): {e}")
                
                logger.error(f"Pipeline {self.id} control loop error: {e}. Retrying in {backoff}s...", exc_info=True)
                
                # If session failed, ensure it's cleared so we try to recreate it
                if self.has_active_session():
                    try:
                        await self._cleanup_leader_tasks()
                        await self.on_session_closed(self.session_id)
                    except Exception as e2:
                        logger.debug(f"Error during cleanup after session loss: {e2}")

                await asyncio.sleep(backoff)

    
    async def _run_leader_sequence(self) -> None:
        """
        Run the leader sequence: Snapshot -> Message Sync + Audit/Sentinel loops.
        """
        # Phase 1: Snapshot sync
        self._set_state(PipelineState.RUNNING | PipelineState.SNAPSHOT_PHASE, "Starting snapshot sync...")
        
        try:
            self._snapshot_task = asyncio.current_task()
            await self._run_snapshot_sync()
        except SessionObsoletedError:
            raise
        except Exception as e:
            self._set_state(PipelineState.ERROR, f"Snapshot sync failed: {e}")
            return
        finally:
            self._snapshot_task = None
        
        # Phase 2: Message sync + background tasks
        self._set_state(PipelineState.RUNNING | PipelineState.MESSAGE_PHASE, "Starting message sync...")
        
        # Start audit and sentinel loops
        if self.audit_interval_sec > 0:
            self._audit_task = asyncio.create_task(self._run_audit_loop())
        if self.sentinel_interval_sec > 0:
            self._sentinel_task = asyncio.create_task(self._run_sentinel_loop())
        
        # Run message sync (this blocks until we lose leader role)
        try:
            # We wrap message sync in a task so it can be cancelled by role change
            self._message_sync_task = asyncio.create_task(self._run_message_sync())
            await self._message_sync_task
        except asyncio.CancelledError:
            if self._message_sync_task and not self._message_sync_task.done():
                self._message_sync_task.cancel()
                try:
                    await self._message_sync_task
                except asyncio.CancelledError:
                    pass
            logger.info(f"Pipeline {self.id}: Message sync task cancelled")
        finally:
            self._message_sync_task = None
    
    async def _run_heartbeat_loop(self) -> None:
        """Maintain session through periodic heartbeats."""
        while self.has_active_session():
            try:
                loop = asyncio.get_event_loop()
                now = loop.time()
                
                # Adaptive heartbeat: skip if we recently got a role update from data push
                elapsed = now - self._last_heartbeat_at
                if elapsed < self.heartbeat_interval_sec:
                    await asyncio.sleep(min(1.0, self.heartbeat_interval_sec - elapsed))
                    continue

                response = await self.sender_handler.send_heartbeat(self.session_id)
                self._update_role_from_response(response)
                
            except SessionObsoletedError as e:
                await self._handle_fatal_error(e)
                break
            except Exception as e:
                logger.warning(f"Pipeline {self.id} heartbeat error: {e}")
                # Don't kill the loop for transient heartbeat errors
                await asyncio.sleep(self.heartbeat_interval_sec)
    
    
    async def _cleanup_leader_tasks(self) -> None:
        """Cancel all active leader-specific tasks."""
        current = asyncio.current_task()
        for task in [self._audit_task, self._sentinel_task, self._message_sync_task, self._snapshot_task]:
            if task and task != current and not task.done():
                task.cancel()
                
    async def _handle_role_change(self, new_role: str) -> None:
        """Handle role change from heartbeat response."""
        old_role = self.current_role
        self.current_role = new_role
        
        logger.info(f"Pipeline {self.id}: Role changed {old_role} -> {new_role}")
        
        if (new_role == "follower" or new_role is None) and old_role == "leader":
            # Lost leadership - cancel leader tasks
            await self._cleanup_leader_tasks()

    async def _handle_fatal_error(self, error: Exception) -> None:
        """Handle fatal errors from background tasks."""
        if isinstance(error, SessionObsoletedError):
            logger.warning(f"Pipeline {self.id} detected obsolete session: {error}")
            # Reset session_id so the control loop knows to reconnect
            if self.has_active_session():
                await self._cleanup_leader_tasks()
                await self.on_session_closed(self.session_id)
            
            # Explicitly cancel message sync task to break leader sequence if active
            if self._message_sync_task and not self._message_sync_task.done():
                self._message_sync_task.cancel()
        else:
            logger.error(f"Pipeline {self.id} fatal background error: {error}", exc_info=True)
            self._set_state(PipelineState.ERROR, str(error))
    async def _aiter_sync(self, sync_iter: Iterator[Any], queue_size: int = 1000):
        """
        Safely and efficiently wrap a synchronous iterator into an async generator.
        """
        from .pipeline.worker import aiter_sync_wrapper
        async for item in aiter_sync_wrapper(sync_iter, self.id, queue_size):
            yield item

    async def _run_snapshot_sync(self) -> None:
        """Execute snapshot synchronization."""
        from .pipeline.phases import run_snapshot_sync
        await run_snapshot_sync(self)

    async def _run_message_sync(self) -> None:
        """Execute realtime message synchronization.
        
        This method implements the Master-style high-throughput pattern:
        1. If bus_service is available, dynamically create/reuse EventBus
        2. If position is lost, trigger supplemental snapshot
        3. Fall back to direct driver mode if no bus_service
        """
        logger.info(f"Pipeline {self.id}: Starting message sync")
        
        # Try to setup EventBus for high-throughput mode
        if self._bus_service and not self._bus:
            try:
                # Get the last committed position from Fusion
                start_position = self.statistics.get("last_pushed_event_id", 0) or 0
                
                self._bus, position_lost = await self._bus_service.get_or_create_bus_for_subscriber(
                    source_id=self.config.get("source"),
                    source_config=self.source_handler._config,
                    sync_id=self.id,
                    required_position=start_position,
                    fields_mapping=self.config.get("fields_mapping", [])
                )
                
                if position_lost:
                    logger.warning(f"Pipeline {self.id}: Position lost, triggering supplemental snapshot")
                    # Trigger supplemental snapshot in a separate task
                    asyncio.create_task(self._run_snapshot_sync())
                    
                logger.info(f"Pipeline {self.id}: EventBus initialized for high-throughput mode")
            except Exception as e:
                logger.warning(f"Pipeline {self.id}: Failed to setup EventBus ({e}), falling back to driver mode")
                self._bus = None
        
        # Choose sync mode based on bus availability
        if self._bus:
            await self._run_bus_message_sync()
        else:
            await self._run_driver_message_sync()

    async def _run_bus_message_sync(self) -> None:
        """Execute message sync reading from an internal event bus."""
        from .pipeline.phases import run_bus_message_sync
        await run_bus_message_sync(self)

    async def _run_driver_message_sync(self) -> None:
        """Execute message sync directly from driver."""
        from .pipeline.phases import run_driver_message_sync
        await run_driver_message_sync(self)

    async def _run_audit_loop(self) -> None:
        """Periodically run audit sync."""
        while self.is_running():
            # Check role at start of loop
            if self.current_role != "leader":
                await asyncio.sleep(self.role_check_interval)
                continue

            try:
                await asyncio.sleep(self.audit_interval_sec)
                
                # Double check status after sleep
                if not self.is_running() or self.current_role != "leader":
                    break
                
                # Skip if no session (might have just disconnected)
                if not self.has_active_session():
                    continue
                
                await self._run_audit_sync()
            except asyncio.CancelledError:
                break
            except SessionObsoletedError as e:
                await self._handle_fatal_error(e)
                break
            except Exception as e:
                logger.error(f"Audit loop error: {e}", exc_info=True)
                # For generic errors, wait longer to avoid flooding
                await asyncio.sleep(self.error_retry_interval * 10)
    
    async def _run_audit_sync(self) -> None:
        """Execute audit synchronization."""
        from .pipeline.phases import run_audit_sync
        await run_audit_sync(self)
    
    async def _run_sentinel_loop(self) -> None:
        """Periodically run sentinel checks."""
        while self.is_running():
            # Check role at start of loop
            if self.current_role != "leader":
                await asyncio.sleep(self.role_check_interval)
                continue

            try:
                await asyncio.sleep(self.sentinel_interval_sec)
                
                # Double check status after sleep
                if not self.is_running() or self.current_role != "leader":
                    break
                
                # Skip if no session
                if not self.has_active_session():
                    continue

                await self._run_sentinel_check()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Sentinel check error: {e}", exc_info=True)
                await asyncio.sleep(10)
    
    async def _run_sentinel_check(self) -> None:
        """Execute sentinel check."""
        from .pipeline.phases import run_sentinel_check
        await run_sentinel_check(self)

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

    async def remap_to_new_bus(
        self, 
        new_bus: "EventBusInstanceRuntime", 
        needed_position_lost: bool
    ) -> None:
        """
        Remap this pipeline to a new EventBus instance.
        
        Called when bus splitting occurs due to subscriber position divergence.
        This allows the pipeline to switch to a new bus without full restart.
        
        Args:
            new_bus: The new bus instance to use
            needed_position_lost: If True, pipeline should trigger resync
                                  because the required position is no longer
                                  available in the new bus
        """
        old_bus_id = self._bus.id if self._bus else None
        self._bus = new_bus
        
        if needed_position_lost:
            logger.warning(
                f"Pipeline {self.id}: Position lost during bus remap "
                f"(old_bus={old_bus_id}, new_bus={new_bus.id}). "
                f"Will trigger resync."
            )
            # Cancel current message sync task - it will be restarted
            if self._message_sync_task and not self._message_sync_task.done():
                self._message_sync_task.cancel()
            
            # Signal that we should trigger a resync via control loop
            # The RECONNECTING state will cause the control loop to 
            # recreate session and restart sync phases
            self._set_state(
                PipelineState.RUNNING | PipelineState.RECONNECTING, 
                f"Bus remap with position loss - triggering resync"
            )
        else:
            logger.info(
                f"Pipeline {self.id}: Remapped to new bus {new_bus.id} "
                f"(from {old_bus_id})"
            )

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

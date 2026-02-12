"""
AgentPipe orchestrates the flow: Source -> Sender
"""
import asyncio
import logging
from typing import Optional, Any, Dict, List, TYPE_CHECKING, Iterator


from fustor_core.pipe import Pipe, PipeState
from fustor_core.pipe.handler import SourceHandler
from fustor_core.pipe.sender import SenderHandler
from fustor_core.models.states import PipeInstanceDTO
from fustor_core.exceptions import SessionObsoletedError, FusionConnectionError
from fustor_core.pipe.mapper import EventMapper
from fustor_core.common.metrics import get_metrics

if TYPE_CHECKING:
    from fustor_core.pipe import PipeContext
    from fustor_agent.services.instances.bus import EventBusInstanceRuntime
    from fustor_core.models.states import PipeInstanceDTO

logger = logging.getLogger("fustor_agent")


from .pipe.lifecycle import PipeLifecycleMixin
from .pipe.leader import PipeLeaderMixin
from .pipe.command import PipeCommandMixin

class AgentPipe(Pipe, PipeLifecycleMixin, PipeLeaderMixin, PipeCommandMixin):
    """
    Agent-side Pipe implementation.
    
    Orchestrates the data flow from Source to Sender:
    1. Session lifecycle management with Fusion
    2. Snapshot sync phase
    3. Message sync phase (EventBus)
    4. Periodic audit and sentinel checks
    5. Heartbeat and role management (Leader/Follower)
    6. Command processing (e.g. On-Demand Scan)
    
    This is the core pipe implementation.
    """
    

    
    def __init__(
        self,
        pipe_id: str,
        task_id: str,
        config: Dict[str, Any],
        source_handler: SourceHandler,
        sender_handler: SenderHandler,
        event_bus: Optional["EventBusInstanceRuntime"] = None,
        bus_service: Any = None,
        context: Optional["PipeContext"] = None
    ):
        """
        Initialize the AgentPipe.
        
        Args:
            pipe_id: Unique identifier for this pipe
            task_id: Full task identifier (agent_id:pipe_id)
            config: Pipe configuration
            source_handler: Handler for reading source data
            sender_handler: Handler for sending data to Fusion
            event_bus: Optional event bus for inter-component messaging
            bus_service: Optional service for bus management
            context: Optional shared context
        """
        super().__init__(pipe_id, config, context)
        
        self.task_id = task_id
        self.source_handler = source_handler
        self.sender_handler = sender_handler
        self._bus = event_bus  # Private attribute for bus
        self._bus_service = bus_service
        
        # Verify architecture: realtime sync REQUIRES a bus
        if self._bus is None:
            logger.warning(f"Pipe {pipe_id}: Initialized without a bus. Realtime sync will not be possible.")
        
        # Field Mapper
        self._mapper = EventMapper(config.get("fields_mapping", []))
        
        # Role tracking (from heartbeat response)
        self.current_role: Optional[str] = None  # "leader" or "follower"
        
        # Task handles
        self._main_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._snapshot_task: Optional[asyncio.Task] = None
        self._message_sync_task: Optional[asyncio.Task] = None
        self._audit_task: Optional[asyncio.Task] = None
        self._sentinel_task: Optional[asyncio.Task] = None
        
        # Configuration (Timeouts and Heartbeats are managed by Fusion and updated in on_session_created)
        # Default to None: let Fusion server decide the timeout from its own config.
        # Agent-side config can override if explicitly set.
        self.session_timeout_seconds = config.get("session_timeout_seconds") or None
        self.heartbeat_interval_sec = 3.0  # Default until session is established
        
        self.audit_interval_sec = config.get("audit_interval_sec", 600)       # Seconds between audit cycles (0 to disable)
        self.sentinel_interval_sec = config.get("sentinel_interval_sec", 120) # Seconds between sentinel checks (0 to disable)
        self.batch_size = config.get("batch_size", 100)
        self.iterator_queue_size = config.get("iterator_queue_size", 1000)                     # Events per push

        # Timing and backoff configurations (Reliability)
        self.control_loop_interval = config.get("control_loop_interval", 1.0) # Seconds between control loop iterations
        self.follower_standby_interval = config.get("follower_standby_interval", 1.0) # Delay while in follower mode
        self.role_check_interval = config.get("role_check_interval", 1.0)      # How often to check for role changes
        self.error_retry_interval = config.get("error_retry_interval", 5.0)    # Initial backoff delay
        self.max_consecutive_errors = int(config.get("max_consecutive_errors", 5))  # Threshold for warning (ensure int)
        self.backoff_multiplier = config.get("backoff_multiplier", 2.0)        # Exponential backoff factor
        self.max_backoff_seconds = config.get("max_backoff_seconds", 60.0)     # Max backoff delay cap
        

        
        self.statistics: Dict[str, Any] = {
            "events_pushed": 0,
            "last_pushed_event_id": None
        }
        self.audit_context: Dict[str, Any] = {} # D-05: Incremental audit state (mtime cache)
        self._consecutive_errors = 0
        self._last_heartbeat_at = 0.0  # Time of last successful role update (monotonic)
        self.is_realtime_ready = False  # Track if realtime is officially active (post-prescan)
        self._initial_snapshot_done = False # Track if initial snapshot complete

    def map_batch(self, events: List[Any]) -> List[Any]:
        """Map events using the configured field mapper."""
        return self._mapper.map_batch(events)

    def _set_state(self, new_state: PipeState, info: Optional[str] = None):
        """Update pipestate only if it actually changed."""
        if self.state == new_state and self.info == info:
            return
        super()._set_state(new_state, info)

    # Error handling provided by PipeLifecycleMixin
    

    
    async def _update_role_from_response(self, response: Dict[str, Any]) -> None:
        """Update role and heartbeat timer based on server response."""
        new_role = response.get("role")
        if new_role and new_role != self.current_role:
            get_metrics().gauge("fustor.agent.role", 1 if new_role == "leader" else 0, {"pipe": self.id, "role": new_role})
            # Role changed - handle synchronously
            await self._handle_role_change(new_role)
        elif new_role:
             # Just update metrics for current role
             get_metrics().gauge("fustor.agent.role", 1 if new_role == "leader" else 0, {"pipe": self.id, "role": new_role})
        
        self._last_heartbeat_at = asyncio.get_event_loop().time()

    async def _handle_role_change(self, new_role: str) -> None:
        """Handle role transition logic."""
        logger.info(f"Pipe {self.id}: Role changed from {self.current_role} to {new_role}")
        self.current_role = new_role
        
        if new_role != "leader":
            # If we are demoted or starting as follower, ensure leader tasks are stopped
            await self._cancel_leader_tasks()
    
    async def start(self) -> None:
        """
        Start the pipe.
        
        This initializes states and starts the main control loop.
        """
        if self.is_running():
            logger.warning(f"Pipe {self.id} is already running")
            return
        
        self._set_state(PipeState.INITIALIZING, "Starting pipe...")
        
        # Initialize handlers
        try:
            await self.source_handler.initialize()
            await self.sender_handler.initialize()
        except Exception as e:
            self._set_state(PipeState.ERROR, f"Initialization failed: {e}")
            logger.error(f"Pipe {self.id} initialization failed: {e}")
            return

        # Start main control loop - it will handle session creation
        self._main_task = asyncio.create_task(self._run_control_loop())
    
    async def stop(self) -> None:
        """
        Stop the pipe gracefully.
        """
        if self.state == PipeState.STOPPED:
            logger.debug(f"Pipe {self.id} is already stopped")
            return
        
        self._set_state(PipeState.STOPPING, "Stopping...")
        
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
        
        # Wait for tasks to finish with a timeout to avoid hanging indefinitely
        active_tasks = [t for t in tasks_to_cancel if t]
        if active_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*active_tasks, return_exceptions=True),
                    timeout=2.0
                )
            except asyncio.TimeoutError:
                logger.warning(f"Pipe {self.id} stop: Timed out waiting for tasks to cancel")
        
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

        self._set_state(PipeState.STOPPED, "Stopped")
    
    async def on_session_created(self, session_id: str, **kwargs) -> None:
        """Handle session creation."""
        self.session_id = session_id
        self.current_role = kwargs.get("role", "follower")
        
        # Auto-adjust heartbeat based on session timeout from server
        # Heartbeat must be significantly shorter than session timeout to prevent expiry
        server_timeout = kwargs.get("session_timeout_seconds")
        if server_timeout:
            # Use timeout/3 as heartbeat interval (gives 2 chances before expiry)
            # This is now the ONLY way heartbeat interval is determined.
            auto_interval = max(0.1, server_timeout / 3.0)
            logger.debug(f"Pipe {self.id}: Calculated heartbeat interval: {auto_interval}s based on session_timeout={server_timeout}s")
            self.heartbeat_interval_sec = auto_interval
        
        # Update sync policy from server response (authoritative)
        server_audit = kwargs.get("audit_interval_sec")
        if server_audit is not None:
            old_audit = self.audit_interval_sec
            self.audit_interval_sec = float(server_audit)
            if old_audit != self.audit_interval_sec:
                logger.debug(f"Pipe {self.id}: Audit interval updated by server: {old_audit} -> {self.audit_interval_sec}")

        server_sentinel = kwargs.get("sentinel_interval_sec")
        if server_sentinel is not None:
             old_sentinel = self.sentinel_interval_sec
             self.sentinel_interval_sec = float(server_sentinel)
             if old_sentinel != self.sentinel_interval_sec:
                 logger.debug(f"Pipe {self.id}: Sentinel interval updated by server: {old_sentinel} -> {self.sentinel_interval_sec}")
            
        # Reset state flags for new session
        self._initial_snapshot_done = False
        self.is_realtime_ready = False
        
        # Recover bus if it was in a failed state from a previous error cycle
        if self._bus and getattr(self._bus, 'failed', False):
            try:
                logger.debug(f"Pipe {self.id}: Recovering EventBus from failed state for new session")
                self._bus.recover()
            except Exception as e:
                logger.warning(f"Pipe {self.id}: Bus recovery failed: {e}")
        
        self._heartbeat_task = None
        if self._heartbeat_task is None or self._heartbeat_task.done():
            self._heartbeat_task = asyncio.create_task(self._run_heartbeat_loop())
            logger.debug(f"Pipe {self.id}: Started heartbeat loop (Session: {session_id})")
        
        msg = f"Session {session_id} created"
        if self._consecutive_errors > 0:
            msg = f"Session {session_id} RECOVERED after {self._consecutive_errors} errors"
            self._consecutive_errors = 0
            
        logger.info(f"Pipe {self.id}: {msg}, role={self.current_role}")
        
    
    async def on_session_closed(self, session_id: str) -> None:
        """Handle session closure."""
        logger.info(f"Pipe {self.id}: Session {session_id} closed")
        self.session_id = None
        self.current_role = None
        self.is_realtime_ready = False
        self._initial_snapshot_done = False
        self.audit_context.clear() # Reset cache for fresh start on next session
        
        # Reset task handles
        self._heartbeat_task = None
        self._snapshot_task = None
        self._audit_task = None
        self._sentinel_task = None
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

                # Check if message sync is running and post-prescan (driver ready)
                can_realtime = self.is_realtime_ready
                logger.debug(f"Pipe {self.id}: Sending heartbeat. can_realtime={can_realtime} (is_realtime_ready={self.is_realtime_ready})")

                response = await self.sender_handler.send_heartbeat(self.session_id, can_realtime=can_realtime)
                await self._update_role_from_response(response)
                
                # Check for commands in response
                if response and "commands" in response:
                    await self._handle_commands(response["commands"])
                
                # Reset error counter on success
                if self._consecutive_errors > 0:
                    logger.debug(f"Pipe {self.id} heartbeat recovered after {self._consecutive_errors} errors")
                    self._consecutive_errors = 0
                
            except SessionObsoletedError as e:
                await self._handle_fatal_error(e)
                break
            except Exception as e:
                backoff = self._handle_loop_error(e, "heartbeat")
                # Don't kill the loop for transient heartbeat errors
                # But use backoff instead of just fixed interval if failing
                await asyncio.sleep(max(self.heartbeat_interval_sec, backoff))
    
    
    async def _cancel_all_tasks(self) -> None:
        """Cancel all pipe tasks (usually on session loss or stop)."""
        tasks = []
        current = asyncio.current_task()
        for task in [self._heartbeat_task, self._audit_task, self._sentinel_task, self._snapshot_task, self._message_sync_task]:
            if task and task != current and not task.done():
                task.cancel()
                tasks.append(task)
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        # Reset handles
        self._heartbeat_task = None
        self._audit_task = None
        self._sentinel_task = None
        self._snapshot_task = None
        self._message_sync_task = None
                
    async def _handle_fatal_error(self, error: Exception) -> None:
        """Handle fatal errors from background tasks."""
        if isinstance(error, asyncio.CancelledError):
            return
            
        logger.warning(f"Pipe {self.id} detected fatal error: {error}. Clearing session and reconnecting.")
        
        # Reset session so the control loop knows to reconnect
        if self.has_active_session():
            await self._cancel_all_tasks()
            await self.on_session_closed(self.session_id)
            
        if not isinstance(error, SessionObsoletedError):
            logger.error(f"Pipe {self.id} fatal background error: {error}", exc_info=True)
            # Increment error counter so control loop will backoff in next iteration
            self._consecutive_errors += 1
            # Keep RUNNING bit so control loop doesn't exit
            self._set_state(PipeState.RUNNING | PipeState.ERROR, str(error))

    async def _aiter_sync_phase(self, phase_iter: Iterator[Any], queue_size: Optional[int] = None, yield_timeout: Optional[float] = None):
        """
        Safely and efficiently wrap a synchronous iterator into an async generator.
        """
        from .pipe.worker import aiter_sync_phase_wrapper
        q_size = queue_size if queue_size is not None else self.iterator_queue_size
        async for item in aiter_sync_phase_wrapper(phase_iter, self.id, q_size, yield_timeout=yield_timeout):
            yield item

    async def _run_message_sync(self) -> None:
        """Execute realtime message sync phase."""
        logger.debug(f"Pipe {self.id}: Starting message sync phase (Unified Bus Mode)")
        self._set_state(self.state | PipeState.MESSAGE_SYNC)
        
        try:
            if not self._bus:
                raise RuntimeError(f"Pipe {self.id}: Cannot run message sync without an EventBus.")
            
            await self._run_bus_message_sync()
        except Exception as e:
            await self._handle_fatal_error(e)
        finally:
            self._set_state(self.state & ~PipeState.MESSAGE_SYNC)

    async def _run_bus_message_sync(self) -> None:
        """Execute message sync phase reading from an internal event bus."""
        from .pipe.phases import run_bus_message_sync
        await run_bus_message_sync(self)
    
    # Control loop provided by PipeLifecycleMixin

    
    # Leader/Snapshot/Audit/Sentinel logic provided by PipeLeaderMixin

    async def remap_to_new_bus(
        self, 
        new_bus: "EventBusInstanceRuntime", 
        needed_position_lost: bool
    ) -> None:
        """
        Remap this pipe to a new EventBus instance.
        
        Called when bus splitting occurs due to subscriber position divergence.
        This allows the pipe to switch to a new bus without full restart.
        
        Args:
            new_bus: The new bus instance to use
            needed_position_lost: If True, pipeshould trigger re-sync
                                  because the required position is no longer
                                  available in the new bus
        """
        old_bus_id = self._bus.id if self._bus else None
        self._bus = new_bus
        
        if needed_position_lost:
            logger.warning(
                f"Pipe {self.id}: Position lost during bus remap "
                f"(old_bus={old_bus_id}, new_bus={new_bus.id}). "
                f"Will trigger re-sync."
            )
            # Cancel current message sync phase task - it will be restarted
            if self._message_sync_task and not self._message_sync_task.done():
                self._message_sync_task.cancel()
            
            # Signal that we should trigger a resync via control loop
            # The RECONNECTING state will cause the control loop to 
            # recreate session and restart pipe phases
            self._set_state(
                 PipeState.RUNNING | PipeState.RECONNECTING, 
                 f"Bus remap with position loss - triggering re-sync"
             )

    # Command handling provided by PipeCommandMixin

    @property
    def bus(self) -> Optional["EventBusInstanceRuntime"]:
        """Legacy access to event bus."""
        return self._bus


    def get_dto(self) -> PipeInstanceDTO:
        """Get pipe data transfer object representation."""
        from fustor_core.models.states import PipeState as TaskState
        
        state = TaskState.STOPPED
        if self.state & PipeState.SNAPSHOT_SYNC:
            state |= TaskState.SNAPSHOT_SYNC
        elif self.state & PipeState.MESSAGE_SYNC:
            state |= TaskState.MESSAGE_SYNC
        elif self.state & PipeState.AUDIT_PHASE:
            state |= TaskState.AUDIT_PHASE
        
        if self.state & PipeState.CONF_OUTDATED:
            state |= TaskState.RUNNING_CONF_OUTDATE
        
        if self.state & PipeState.ERROR:
            state |= TaskState.ERROR
        
        if self.state & PipeState.RECONNECTING:
            state |= TaskState.RECONNECTING

        if self.state & PipeState.RUNNING and state == TaskState.STOPPED:
            state = TaskState.STARTING

        return PipeInstanceDTO(
            id=self.id,
            state=state,
            info=self.info or "",
            statistics=self.statistics.copy(),
            bus_id=self._bus.id if self._bus else None,
            task_id=self.task_id,
            current_role=self.current_role,
        )

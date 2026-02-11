"""
AgentPipe orchestrates the flow: Source -> Sender
"""
import asyncio
import logging
import threading
import time
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


class AgentPipe(Pipe):
    """
    Agent-side Pipe implementation.
    
    Orchestrates the data flow from Source to Sender:
    1. Session lifecycle management with Fusion
    2. Snapshot sync phase
    3. Realtime message sync phase
    4. Periodic audit and sentinel checks
    5. Heartbeat and role management (Leader/Follower)
    
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

    def _set_state(self, new_state: PipeState, info: Optional[str] = None):
        """Update pipestate only if it actually changed."""
        if self.state == new_state and self.info == info:
            return
        super()._set_state(new_state, info)

    def _calculate_backoff(self, consecutive_errors: int) -> float:
        """Standardized exponential backoff calculation."""
        if consecutive_errors <= 0:
            return 0.0
        backoff = min(
            self.error_retry_interval * (self.backoff_multiplier ** (consecutive_errors - 1)),
            self.max_backoff_seconds
        )
        return backoff


    def _handle_loop_error(self, error: Exception, loop_name: str) -> float:
        """Common error handling for loops: increment counter, alert if needed, return backoff.
        
        NOTE: This method intentionally does NOT set a terminal ERROR state.
        Even after max_consecutive_errors, the pipe stays in RUNNING | ERROR
        so the control loop can continue retrying with max backoff.
        This ensures self-healing when the upstream issue resolves.
        (Spec: 05-Stability.md §4 — "不崩溃")
        """
        self._consecutive_errors += 1
        backoff = self._calculate_backoff(self._consecutive_errors)
        
        if self._consecutive_errors >= self.max_consecutive_errors:
            logger.critical(
                f"Pipe {self.id} {loop_name} loop reached threshold of {self._consecutive_errors} "
                f"consecutive errors. Continuing with max backoff ({self.max_backoff_seconds}s)."
            )
            # Keep RUNNING so control loop does NOT exit — pipe can self-heal
            self._set_state(
                PipeState.RUNNING | PipeState.ERROR | PipeState.RECONNECTING,
                f"Max retries ({self.max_consecutive_errors}) exceeded: {error}"
            )
            # Cap at max backoff from here on
            backoff = self.max_backoff_seconds
        else:
            logger.error(f"Pipe {self.id} {loop_name} loop error: {error}. Retrying in {backoff}s...")
            
        return backoff
    

    
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
                logger.info(f"Pipe {self.id}: Recovering EventBus from failed state for new session")
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
        self._message_sync_task = None
        self._audit_task = None
        self._sentinel_task = None
    
    async def _run_control_loop(self) -> None:
        """Main control loop for session management and error recovery."""
        logger.info(f"Pipe {self.id}: Control loop started")
        
        self._set_state(PipeState.RUNNING, "Waiting for role assignment...")
        while self.is_running():
            # Debug state
            if self._consecutive_errors > 0:
                logger.debug(f"Pipe {self.id}: Control loop iteration starting (Role: {self.current_role}, Session: {self.session_id}, Errors: {self._consecutive_errors})")
            
            # If we have consecutive errors, we MUST backoff here to avoid tight loops
            if self._consecutive_errors > 0:
                backoff = self._calculate_backoff(self._consecutive_errors)
                logger.info(f"Pipe {self.id}: Backing off for {backoff:.2f}s due to previous errors")
                await asyncio.sleep(backoff)
                
                # Re-check state after sleep
                if not self.is_running():
                    break

            try:
                # D-03/D-04: Session Recovery & Leadership
                # Wait until we have a session (in case of disconnection)
                if not self.has_active_session():
                    logger.info(f"Pipe {self.id}: No active session. Reconnecting...")
                    self._set_state(PipeState.RUNNING | PipeState.RECONNECTING, "Attempting to create session...")
                    try:
                        logger.info(f"Pipe {self.id}: Creating session with task_id={self.task_id}, timeout={self.session_timeout_seconds}")
                        # Source URI extraction (Best effort, handling both Pydantic models and dicts)
                        config = self.source_handler.config
                        if isinstance(config, dict):
                            source_uri = config.get("uri") or config.get("driver_params", {}).get("uri")
                        else:
                            source_uri = getattr(config, "uri", None) or \
                                         getattr(config, "driver_params", {}).get("uri")

                        session_id, metadata = await self.sender_handler.create_session(
                            task_id=self.task_id,
                            source_type=self.source_handler.schema_name,
                            session_timeout_seconds=self.session_timeout_seconds,
                            source_uri=source_uri
                        )
                        await self.on_session_created(session_id, **metadata)
                        
                        # D-04: Restore Resume Capability
                        # Query Fusion for the last committed index to ensure we resume from where we left off
                        try:
                            committed_index = await self.sender_handler.get_latest_committed_index(session_id)
                            current_stats_index = self.statistics.get("last_pushed_event_id")
                            
                            # Only update if we don't have a newer local value (unlikely on fresh start, but safe)
                            if current_stats_index is None or committed_index > current_stats_index:
                                self.statistics["last_pushed_event_id"] = committed_index
                                logger.info(f"Pipe {self.id}: Resumed from committed index {committed_index}")
                        except Exception as e:
                            logger.warning(f"Pipe {self.id}: Failed to fetch committed index: {e}. Defaulting to 0/Latest.")

                    except FusionConnectionError:
                        # Re-raise FusionConnectionError to be handled by outer loop
                        raise
                    except RuntimeError as e:
                        logger.error(f"Pipe {self.id}: Detailed session creation error: {e}", exc_info=True)
                        raise
                    except Exception as e:
                        logger.error(f"Pipe {self.id}: Detailed session creation error: {e}", exc_info=True)
                        raise RuntimeError(f"Session creation failed: {e}")

                if self.current_role == "leader":
                    # Run leader-specific tasks (Snapshot, Audit, Sentinel)
                    await self._run_leader_sequence()
                    await asyncio.sleep(self.control_loop_interval)
                else:
                    # Not a leader - ensure leader-specific tasks are NOT running
                    await self._cancel_leader_tasks()
                    
                    if self.current_role == "follower":
                        # Follower: standby
                        # Ensure we are RUNNING but also mark as PAUSED (follower standby)
                        self._set_state((self.state | PipeState.RUNNING | PipeState.PAUSED) & ~PipeState.SNAPSHOT_SYNC, "Follower mode - standby")
                        await asyncio.sleep(self.follower_standby_interval)
                    else:
                        # No role assigned yet
                        self._set_state(PipeState.RUNNING, "Waiting for role assignment (retrying)...")
                        await asyncio.sleep(self.role_check_interval)
                        # Increment errors if we are stuck without a role for too long
                        self._consecutive_errors += 1
                        continue
                
                # Reset error counter on successful iteration or state achievement
                # ONLY if background tasks that are supposed to be running are actually alive
                bg_tasks_ok = True
                if self.session_id and self.current_role:
                    # Ensure Message Sync is running (for both Leader and Follower)
                    if self._message_sync_task is None or self._message_sync_task.done():
                        logger.info(f"Pipe {self.id}: Starting message sync phase (Role: {self.current_role})")
                        self._message_sync_task = asyncio.create_task(self._run_message_sync())

                    if self._message_sync_task and self._message_sync_task.done():
                        # Something crashed in the background but it didn't throw to us yet
                        logger.warning(f"Pipe {self.id}: Detected crashed message sync task")
                        self._consecutive_errors += 1
                        bg_tasks_ok = False
                
                if self._consecutive_errors > 0 and bg_tasks_ok:
                    logger.info(f"Pipe {self.id} recovered after {self._consecutive_errors} errors")
                    self._consecutive_errors = 0
                
            except asyncio.CancelledError:
                if (self.state & PipeState.STOPPING) or (self.state == PipeState.STOPPED):
                    logger.info(f"Pipe {self.id} control loop gracefully terminated")
                    break
                else:
                    logger.debug(f"Pipe {self.id} control loop received CancelledError, but not stopping. Continuing...")
                    continue
            except SessionObsoletedError as e:
                logger.warning(f"Pipe {self.id} session is obsolete: {e}. Reconnecting immediately.")
                # Clear session so we recreate it in the next iteration
                if self.has_active_session():
                    await self._cancel_all_tasks()
                    await self.on_session_closed(self.session_id)
                
                # Yield to prevent busy loop if reconnection fails repeatedly
                await asyncio.sleep(0.1)
                continue

            except RuntimeError as e:
                backoff = self._handle_loop_error(e, "control")
                
                # Update detailed status if not overridden by critical error
                if self._consecutive_errors < self.max_consecutive_errors:
                    self._set_state(PipeState.RUNNING | PipeState.ERROR | PipeState.RECONNECTING, 
                                    f"RuntimeError (retry {self._consecutive_errors}, backoff {backoff}s): {e}")
                await asyncio.sleep(backoff)

            except Exception as e:
                backoff = self._handle_loop_error(e, "control")
                
                # Update detailed status if not overridden by critical error
                if self._consecutive_errors < self.max_consecutive_errors:
                    self._set_state(PipeState.RUNNING | PipeState.ERROR | PipeState.RECONNECTING, 
                                    f"Error (retry {self._consecutive_errors}, backoff {backoff}s): {e}")
                
                # If session failed, ensure it's cleared so we try to recreate it
                if self.has_active_session():
                    try:
                        await self._cancel_all_tasks()
                        await self.on_session_closed(self.session_id)
                    except Exception as e2:
                        logger.debug(f"Error during cleanup after session loss: {e2}")

                await asyncio.sleep(backoff)

    
    async def _run_leader_sequence(self) -> None:
        """
        Run the leader-specific sequence: Snapshot and background Audit/Sentinel.
        """
        # Sync Phase 1: Initial Snapshot sync phase
        if not self._initial_snapshot_done and (self._snapshot_task is None or self._snapshot_task.done()):
            self._set_state(PipeState.RUNNING | PipeState.SNAPSHOT_SYNC, "Starting initial snapshot sync phase...")
            try:
                # Start Snapshot in a background task. 
                # We do NOT await it here to keep the control loop responsive to role changes.
                self._snapshot_task = asyncio.create_task(self._run_snapshot_sync())
                # Note: We don't await self._snapshot_task anymore to allow role transition detection
                # The next iteration will see it running and won't restart it.
                logger.info(f"Pipe {self.id}: Initial snapshot sync phase started in background")
            except Exception as e:
                self._set_state(PipeState.ERROR, f"Failed to start snapshot sync phase: {e}")
                return
        
        # Once snapshot is done, update the internal flag
        if self._snapshot_task and self._snapshot_task.done() and not self._initial_snapshot_done:
            try:
                # Check if it failed
                self._snapshot_task.result()
                self._initial_snapshot_done = True
                logger.info(f"Pipe {self.id}: Initial snapshot sync phase complete")
            except asyncio.CancelledError:
                logger.info(f"Pipe {self.id}: Snapshot sync phase task was cancelled")
            except Exception as e:
                logger.error(f"Pipe {self.id}: Snapshot sync phase failed: {e}")
                self._set_state(PipeState.ERROR, f"Snapshot sync phase failed: {e}")
            finally:
                self._snapshot_task = None
        
        # Double check session and running state before starting more background tasks
        if not self.has_active_session() or not self.is_running():
            return

        # Background Leader Tasks: Audit and Sentinel
        if self.audit_interval_sec > 0 and (self._audit_task is None or self._audit_task.done()):
            self._audit_task = asyncio.create_task(self._run_audit_loop())
            
        if self.sentinel_interval_sec > 0 and (self._sentinel_task is None or self._sentinel_task.done()):
            self._sentinel_task = asyncio.create_task(self._run_sentinel_loop())
    
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
                    logger.info(f"Pipe {self.id} heartbeat recovered after {self._consecutive_errors} errors")
                    self._consecutive_errors = 0
                
            except SessionObsoletedError as e:
                await self._handle_fatal_error(e)
                break
            except Exception as e:
                backoff = self._handle_loop_error(e, "heartbeat")
                # Don't kill the loop for transient heartbeat errors
                # But use backoff instead of just fixed interval if failing
                await asyncio.sleep(max(self.heartbeat_interval_sec, backoff))
    
    
    async def _cancel_leader_tasks(self) -> None:
        """Cancel leader-specific background tasks and clear handles."""
        tasks_to_cancel = []
        if self._snapshot_task and not self._snapshot_task.done():
            tasks_to_cancel.append(self._snapshot_task)
        if self._audit_task and not self._audit_task.done():
            tasks_to_cancel.append(self._audit_task)
        if self._sentinel_task and not self._sentinel_task.done():
            tasks_to_cancel.append(self._sentinel_task)
            
        if not tasks_to_cancel:
            # Ensure handles are cleared if they were finished
            self._snapshot_task = None
            self._audit_task = None
            self._sentinel_task = None
            return

        logger.info(f"Pipe {self.id}: Cancelling {len(tasks_to_cancel)} leader tasks")
        for task in tasks_to_cancel:
            task.cancel()

        try:
            # Wait briefly for tasks to acknowledge cancellation
            await asyncio.wait(tasks_to_cancel, timeout=0.1)
        except Exception:
            pass
            
        # Clear handles to prevent re-entering this block unnecessarily
        self._snapshot_task = None
        self._audit_task = None
        self._sentinel_task = None

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
                
    async def _handle_role_change(self, new_role: str) -> None:
        """Handle role change from heartbeat response."""
        old_role = self.current_role
        self.current_role = new_role
        
        logger.info(f"Pipe {self.id}: Role changed {old_role} -> {new_role}")
        
        if (new_role == "follower" or new_role is None) and old_role == "leader":
            # Lost leadership - cancel leader tasks
            await self._cancel_leader_tasks()
        
        if new_role == "leader" and old_role != "leader":
            # Gained leadership - clear audit context to ensure fresh scan
            logger.info(f"Pipe {self.id}: Promoted to LEADER. Clearing audit cache for fresh scan.")
            self.audit_context.clear()

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

    def map_batch(self, batch: List[Any]) -> List[Any]:
        """Apply field mapping to a batch of events."""
        if self._mapper.mappings:
            logger.info(f"Pipe {self.id}: Mapping batch of {len(batch)} events")
        return self._mapper.map_batch(batch)

    async def _run_snapshot_sync(self) -> None:
        """Execute snapshot sync phase."""
        logger.info(f"Pipe {self.id}: Snapshot sync phase starting")
        try:
            from .pipe.phases import run_snapshot_sync
            await run_snapshot_sync(self)
            
            # In Bus mode, we also signal completion so Fusion can transition its view state
            if self._bus:
                logger.info(f"Pipe {self.id}: Bus mode snapshot scan complete - signaling readiness to Fusion")
                await self.sender_handler.send_batch(
                    self.session_id, [], {"phase": "snapshot", "is_final": True}
                )
                
            self._initial_snapshot_done = True
            logger.info(f"Pipe {self.id}: Initial snapshot sync phase complete")
        except Exception as e:
            await self._handle_fatal_error(e)
        finally:
            self._set_state(self.state & ~PipeState.SNAPSHOT_SYNC)

    async def _run_message_sync(self) -> None:
        """Execute realtime message sync phase."""
        logger.info(f"Pipe {self.id}: Starting message sync phase (Unified Bus Mode)")
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



    async def _run_audit_loop(self) -> None:
        """Periodically run audit phase."""
        while self.is_running():
            # Check role at start of loop
            if self.current_role != "leader":
                await asyncio.sleep(self.role_check_interval)
                continue

            if self.audit_interval_sec <= 0:
                logger.debug(f"Pipe {self.id}: Audit disabled (interval={self.audit_interval_sec})")
                break

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
                backoff = self._handle_loop_error(e, "audit")
                await asyncio.sleep(backoff)
    
    async def _run_audit_sync(self) -> None:
        """Execute audit phase."""
        from .pipe.phases import run_audit_sync
        await run_audit_sync(self)
    
    async def _run_sentinel_loop(self) -> None:
        """Periodically run sentinel checks."""
        while self.is_running():
            # Check role at start of loop
            if self.current_role != "leader":
                await asyncio.sleep(self.role_check_interval)
                continue

            if self.sentinel_interval_sec <= 0:
                logger.debug(f"Pipe {self.id}: Sentinel disabled (interval={self.sentinel_interval_sec})")
                break

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
                backoff = self._handle_loop_error(e, "sentinel")
                await asyncio.sleep(backoff)
    
    async def _run_sentinel_check(self) -> None:
        """Execute sentinel check."""
        from .pipe.phases import run_sentinel_check
        await run_sentinel_check(self)

    async def trigger_audit(self) -> None:

        """Manually trigger an audit cycle."""
        if self.current_role == "leader":
            asyncio.create_task(self._run_audit_sync())
        else:
            logger.warning(f"Pipe {self.id}: Cannot trigger audit, not a leader")

    async def trigger_sentinel(self) -> None:
        """Manually trigger a sentinel check."""
        if self.current_role == "leader":
            asyncio.create_task(self._run_sentinel_check())
        else:
            logger.warning(f"Pipe {self.id}: Cannot trigger sentinel, not a leader")

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

    async def _handle_commands(self, commands: List[Dict[str, Any]]):
        """Process commands received from Fusion."""
        for cmd in commands:
            try:
                cmd_type = cmd.get("type")
                logger.info(f"Pipe {self.id}: Received command '{cmd_type}'")
                
                if cmd_type == "scan":
                    await self._handle_command_scan(cmd)
                else:
                    logger.warning(f"Pipe {self.id}: Unknown command type '{cmd_type}'")
            except Exception as e:
                logger.error(f"Pipe {self.id}: Error processing command {cmd}: {e}")

    async def _handle_command_scan(self, cmd: Dict[str, Any]):
        """Handle 'scan' command."""
        path = cmd.get("path")
        recursive = cmd.get("recursive", True)
        job_id = cmd.get("job_id")
        
        if not path:
            return

        logger.info(f"Pipe {self.id}: Executing realtime find (id={job_id}) for '{path}' (recursive={recursive})")
        
        # Check if source handler supports scan_path
        if hasattr(self.source_handler, "scan_path"):
            # Execute scan in background to not block heartbeat/control loop
            asyncio.create_task(self._run_on_demand_job(path, recursive, job_id))
        else:
            logger.warning(f"Pipe {self.id}: Source handler does not support 'scan_path' for realtime find")

    async def _run_on_demand_job(self, path: str, recursive: bool, job_id: Optional[str] = None):
        """Run the actual find task."""
        try:
            # We use the source handler to get events and push them immediately
            # This bypasses the normal message/snapshot loop but uses the same sender
            
            # Use iterator from source handler
            iterator = self.source_handler.scan_path(path, recursive=recursive) #TODO agent dont known what is scan
            
            # Push batch
            batch = []
            count = 0
            for event in iterator:
                batch.append(event)
                if len(batch) >= self.batch_size:
                    mapped_batch = self.map_batch(batch)
                    await self.sender_handler.send_batch(self.session_id, mapped_batch, {"phase": "on_demand_job"})
                    count += len(batch)
                    batch = []
            
            if batch:
                mapped_batch = self.map_batch(batch)
                await self.sender_handler.send_batch(self.session_id, mapped_batch, {"phase": "on_demand_job"})
                count += len(batch)
            
            # Notify Fusion that find is complete
            metadata = {"scan_path": path}# todo agent should send job_id
            if job_id:
                metadata["job_id"] = job_id
                
            await self.sender_handler.send_batch(self.session_id, [], {
                "phase": "job_complete",
                "metadata": metadata
            })
                
            logger.info(f"Pipe {self.id}: Realtime find complete (id={job_id}) for '{path}'. Sent {count} events.")
            
        except Exception as e:
            logger.error(f"Pipe {self.id}: On-demand scan failed: {e}")

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

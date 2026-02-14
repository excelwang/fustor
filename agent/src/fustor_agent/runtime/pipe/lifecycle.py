import asyncio
import logging
from typing import Optional, TYPE_CHECKING
from fustor_core.pipe import PipeState
from fustor_core.exceptions import SessionObsoletedError, FusionConnectionError
from fustor_agent.config.unified import get_outbound_ip

if TYPE_CHECKING:
    from ..agent_pipe import AgentPipe

logger = logging.getLogger("fustor_agent.pipe.lifecycle")

class PipeLifecycleMixin:
    """
    Mixin for AgentPipe lifecycle and control loop management.
    Expected to be mixed into AgentPipe class.
    """
    
    def _calculate_backoff(self: "AgentPipe", consecutive_errors: int) -> float:
        """Standardized exponential backoff calculation."""
        if consecutive_errors <= 0:
            return 0.0
        backoff = min(
            self.error_retry_interval * (self.backoff_multiplier ** (consecutive_errors - 1)),
            self.max_backoff_seconds
        )
        return backoff

    def _handle_control_error(self: "AgentPipe", error: Exception, loop_name: str) -> float:
        """Control plane error handling: affects session recovery speed."""
        self._control_errors += 1
        return self._common_error_handler(self._control_errors, error, loop_name, is_control=True)

    def _handle_data_error(self: "AgentPipe", error: Exception, loop_name: str) -> float:
        """Data plane error handling: does not affect heartbeat or session."""
        self._data_errors += 1
        return self._common_error_handler(self._data_errors, error, loop_name, is_control=False)

    def _common_error_handler(self: "AgentPipe", error_count: int, error: Exception, loop_name: str, is_control: bool) -> float:
        """Shared logic for error logging and backoff calculation."""
        backoff = self._calculate_backoff(error_count)
        
        if error_count >= self.max_consecutive_errors:
            logger.critical(
                f"Pipe {self.id} {loop_name} loop reached threshold of {error_count} "
                f"consecutive errors. Continuing with max backoff ({self.max_backoff_seconds}s)."
            )
            # Only control errors affect the pipe state for the control loop to see
            if is_control:
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

    async def _run_control_loop(self: "AgentPipe") -> None:
        """Main control loop for session management and error recovery."""
        logger.info(f"Pipe {self.id}: Control loop started")
        
        self._set_state(PipeState.RUNNING, "Waiting for role assignment...")
        while self.is_running():
            # Debug state
            if self._control_errors > 0:
                logger.debug(f"Pipe {self.id}: Control loop iteration starting (Role: {self.current_role}, Session: {self.session_id}, Errors: {self._control_errors})")
            
            # If we have consecutive errors, we MUST backoff here to avoid tight loops
            if self._control_errors > 0:
                backoff = self._calculate_backoff(self._control_errors)
                logger.debug(f"Pipe {self.id}: Backing off for {backoff:.2f}s due to previous errors")
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
                        # Resolve Agent ID: prefer configured id, fallback to dynamic IP
                        from fustor_agent.config.unified import agent_config
                        agent_id = agent_config.agent_id
                        
                        if not agent_id:
                            # Resolve dynamic Agent ID (IP) based on Sender endpoint
                            fusion_uri = getattr(self.sender_handler, "endpoint", "8.8.8.8")
                            if not isinstance(fusion_uri, str):
                                fusion_uri = "8.8.8.8"
                            agent_id = get_outbound_ip(fusion_uri)
                        
                        self.task_id = f"{agent_id}:{self.id}"
                        
                        logger.debug(f"Pipe {self.id}: Resolved identity task_id={self.task_id}")
                        
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
                                logger.debug(f"Pipe {self.id}: Resumed from committed index {committed_index}")
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

                # GAP-1: Control Loop only supervises, does not do data work directly
                self._supervise_data_tasks()
                
                # Sleep is now the primary pacer for the control loop
                await asyncio.sleep(self.control_loop_interval)
                
                # Reset error counter on successful iteration
                if self._control_errors > 0:
                    logger.info(f"Pipe {self.id} recovered after {self._control_errors} control errors")
                    self._control_errors = 0
                
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
                backoff = self._handle_control_error(e, "control")
                
                # Update detailed status if not overridden by critical error
                if self._control_errors < self.max_consecutive_errors:
                    self._set_state(PipeState.RUNNING | PipeState.ERROR | PipeState.RECONNECTING, 
                                    f"RuntimeError (retry {self._control_errors}, backoff {backoff}s): {e}")
                await asyncio.sleep(backoff)

            except Exception as e:
                backoff = self._handle_control_error(e, "control")
                
                # Update detailed status if not overridden by critical error
                if self._control_errors < self.max_consecutive_errors:
                    self._set_state(PipeState.RUNNING | PipeState.ERROR | PipeState.RECONNECTING, 
                                    f"Error (retry {self._control_errors}, backoff {backoff}s): {e}")
                
                # If session failed, ensure it's cleared so we try to recreate it
                if self.has_active_session():
                    try:
                        await self._cancel_all_tasks()
                        await self.on_session_closed(self.session_id)
                    except Exception as e2:
                        logger.debug(f"Error during cleanup after session loss: {e2}")

                await asyncio.sleep(backoff)

    def _supervise_data_tasks(self: "AgentPipe") -> None:
        """
        Monitor and manage data plane tasks based on current role.
        This is a synchronous supervisor that ensures tasks are running but does not block.
        GAP-1 decoupled logic.
        """
        if not self.has_active_session() or not self.is_running():
            return

        # 1. Message Sync (Realtime) - Always needs to be running or ready
        if self._message_sync_task is None or self._message_sync_task.done():
             # If it finished with exception, it should have been handled by _handle_fatal_error
             # We just restart it here.
             if self._message_sync_task and self._message_sync_task.done() and not self._message_sync_task.cancelled():
                 # Log if it crashed silently (though _handle_fatal_error should catch it)
                 exc = self._message_sync_task.exception()
                 if exc:
                     logger.warning(f"Pipe {self.id}: Restarting crashed message sync: {exc}")
             
             logger.debug(f"Pipe {self.id}: Starting message sync phase (Role: {self.current_role})")
             self._message_sync_task = asyncio.create_task(self._run_message_sync())
        
        # DEBUG LOGGING for snapshot startup
        if self.current_role == "leader" and not self._initial_snapshot_done:
             if self.is_realtime_ready:
                 if self._snapshot_task is None or self._snapshot_task.done():
                    logger.debug(f"Pipe {self.id}: SUPERVISOR starting snapshot task. State: {self.state}")
                 else:
                     pass # Task running
             else:
                 logger.debug(f"Pipe {self.id}: SUPERVISOR waiting for realtime ready before snapshot")
        elif self.current_role == "leader" and self._initial_snapshot_done:
             pass # Snapshot done
        else:
             pass # Not leader

        if self.current_role == "leader":
            # Leader Sequence: Prescan -> Message Sync -> Snapshot -> Audit/Sentinel
            
            # Check for initial snapshot need
            if not self._initial_snapshot_done:
                # Only start snapshot if message sync (which handles prescan) is running
                # and ideally 'ready' (prescan done).
                # Note: is_realtime_ready is set by message sync phase.
                if self.is_realtime_ready:
                    if self._snapshot_task is None or self._snapshot_task.done():
                        # Restart if failed or not started
                         self._snapshot_task = asyncio.create_task(self._run_snapshot_sync())
                         logger.debug(f"Pipe {self.id}: Initial snapshot sync phase started in background")
            
            # Check snapshot completion
            if self._snapshot_task and self._snapshot_task.done():
                # We don't await result here to avoid raising. 
                # Exceptions are handled in _run_snapshot_sync via _handle_fatal_error
                # We just check if it finished successfully (logic inside sets flag)
                # But we can clear the task handle
                self._snapshot_task = None

            # Maintenance tasks - only after snapshot
            if self._initial_snapshot_done:
                if self.audit_interval_sec > 0 and (self._audit_task is None or self._audit_task.done()):
                    self._audit_task = asyncio.create_task(self._run_audit_loop())
                
                if self.sentinel_interval_sec > 0 and (self._sentinel_task is None or self._sentinel_task.done()):
                    self._sentinel_task = asyncio.create_task(self._run_sentinel_loop())
        else:
             # Follower: Ensure leader tasks are NOT running
             # Fire and forget cancellation
             asyncio.create_task(self._cancel_leader_tasks())
             
             if self.current_role == "follower":
                 # Update state info for visibility (since we don't have the blocking wait logic anymore)
                 if self.state & PipeState.RUNNING and not (self.state & PipeState.PAUSED):
                      self._set_state((self.state | PipeState.PAUSED) & ~PipeState.SNAPSHOT_SYNC, "Follower mode - standby")

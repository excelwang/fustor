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

    def _handle_loop_error(self: "AgentPipe", error: Exception, loop_name: str) -> float:
        """Common error handling for loops: increment counter, alert if needed, return backoff."""
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

    async def _run_control_loop(self: "AgentPipe") -> None:
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

                if self.current_role == "leader":
                    # Run leader-specific tasks (Snapshot, Audit, Sentinel)
                    # NOTE: _run_leader_sequence comes from PipeLeaderMixin
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
                        continue
                
                # Reset error counter on successful iteration or state achievement
                # ONLY if background tasks that are supposed to be running are actually alive
                bg_tasks_ok = True
                if self.session_id and self.current_role:
                    # Ensure Message Sync is running (for both Leader and Follower)
                    if self._message_sync_task is None or self._message_sync_task.done():
                        logger.debug(f"Pipe {self.id}: Starting message sync phase (Role: {self.current_role})")
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

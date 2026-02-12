import asyncio
import logging
from typing import Optional, TYPE_CHECKING
from fustor_core.pipe import PipeState
from fustor_core.exceptions import SessionObsoletedError

if TYPE_CHECKING:
    from ..agent_pipe import AgentPipe

logger = logging.getLogger("fustor_agent.pipe.leader")

class PipeLeaderMixin:
    """
    Mixin for AgentPipe leader task orchestration.
    """

    async def _run_leader_sequence(self: "AgentPipe") -> None:
        """Run the leader-specific sequence: Snapshot and background Audit/Sentinel."""
        # Sync Phase 1: Initial Snapshot sync phase
        if not self._initial_snapshot_done and (self._snapshot_task is None or self._snapshot_task.done()):
            self._set_state(PipeState.RUNNING | PipeState.SNAPSHOT_SYNC, "Starting initial snapshot sync phase...")
            try:
                # Start Snapshot in a background task. 
                self._snapshot_task = asyncio.create_task(self._run_snapshot_sync())
                logger.debug(f"Pipe {self.id}: Initial snapshot sync phase started in background")
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
                logger.debug(f"Pipe {self.id}: Snapshot sync phase task was cancelled")
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

    async def _run_snapshot_sync(self: "AgentPipe") -> None:
        """Execute snapshot sync phase."""
        logger.debug(f"Pipe {self.id}: Snapshot sync phase starting")
        try:
            from .phases import run_snapshot_sync
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
            # Lifecycle mixin provides this
            await self._handle_fatal_error(e)
        finally:
            self._set_state(self.state & ~PipeState.SNAPSHOT_SYNC)

    async def _run_audit_loop(self: "AgentPipe") -> None:
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

    async def _run_audit_sync(self: "AgentPipe") -> None:
        """Execute audit phase."""
        from .phases import run_audit_sync
        await run_audit_sync(self)

    async def _run_sentinel_loop(self: "AgentPipe") -> None:
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

    async def _run_sentinel_check(self: "AgentPipe") -> None:
        """Execute sentinel check."""
        from .phases import run_sentinel_check
        await run_sentinel_check(self)

    async def trigger_audit(self: "AgentPipe") -> None:
        """Manually trigger an audit cycle."""
        if self.current_role == "leader":
            asyncio.create_task(self._run_audit_sync())
        else:
            logger.warning(f"Pipe {self.id}: Cannot trigger audit, not a leader")

    async def trigger_sentinel(self: "AgentPipe") -> None:
        """Manually trigger a sentinel check."""
        if self.current_role == "leader":
            asyncio.create_task(self._run_sentinel_check())
        else:
            logger.warning(f"Pipe {self.id}: Cannot trigger sentinel, not a leader")

    async def _cancel_leader_tasks(self: "AgentPipe") -> None:
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

        logger.debug(f"Pipe {self.id}: Cancelling {len(tasks_to_cancel)} leader tasks")
        for task in tasks_to_cancel:
            task.cancel()

        try:
            # Wait briefly for tasks to acknowledge cancellation
            await asyncio.wait(tasks_to_cancel, timeout=0.1)
        except Exception:
            pass
            
        # Clear handles
        self._snapshot_task = None
        self._audit_task = None
        self._sentinel_task = None

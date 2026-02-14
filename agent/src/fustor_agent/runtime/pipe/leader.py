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

    # _run_leader_sequence removed (Blocking logic deprecated)


    async def _run_snapshot_sync(self: "AgentPipe") -> None:
        """Execute snapshot sync phase."""
        logger.debug(f"Pipe {self.id}: Snapshot sync phase starting")
        # Set state bit
        self._set_state(self.state | PipeState.SNAPSHOT_SYNC, "Starting initial snapshot sync phase...")
        try:
            from .phases import run_snapshot_sync
            await run_snapshot_sync(self)
            
            # In Bus mode, we also signal completion so Fusion can transition its view state
            if self._bus:
                logger.info(f"Pipe {self.id}: Bus mode snapshot scan complete - signaling readiness to Fusion")
                await self.sender_handler.send_batch(
                    self.session_id, [], {"phase": "snapshot", "is_final": True}
                )
                
                await self.sender_handler.send_batch(
                    self.session_id, [], {"phase": "snapshot", "is_final": True}
                )
                
            logger.info(f"Pipe {self.id}: Initial snapshot sync phase complete")
            self._initial_snapshot_done = True
            
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
                backoff = self._handle_data_error(e, "audit")
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
                backoff = self._handle_data_error(e, "sentinel")
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
        except Exception as e:
            logger.debug(f"Pipe {self.id}: Ignore error while cancelling leader tasks: {e}")
            
        # Clear handles
        self._snapshot_task = None
        self._audit_task = None
        self._sentinel_task = None

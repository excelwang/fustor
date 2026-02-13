import asyncio
import logging
import os
import signal
from typing import Dict, List, Any, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ..agent_pipe import AgentPipe

logger = logging.getLogger("fustor_agent.pipe.command")

class PipeCommandMixin:
    """
    Mixin for AgentPipe command processing logic.
    Handles commands from Fusion (e.g., on-demand scans, reload, stop).
    """

    async def _handle_commands(self: "AgentPipe", commands: List[Dict[str, Any]]) -> None:
        """Process commands received from Fusion."""
        for cmd in commands:
            try:
                cmd_type = cmd.get("type")
                logger.info(f"Pipe {self.id}: Received command '{cmd_type}'")
                
                if cmd_type == "scan":
                    await self._handle_command_scan(cmd)
                elif cmd_type == "reload_config":
                    self._handle_command_reload()
                elif cmd_type == "stop_pipe":
                    await self._handle_command_stop_pipe(cmd)
                else:
                    logger.warning(f"Pipe {self.id}: Unknown command type '{cmd_type}'")
            except Exception as e:
                logger.error(f"Pipe {self.id}: Error processing command {cmd}: {e}")

    async def _handle_command_scan(self: "AgentPipe", cmd: Dict[str, Any]) -> None:
        """Handle 'scan' command."""
        path = cmd.get("path")
        recursive = cmd.get("recursive", True)
        job_id = cmd.get("job_id")
        
        if not path:
            return

        logger.info(f"Pipe {self.id}: Executing On-Demand scan (id={job_id}) for '{path}' (recursive={recursive})")
        
        # Check if source handler supports scan_path
        if hasattr(self.source_handler, "scan_path"):
            # Execute scan in background to not block heartbeat/control loop
            asyncio.create_task(self._run_on_demand_job(path, recursive, job_id))
        else:
            logger.warning(f"Pipe {self.id}: Source handler does not support 'scan_path' for On-Demand scan")

    async def _run_on_demand_job(self: "AgentPipe", path: str, recursive: bool, job_id: Optional[str] = None) -> None:
        """Run the actual find task."""
        try:
            # We use the source handler to get events and push them immediately
            # This bypasses the normal message/snapshot loop but uses the same sender
            
            # Use iterator from source handler
            iterator = self.source_handler.scan_path(path, recursive=recursive)
            
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
            
            # Notify Fusion that On-Demand scan is complete
            metadata: Dict[str, Any] = {"scan_path": path}
            if job_id:
                metadata["job_id"] = job_id
                
            await self.sender_handler.send_batch(self.session_id, [], {
                "phase": "job_complete",
                "metadata": metadata
            })
                
            logger.info(f"Pipe {self.id}: On-Demand scan completed (id={job_id}) for '{path}'. Sent {count} events.")
            
        except Exception as e:
            logger.error(f"Pipe {self.id}: On-demand scan failed: {e}")

    def _handle_command_reload(self: "AgentPipe") -> None:
        """
        Handle 'reload_config' command.
        
        Sends SIGHUP to the current process to trigger configuration hot-reload.
        This is the same mechanism used by local admin tools.
        """
        logger.info(f"Pipe {self.id}: Received remote reload command. Sending SIGHUP.")
        try:
            os.kill(os.getpid(), signal.SIGHUP)
        except Exception as e:
            logger.error(f"Pipe {self.id}: Failed to send SIGHUP for reload: {e}")

    async def _handle_command_stop_pipe(self: "AgentPipe", cmd: Dict[str, Any]) -> None:
        """
        Handle 'stop_pipe' command.
        
        Stops a specific pipe identified by pipe_id in the command.
        If pipe_id matches this pipe, stops self.
        """
        target_pipe_id = cmd.get("pipe_id")
        if not target_pipe_id:
            logger.warning(f"Pipe {self.id}: stop_pipe command missing 'pipe_id'")
            return

        if target_pipe_id == self.id:
            logger.info(f"Pipe {self.id}: Received remote stop command. Stopping.")
            asyncio.create_task(self.stop())
        else:
            logger.debug(f"Pipe {self.id}: stop_pipe command for '{target_pipe_id}' (not me, ignoring)")

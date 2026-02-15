import asyncio
import logging
import os
import signal
import shutil
from typing import Dict, List, Any, Optional

from fustor_agent_sdk.interfaces import CommandProcessorInterface, PipeInterface

logger = logging.getLogger("fustor_agent.mgmt")

class CommandProcessor(CommandProcessorInterface):
    """
    Management (L3) processor for AgentPipe.
    Handles commands from Fusion (e.g., on-demand scans, reload, stop).
    """

    async def initialize(self, pipe: PipeInterface) -> None:
        """Initialize the processor (placeholder for any setup)."""
        logger.debug(f"CommandProcessor initialized for Pipe {pipe.id}")

    async def process_commands(self, pipe: PipeInterface, commands: List[Dict[str, Any]]) -> None:
        """Process commands received from Fusion."""
        for cmd in commands:
            try:
                cmd_type = cmd.get("type")
                logger.debug(f"Pipe {pipe.id}: Received command '{cmd_type}'")
                
                if cmd_type == "scan":
                    await self._handle_command_scan(pipe, cmd)
                elif cmd_type == "reload_config":
                    self._handle_command_reload(pipe)
                elif cmd_type == "stop_pipe":
                    await self._handle_command_stop_pipe(pipe, cmd)
                elif cmd_type == "update_config":
                    self._handle_command_update_config(pipe, cmd)
                elif cmd_type == "report_config":
                    self._handle_command_report_config(pipe, cmd)
                elif cmd_type == "upgrade":
                    await self._handle_command_upgrade(pipe, cmd)
                else:
                    logger.warning(f"Pipe {pipe.id}: Unknown command type '{cmd_type}'")
            except Exception as e:
                logger.error(f"Pipe {pipe.id}: Error processing command {cmd}: {e}")

    async def _handle_command_scan(self, pipe: PipeInterface, cmd: Dict[str, Any]) -> None:
        """Handle 'scan' command."""
        path = cmd.get("path")
        recursive = cmd.get("recursive", True)
        job_id = cmd.get("job_id")
        
        if not path:
            return

        logger.info(f"Pipe {pipe.id}: Executing On-Demand scan (id={job_id}) for '{path}' (recursive={recursive})")
        
        # Check if source handler supports scan_path
        if hasattr(pipe.source_handler, "scan_path"):
            # Execute scan in background to not block heartbeat/control loop
            asyncio.create_task(self._run_on_demand_job(pipe, path, recursive, job_id))
        else:
            logger.warning(f"Pipe {pipe.id}: Source handler does not support 'scan_path' for On-Demand scan")

    async def _run_on_demand_job(self, pipe: PipeInterface, path: str, recursive: bool, job_id: Optional[str] = None) -> None:
        """Run the actual find task."""
        try:
            # Use iterator from source handler
            iterator = pipe.source_handler.scan_path(path, recursive=recursive)
            
            # Push batch
            batch = []
            count = 0
            for event in iterator:
                batch.append(event)
                if len(batch) >= pipe.batch_size:
                    mapped_batch = pipe.map_batch(batch)
                    await pipe.sender_handler.send_batch(pipe.session_id, mapped_batch, {"phase": "on_demand_job"})
                    count += len(batch)
                    batch = []
            
            if batch:
                mapped_batch = pipe.map_batch(batch)
                await pipe.sender_handler.send_batch(pipe.session_id, mapped_batch, {"phase": "on_demand_job"})
                count += len(batch)
            
            # Notify Fusion that On-Demand scan is complete
            metadata: Dict[str, Any] = {"scan_path": path}
            if job_id:
                metadata["job_id"] = job_id
                
            await pipe.sender_handler.send_batch(pipe.session_id, [], {
                "phase": "job_complete",
                "metadata": metadata
            })
                
            logger.info(f"Pipe {pipe.id}: On-Demand scan completed (id={job_id}) for '{path}'. Sent {count} events.")
            
        except Exception as e:
            logger.error(f"Pipe {pipe.id}: On-demand scan failed: {e}")

    def _handle_command_reload(self, pipe: PipeInterface) -> None:
        """Handle 'reload_config' command."""
        logger.info(f"Pipe {pipe.id}: Received remote reload command. Sending SIGHUP.")
        try:
            os.kill(os.getpid(), signal.SIGHUP)
        except Exception as e:
            logger.error(f"Pipe {pipe.id}: Failed to send SIGHUP for reload: {e}")

    async def _handle_command_stop_pipe(self, pipe: PipeInterface, cmd: Dict[str, Any]) -> None:
        """Handle 'stop_pipe' command."""
        target_agent_pipe_id = cmd.get("agent_pipe_id") or cmd.get("pipe_id")
        if not target_agent_pipe_id:
            logger.warning(f"Pipe {pipe.id}: stop_pipe command missing 'agent_pipe_id'")
            return

        if target_agent_pipe_id == pipe.id:
            logger.info(f"Pipe {pipe.id}: Received remote stop command. Stopping.")
            asyncio.create_task(pipe.stop())
        else:
            logger.debug(f"Pipe {pipe.id}: stop_pipe command for '{target_agent_pipe_id}' (not me, ignoring)")

    def _handle_command_update_config(self, pipe: PipeInterface, cmd: Dict[str, Any]) -> None:
        """Handle 'update_config' command."""
        config_yaml = cmd.get("config_yaml")
        filename = cmd.get("filename", "default.yaml")

        if not config_yaml:
            logger.warning(f"Pipe {pipe.id}: update_config command missing 'config_yaml'")
            return

        import fustor_agent.config.validator as validator
        from fustor_core.common import get_fustor_home_dir
        import yaml

        # Validate YAML syntax before writing
        try:
            config_dict = yaml.safe_load(config_yaml)
            
            # Semantic validation
            success, errors = validator.ConfigValidator().validate_config(config_dict)
            if not success:
                logger.error(f"Pipe {pipe.id}: Received semantically invalid config: {errors}")
                return
                
        except yaml.YAMLError as e:
            logger.error(f"Pipe {pipe.id}: Received invalid YAML syntax: {e}")
            return
        except Exception as e:
            logger.error(f"Pipe {pipe.id}: Unexpected error during config validation: {e}")
            return

        # Sanitize filename
        safe_name = os.path.basename(filename)
        if not safe_name.endswith((".yaml", ".yml")):
            safe_name += ".yaml"

        config_dir = get_fustor_home_dir() / "agent-config"
        target_path = config_dir / safe_name
        backup_path = config_dir / f"{safe_name}.bak"

        try:
            if target_path.exists():
                shutil.copy2(target_path, backup_path)
            target_path.write_text(config_yaml, encoding="utf-8")
            os.kill(os.getpid(), signal.SIGHUP)
            logger.info(f"Pipe {pipe.id}: Config updated and reload triggered")
        except Exception as e:
            logger.error(f"Pipe {pipe.id}: Failed to write config: {e}")
            if backup_path.exists():
                shutil.copy2(backup_path, target_path)

    def _handle_command_report_config(self, pipe: PipeInterface, cmd: Dict[str, Any]) -> None:
        """Handle 'report_config' command."""
        filename = cmd.get("filename", "default.yaml")
        from fustor_core.common import get_fustor_home_dir
        
        safe_name = os.path.basename(filename)
        if not safe_name.endswith((".yaml", ".yml")):
            safe_name += ".yaml"
            
        config_path = get_fustor_home_dir() / "agent-config" / safe_name
        
        try:
            if config_path.exists():
                config_yaml = config_path.read_text(encoding="utf-8")
                asyncio.create_task(pipe.sender_handler.send_batch(
                    pipe.session_id, 
                    [], 
                    {
                        "phase": "config_report",
                        "metadata": {
                            "filename": safe_name,
                            "config_yaml": config_yaml
                        }
                    }
                ))
                logger.info(f"Pipe {pipe.id}: Reported config from {config_path}")
            else:
                logger.error(f"Pipe {pipe.id}: Config file {config_path} not found for reporting")
        except Exception as e:
            logger.error(f"Pipe {pipe.id}: Failed to report config: {e}")

    async def _handle_command_upgrade(self, pipe: PipeInterface, cmd: Dict[str, Any]) -> None:
        """Handle 'upgrade' command."""
        version = cmd.get("version")
        if not version:
            return

        logger.info(f"Pipe {pipe.id}: Starting remote upgrade to version {version}...")
        
        import sys
        try:
            cmd_args = [sys.executable, "-m", "pip", "install", f"fustor-agent=={version}"]
            process = await asyncio.create_subprocess_exec(
                *cmd_args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()

            if process.returncode != 0:
                logger.error(f"Pipe {pipe.id}: Upgrade failed: {stderr.decode()}")
                return

            if pipe.has_active_session():
                await pipe.sender_handler.close_session(pipe.session_id)

            os.execv(sys.executable, [sys.executable] + sys.argv)
            
        except Exception as e:
            logger.error(f"Pipe {pipe.id}: Unexpected error during upgrade: {e}")

import asyncio
import logging
import os
import signal
import shutil
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
                elif cmd_type == "update_config":
                    self._handle_command_update_config(cmd)
                elif cmd_type == "report_config":
                    self._handle_command_report_config(cmd)
                elif cmd_type == "upgrade":
                    await self._handle_command_upgrade(cmd)
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

    def _handle_command_update_config(self: "AgentPipe", cmd: Dict[str, Any]) -> None:
        """
        Handle 'update_config' command.

        Writes the received YAML content to the agent's config directory,
        creates a backup of the existing file, then triggers SIGHUP reload.
        """
        config_yaml = cmd.get("config_yaml")
        filename = cmd.get("filename", "default.yaml")

        if not config_yaml:
            logger.warning(f"Pipe {self.id}: update_config command missing 'config_yaml'")
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
                logger.error(f"Pipe {self.id}: Received semantically invalid config: {errors}")
                return
                
        except yaml.YAMLError as e:
            logger.error(f"Pipe {self.id}: Received invalid YAML syntax: {e}")
            return
        except Exception as e:
            logger.error(f"Pipe {self.id}: Unexpected error during config validation: {e}")
            return

        # Sanitize filename to prevent path traversal
        safe_name = os.path.basename(filename)
        if not safe_name.endswith((".yaml", ".yml")):
            safe_name += ".yaml"

        config_dir = get_fustor_home_dir() / "agent-config"
        target_path = config_dir / safe_name
        backup_path = config_dir / f"{safe_name}.bak"

        try:
            # Backup existing file
            if target_path.exists():
                shutil.copy2(target_path, backup_path)
                logger.info(f"Pipe {self.id}: Backed up {target_path} -> {backup_path}")

            # Write new config
            target_path.write_text(config_yaml, encoding="utf-8")
            logger.info(f"Pipe {self.id}: Config written to {target_path} ({len(config_yaml)} bytes)")

            # Trigger reload
            os.kill(os.getpid(), signal.SIGHUP)
            logger.info(f"Pipe {self.id}: Config updated and reload triggered")

        except Exception as e:
            logger.error(f"Pipe {self.id}: Failed to write config: {e}")
            # Restore backup on failure
            if backup_path.exists():
                try:
                    shutil.copy2(backup_path, target_path)
                    logger.info(f"Pipe {self.id}: Restored backup after write failure")
                except Exception as restore_err:
                    logger.error(f"Pipe {self.id}: Failed to restore backup: {restore_err}")

    def _handle_command_report_config(self: "AgentPipe", cmd: Dict[str, Any]) -> None:
        """
        Handle 'report_config' command.
        Reads the local config file and sends it back to Fusion.
        """
        filename = cmd.get("filename", "default.yaml")
        from fustor_core.common import get_fustor_home_dir
        
        # Sanitize filename
        safe_name = os.path.basename(filename)
        if not safe_name.endswith((".yaml", ".yml")):
            safe_name += ".yaml"
            
        config_path = get_fustor_home_dir() / "agent-config" / safe_name
        
        try:
            if config_path.exists():
                config_yaml = config_path.read_text(encoding="utf-8")
                
                # Send back as a metadata-only batch
                asyncio.create_task(self.sender_handler.send_batch(
                    self.session_id, 
                    [], 
                    {
                        "phase": "config_report",
                        "metadata": {
                            "filename": safe_name,
                            "config_yaml": config_yaml
                        }
                    }
                ))
                logger.info(f"Pipe {self.id}: Reported config from {config_path}")
            else:
                logger.error(f"Pipe {self.id}: Config file {config_path} not found for reporting")
        except Exception as e:
            logger.error(f"Pipe {self.id}: Failed to report config: {e}")

    async def _handle_command_upgrade(self: "AgentPipe", cmd: Dict[str, Any]) -> None:
        """
        Handle 'upgrade' command.
        
        1. Runs 'pip install fustor-agent==<version>'
        2. If successful, restarts itself using os.execv()
        """
        version = cmd.get("version")
        if not version:
            logger.warning(f"Pipe {self.id}: upgrade command missing 'version'")
            return

        logger.info(f"Pipe {self.id}: Starting remote upgrade to version {version}...")
        
        import sys
        import subprocess

        try:
            # 1. Run pip install
            # Use sys.executable to ensure we use the same venv
            # If in a dev environment (like current), we might need --index-url or similar, 
            # but usually it's public PyPI.
            cmd_args = [sys.executable, "-m", "pip", "install", f"fustor-agent=={version}"]
            
            # Using asyncio.create_subprocess_exec would be cleaner but 
            # we need to be careful about blocking.
            process = await asyncio.create_subprocess_exec(
                *cmd_args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()

            if process.returncode != 0:
                logger.error(f"Pipe {self.id}: Upgrade failed (pip return code {process.returncode})")
                logger.error(f"pip stderr: {stderr.decode()}")
                return

            logger.info(f"Pipe {self.id}: Upgrade successful. Restarting process...")
            
            # 2. Seamless restart
            # Close loop and execv
            # We don't call self.stop() fully because we want a quick replacement.
            # But we should try to close the session if possible.
            if self.has_active_session():
                try:
                    await self.sender_handler.close_session(self.session_id)
                except:
                    pass

            os.execv(sys.executable, [sys.executable] + sys.argv)
            
        except Exception as e:
            logger.error(f"Pipe {self.id}: Unexpected error during upgrade: {e}")


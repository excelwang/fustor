#!/usr/bin/env python
import click
import asyncio
import signal
import os
import logging
import sys
import subprocess
import time

from fustor_core.common import setup_logging, get_fustor_home_dir
from . import CONFIG_DIR, ConfigurationError

# Define common logging path
HOME_FUSTOR_DIR = get_fustor_home_dir()
AGENT_LOG_FILE = os.path.join(HOME_FUSTOR_DIR, "agent.log")
PID_FILE = os.path.join(HOME_FUSTOR_DIR, "agent.pid")


def _is_running():
    if not os.path.exists(PID_FILE):
        return False
    try:
        with open(PID_FILE, 'r') as f:
            pid = int(f.read().strip())
        os.kill(pid, 0)
    except (IOError, ValueError, OSError):
        try:
            os.remove(PID_FILE)
        except OSError:
            pass
        return False
    else:
        return pid

@click.group()
def cli():
    """FuAgent Command-Line Interface Tool"""
    pass

@cli.command()
@click.option("-D", "--daemon", is_flag=True, help="Run the service as a background daemon.")
@click.option("-V", "--verbose", is_flag=True, help="Enable verbose (DEBUG level) logging.")
@click.option("--no-console-log", is_flag=True, hidden=True, help="Internal: Disable console logging.")
def start(daemon, verbose, no_console_log):
    """Starts the FuAgent monitoring service."""
    log_level = "DEBUG" if verbose else "INFO"
    setup_logging(log_file_path=AGENT_LOG_FILE, base_logger_name="fustor_agent", level=log_level.upper(), console_output=(not no_console_log))
    logger = logging.getLogger("fustor_agent")

    if daemon:
        pid = _is_running()
        if pid:
            click.echo(f"FuAgent is already running with PID: {pid}")
            return
        
        click.echo("Starting FuAgent in the background...")
        
        # Self-execute as a background process using nohup-like behavior
        # We re-run the same command but without --daemon and with --no-console-log
        
        # Construct the command line arguments
        # Assuming 'fustor-agent' is in the path or we use sys.argv[0]
        cmd = [sys.executable, sys.argv[0], "start", "--no-console-log"]
        if verbose:
            cmd.append("--verbose")
            
        # Launch subprocess independent of parent
        try:
            # Create session, direct pipes to devnull/log
            kwargs = {}
            if sys.platform != 'win32':
                kwargs['start_new_session'] = True
                
            with open(AGENT_LOG_FILE, 'a') as log_f:
                proc = subprocess.Popen(
                    cmd,
                    stdin=subprocess.DEVNULL,
                    stdout=log_f, 
                    stderr=subprocess.STDOUT,
                    **kwargs
                )
            
            click.echo(f"FuAgent started in background with PID: {proc.pid}")
            # PID file will be written by the child process logic below
            # But since child process writes it, we might race if we check immediately.
            # Ideally child writes PID.
        except Exception as e:
            click.echo(click.style(f"Failed to start background process: {e}", fg="red"))
        return

    # --- Foreground Execution Logic ---
    if _is_running():
        # Check if it's really another process (race condition with daemon spawner above?)
        # For simplicity, if we are the child process, we might just overwrite or error.
        # But _is_running checks the file.
        # If we just spawned, the file might NOT operate yet.
        # However, typically 'start' blocks.
        # Let's overwrite PID file for current process.
        pass

    try:
        if not os.path.exists(CONFIG_DIR):
            os.makedirs(CONFIG_DIR)
            
        with open(PID_FILE, 'w') as f:
            f.write(str(os.getpid()))

        from .runner import run_agent
        
        click.echo(f"FuAgent starting... Logs: {AGENT_LOG_FILE}")
        asyncio.run(run_agent(CONFIG_DIR))

    except KeyboardInterrupt:
        click.echo("\nFuAgent is shutting down...")
    except ConfigurationError as e:
        click.echo("="*60)
        click.echo(click.style(f"FuAgent Configuration Error: {e}", fg="red"))
        click.echo(f"Please check your configuration files in: '{CONFIG_DIR}'")
        click.echo("="*60)
    except Exception as e:
        logger.critical(f"An unexpected error occurred during startup: {e}", exc_info=True)
        click.echo(click.style(f"\nFATAL: An unexpected error occurred: {e}", fg="red"))
    finally:
        if os.path.exists(PID_FILE):
            # Only remove if it contains our PID (to avoid removing if we crashed but restarted fast?)
            # Actually standard practice is just remove.
            try:
                with open(PID_FILE, 'r') as f:
                    if int(f.read().strip()) == os.getpid():
                        os.remove(PID_FILE)
            except Exception:
                pass

@cli.command()
def stop():
    """Stops the background FuAgent service."""
    pid = _is_running()
    if not pid:
        click.echo("FuAgent is not running.")
        return

    click.echo(f"Stopping FuAgent with PID: {pid}...")
    try:
        os.kill(pid, signal.SIGTERM)
        for _ in range(10):
            if not _is_running():
                break
            time.sleep(1)
        else:
            click.echo(click.style("FuAgent did not stop in time. Forcing shutdown.", fg="yellow"))
            os.kill(pid, signal.SIGKILL)

        click.echo("FuAgent stopped successfully.")
    except OSError as e:
        click.echo(click.style(f"Error stopping process: {e}", fg="red"))
    finally:
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)

@cli.command()
@click.option("--source-id", required=True, help="ID of the source configuration (e.g., 'mysql-prod').")
@click.option("--admin-user", required=False, default=None, help="Admin username for the source database (if required).")
@click.option("--admin-password", required=False, default=None, help="Admin password for the source database (if required).")
@click.pass_context
def discover_schema(ctx, source_id, admin_user, admin_password):
    """Discovers and caches the schema for a given source configuration."""
    setup_logging(log_file_path=AGENT_LOG_FILE, base_logger_name="fustor_agent", level="INFO")
    logger = logging.getLogger("fustor_agent")

    click.echo(f"Attempting to discover and cache schema for source: {source_id}...")
    try:
        from .app import App
        app_instance = App(config_dir=CONFIG_DIR)
        asyncio.run(app_instance.source_config_service.discover_and_cache_fields(
            source_id, admin_user, admin_password
        ))
        click.echo(f"Successfully discovered and cached schema for source '{source_id}'.")
    except ValueError as e:
        click.echo(f"Error: {e}", err=True)
        ctx.exit(1)
    except Exception as e:
        click.echo(f"An unexpected error occurred: {e}", err=True)
        logger.error(f"An unexpected error occurred while discovering schema for '{source_id}': {e}", exc_info=True)
        ctx.exit(1)
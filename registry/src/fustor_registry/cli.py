#!/usr/bin/env python
import click
import asyncio
import signal
import uvicorn
import os
import logging
import sys
import subprocess
import time

from fustor_common.logging_config import setup_logging
from fustor_common.exceptions import ConfigurationError # Re-use common ConfigurationError
from fustor_common.paths import get_fustor_home_dir # NEW import

# Define standard directories and file names for registry
HOME_FUSTOR_DIR = get_fustor_home_dir() # Use the common function
REGISTRY_PID_FILE = os.path.join(HOME_FUSTOR_DIR, "registry.pid") # Renamed from fustor_registry.pid
REGISTRY_LOG_FILE = os.path.join(HOME_FUSTOR_DIR, "registry.log") # Renamed from fustor_registry.log


def _is_running():
    """Check if the registry daemon is already running by checking the PID file."""
    if not os.path.exists(REGISTRY_PID_FILE):
        return False
    try:
        with open(REGISTRY_PID_FILE, 'r') as f:
            pid = int(f.read().strip())
        # Check if the process with this PID is actually running
        os.kill(pid, 0) # Signal 0 doesn't do anything, but checks if PID exists
    except (IOError, ValueError, OSError):
        # If PID file is invalid or process not found, clean up the PID file
        try:
            os.remove(REGISTRY_PID_FILE)
        except OSError:
            pass # Ignore if removal fails
        return False
    else:
        return pid

@click.group()
def cli():
    """Fustor Registry Command-Line Interface Tool"""
    pass

@cli.command()
@click.option("--reload", is_flag=True, help="Enable auto-reloading of the server on code changes (foreground only).")
@click.option("-p", "--port", default=8107, help="Port to run the server on.")
@click.option("-D", "--daemon", is_flag=True, help="Run the service as a background daemon.")
@click.option("-V", "--verbose", is_flag=True, help="Enable verbose (DEBUG level) logging.")
@click.option("--no-console-log", is_flag=True, hidden=True, help="Internal: Disable console logging for daemon process.")
def start(reload, port, daemon, verbose, no_console_log):
    """Starts the Fustor Registry service (in the foreground by default)."""
    log_level = "DEBUG" if verbose else "INFO"
    
    # Ensure log directory exists for the COMMON_LOG_FILE
    os.makedirs(os.path.dirname(COMMON_LOG_FILE), exist_ok=True)

    # Setup logging for the registry CLI
    setup_logging(
        log_file_path=REGISTRY_LOG_FILE,
        base_logger_name="fustor_registry",
        level=log_level.upper(),
        console_output=(not no_console_log)
    )
    logger = logging.getLogger("fustor_registry")

    if daemon:
        pid = _is_running()
        if pid:
            click.echo(f"Fustor Registry is already running with PID: {pid}")
            return
        
        click.echo("Starting Fustor Registry in the background...")
        command = [sys.executable, '-m', 'fustor_registry.cli', 'start', f'--port={port}', '--no-console-log']
        if verbose:
            command.append('--verbose')
        
        # Detach the child process
        # Using preexec_fn=os.setsid ensures the child process is not killed when the parent exits
        # and redirects stdout/stderr to /dev/null
        process = subprocess.Popen(
            command, 
            stdout=subprocess.DEVNULL, 
            stderr=subprocess.DEVNULL, 
            stdin=subprocess.DEVNULL, 
            close_fds=True,
            preexec_fn=os.setsid # Detach from parent process group
        )
        # Write PID to file in the daemon process
        # The child process needs to write its own PID, not the parent's subprocess PID
        # We need to wait a bit for the daemon process to start and write its PID
        time.sleep(2) # Give the daemon time to start and write its PID file
        pid = _is_running()
        if pid:
            click.echo(f"Fustor Registry daemon started successfully with PID: {pid}")
        else:
            click.echo(click.style("Failed to start Fustor Registry daemon. Check logs for details.", fg="red"))
        return

    # --- Foreground Execution Logic ---
    if _is_running():
        click.echo("Fustor Registry is already running in the background. Stop it first.")
        return

    try:
        os.makedirs(HOME_FUSTOR_DIR, exist_ok=True) # Ensure ~/.fustor exists for PID file
        with open(REGISTRY_PID_FILE, 'w') as f:
            f.write(str(os.getpid()))

        from .main import app as fastapi_app # Import the FastAPI app

        click.echo("\n" + "="*60)
        click.echo("Fustor Registry")
        click.echo(f"Web : http://127.0.0.1:{port}")
        click.echo("="*60 + "\n")
        
        app_to_run = fastapi_app
        if reload:
            app_to_run = "fustor_registry.main:app" # Uvicorn needs string path for reload

        uvicorn.run(
            app_to_run,
            host="127.0.0.1",
            port=port,
            log_config=None, # Logging handled by setup_logging
            access_log=True,
            reload=reload,
        )

    except KeyboardInterrupt:
        logger.info("Fustor Registry is shutting down...")
        click.echo("\nFustor Registry is shutting down...")
    except ConfigurationError as e:
        logger.critical(f"Fustor Registry Configuration Error: {e}", exc_info=True)
        click.echo("="*60)
        click.echo(click.style(f"Fustor Registry Configuration Error: {e}", fg="red"))
        click.echo(f"Please check your configuration file at: '{os.path.join(REGISTRY_CONFIG_DIR, REGISTRY_CONFIG_FILE_NAME)}'")
        click.echo("="*60)
    except Exception as e:
        logger.critical(f"An unexpected error occurred during startup: {e}", exc_info=True)
        click.echo(click.style(f"\nFATAL: An unexpected error occurred: {e}", fg="red"))
    finally:
        if os.path.exists(REGISTRY_PID_FILE):
            os.remove(REGISTRY_PID_FILE)
            logger.info("Removed PID file.")

@cli.command()
def stop():
    """Stops the background Fustor Registry service."""
    pid = _is_running()
    if not pid:
        click.echo("Fustor Registry is not running.")
        return

    click.echo(f"Stopping Fustor Registry with PID: {pid}...")
    try:
        os.kill(pid, signal.SIGTERM)
        for _ in range(10): # Wait up to 10 seconds for the process to terminate
            if not _is_running():
                break
            time.sleep(1)
        else:
            click.echo(click.style("Fustor Registry did not stop in time. Forcing shutdown.", fg="yellow"))
            os.kill(pid, signal.SIGKILL) # Force kill if SIGTERM fails

        click.echo("Fustor Registry stopped successfully.")
    except OSError as e:
        click.echo(click.style(f"Error stopping process: {e}", fg="red"))
        logger.error(f"Error stopping process with PID {pid}: {e}")
    finally:
        if os.path.exists(REGISTRY_PID_FILE):
            os.remove(REGISTRY_PID_FILE)
            logger.info("Removed PID file.")

# Define REGISTRY_LOG_FILE_NAME here as it's used in setup_logging
REGISTRY_LOG_FILE_NAME = "fustor_registry.log"
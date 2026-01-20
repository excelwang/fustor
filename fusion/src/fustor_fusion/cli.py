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

# Define standard directories and file names for fusion
HOME_FUSTOR_DIR = get_fustor_home_dir() # Use the common function
FUSION_PID_FILE = os.path.join(HOME_FUSTOR_DIR, "fusion.pid") # Renamed from fustor_fusion.pid
FUSION_LOG_FILE = os.path.join(HOME_FUSTOR_DIR, "fusion.log") # Renamed from fustor_fusion.log


def _is_running():
    """Check if the fusion daemon is already running by checking the PID file."""
    if not os.path.exists(FUSION_PID_FILE):
        return False
    try:
        with open(FUSION_PID_FILE, 'r') as f:
            pid = int(f.read().strip())
        # Check if the process with this PID is actually running
        os.kill(pid, 0) # Signal 0 doesn't do anything, but checks if PID exists
    except (IOError, ValueError, OSError):
        # If PID file is invalid or process not found, clean up the PID file
        try:
            os.remove(FUSION_PID_FILE)
        except OSError:
            pass # Ignore if removal fails
        return False
    else:
        return pid

@click.group()
def cli():
    """Fustor Fusion Command-Line Interface Tool"""
    pass

@cli.command()
@click.option("--reload", is_flag=True, help="Enable auto-reloading of the server on code changes (foreground only).")
@click.option("-p", "--port", default=8102, help="Port to run the server on.")
@click.option("-h", "--host", default="0.0.0.0", help="Host to bind the server to.")
@click.option("-D", "--daemon", is_flag=True, help="Run the service as a background daemon.")
@click.option("-V", "--verbose", is_flag=True, help="Enable verbose (DEBUG level) logging.")
@click.option("--no-console-log", is_flag=True, hidden=True, help="Internal: Disable console logging for daemon process.")
def start(reload, port, host, daemon, verbose, no_console_log):
    """Starts the Fustor Fusion service (in the foreground by default)."""
    log_level = "DEBUG" if verbose else "INFO"
    
    # Ensure log directory exists for the FUSION_LOG_FILE
    os.makedirs(os.path.dirname(FUSION_LOG_FILE), exist_ok=True)

    # Setup logging for the Fusion CLI
    setup_logging(
        log_file_path=FUSION_LOG_FILE,
        base_logger_name="fustor_fusion",
        level=log_level.upper(),
        console_output=(not no_console_log)
    )
    logger = logging.getLogger("fustor_fusion")

    if daemon:
        pid = _is_running()
        if pid:
            click.echo(f"Fustor Fusion is already running with PID: {pid}")
            return
        
        click.echo("Starting Fustor Fusion in the background...")
        # Use a common daemon launcher function to avoid module path issues
        import fustor_common.daemon as daemon_module
        daemon_module.start_daemon(
            service_module_path='fustor_fusion.main',
            app_var_name='app',
            pid_file_name='fusion.pid',
            log_file_name='fusion.log',
            display_name='FuFusion',
            port=port,
            host=host,  # Use the host parameter
            verbose=verbose,
            reload=reload  # Pass reload parameter
        )
        time.sleep(2) # Give the daemon time to start and write its PID file
        pid = _is_running()
        if pid:
            click.echo(f"Fustor Fusion daemon started successfully with PID: {pid}")
        else:
            click.echo(click.style("Failed to start Fustor Fusion daemon. Check logs for details.", fg="red"))
        return

    # --- Foreground Execution Logic ---
    if _is_running():
        click.echo("Fustor Fusion is already running in the background. Stop it first.")
        return

    try:
        os.makedirs(HOME_FUSTOR_DIR, exist_ok=True) # Ensure Fustor home directory exists for PID file
        with open(FUSION_PID_FILE, 'w') as f:
            f.write(str(os.getpid()))

        from .main import app as fastapi_app

        click.echo("\n" + "="*60)
        click.echo("Fustor Fusion")
        click.echo(f"Web : http://{host}:{port}")
        click.echo("="*60 + "\n")
        
        app_to_run = fastapi_app
        if reload:
            app_to_run = "fustor_fusion.main:app"

        # Configure uvicorn to use DEBUG level for access logs to reduce verbosity
        uvicorn_logger = logging.getLogger("uvicorn.access")
        uvicorn_logger.setLevel(logging.DEBUG)

        uvicorn.run(
            app_to_run,
            host=host,
            port=port,
            log_config=None, # Logging handled by setup_logging
            access_log=True,
            reload=reload,
        )

    except KeyboardInterrupt:
        logger.info("Fustor Fusion is shutting down...")
        click.echo("\nFustor Fusion is shutting down...")
    except ConfigurationError as e:
        logger.critical(f"Fustor Fusion Configuration Error: {e}", exc_info=True)
        click.echo("="*60)
        click.echo(click.style(f"Fustor Fusion Configuration Error: {e}", fg="red"))
        click.echo("Please check your environment variables and .env file in the home directory.")
        click.echo("="*60)
    except Exception as e:
        logger.critical(f"An unexpected error occurred during startup: {e}", exc_info=True)
        click.echo(click.style(f"\nFATAL: An unexpected error occurred: {e}", fg="red"))
    finally:
        if os.path.exists(FUSION_PID_FILE):
            os.remove(FUSION_PID_FILE)
            logger.info("Removed PID file.")

@cli.command()
def stop():
    """Stops the background Fustor Fusion service."""
    pid = _is_running()
    if not pid:
        click.echo("Fustor Fusion is not running.")
        return

    click.echo(f"Stopping Fustor Fusion with PID: {pid}...")
    try:
        os.kill(pid, signal.SIGTERM)
        for _ in range(10): # Wait up to 10 seconds for the process to terminate
            if not _is_running():
                break
            time.sleep(1)
        else:
            click.echo(click.style("Fustor Fusion did not stop in time. Forcing shutdown.", fg="yellow"))
            os.kill(pid, signal.SIGKILL) # Force kill if SIGTERM fails

        click.echo("Fustor Fusion stopped successfully.")
    except OSError as e:
        click.echo(click.style(f"Error stopping process: {e}", fg="red"))
        logger.error(f"Error stopping process with PID {pid}: {e}")
    finally:
        if os.path.exists(FUSION_PID_FILE):
            os.remove(FUSION_PID_FILE)
            logger.info("Removed PID file.")

# Define FUSION_LOG_FILE_NAME here as it's used in setup_logging
FUSION_LOG_FILE_NAME = "fustor_fusion.log"
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
from fustor_common.paths import get_fustor_home_dir # NEW import
from . import CONFIG_DIR, ConfigurationError

# Define common logging path
HOME_FUSTOR_DIR = get_fustor_home_dir() # Use the common function
AGENT_LOG_FILE = os.path.join(HOME_FUSTOR_DIR, "agent.log") # Renamed from fustor_agent.log

# PID file location
PID_FILE = os.path.join(HOME_FUSTOR_DIR, "agent.pid") # Renamed from fustor_agent.pid


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
@click.option("--reload", is_flag=True, help="Enable auto-reloading of the server on code changes (foreground only).")
@click.option("-p", "--port", default=8103, help="Port to run the server on.")
@click.option("-h", "--host", default="127.0.0.1", help="Host to bind the server to.")
@click.option("-D", "--daemon", is_flag=True, help="Run the service as a background daemon.")
@click.option("-V", "--verbose", is_flag=True, help="Enable verbose (DEBUG level) logging.")
@click.option("--no-console-log", is_flag=True, hidden=True, help="Internal: Disable console logging.")
def start(reload, port, host, daemon, verbose, no_console_log):
    """Starts the FuAgent monitoring service (in the foreground by default)."""
    log_level = "DEBUG" if verbose else "INFO"
    # Disable console logging if --no-console-log is passed (used by daemonized process)
    setup_logging(log_file_path=AGENT_LOG_FILE, base_logger_name="fustor_agent", level=log_level.upper(), console_output=(not no_console_log))
    logger = logging.getLogger("fustor_agent")

    if daemon:
        pid = _is_running()
        if pid:
            click.echo(f"FuAgent is already running with PID: {pid}")
            return
        
        click.echo("Starting FuAgent in the background...")
        # Use a common daemon launcher function to avoid module path issues
        import fustor_common.daemon as daemon_module
        daemon_module.start_daemon(
            service_module_path='fustor_agent.api.routes',
            app_var_name='web_app',
            pid_file_name='agent.pid',
            log_file_name='agent.log',
            display_name='FuAgent',
            port=port,
            host=host,  # Use the host parameter
            verbose=verbose,
            reload=reload  # Pass reload parameter
        )
        time.sleep(2) # Give the daemon time to start and write its PID file
        pid = _is_running()
        if pid:
            click.echo(f"FuAgent daemon started successfully with PID: {pid}")
        else:
            click.echo(click.style("Failed to start FuAgent daemon. Check logs for details.", fg="red"))
        return

    # --- Foreground Execution Logic ---
    if _is_running():
        click.echo("FuAgent is already running in the background. Stop it first.")
        return

    try:
        if not os.path.exists(CONFIG_DIR):
            os.makedirs(CONFIG_DIR)
        with open(PID_FILE, 'w') as f:
            f.write(str(os.getpid()))

        from .api.routes import web_app

        click.echo("\n" + "="*60)
        click.echo("FuAgent ")
        click.echo(f"Web : http://{host}:{port}/ui")
        click.echo("="*60 + "\n")
        
        app_to_run = web_app
        if reload:
            app_to_run = "fustor_agent.api.routes:web_app"

        # Configure uvicorn to use DEBUG level for access logs to reduce verbosity
        uvicorn_logger = logging.getLogger("uvicorn.access")
        uvicorn_logger.setLevel(logging.DEBUG)

        uvicorn.run(
            app_to_run,
            host=host,
            port=port,
            log_config=None,
            access_log=True,
            reload=reload,
        )

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
            os.remove(PID_FILE)

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
    # Setup with default INFO level for this one-off command
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


@cli.command("start-sync")
@click.argument("sync_id")
def start_sync(sync_id: str):
    """Start a single sync task by ID. Starts daemon if not running."""
    import requests
    
    # Check if daemon is running
    pid = _is_running()
    if not pid:
        click.echo("Agent daemon not running. Starting it first...")
        ctx = click.get_current_context()
        ctx.invoke(start, daemon=True, port=8103)
        time.sleep(3)
        
        pid = _is_running()
        if not pid:
            click.echo(click.style("Failed to start Agent daemon.", fg="red"))
            return
    
    # Call management API to start sync
    try:
        response = requests.post(
            f"http://localhost:8103/api/v1/management/syncs/{sync_id}/start",
            timeout=10
        )
        if response.ok:
            click.echo(click.style(f"Sync '{sync_id}' started successfully.", fg="green"))
        else:
            detail = response.json().get("detail", "Unknown error")
            click.echo(click.style(f"Failed to start sync '{sync_id}': {detail}", fg="red"))
    except requests.exceptions.ConnectionError:
        click.echo(click.style("Cannot connect to Agent daemon.", fg="red"))
    except Exception as e:
        click.echo(click.style(f"Error: {e}", fg="red"))


@cli.command("stop-sync")
@click.argument("sync_id")
def stop_sync(sync_id: str):
    """Stop a single sync task by ID. Stops daemon if no syncs left."""
    import requests
    
    pid = _is_running()
    if not pid:
        click.echo("Agent daemon is not running.")
        return
    
    try:
        response = requests.post(
            f"http://localhost:8103/api/v1/management/syncs/{sync_id}/stop",
            timeout=10
        )
        if response.ok:
            data = response.json()
            click.echo(click.style(f"Sync '{sync_id}' stopped.", fg="green"))
            
            if data.get("should_shutdown"):
                click.echo("No active syncs remaining. Stopping Agent daemon...")
                ctx = click.get_current_context()
                ctx.invoke(stop)
        else:
            detail = response.json().get("detail", "Unknown error")
            click.echo(click.style(f"Failed to stop sync '{sync_id}': {detail}", fg="red"))
    except requests.exceptions.ConnectionError:
        click.echo(click.style("Cannot connect to Agent daemon.", fg="red"))
    except Exception as e:
        click.echo(click.style(f"Error: {e}", fg="red"))
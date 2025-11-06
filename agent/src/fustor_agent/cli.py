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

from .logging_setup import setup_logging
from . import CONFIG_DIR, CONFIG_FILE_NAME, ConfigurationError

# Use an absolute path for the config directory to ensure consistency
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
ABSOLUTE_CONFIG_DIR = os.path.join(PROJECT_ROOT, CONFIG_DIR)
PID_FILE = os.path.join(ABSOLUTE_CONFIG_DIR, "fustor_agent.pid")


def _is_running():
    """Check if the daemon is already running by checking the PID file."""
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
@click.option("-p", "--port", default=8106, help="Port to run the server on.")
@click.option("-D", "--daemon", is_flag=True, help="Run the service as a background daemon.")
@click.option("-V", "--verbose", is_flag=True, help="Enable verbose (DEBUG level) logging.")
@click.option("--no-console-log", is_flag=True, hidden=True, help="Internal: Disable console logging.")
def start(reload, port, daemon, verbose, no_console_log):
    """Starts the FuAgent monitoring service (in the foreground by default)."""
    log_level = logging.DEBUG if verbose else logging.INFO
    # Disable console logging if --no-console-log is passed (used by daemonized process)
    setup_logging(ABSOLUTE_CONFIG_DIR, level=log_level, console_output=(not no_console_log))
    logger = logging.getLogger("fustor_agent")

    if daemon:
        pid = _is_running()
        if pid:
            click.echo(f"FuAgent is already running with PID: {pid}")
            return
        
        click.echo("Starting FuAgent in the background...")
        command = [sys.executable, '-m', 'fustor_agent.cli', 'start', f'--port={port}', '--no-console-log']
        if verbose:
            command.append('--verbose')
        
        # Note: Reload is not recommended for daemon mode as it can cause issues.
        # If a user really wants it, they can add it manually, but we won't pass it by default.

        subprocess.Popen(command, stdout=None, stderr=None, stdin=None, close_fds=True)
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
        if not os.path.exists(ABSOLUTE_CONFIG_DIR):
            os.makedirs(ABSOLUTE_CONFIG_DIR)
        with open(PID_FILE, 'w') as f:
            f.write(str(os.getpid()))

        from .api.routes import web_app

        click.echo("\n" + "="*60)
        click.echo("FuAgent ")
        click.echo(f"Web : http://127.0.0.1:{port}/ui")
        click.echo("="*60 + "\n")
        
        app_to_run = web_app
        if reload:
            app_to_run = "fustor_agent.api.routes:web_app"

        uvicorn.run(
            app_to_run,
            host="127.0.0.1",
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
        click.echo(f"Please check your configuration file at: '{os.path.join(ABSOLUTE_CONFIG_DIR, CONFIG_FILE_NAME)}'")
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
    setup_logging(ABSOLUTE_CONFIG_DIR, level=logging.INFO)
    logger = logging.getLogger("fustor_agent")

    click.echo(f"Attempting to discover and cache schema for source: {source_id}...")
    try:
        from .app import App
        app_instance = App(config_dir=ABSOLUTE_CONFIG_DIR)
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
#!/usr/bin/env python
"""
Fustor Fusion CLI - Simplified WireGuard-style interface.

Commands:
  fustor-fusion start [configs...]  # Start fusion + pipe(s), default.yaml if no args
  fustor-fusion stop [configs...]   # Stop pipe(s) or the entire fusion
  fustor-fusion list                # List running pipes
  fustor-fusion status [pipe]       # Show status
  fustor-fusion reload              # Reload configuration (SIGHUP)
"""
import click
import asyncio
import signal
import uvicorn
import os
import logging
import sys
import subprocess
import time

from fustor_core.common import setup_logging, get_fustor_home_dir
from .config.unified import fusion_config

# Define standard directories and file names for fusion
HOME_FUSTOR_DIR = get_fustor_home_dir()
LOG_DIR = os.path.join(HOME_FUSTOR_DIR, "logs")
FUSION_PID_FILE = os.path.join(HOME_FUSTOR_DIR, "fusion.pid")
FUSION_LOG_FILE = os.path.join(LOG_DIR, "fusion.log")


def _is_running():
    """Check if the fusion daemon is already running."""
    if not os.path.exists(FUSION_PID_FILE):
        return False
    try:
        with open(FUSION_PID_FILE, 'r') as f:
            pid = int(f.read().strip())
        if pid == os.getpid():
            return False
        os.kill(pid, 0)
    except (IOError, ValueError, OSError):
        try:
            os.remove(FUSION_PID_FILE)
        except OSError:
            pass
        return False
    else:
        return pid


@click.group()
def cli():
    """Fustor Fusion CLI"""
    pass


@cli.command()
@click.argument("configs", nargs=-1)
@click.option("--reload", is_flag=True, help="Enable auto-reloading (foreground only).")
@click.option("-p", "--port", type=int, help="Port to run the management server on.")
@click.option("-h", "--host", help="Host to bind the server to.")
@click.option("-D", "--daemon", is_flag=True, help="Run as background daemon.")
@click.option("-V", "--verbose", is_flag=True, help="Enable DEBUG logging.")
@click.option("--no-console-log", is_flag=True, hidden=True)
def start(configs, reload, port, host, daemon, verbose, no_console_log):
    """
    Start Fusion and pipe(s).
    
    Examples:
        fustor-fusion start                   # Start all enabled from default.yaml
        fustor-fusion start ingest-main       # Start single pipe
        fustor-fusion start custom.yaml       # Start from managed file
    """
    fusion_config.ensure_loaded()
    
    # Use YAML defaults if not overridden by CLI args
    host = host or fusion_config.fusion.host
    port = port or fusion_config.fusion.port
    
    log_level = "DEBUG" if verbose else fusion_config.logging.level
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
            click.echo(f"Fusion already running with PID: {pid}")
            return
        
        click.echo("Starting Fusion in background...")
        
        cmd = [sys.executable, sys.argv[0], "start", "--no-console-log"]
        if verbose: cmd.append("--verbose")
        if port: cmd.extend(["--port", str(port)])
        if host: cmd.extend(["--host", host])
        cmd.extend(configs)
        
        try:
            kwargs = {}
            if sys.platform != 'win32':
                kwargs['start_new_session'] = True
            
            with open(FUSION_LOG_FILE, 'a') as log_f:
                proc = subprocess.Popen(
                    cmd,
                    stdin=subprocess.DEVNULL,
                    stdout=log_f,
                    stderr=subprocess.STDOUT,
                    **kwargs
                )
            click.echo(f"Fusion started with PID: {proc.pid}")
        except Exception as e:
            click.echo(click.style(f"Failed to start: {e}", fg="red"))
        return

    # --- Foreground Execution ---
    if _is_running():
        pass # Allow overwrite if child

    try:
        os.makedirs(HOME_FUSTOR_DIR, exist_ok=True)
        os.makedirs(LOG_DIR, exist_ok=True)
        with open(FUSION_PID_FILE, 'w') as f:
            f.write(str(os.getpid()))

        # Set environment variables for config_list to be read by main.py/lifespan
        if configs:
            os.environ["FUSTOR_FUSION_CONFIGS"] = ",".join(configs)
        
        from .main import app as fastapi_app
        
        click.echo(f"Fusion starting on http://{host}:{port}")
        
        uvicorn.run(
            fastapi_app if not reload else "fustor_fusion.main:app",
            host=host,
            port=port,
            log_config=None,
            access_log=True,
            reload=reload,
        )

    except KeyboardInterrupt:
        click.echo("\nFusion shutting down...")
    except Exception as e:
        logger.critical(f"Startup error: {e}", exc_info=True)
        click.echo(click.style(f"\nFATAL: {e}", fg="red"))
    finally:
        if os.path.exists(FUSION_PID_FILE):
            try:
                with open(FUSION_PID_FILE, 'r') as f:
                    if int(f.read().strip()) == os.getpid():
                        os.remove(FUSION_PID_FILE)
            except Exception:
                pass


@cli.command()
@click.argument("configs", nargs=-1)
def stop(configs):
    """Stop pipe(s) or the entire Fusion server."""
    pid = _is_running()
    
    if not configs:
        if not pid:
            click.echo("Fusion is not running.")
            return
        
        click.echo(f"Stopping Fusion (PID: {pid})...")
        try:
            os.kill(pid, signal.SIGTERM)
            for _ in range(10):
                if not _is_running(): break
                time.sleep(1)
            else:
                os.kill(pid, signal.SIGKILL)
            click.echo("Fusion stopped.")
        except OSError as e:
            click.echo(click.style(f"Error: {e}", fg="red"))
    else:
        # Hot-stop specific pipes/configs via API is deprecated in favor of 'reload'
        click.echo("Individual pipe stopping via CLI is deprecated. Please modify config and use 'reload'.")


@cli.command("list")
def list_pipes():
    """List all pipes defined in configuration."""
    fusion_config.ensure_loaded()
    pipes = fusion_config.get_all_pipes()
    
    if not pipes:
        click.echo("No pipes defined.")
        return

    click.echo(f"{'PIPE ID':<30} {'STATUS':<15} {'VIEWS':<20}")
    click.echo("-" * 65)
    
    for pid, pcfg in pipes.items():
        # Check if enabled based on views
        has_enabled_view = False
        for v_id in pcfg.views:
            view = fusion_config.get_view(v_id)
            if view and not view.disabled:
                has_enabled_view = True
                break
        
        recv = fusion_config.get_receiver(pcfg.receiver)
        recv_enabled = recv and not recv.disabled
        
        status = "ENABLED" if (has_enabled_view and recv_enabled) else "DISABLED"
        click.echo(f"{pid:<30} {status:<15} {', '.join(pcfg.views):<20}")


@cli.command()
@click.argument("pipe_id", required=False)
def status(pipe_id):
    """Show status of Fusion or specific pipe."""
    pid = _is_running()
    if not pid:
        click.echo("Fusion Status: " + click.style("STOPPED", fg="red"))
    else:
        click.echo("Fusion Status: " + click.style(f"RUNNING (PID: {pid})", fg="green"))
    
    if pipe_id:
        fusion_config.ensure_loaded()
        pipe = fusion_config.get_pipe(pipe_id)
        if not pipe:
            click.echo(click.style(f"Pipe '{pipe_id}' not found.", fg="yellow"))
            return
            
        click.echo(f"\nPipe ID: {pipe_id}")
        click.echo(f"  Receiver: {pipe.receiver}")
        click.echo(f"  Views:    {', '.join(pipe.views)}")
        
        # Check if enabled based on views
        has_enabled_view = False
        for v_id in pipe.views:
            view = fusion_config.get_view(v_id)
            if view and not view.disabled:
                has_enabled_view = True
                break
        
        recv = fusion_config.get_receiver(pipe.receiver)
        recv_enabled = recv and not recv.disabled
        
        status_str = "ENABLED" if (has_enabled_view and recv_enabled) else "DISABLED"
        click.echo(f"  Status:   {status_str} (Config)")


@cli.command()
def reload():
    """Reload configuration by sending SIGHUP to the Fusion daemon."""
    if not os.path.exists(FUSION_PID_FILE):
        click.echo("Fusion is not running.")
        return
    
    try:
        with open(FUSION_PID_FILE, 'r') as f:
            pid = int(f.read().strip())
        import signal
        os.kill(pid, signal.SIGHUP)
        click.echo(click.style("✓ Reload signal sent (SIGHUP)", fg="green"))
    except ProcessLookupError:
        click.echo(click.style("✗ Process not found. Deleting stale PID file.", fg="red"))
        os.remove(FUSION_PID_FILE)
    except Exception as e:
        click.echo(click.style(f"✗ Failed to reload: {e}", fg="red"))


if __name__ == "__main__":
    cli()
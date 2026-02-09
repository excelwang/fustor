#!/usr/bin/env python
"""
Fustor Agent CLI - Simplified WireGuard-style interface.

Commands:
  fustor-agent start [configs...]  # Start pipe(s), default.yaml if no args
  fustor-agent stop [configs...]   # Stop pipe(s)
  fustor-agent list                # List running pipes
  fustor-agent status [pipe]       # Show status
"""
import click
import asyncio
import signal
import os
import logging
import sys
import subprocess
import time

from fustor_core.common import setup_logging, get_fustor_home_dir

# Define common paths
HOME_FUSTOR_DIR = get_fustor_home_dir()
AGENT_LOG_FILE = os.path.join(HOME_FUSTOR_DIR, "agent.log")
PID_FILE = os.path.join(HOME_FUSTOR_DIR, "agent.pid")


def _is_running():
    """Check if agent daemon is running."""
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
    """Fustor Agent CLI"""
    pass


@cli.command()
@click.argument("configs", nargs=-1)
@click.option("-D", "--daemon", is_flag=True, help="Run as background daemon.")
@click.option("-V", "--verbose", is_flag=True, help="Enable DEBUG logging.")
@click.option("--no-console-log", is_flag=True, hidden=True)
def start(configs, daemon, verbose, no_console_log):
    """
    Start pipe(s).
    
    If no configs specified, starts all pipes listed in default.yaml.
    
    Examples:
        fustor-agent start                    # Start all from default.yaml
        fustor-agent start research-sync      # Start single pipe
        fustor-agent start pipe-a pipe-b      # Start multiple pipes
        fustor-agent start custom.yaml        # Start from managed file
    """
    log_level = "DEBUG" if verbose else "INFO"
    setup_logging(
        log_file_path=AGENT_LOG_FILE,
        base_logger_name="fustor_agent",
        level=log_level.upper(),
        console_output=(not no_console_log)
    )
    logger = logging.getLogger("fustor_agent")

    if daemon:
        pid = _is_running()
        if pid:
            click.echo(f"Agent already running with PID: {pid}")
            return
        
        click.echo("Starting Agent in background...")
        
        cmd = [sys.executable, sys.argv[0], "start", "--no-console-log"]
        if verbose:
            cmd.append("--verbose")
        cmd.extend(configs)  # Pass config args to child
        
        try:
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
            
            click.echo(f"Agent started with PID: {proc.pid}")
        except Exception as e:
            click.echo(click.style(f"Failed to start: {e}", fg="red"))
        return

    # --- Foreground Execution ---
    if _is_running():
        pass  # Allow overwrite if we're the child process

    try:
        os.makedirs(HOME_FUSTOR_DIR, exist_ok=True)
        with open(PID_FILE, 'w') as f:
            f.write(str(os.getpid()))

        from .runner import run_agent
        
        # Convert configs tuple to list, empty means use default.yaml
        config_list = list(configs) if configs else None
        
        click.echo(f"Agent starting... Logs: {AGENT_LOG_FILE}")
        asyncio.run(run_agent(config_list))

    except KeyboardInterrupt:
        click.echo("\nAgent shutting down...")
    except Exception as e:
        logger.critical(f"Startup error: {e}", exc_info=True)
        click.echo(click.style(f"\nFATAL: {e}", fg="red"))
    finally:
        if os.path.exists(PID_FILE):
            try:
                with open(PID_FILE, 'r') as f:
                    if int(f.read().strip()) == os.getpid():
                        os.remove(PID_FILE)
            except Exception:
                pass


@cli.command()
@click.argument("configs", nargs=-1)
def stop(configs):
    """
    Stop pipe(s) or the entire agent.
    
    If no configs specified, stops all pipes and the daemon.
    
    Examples:
        fustor-agent stop                     # Stop everything
        fustor-agent stop research-sync       # Stop single pipe (hot)
    """
    pid = _is_running()
    
    if not configs:
        # Stop entire daemon
        if not pid:
            click.echo("Agent is not running.")
            return
        
        click.echo(f"Stopping Agent (PID: {pid})...")
        try:
            os.kill(pid, signal.SIGTERM)
            for _ in range(10):
                if not _is_running():
                    break
                time.sleep(1)
            else:
                click.echo(click.style("Forcing shutdown...", fg="yellow"))
                os.kill(pid, signal.SIGKILL)
            
            click.echo("Agent stopped.")
        except OSError as e:
            click.echo(click.style(f"Error: {e}", fg="red"))
        finally:
            if os.path.exists(PID_FILE):
                os.remove(PID_FILE)
    else:
        # Hot-stop specific pipes via API
        if not pid:
            click.echo("Agent is not running. Cannot stop individual pipes.")
            return
        
        import requests
        for config in configs:
            try:
                response = requests.post(
                    f"http://localhost:8103/api/v1/pipes/{config}/stop",
                    timeout=10
                )
                if response.ok:
                    click.echo(click.style(f"[{config}] ✓ Stopped", fg="green"))
                else:
                    detail = response.json().get("detail", "Unknown error")
                    click.echo(click.style(f"[{config}] ✗ {detail}", fg="red"))
            except requests.exceptions.ConnectionError:
                click.echo(click.style("Cannot connect to Agent.", fg="red"))
                break
            except Exception as e:
                click.echo(click.style(f"[{config}] ✗ {e}", fg="red"))


@cli.command("list")
def list_pipes():
    """List all pipes and their status."""
    pid = _is_running()
    if not pid:
        click.echo("Agent is not running.")
        return
    
    import requests
    try:
        response = requests.get("http://localhost:8103/api/v1/pipes", timeout=10)
        if response.ok:
            pipes = response.json()
            if not pipes:
                click.echo("No pipes configured.")
                return
            
            click.echo(f"{'ID':<30} {'STATUS':<15} {'SOURCE':<20}")
            click.echo("-" * 65)
            for p in pipes:
                status_color = "green" if p.get("running") else "yellow"
                click.echo(
                    f"{p['id']:<30} "
                    f"{click.style(p.get('state', 'UNKNOWN'), fg=status_color):<15} "
                    f"{p.get('source', 'N/A'):<20}"
                )
        else:
            click.echo(click.style(f"Error: {response.text}", fg="red"))
    except requests.exceptions.ConnectionError:
        click.echo(click.style("Cannot connect to Agent.", fg="red"))
    except Exception as e:
        click.echo(click.style(f"Error: {e}", fg="red"))


@cli.command()
@click.argument("pipe_id", required=False)
def status(pipe_id):
    """Show status of agent or specific pipe."""
    pid = _is_running()
    
    if not pid:
        click.echo("Agent Status: " + click.style("STOPPED", fg="red"))
        return
    
    click.echo("Agent Status: " + click.style(f"RUNNING (PID: {pid})", fg="green"))
    
    if pipe_id:
        import requests
        try:
            response = requests.get(
                f"http://localhost:8103/api/v1/pipes/{pipe_id}",
                timeout=10
            )
            if response.ok:
                data = response.json()
                click.echo(f"\nPipe: {pipe_id}")
                click.echo(f"  State: {data.get('state', 'UNKNOWN')}")
                click.echo(f"  Source: {data.get('source', 'N/A')}")
                click.echo(f"  Sender: {data.get('sender', 'N/A')}")
            else:
                click.echo(click.style(f"Pipe '{pipe_id}' not found.", fg="yellow"))
        except Exception as e:
            click.echo(click.style(f"Error: {e}", fg="red"))
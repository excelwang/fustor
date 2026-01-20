import click
import asyncio
import signal
import uvicorn
import os
import logging
import sys
import subprocess
import time
import json
from pathlib import Path
import secrets
from jose import jwt
from datetime import datetime, timezone
from fustor_registry_client.client import RegistryClient
from .config import register_config

from fustor_common.logging_config import setup_logging
from fustor_common.exceptions import ConfigurationError # Re-use common ConfigurationError
from fustor_common.paths import get_fustor_home_dir # NEW import
from fustor_common import daemon as fustor_daemon

# Define standard directories and file names for registry
HOME_FUSTOR_DIR = get_fustor_home_dir() # Use the common function
REGISTRY_PID_FILE = os.path.join(HOME_FUSTOR_DIR, "registry.pid") # Renamed from fustor_registry.pid
REGISTRY_LOG_FILE = os.path.join(HOME_FUSTOR_DIR, "registry.log") # Renamed from fustor_registry.log


def ensure_registry_token():
    """
    Ensures that a registry API token exists.
    If no token is set in environment variables, generates a random one and stores it in the .env file.
    """
    from .config import register_config
    token = register_config.FUSTOR_REGISTRY_CLIENT_TOKEN

    # If no token is set in environment variables, generate one
    if not token:
        # Generate a random API token
        token = secrets.token_urlsafe(32)  # Generates a secure random token

        # Store it in the .env file in HOME_FUSTOR_DIR
        env_file_path = HOME_FUSTOR_DIR / ".env"

        # Read the current content of the .env file
        env_content = ""
        if env_file_path.exists():
            with open(env_file_path, 'r') as f:
                env_content = f.read()

        # Check if the token is already in the file
        token_line_prefix = "FUSTOR_REGISTRY_CLIENT_TOKEN="
        token_exists = any(line.startswith(token_line_prefix) for line in env_content.split('\n'))

        # If token doesn't exist in the file, append it
        if not token_exists:
            with open(env_file_path, 'a') as f:
                f.write(f"\nFUSTOR_REGISTRY_CLIENT_TOKEN={token}\n")

        # Update the register_config so that the token is available for this session
        os.environ["FUSTOR_REGISTRY_CLIENT_TOKEN"] = token
        return token
    else:
        return token


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
@click.option("--base-url", default="http://127.0.0.1:8101", help="Base URL for the Registry API.")
@click.pass_context
def cli(ctx, base_url: str):
    ctx.ensure_object(dict)
    ctx.obj["BASE_URL"] = base_url
    # Try to read token from temporary file
    token_file_path = Path(HOME_FUSTOR_DIR) / "registry-client-access-token.tmp"
    if token_file_path.exists():
        try:
            with open(token_file_path, 'r') as f:
                stored_token = f.read().strip()
                if stored_token:
                    # Check if the token is still valid (not expired)
                    try:
                        # Decode the token to check its expiration
                        payload = jwt.decode(stored_token, register_config.FUSTOR_CORE_SECRET_KEY, algorithms=[register_config.FUSTOR_CORE_JWT_ALGORITHM])
                        exp = payload.get("exp")
                        if exp:
                            # Check if token is expired
                            if datetime.now(timezone.utc).timestamp() > exp:
                                # Token is expired, remove the temp file and set token to None
                                os.remove(token_file_path)
                                ctx.obj["TOKEN"] = None
                                click.echo("Stored token has expired. Please run 'fustor registry login' again.", err=True)
                            else:
                                # Token is still valid
                                ctx.obj["TOKEN"] = stored_token
                        else:
                            # No expiration claim found, assume token is invalid
                            ctx.obj["TOKEN"] = None
                    except jwt.JWTError:
                        # Token is invalid/corrupted, remove the temp file
                        os.remove(token_file_path)
                        ctx.obj["TOKEN"] = None
                else:
                    ctx.obj["TOKEN"] = None
        except Exception:
            ctx.obj["TOKEN"] = None
    else:
        ctx.obj["TOKEN"] = None

@cli.command()
@click.option("--host", default="127.0.0.1", help="Host address to bind to.")
@click.option("--port", default=8101, type=int, help="Port to bind to.")
@click.option("--reload", is_flag=True, help="Enable auto-reloading.")
def start_fg(host: str, port: int, reload: bool):
    """Starts the Fustor Registry API server in foreground mode."""
    uvicorn.run("fustor_registry.main:app", host=host, port=port, reload=reload)

@cli.command()
@click.option("--reload", is_flag=True, help="Enable auto-reloading of the server on code changes (foreground only).")
@click.option("-p", "--port", default=8101, help="Port to run the server on.")
@click.option("-h", "--host", default="127.0.0.1", help="Host to bind the server to.")
@click.option("-D", "--daemon", is_flag=True, help="Run the service as a background daemon.")
@click.option("-V", "--verbose", is_flag=True, help="Enable verbose (DEBUG level) logging.")
@click.option("--no-console-log", is_flag=True, hidden=True, help="Client: Disable console logging for daemon process.")
def start(reload, port, host, daemon, verbose, no_console_log):
    """Starts the Fustor Registry service (in the foreground by default)."""
    log_level = "DEBUG" if verbose else "INFO"

    # Ensure log directory exists for the REGISTRY_LOG_FILE
    os.makedirs(os.path.dirname(REGISTRY_LOG_FILE), exist_ok=True)

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

        # Ensure API token exists before starting the service daemon
        token = ensure_registry_token()
        logger.info(f"Registry API token ensured for daemon mode (length: {len(token)})")

        click.echo("Starting Fustor Registry in the background...")
        # Use a common daemon launcher function to avoid module path issues
        fustor_daemon.start_daemon(
            service_module_path='fustor_registry.main',
            app_var_name='app',
            pid_file_name='registry.pid',
            log_file_name='registry.log',
            display_name='Fustor Registry',
            port=port,
            host=host,
            verbose=verbose,
            reload=reload
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
        os.makedirs(HOME_FUSTOR_DIR, exist_ok=True) # Ensure Fustor home directory exists for PID file

        # Ensure API token exists before starting the service
        token = ensure_registry_token()
        logger.info(f"Registry API token ensured (length: {len(token)})")

        # Write PID file in both modes, as the actual server process needs to own it
        with open(REGISTRY_PID_FILE, 'w') as f:
            f.write(str(os.getpid()))

        from .main import app as fastapi_app # Import the FastAPI app

        click.echo("\n" + "="*60)
        click.echo("Fustor Registry")
        click.echo(f"Web : http://{host}:{port}")
        click.echo("="*60 + "\n")

        app_to_run = fastapi_app
        if reload:
            app_to_run = "fustor_registry.main:app" # Uvicorn needs string path for reload

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
        logger.info("Fustor Registry is shutting down...")
        click.echo("\nFustor Registry is shutting down...")
    except ConfigurationError as e:
        logger.critical(f"Fustor Registry Configuration Error: {e}", exc_info=True)
        click.echo("="*60)
        click.echo(click.style(f"Fustor Registry Configuration Error: {e}", fg="red"))
        click.echo("Please check your environment variables and .env file in the home directory.")
        click.echo("="*60)
    except Exception as e:
        logger.critical(f"An unexpected error occurred during startup: {e}", exc_info=True)
        click.echo(click.style(f"\nFATAL: An unexpected error occurred: {e}", fg="red"))
    finally:
        if os.path.exists(REGISTRY_PID_FILE):
            os.remove(REGISTRY_PID_FILE)
            logger.info("Removed PID file.")

@cli.command()
@click.option("--email", prompt=True, help="Admin user email.")
@click.option("--password", prompt=True, hide_input=True, help="Admin user password.")
@click.pass_context
def login(ctx, email: str, password: str):
    """Logs in to the Registry and saves the JWT token to a temporary file."""

    async def _login():
        async with RegistryClient(base_url=ctx.obj["BASE_URL"]) as client:
            try:
                token_response = await client.login(email=email, password=password)

                # Save token to a temporary file in HOME_FUSTOR_DIR
                token_file_path = Path(HOME_FUSTOR_DIR) / "registry-client-access-token.tmp"

                with open(token_file_path, 'w') as f:
                    f.write(token_response.access_token)

                click.echo("Login successful. Token saved to temporary file.")
            except Exception as e:
                click.echo(f"Login failed: {e}", err=True)
    asyncio.run(_login())

@cli.group()
def datastore():
    """Manages datastores."""
    pass

@datastore.command("list")
@click.pass_context
def datastore_list(ctx):
    """Lists all datastores."""
    async def _list():
        if not ctx.obj["TOKEN"]:
            click.echo("Error: Not logged in. Please run 'fustor registry login' first or provide --token.", err=True)
            return
        async with RegistryClient(base_url=ctx.obj["BASE_URL"], token=ctx.obj["TOKEN"]) as client:
            datastores = await client.list_datastores()
            datastores_dict = [ds.model_dump() for ds in datastores]
            click.echo(json.dumps(datastores_dict, indent=2, ensure_ascii=False))
    asyncio.run(_list())

@datastore.command("create")
@click.argument("name")
@click.option("--meta", "meta_items", multiple=True, help="Key-value pairs for metadata (e.g., --meta key1=value1 --meta key2=value2).")
@click.option("--visible/--hidden", default=False, help="Visibility of the datastore.")
@click.option("--allow-concurrent-push/--no-concurrent-push", default=False, help="Allow concurrent pushes.")
@click.option("--session-timeout", type=int, default=30, help="Session timeout in seconds.")
@click.pass_context
def datastore_create(ctx, name: str, meta_items: tuple[str], visible: bool, allow_concurrent_push: bool, session_timeout: int):
    """Creates a new datastore."""
    async def _create():
        if not ctx.obj["TOKEN"]:
            click.echo("Error: Not logged in. Please run 'fustor registry login' first or provide --token.", err=True)
            return
        async with RegistryClient(base_url=ctx.obj["BASE_URL"], token=ctx.obj["TOKEN"]) as client:
            try:
                meta_dict = {}
                for item in meta_items:
                    if '=' in item:
                        key, value = item.split('=', 1)
                        meta_dict[key] = value
                    else:
                        click.echo(f"Warning: Invalid meta item format '{item}'. Expected KEY=VALUE.", err=True)
                
                datastore = await client.create_datastore(name=name, meta=meta_dict if meta_dict else None, visible=visible, allow_concurrent_push=allow_concurrent_push, session_timeout_seconds=session_timeout)
                datastore_dict = datastore.model_dump()
                click.echo(json.dumps(datastore_dict, indent=2, ensure_ascii=False))
            except Exception as e:
                click.echo(f"Error creating datastore: {e}", err=True)
    asyncio.run(_create())

@datastore.command("get")
@click.argument("id", type=int)
@click.pass_context
def datastore_get(ctx, id: int):
    """Gets a datastore by ID."""
    async def _get():
        if not ctx.obj["TOKEN"]:
            click.echo("Error: Not logged in. Please run 'fustor registry login' first or provide --token.", err=True)
            return
        async with RegistryClient(base_url=ctx.obj["BASE_URL"], token=ctx.obj["TOKEN"]) as client:
            try:
                datastore = await client.get_datastore(datastore_id=id)
                click.echo(json.dumps(datastore, indent=2, ensure_ascii=False))
            except Exception as e:
                click.echo(f"Error getting datastore: {e}", err=True)
    asyncio.run(_get())

@datastore.command("update")
@click.argument("id", type=int)
@click.option("--name", help="New name for the datastore.")
@click.option("--meta", "meta_items", multiple=True, help="Key-value pairs for metadata (e.g., --meta key1=value1 --meta key2=value2). Existing meta will be overwritten.")
@click.option("--visible/--hidden", help="Visibility of the datastore.")
@click.option("--allow-concurrent-push/--no-concurrent-push", help="Allow concurrent pushes.")
@click.option("--session-timeout", type=int, help="Session timeout in seconds.")
@click.pass_context
def datastore_update(ctx, id: int, name: str, meta_items: tuple[str], visible: bool, allow_concurrent_push: bool, session_timeout: int):
    """Updates a datastore by ID."""
    async def _update():
        if not ctx.obj["TOKEN"]:
            click.echo("Error: Not logged in. Please run 'fustor registry login' first or provide --token.", err=True)
            return
        async with RegistryClient(base_url=ctx.obj["BASE_URL"], token=ctx.obj["TOKEN"]) as client:
            try:
                meta_dict = {}
                for item in meta_items:
                    if '=' in item:
                        key, value = item.split('=', 1)
                        meta_dict[key] = value
                    else:
                        click.echo(f"Warning: Invalid meta item format '{item}'. Expected KEY=VALUE.", err=True)
                
                datastore = await client.update_datastore(datastore_id=id, name=name, meta=meta_dict if meta_dict else None, visible=visible, allow_concurrent_push=allow_concurrent_push, session_timeout_seconds=session_timeout)
                click.echo(json.dumps(datastore, indent=2, ensure_ascii=False))
            except Exception as e:
                click.echo(f"Error updating datastore: {e}", err=True)
    asyncio.run(_update())

@datastore.command("delete")
@click.argument("id", type=int)
@click.pass_context
def datastore_delete(ctx, id: int):
    """Deletes a datastore by ID."""
    async def _delete():
        if not ctx.obj["TOKEN"]:
            click.echo("Error: Not logged in. Please run 'fustor registry login' first or provide --token.", err=True)
            return
        async with RegistryClient(base_url=ctx.obj["BASE_URL"], token=ctx.obj["TOKEN"]) as client:
            try:
                result = await client.delete_datastore(datastore_id=id)
                click.echo(json.dumps(result, indent=2, ensure_ascii=False))
            except Exception as e:
                click.echo(f"Error deleting datastore: {e}", err=True)
    asyncio.run(_delete())

@cli.group()
def apikey():
    """Manages API keys."""
    pass

@apikey.command("list")
@click.pass_context
def apikey_list(ctx):
    """Lists all API keys."""
    async def _list():
        if not ctx.obj["TOKEN"]:
            click.echo("Error: Not logged in. Please run 'fustor registry login' first or provide --token.", err=True)
            return
        async with RegistryClient(base_url=ctx.obj["BASE_URL"], token=ctx.obj["TOKEN"]) as client:
            api_keys = await client.list_api_keys()
            api_keys_dict = [key.model_dump() for key in api_keys]
            click.echo(json.dumps(api_keys_dict, indent=2, ensure_ascii=False))
    asyncio.run(_list())

@apikey.command("create")
@click.argument("name")
@click.argument("datastore_id", type=int)
@click.pass_context
def apikey_create(ctx, name: str, datastore_id: int):
    """Creates a new API key."""
    async def _create():
        if not ctx.obj["TOKEN"]:
            click.echo("Error: Not logged in. Please run 'fustor registry login' first or provide --token.", err=True)
            return
        async with RegistryClient(base_url=ctx.obj["BASE_URL"], token=ctx.obj["TOKEN"]) as client:
            try:
                api_key = await client.create_api_key(name=name, datastore_id=datastore_id)
                api_key_dict = api_key.model_dump()
                click.echo(json.dumps(api_key_dict, indent=2, ensure_ascii=False))
            except Exception as e:
                click.echo(f"Error creating API key: {e}", err=True)
    asyncio.run(_create())

@apikey.command("get")
@click.argument("id", type=int)
@click.pass_context
def apikey_get(ctx, id: int):
    """Gets an API key by ID."""
    async def _get():
        if not ctx.obj["TOKEN"]:
            click.echo("Error: Not logged in. Please run 'fustor registry login' first or provide --token.", err=True)
            return
        async with RegistryClient(base_url=ctx.obj["BASE_URL"], token=ctx.obj["TOKEN"]) as client:
            try:
                # The client.get_api_key currently raises NotImplementedError
                # This command will need to be updated if the API adds a direct get endpoint
                click.echo(f"Getting API key {id} (Note: Direct GET by ID is not yet supported by the API)")
                # For now, we can list all and filter, but this is inefficient for a real API
                api_keys = await client.list_api_keys()
                found_key = next((key for key in api_keys if key.get("id") == id), None)
                if found_key:
                    click.echo(json.dumps(found_key, indent=2, ensure_ascii=False))
                else:
                    click.echo(f"API Key with ID {id} not found.", err=True)
            except Exception as e:
                click.echo(f"Error getting API key: {e}", err=True)
    asyncio.run(_get())

@apikey.command("update")
@click.argument("id", type=int)
@click.argument("name")
@click.pass_context
def apikey_update(ctx, id: int, name: str):
    """Updates an API key by ID."""
    click.echo(f"Updating API key: {id} with new name {name} (Not yet implemented via API)")

@apikey.command("delete")
@click.argument("id", type=int)
@click.pass_context
def apikey_delete(ctx, id: int):
    """Deletes an API key by ID."""
    async def _delete():
        if not ctx.obj["TOKEN"]:
            click.echo("Error: Not logged in. Please run 'fustor registry login' first or provide --token.", err=True)
            return
        async with RegistryClient(base_url=ctx.obj["BASE_URL"], token=ctx.obj["TOKEN"]) as client:
            try:
                result = await client.delete_api_key(key_id=id)
                click.echo(json.dumps(result, indent=2, ensure_ascii=False))
            except Exception as e:
                click.echo(f"Error deleting API key: {e}", err=True)
    asyncio.run(_delete())

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
        logger = logging.getLogger("fustor_registry")
        logger.error(f"Error stopping process with PID {pid}: {e}")
    finally:
        if os.path.exists(REGISTRY_PID_FILE):
            os.remove(REGISTRY_PID_FILE)
            logger = logging.getLogger("fustor_registry")
            logger.info("Removed PID file.")
import click
import uvicorn
import asyncio
import json
import os
from fustor_registry_client.client import RegistryClient

# Define a path for storing the token
TOKEN_FILE = os.path.expanduser("~/.fustor/registry_token")

@click.group()
@click.option("--base-url", default="http://127.0.0.1:8000", help="Base URL for the Registry API.")
@click.option("--token", envvar="FUSTOR_REGISTRY_TOKEN", help="JWT Token for authentication. Can be set via FUSTOR_REGISTRY_TOKEN environment variable.")
@click.pass_context
def cli(ctx, base_url: str, token: str):
    ctx.ensure_object(dict)
    ctx.obj["BASE_URL"] = base_url
    # Prioritize token from command line/env, then from file
    if not token and os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "r") as f:
            token = f.read().strip()
    ctx.obj["TOKEN"] = token

@cli.command()
@click.option("--host", default="127.0.0.1", help="Host address to bind to.")
@click.option("--port", default=8002, type=int, help="Port to bind to.")
@click.option("--reload", is_flag=True, help="Enable auto-reloading.")
def start(host: str, port: int, reload: bool):
    """Starts the Fustor Registry API server."""
    uvicorn.run("fustor_registry.main:app", host=host, port=port, reload=reload)

@cli.command()
@click.option("--email", prompt=True, help="Admin user email.")
@click.option("--password", prompt=True, hide_input=True, help="Admin user password.")
@click.pass_context
def login(ctx, email: str, password: str):
    """Logs in to the Registry and saves the JWT token."""
    async def _login():
        async with RegistryClient(base_url=ctx.obj["BASE_URL"]) as client:
            try:
                token = await client.login(email=email, password=password)
                os.makedirs(os.path.dirname(TOKEN_FILE), exist_ok=True)
                with open(TOKEN_FILE, "w") as f:
                    f.write(token.access_token)
                click.echo("Login successful. Token saved.")
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
            click.echo("Error: Not logged in. Please run 'fustor-registry login' first or provide --token.", err=True)
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
            click.echo("Error: Not logged in. Please run 'fustor-registry login' first or provide --token.", err=True)
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
            click.echo("Error: Not logged in. Please run 'fustor-registry login' first or provide --token.", err=True)
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
            click.echo("Error: Not logged in. Please run 'fustor-registry login' first or provide --token.", err=True)
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
            click.echo("Error: Not logged in. Please run 'fustor-registry login' first or provide --token.", err=True)
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
            click.echo("Error: Not logged in. Please run 'fustor-registry login' first or provide --token.", err=True)
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
            click.echo("Error: Not logged in. Please run 'fustor-registry login' first or provide --token.", err=True)
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
            click.echo("Error: Not logged in. Please run 'fustor-registry login' first or provide --token.", err=True)
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
            click.echo("Error: Not logged in. Please run 'fustor-registry login' first or provide --token.", err=True)
            return
        async with RegistryClient(base_url=ctx.obj["BASE_URL"], token=ctx.obj["TOKEN"]) as client:
            try:
                result = await client.delete_api_key(key_id=id)
                click.echo(json.dumps(result, indent=2, ensure_ascii=False))
            except Exception as e:
                click.echo(f"Error deleting API key: {e}", err=True)
    asyncio.run(_delete())

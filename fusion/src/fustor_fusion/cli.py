import click
import uvicorn

@click.group()
def cli():
    pass

@cli.command()
@click.option("--host", default="127.0.0.1", help="Host address to bind to.")
@click.option("--port", default=8003, type=int, help="Port to bind to.")
@click.option("--reload", is_flag=True, help="Enable auto-reloading.")
@click.option("--log-level", default="info", help="Set the Uvicorn log level.")
def start(host: str, port: int, reload: bool, log_level: str):
    """Starts the Fustor Fusion API server."""
    uvicorn.run("fustor_fusion.main:app", host=host, port=port, reload=reload, log_level=log_level)

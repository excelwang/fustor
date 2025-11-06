from fastapi import APIRouter, FastAPI
from fastapi.staticfiles import StaticFiles
import os
from contextlib import asynccontextmanager

from ..app import App # Import App
from .. import CONFIG_DIR # Import CONFIG_DIR

from . import sources_api, pushers_api, syncs_api, system_handlers, sync_instances_handlers, logs_handlers, metrics_api, configs_api, drivers_api

api_router = APIRouter()

api_router.include_router(configs_api.router, prefix="/configs", tags=["Configuration"])
api_router.include_router(sources_api.router, prefix="/configs/sources", tags=["Sources"])
api_router.include_router(pushers_api.router, prefix="/configs/pushers", tags=["Pushers"])
api_router.include_router(syncs_api.router, prefix="/configs/syncs", tags=["Syncs"])
api_router.include_router(logs_handlers.router, prefix="/logs", tags=["Logs"])
api_router.include_router(system_handlers.router, prefix="/instances", tags=["System"])
api_router.include_router(sync_instances_handlers.router, prefix="/instances", tags=["Instances"])
api_router.include_router(metrics_api.router, prefix="", tags=["Metrics"])
api_router.include_router(drivers_api.router, prefix="/drivers", tags=["Drivers"])

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("DEBUG: Inside startup_event")
    # Ensure CONFIG_DIR is absolute for App initialization
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    absolute_config_dir = os.path.join(project_root, CONFIG_DIR)
    
    app_instance = App(config_dir=absolute_config_dir)
    app.state.app = app_instance
    await app_instance.startup()
    yield
    # --- FIX: Add shutdown logic ---
    # This code runs when the application is shutting down.
    await app.state.app.shutdown()

# Create the FastAPI app
web_app = FastAPI(title="FuAgent API", lifespan=lifespan)

# Include the API router with a prefix
web_app.include_router(api_router, prefix="/api")

# Conditionally serve static files for the UI if the package is installed
try:
    from importlib import resources
    from fastapi.responses import RedirectResponse
    # Find the path to the 'ui' directory within the installed 'fustor_web_ui' package
    ui_dir_path = resources.files('fustor_web_ui') / 'ui'
    web_app.mount("/ui", StaticFiles(directory=str(ui_dir_path), html=True), name="ui")
    # Redirect /ui to /ui/index.html
    @web_app.get("/ui", include_in_schema=False)
    async def redirect_ui():
        return RedirectResponse(url="/ui/index.html")
    print("INFO: 'fustor_agent-web-ui' package found. UI is available at /ui")
except (ImportError, ModuleNotFoundError):
    print("INFO: 'fustor_agent-web-ui' package not installed. UI will not be available.")

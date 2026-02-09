from fastapi import FastAPI, APIRouter, Request, HTTPException, status
from fastapi.responses import FileResponse
import os
import asyncio
from contextlib import asynccontextmanager
from typing import Optional, List

import sys
import logging

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout
)

logger = logging.getLogger(__name__)

# --- Ingestor Service Specific Imports ---
from .core.session_manager import session_manager
from .view_state_manager import view_state_manager
from . import runtime_objects
from fustor_core.event import EventBase

# --- View Manager Module Imports ---
from .view_manager.manager import process_event as process_single_event, cleanup_all_expired_suspects
from .api.views import view_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Application startup initiated.")
    
    # Initialize the Pipe Manager
    from .runtime.pipe_manager import pipe_manager as pm
    runtime_objects.pipe_manager = pm
    
    # Read target configs from environment (set by CLI)
    config_env = os.environ.get("FUSTOR_FUSION_CONFIGS")
    config_list = config_env.split(",") if config_env else None
    
    # Initialize pipes based on targets
    await pm.initialize_pipes(config_list)
    
    # Setup Pipe API routers
    from .api.pipe import setup_pipe_routers
    setup_pipe_routers()
    
    # Start all pipes and receivers
    await pm.start()

    # Start periodic suspect cleanup
    async def periodic_suspect_cleanup():
        logger.info("Starting periodic suspect cleanup task")
        while True:
            try:
                await asyncio.sleep(0.5)
                await cleanup_all_expired_suspects()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in periodic_suspect_cleanup task: {e}")
                await asyncio.sleep(0.5)
            
    suspect_cleanup_task = asyncio.create_task(periodic_suspect_cleanup())
    
    # Start periodic session cleanup
    # We use a default interval since receivers are now per-pipe
    cleanup_interval = 10
    logger.info(f"Starting session cleanup (Interval: {cleanup_interval}s)")
    await session_manager.start_periodic_cleanup(cleanup_interval)

    # Note: View auto-start is now handled by PipeManager via pipe config
    
    # Setup View routers
    from .api.views import setup_view_routers
    setup_view_routers()

    # --- Register Routers ---
    from .api.pipe import pipe_router
    from .api.management import router as management_router
    from .api.views import view_router

    api_v1 = APIRouter()
    api_v1.include_router(pipe_router, prefix="/pipe")
    api_v1.include_router(view_router, prefix="/views")
    api_v1.include_router(management_router)
    
    app.include_router(api_v1, prefix="/api/v1", tags=["v1"])

    logger.info("Application lifespan initialization complete. READY.")
    yield # Ready

    logger.info("Application shutdown initiated.")
    suspect_cleanup_task.cancel()
    
    if runtime_objects.pipe_manager:
        await runtime_objects.pipe_manager.stop()
        
    await session_manager.stop_periodic_cleanup()
    logger.info("Application shutdown complete.")


def create_app() -> FastAPI:
    app = FastAPI(lifespan=lifespan, title="Fusion Storage Engine API", version="1.0.0")
    ui_dir = os.path.dirname(__file__)

    @app.get("/", tags=["Root"])
    async def read_web_api_root():
        return {"message": "Welcome to Fusion Storage Engine Ingest API"}

    @app.get("/view", tags=["UI"])
    async def read_view_ui(request: Request):
        return FileResponse(f"{ui_dir}/view.html")

    return app

app = create_app()
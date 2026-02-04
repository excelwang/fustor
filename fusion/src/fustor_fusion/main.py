from fastapi import FastAPI, APIRouter, Request, HTTPException, status
from fastapi.responses import FileResponse
import os
import asyncio
from contextlib import asynccontextmanager
from typing import Optional

import sys
import logging
import sys

# Configure basic logging for the entire application
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout
)

logger = logging.getLogger(__name__)

# --- Ingestor Service Specific Imports ---
from .config import receivers_config
from .core.session_manager import session_manager
from .view_state_manager import view_state_manager
from . import runtime_objects
from fustor_core.event import EventBase

# --- View Manager Module Imports ---
from .view_manager.manager import process_event as process_single_event, cleanup_all_expired_suspects
from .api.views import view_router


logger = logging.getLogger(__name__) # Re-initialize logger after setting levels
logging.getLogger("fustor_fusion.auth.dependencies").setLevel(logging.DEBUG)
# Silence uvicorn noisy logs
logging.getLogger("uvicorn.error").setLevel(logging.WARNING)
logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Application startup initiated.")
    
    # Initialize the Pipeline Manager
    from .runtime.pipeline_manager import pipeline_manager as pm
    runtime_objects.pipeline_manager = pm
    
    # Initialize pipelines and receivers first (so they are available for router setup)
    await pm.initialize_pipelines()
    
    # Setup Pipeline API routers after pipeline_manager/receivers are available
    from .api.pipe import setup_pipe_routers
    setup_pipe_routers()
    
    await pm.start()

    # Perform initial configuration load
    try:
        receivers_config.reload()
    except Exception as e:
        logger.error(f"Initial configuration load failed: {e}. Aborting startup.")
        raise
    logger.info("Initial configuration load successful.")

    # Start periodic suspect cleanup (Every 0.5 seconds)
    async def periodic_suspect_cleanup():
        logger.info("Starting periodic suspect cleanup task")
        while True:
            try:
                await asyncio.sleep(0.5)
                await cleanup_all_expired_suspects()
            except asyncio.CancelledError:
                logger.info("Periodic suspect cleanup task cancelled")
                break
            except Exception as e:
                logger.error(f"Error in periodic_suspect_cleanup task: {e}", exc_info=True)
                await asyncio.sleep(0.5)
            
    suspect_cleanup_task = asyncio.create_task(periodic_suspect_cleanup())
    
    # Start periodic session cleanup
    min_timeout = 30
    all_receivers = receivers_config.get_all()
    if all_receivers:
        min_timeout = min(r.session_timeout_seconds for r in all_receivers.values())
    
    cleanup_interval = max(1, min_timeout // 5)
    logger.info(f"Starting session cleanup (Interval: {cleanup_interval}s, Min Timeout: {min_timeout}s)")
    await session_manager.start_periodic_cleanup(cleanup_interval)

    # Auto-start enabled views from YAML
    try:
        from .config import views_config
        from .view_manager.manager import get_cached_view_manager
        
        views_config.reload()
        enabled_views = views_config.get_enabled()
        logger.info(f"Auto-starting {len(enabled_views)} enabled views...")
        
        for view_instance_id, config in enabled_views.items():
            try:
                v_group_id = config.view_id
                vm = await get_cached_view_manager(v_group_id)
                if view_instance_id in vm.providers:
                    logger.info(f"View {view_instance_id} already initialized by manager.")
                    continue
                logger.info(f"Verified view {view_instance_id} is active in manager for group {v_group_id}")
            except Exception as e:
                logger.error(f"Failed to auto-start view {view_instance_id}: {e}", exc_info=True)
        
        # After starting views, re-setup API routers
        from .api.views import setup_view_routers
        setup_view_routers()
                
    except Exception as e:
        logger.error(f"Error during view auto-start: {e}", exc_info=True)

    # --- Register Routers (AFTER all pipelinesetup is complete) ---
    # Registering inside lifespan ensure that setup_pipe_routers has populated pipe_router
    # AND setup_view_routers has populated view_router.
    # Note: Modifying app.routes during lifespan is technically "late", but necessary for this plugin architecture.
    
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
    
    if runtime_objects.pipeline_manager:
        await runtime_objects.pipeline_manager.stop()
        
    await session_manager.stop_periodic_cleanup()
    logger.info("Application shutdown complete.")


def create_app() -> FastAPI:
    """
    Factory function to create the FastAPI application.
    """
    app = FastAPI(lifespan=lifespan, title="Fusion Storage Engine API", version="1.0.0")

    # 2. Root & UI Endpoints
    ui_dir = os.path.dirname(__file__)

    @app.get("/", tags=["Root"])
    async def read_web_api_root():
        return {"message": "Welcome to Fusion Storage Engine Ingest API"}

    @app.get("/view", tags=["UI"])
    async def read_view_ui(request: Request):
        return FileResponse(f"{ui_dir}/view.html")

    return app

# Entry point for uvicorn
app = create_app()
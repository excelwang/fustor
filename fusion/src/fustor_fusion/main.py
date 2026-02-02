from fastapi import FastAPI, APIRouter, Request, HTTPException, status
from fastapi.responses import FileResponse
import os
import asyncio
from contextlib import asynccontextmanager
from typing import Optional

import sys
import logging

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

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Application startup initiated.")
    
    # NEW: Initialize the global task manager reference
    # NEW: Initialize the global task manager reference
    # runtime_objects.task_manager = processing_manager # Deprecated: Legacy queue removed
    
    # Initialize the Pipeline Manager
    from .runtime.pipeline_manager import pipeline_manager as pm
    runtime_objects.pipeline_manager = pm
    
    # NEW: Initialize pipelines and receivers first (so they are available for router setup)
    await pm.initialize_pipelines()
    
    # NEW: Setup Pipeline API routers after pipeline_manager/receivers are available
    from .api.pipe import setup_pipe_routers
    setup_pipe_routers()
    
    await pm.start()


    # Perform initial configuration load and start processors
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
                await asyncio.sleep(0.5) # Avoid tight error loop
            
    suspect_cleanup_task = asyncio.create_task(periodic_suspect_cleanup())
    
    # Start periodic session cleanup (Every 5 seconds for fast failover)
    await session_manager.start_periodic_cleanup(5)

    # NEW: Auto-start enabled views from YAML
    try:
        from .config import views_config
        from .view_manager.manager import get_cached_view_manager
        
        # Reload to ensure fresh config
        views_config.reload()
        enabled_views = views_config.get_enabled()
        logger.info(f"Auto-starting {len(enabled_views)} enabled views...")
        
        for view_instance_id, config in enabled_views.items():
            try:
                v_group_id = config.view_id
                
                # Use the centralized cache to ensure consistency with API/Ingestion
                vm = await get_cached_view_manager(v_group_id)
                
                # Check if provider is already loaded (get_cached_view_manager initializes them)
                if view_instance_id in vm.providers:
                    logger.info(f"View {view_instance_id} already initialized by manager.")
                    continue
                
                logger.info(f"Verified view {view_instance_id} is active in manager for group {v_group_id}")
                
            except Exception as e:
                logger.error(f"Failed to auto-start view {view_instance_id}: {e}", exc_info=True)
                
    except Exception as e:
        logger.error(f"Error during view auto-start: {e}", exc_info=True)

    # --- Register Routers (AFTER all pipeline setup is complete) ---
    # Registering inside lifespan ensures that setup_pipe_routers has correctly 
    # populated pipe_router.
    
    # 1. Pipeline Domain (/api/v1/pipe) - Main API
    api_v1 = APIRouter()
    api_v1.include_router(pipe_router, prefix="/pipe")
    api_v1.include_router(view_router, prefix="/views")
    api_v1.include_router(management_router)
    
    # Finally include versioned router in app
    app.include_router(api_v1, prefix="/api/v1", tags=["v1"])
    
    logger.info("Application lifespan initialization complete. READY.")
    yield # Ready

    logger.info("Application shutdown initiated.")
    suspect_cleanup_task.cancel()
    
    # Stop clinical pipeline
    if runtime_objects.pipeline_manager:
        await runtime_objects.pipeline_manager.stop()
        
    await session_manager.stop_periodic_cleanup()
    logger.info("Application shutdown complete.")


# 实例化
app = FastAPI(lifespan=lifespan)

# --- API Routing Version 1 ---
from .api.pipe import pipe_router, setup_pipe_routers
from .api.management import router as management_router
from .api.views import view_router

# NOTE: Routers are now included inside lifespan() after pipeline initialization.



ui_dir = os.path.dirname(__file__)

@app.get("/", tags=["Root"])
async def read_web_api_root():
    return {"message": "Welcome to Fusion Storage Engine Ingest API"}

@app.get("/view", tags=["UI"])
async def read_web_api_root(request: Request):
    return FileResponse(f"{ui_dir}/view.html")
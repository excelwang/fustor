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
from .config import fusion_config, datastores_config
from .core.session_manager import session_manager
from .datastore_state_manager import datastore_state_manager
from .queue_integration import queue_based_ingestor, get_events_from_queue
from .in_memory_queue import memory_event_queue
from .processing_manager import processing_manager
from . import runtime_objects
from fustor_event_model.models import EventBase

# --- View Manager Module Imports ---
from .view_manager.manager import process_event as process_single_event, cleanup_all_expired_suspects
from .api.views import view_router


logger = logging.getLogger(__name__) # Re-initialize logger after setting levels
logging.getLogger("fustor_fusion.auth.dependencies").setLevel(logging.DEBUG)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Application startup initiated.")
    
    # NEW: Initialize the global task manager reference
    runtime_objects.task_manager = processing_manager

    # Perform initial configuration load and start processors
    try:
        datastores_config.reload()
        await processing_manager.sync_tasks(list(datastores_config.get_all_datastores().values()))
    except Exception as e:
        logger.error(f"Initial configuration load failed: {e}. Aborting startup.")
        raise
    logger.info("Initial configuration load successful.")

    # Start periodic suspect cleanup (Every 5 seconds)
    async def periodic_suspect_cleanup():
        logger.info("Starting periodic suspect cleanup task")
        while True:
            try:
                await asyncio.sleep(5)
                await cleanup_all_expired_suspects()
            except asyncio.CancelledError:
                logger.info("Periodic suspect cleanup task cancelled")
                break
            except Exception as e:
                logger.error(f"Error in periodic_suspect_cleanup task: {e}", exc_info=True)
                await asyncio.sleep(5) # Avoid tight error loop
            
    suspect_cleanup_task = asyncio.create_task(periodic_suspect_cleanup())
    
    # Start periodic session cleanup (Every 5 seconds for fast failover)
    await session_manager.start_periodic_cleanup(5)

    # NEW: Auto-start enabled views from YAML
    try:
        from .config import views_config
        from .view_manager.manager import ViewManager
        from fustor_fusion_sdk.loaders import load_view
        
        # Reload to ensure fresh config
        views_config.reload()
        enabled_views = views_config.get_enabled()
        logger.info(f"Auto-starting {len(enabled_views)} enabled views...")
        
        for view_id, config in enabled_views.items():
            try:
                datastore_id = config.datastore_id
                
                # Ensure ViewManager exists
                if datastore_id not in runtime_objects.view_managers:
                     runtime_objects.view_managers[datastore_id] = ViewManager(datastore_id=datastore_id)
                
                vm = runtime_objects.view_managers[datastore_id]
                
                # Check if already running (unlikely on fresh start, but good safety)
                if view_id in vm.providers:
                    continue

                # Start Provider
                provider_class = load_view(config.driver)
                provider = provider_class(
                    view_id=view_id,
                    datastore_id=datastore_id,
                    config=config.driver_params
                )
                await provider.initialize()
                vm.providers[view_id] = provider
                logger.info(f"Auto-started view: {view_id}")
                
            except Exception as e:
                logger.error(f"Failed to auto-start view {view_id}: {e}", exc_info=True)
                
    except Exception as e:
        logger.error(f"Error during view auto-start: {e}", exc_info=True)

    yield # Ready

    logger.info("Application shutdown initiated.")
    suspect_cleanup_task.cancel()
    await processing_manager.stop_all()
    await session_manager.stop_periodic_cleanup()
    logger.info("Application shutdown complete.")


# 实例化
app = FastAPI(lifespan=lifespan)

# --- API Routing Version 1 ---
from .api.ingestion import ingestion_router
from .api.session import session_router
from .api.consistency import consistency_router
from .api.management import router as management_router

# Core versioned router
api_v1 = APIRouter()

# 1. Ingestion Domain (/api/v1/ingest)
ingest_api = APIRouter(prefix="/ingest")
ingest_api.include_router(session_router, prefix="/sessions")
ingest_api.include_router(ingestion_router, prefix="/events")
ingest_api.include_router(consistency_router) # already has /consistency prefix

api_v1.include_router(ingest_api)

# 2. View Domain (/api/v1/views)
api_v1.include_router(view_router, prefix="/views")

# 3. Management Domain (/api/v1/management)
api_v1.include_router(management_router)

# Register the unified v1 router
app.include_router(api_v1, prefix="/api/v1", tags=["v1"])


ui_dir = os.path.dirname(__file__)

@app.get("/", tags=["Root"])
async def read_web_api_root():
    return {"message": "Welcome to Fusion Storage Engine Ingest API"}

@app.get("/view", tags=["UI"])
async def read_web_api_root(request: Request):
    return FileResponse(f"{ui_dir}/view.html")
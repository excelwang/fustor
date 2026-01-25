from fastapi import FastAPI, APIRouter, Request, HTTPException, status
print("DEBUG: FUSTOR_FUSION MAIN MODULE LOADED", flush=True)
from fastapi.responses import FileResponse
import os
import asyncio
from contextlib import asynccontextmanager
from typing import Optional

import sys
import logging

logger = logging.getLogger(__name__)

# --- Ingestor Service Specific Imports ---
from .config import fusion_config
from .auth.cache import api_key_cache
from .jobs.sync_cache import sync_caches_job
from .core.session_manager import session_manager
from .datastore_state_manager import datastore_state_manager
from .queue_integration import queue_based_ingestor, get_events_from_queue
from .in_memory_queue import memory_event_queue
from .processing_manager import processing_manager
from . import runtime_objects
from fustor_event_model.models import EventBase

# --- Parser Module Imports ---
from .parsers.manager import process_event as process_single_event
from .api.views import parser_router


logger = logging.getLogger(__name__) # Re-initialize logger after setting levels
logging.getLogger("fustor_fusion.auth.dependencies").setLevel(logging.DEBUG)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Application startup initiated.")
    
    # NEW: Initialize the global task manager reference
    runtime_objects.task_manager = processing_manager

    # Perform initial cache sync
    try:
        await sync_caches_job()
    except RuntimeError as e:
        logger.error(f"Initial cache synchronization failed: {e}. Aborting startup.")
        raise
    logger.info("Initial cache synchronization successful.")

    # Start processors for initial datastores
    active_datastores = list(api_key_cache._cache.values())
    for datastore_id in active_datastores:
        await processing_manager.ensure_processor(datastore_id)

    # Schedule periodic cache synchronization
    print("DEBUG: DEFINING periodic_sync", flush=True)
    async def periodic_sync():
        print(f"DEBUG: STARTING periodic_sync task. Config Interval: {fusion_config.FUSTOR_FUSION_API_KEY_CACHE_SYNC_INTERVAL_SECONDS}", flush=True)
        while True:
            await asyncio.sleep(fusion_config.FUSTOR_FUSION_API_KEY_CACHE_SYNC_INTERVAL_SECONDS)
            print("DEBUG: Periodic Sync Loop WOKE UP", flush=True)
            logger.info("Performing periodic cache synchronization...")
            try:
                await sync_caches_job()
                # Dynamic check: ensure new datastores have processors
                active_ds = list(api_key_cache._cache.values())
                for ds_id in active_ds:
                    await processing_manager.ensure_processor(ds_id)
            except Exception as e:
                logger.error(f"Periodic cache synchronization failed: {e}", exc_info=True)

    print("DEBUG: CALLING create_task for periodic_sync", flush=True)
    sync_task = asyncio.create_task(periodic_sync())
    print(f"DEBUG: Task created: {sync_task}", flush=True)
    
    # Start periodic session cleanup
    await session_manager.start_periodic_cleanup()

    yield # Ready

    logger.info("Application shutdown initiated.")
    sync_task.cancel()
    await processing_manager.stop_all()
    await session_manager.stop_periodic_cleanup()
    logger.info("Application shutdown complete.")


# 实例化
app = FastAPI(lifespan=lifespan)

# --- API Routing Version 1 ---
from .api.ingestion import ingestion_router
from .api.session import session_router
from .api.consistency import consistency_router

# Core versioned router
api_v1 = APIRouter()

# 1. Ingestion Domain (/api/v1/ingest)
ingest_api = APIRouter(prefix="/ingest")
ingest_api.include_router(session_router, prefix="/sessions")
ingest_api.include_router(ingestion_router, prefix="/events")
ingest_api.include_router(consistency_router) # already has /consistency prefix

api_v1.include_router(ingest_api)

# 2. View Domain (/api/v1/views)
api_v1.include_router(parser_router, prefix="/views")

# Register the unified v1 router
app.include_router(api_v1, prefix="/api/v1", tags=["v1"])


ui_dir = os.path.dirname(__file__)

@app.get("/", tags=["Root"])
async def read_web_api_root():
    return {"message": "Welcome to Fusion Storage Engine Ingest API"}

@app.get("/view", tags=["UI"])
async def read_web_api_root(request: Request):
    return FileResponse(f"{ui_dir}/view.html")
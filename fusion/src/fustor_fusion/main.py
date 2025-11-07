from fastapi import FastAPI, APIRouter, Request, HTTPException, status
from fastapi.responses import FileResponse
import os
import logging
from contextlib import asynccontextmanager
import asyncio
from typing import Optional

import sys

# --- Forcefully suppress SQLAlchemy INFO logs at the earliest possible moment ---
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
root_logger = logging.getLogger()
root_logger.setLevel(logging.WARNING)  # Set the desired level

# Create a console handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.DEBUG)

# Create a formatter and set it for the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)

# Add the handlers to the root logger
root_logger.addHandler(console_handler)

# Silence uvicorn.access logger
# logging.getLogger("uvicorn.access").setLevel(logging.CRITICAL)

# --- Ingestor Service Specific Imports ---
from .config import fusion_config
from .auth.cache import api_key_cache
from .jobs.sync_cache import sync_caches_job
from .core.session_manager import session_manager
from .datastore_state_manager import datastore_state_manager
from .queue_integration import queue_based_ingestor, get_events_from_queue
from .in_memory_queue import memory_event_queue
from fustor_event_model.models import EventBase

# from .runtime import datastore_event_manager # Removed
# from .processing_manager import ParserProcessingTaskManager # Removed
# from .runtime_objects import task_manager as runtime_task_manager # Removed

# --- Parser Module Imports ---
from .parsers.manager import process_event as process_single_event # Renamed to avoid conflict
from .api.views import parser_router


# New polling interval for event processing
EVENT_PROCESSING_POLLING_INTERVAL_SECONDS = 5


async def per_datastore_processing_loop(datastore_id: int):
    """A long-running task that periodically processes events for a single datastore."""
    logger = logging.getLogger(f"fustor.background.datastore.{datastore_id}")
    
    while True:
        try:
            events = await get_events_from_queue(datastore_id)
            if not events:
                logger.debug(f"No new events in queue for datastore {datastore_id}. Waiting...")
                await asyncio.sleep(EVENT_PROCESSING_POLLING_INTERVAL_SECONDS) # Poll every X seconds
                continue # Continue the loop

            logger.info(f"Found {len(events)} events in queue for datastore {datastore_id}. Starting processing.")
            processed_count = 0
            for event_obj in events:
                try:
                    # Store the original event_obj.id before processing, as processed_event is a dict
                    event_id_for_logging = getattr(event_obj, 'id', 'N/A')
                    logger.debug(f"DEBUG: Type of event_obj: {type(event_obj)}")
                    logger.debug(f"DEBUG: event_obj.id: {getattr(event_obj, 'id', 'N/A')}")
                    logger.debug(f"DEBUG: event_obj has rows: {hasattr(event_obj, 'rows')}")
                    processed_event = await process_single_event(event_obj, datastore_id)
                    if all(processed_event.values()): # Assuming all parsers must succeed
                        processed_count += 1
                        logger.info(f"Successfully processed event {event_id_for_logging} for datastore {datastore_id}.")
                    else:
                        logger.warning(f"Event {event_id_for_logging} for datastore {datastore_id} partially processed or failed: {processed_event}")
                        # Depending on policy, might move to dead-letter or retry later
                except Exception as e:
                    logger.error(f"Error processing event {event_id_for_logging} for datastore {datastore_id}: {e}", exc_info=True)
                    # No database session to rollback, just continue
                    
            logger.info(f"Processed {processed_count} events for datastore {datastore_id}.")

        except asyncio.CancelledError:
            logger.info(f"Processing loop for datastore {datastore_id} cancelled.")
            break
        except Exception as e:
            logger.error(f"Error in outer processing loop for datastore {datastore_id}: {e}", exc_info=True)
        
        await asyncio.sleep(EVENT_PROCESSING_POLLING_INTERVAL_SECONDS) # Poll every X seconds


logger = logging.getLogger(__name__) # Re-initialize logger after setting levels
logging.getLogger("fustor_fusion.auth.dependencies").setLevel(logging.DEBUG)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Application startup initiated.")

    # Perform initial cache sync and ensure success for service availability
    logger.info("Performing initial cache synchronization...")
    try:
        await sync_caches_job() # This will now raise RuntimeError on failure
    except RuntimeError as e:
        logger.error(f"Initial cache synchronization failed: {e}. Aborting startup.")
        raise # Re-raise the RuntimeError to stop the application
    logger.info("Initial cache synchronization successful.")

    # Get all active datastores to start processing loops for them
    active_datastores = list(api_key_cache._cache.values()) # Corrected line
    processing_tasks = []
    for datastore_id in active_datastores:
        task = asyncio.create_task(per_datastore_processing_loop(datastore_id))
        processing_tasks.append(task)
        logger.info(f"Started background processing task for datastore {datastore_id}.")

    # Schedule periodic cache synchronization
    async def periodic_sync():
        while True:
            await asyncio.sleep(fusion_config.API_KEY_CACHE_SYNC_INTERVAL_SECONDS)
            logger.info("Performing periodic cache synchronization...")
            await sync_caches_job() # No need to check return value for periodic sync, just log errors

    sync_task = asyncio.create_task(periodic_sync())
    logger.info("Periodic cache synchronization scheduled.")

    # Start periodic session cleanup
    await session_manager.start_periodic_cleanup()

    yield # Application is now ready to serve requests

    logger.info("Application shutdown initiated.")
    sync_task.cancel()
    for task in processing_tasks:
        task.cancel()
    try:
        await asyncio.gather(sync_task, *processing_tasks, return_exceptions=True)
    except asyncio.CancelledError:
        logger.info("Background tasks cancelled.")
    
    # Stop periodic session cleanup
    await session_manager.stop_periodic_cleanup()
    
    logger.info("Application shutdown complete.")


# 修改 FastAPI 实例化，加入 lifespan
app = FastAPI(lifespan=lifespan)

# Update the router structure to have individual routers at the same level
# Import individual routers for direct inclusion
from .api.ingestion import ingestion_router
from .api.session import session_router

router_v1 = APIRouter()

# Include routers at the same level with appropriate prefixes
router_v1.include_router(session_router, prefix="/sessions")       # /session/*
router_v1.include_router(ingestion_router, prefix="/events")      # /events/*

app.include_router(router_v1, prefix="/ingestor-api/v1", tags=["v1"])

# Include parser router at the root level (not under /ingestor-api/v1)
app.include_router(parser_router, prefix="/views", tags=["Views"])
ui_dir = os.path.dirname(__file__)

@app.get("/", tags=["Root"])
async def read_web_api_root():
    return {"message": "Welcome to Fusion Storage Engine Ingest API"}

@app.get("/view", tags=["UI"])
async def read_web_api_root(request: Request):
    return FileResponse(f"{ui_dir}/view.html")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8003,
        log_level="warning",
        access_log=True
    )
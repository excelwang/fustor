from fastapi import FastAPI, APIRouter, Request, HTTPException, status
from fastapi.responses import FileResponse
import os
import asyncio
from contextlib import asynccontextmanager
from typing import Optional, List

import sys
import logging

# --- Configuration and Core Imports ---
from .config.unified import fusion_config
from fustor_core.common import logging_config

logger = logging.getLogger(__name__)

# --- Ingestor Service Specific Imports ---
from .core.session_manager import session_manager
from .view_state_manager import view_state_manager
from . import runtime_objects
from fustor_core.event import EventBase

# --- View Manager Module Imports ---
from .view_manager.manager import process_event as process_single_event, cleanup_all_expired_suspects
from .api.views import view_router
import fustor_management_ui


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Application startup initiated.")
    
    # Initialize the Pipe Manager
    from .runtime.pipe_manager import pipe_manager as pm
    runtime_objects.pipe_manager = pm
    
    # 2. Setup Logging from config
    from fustor_core.common import get_fustor_home_dir
    log_path = get_fustor_home_dir() / "logs" / "fusion.log"
    logging_config.setup_logging(
        log_file_path=str(log_path),
        base_logger_name="fustor_fusion",
        level=fusion_config.logging.level.upper(),
        console_output=True
    )
    
    # Read target configs from environment (deprecated, but kept for migration)
    config_env = os.environ.get("FUSTOR_FUSION_CONFIGS")
    config_list = config_env.split(",") if config_env else None
    
    # 3. Initialize pipes based on targets (if list is None, uses get_enabled_pipes from default.yaml)
    await pm.initialize_pipes(config_list)
    
    # 4. Check for port conflicts and synchronize with global config
    current_port = fusion_config.fusion.port
    current_port_env = os.environ.get("FUSTOR_FUSION_PORT")
    if current_port_env:
        try:
            current_port = int(current_port_env)
        except ValueError:
            pass
            
    logger.info(f"Port configuration: {current_port} (Source: {'Env' if current_port_env else 'YAML'})")
    
    from fustor_receiver_http import HTTPReceiver
    
    logger.info(f"Current receivers in PM: {list(pm._receivers.keys())}")
    
    for sig, receiver in pm._receivers.items():
        logger.info(f"Checking receiver {receiver.id}: type={type(receiver)}, port={receiver.port}")
        if isinstance(receiver, HTTPReceiver) and receiver.port == current_port:
            logger.info(f"Receiver {receiver.id} matches main port {current_port} - attaching routers to main app")
            
            # Attach routers to main app to serve on the same port
            app.include_router(receiver.get_session_router(), prefix="/api/v1/pipe/session")
            app.include_router(receiver.get_ingestion_router(), prefix="/api/v1/pipe/ingest")
            
            # Disable standalone start/stop for this receiver
            async def noop(): 
                logger.info(f"Skipping start for attached receiver {receiver.id}")
            receiver.start = noop
            receiver.stop = noop
    
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
    
    # Setup SIGHUP for reload
    async def _async_reload():
        try:
            # 1. Clear ViewManager cache first to force re-init of drivers
            if hasattr(runtime_objects, 'view_managers'):
                for name, mgr in list(runtime_objects.view_managers.items()):
                    try:
                        if hasattr(mgr, 'close'):
                            if asyncio.iscoroutinefunction(mgr.close):
                                await mgr.close()
                            else:
                                mgr.close()
                    except Exception as e:
                        logger.warning(f"Error closing view manager {name}: {e}")
                runtime_objects.view_managers.clear()
                logger.info("Cleared ViewManager cache")

            # 2. Reload Pipes (re-reading config and restarting pipes)
            await pm.reload()
            
            # 3. Refresh View API routers
            from .api.views import setup_view_routers
            setup_view_routers()
            logger.info("Refreshed View API routers")
            logger.info("Hot reload complete")
        except Exception as e:
            logger.error(f"Hot reload failed: {e}", exc_info=True)

    def handle_reload():
        logger.info("Received SIGHUP - initiating hot reload")
        asyncio.create_task(_async_reload())

    try:
        loop = asyncio.get_running_loop()
        import signal
        if hasattr(signal, 'SIGHUP'):
            loop.add_signal_handler(signal.SIGHUP, handle_reload)
    except Exception as e:
        logger.warning(f"Could not register SIGHUP handler: {e}")

    # Start periodic session cleanup
    cleanup_interval = fusion_config.fusion.session_cleanup_interval
    logger.info(f"Starting session cleanup (Interval: {cleanup_interval}s)")
    await session_manager.start_periodic_cleanup(cleanup_interval)

    # Note: View auto-start is now handled by PipeManager via pipe config
    
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
    
    # --- CORS Middleware ---
    from fastapi.middleware.cors import CORSMiddleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # For management UI, we allow all for now
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    static_dir = fustor_management_ui.get_static_dir()

    # 1. Load configuration
    fusion_config.ensure_loaded()

    # 2. Setup View routers
    from .api.views import setup_view_routers
    setup_view_routers()

    # --- Register Routers ---
    from .api.pipe import pipe_router
    from .api.session import session_router
    from .api.management import router as management_router
    from .api.views import view_router

    api_v1 = APIRouter()
    api_v1.include_router(pipe_router, prefix="/pipe")
    api_v1.include_router(view_router, prefix="/views")
    api_v1.include_router(management_router)
    
    app.include_router(api_v1, prefix="/api/v1", tags=["v1"])

    # --- Static Files ---
    from fastapi.staticfiles import StaticFiles
    app.mount("/management/static", StaticFiles(directory=str(static_dir)), name="static")

    @app.get("/", tags=["Root"])
    async def read_web_api_root():
        return {"message": "Welcome to Fusion Storage Engine Ingest API"}

    @app.get("/view", tags=["UI"])
    async def read_view_ui(request: Request):
        return FileResponse(str(static_dir / "view.html"))

    @app.get("/management", tags=["UI"])
    async def read_management_ui(request: Request):
        return FileResponse(str(static_dir / "management.html"))

    return app

app = create_app()
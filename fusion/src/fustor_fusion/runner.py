import asyncio
import logging
import uvicorn
import signal
from typing import Optional

# Setup logger for the runner itself
logger = logging.getLogger("fustor_fusion.runner")

async def run_fusion(
    host: str, 
    port: int, 
    reload: bool = False, 
    access_log: bool = True,
    app_import_str: str = "fustor_fusion.main:app"
) -> None:
    """
    Fusion supervisor loop - supervises the uvicorn server and restarts it on failure.
    
    Args:
        host: Host to bind to
        port: Port to bind to
        reload: Whether to enable uvicorn's auto-reload (disables internal supervisor)
        access_log: Whether to enable uvicorn's access log
        app_import_str: Import string for the ASGI app
    """
    
    # If reload is requested, we strictly use Uvicorn's reloader and bypass our supervisor
    # because they are mutually exclusive in how they handle the process/signals.
    if reload:
        logger.info("Starting Fusion with uvicorn auto-reloader (internal supervisor disabled)")
        config = uvicorn.Config(
            app_import_str,
            host=host,
            port=port,
            log_config=None, # We configured logging globally already
            access_log=access_log,
            reload=True
        )
        server = uvicorn.Server(config)
        await server.serve()
        return

    # Supervisor Loop
    restart_count = 0
    max_restarts = 100
    
    logger.info(f"Starting Fusion Supervisor on {host}:{port}")
    
    while True:
        try:
            logger.info(f"Starting Fusion Instance (Attempt {restart_count+1})...")
            
            config = uvicorn.Config(
                app_import_str,
                host=host,
                port=port,
                log_config=None,
                access_log=access_log,
                reload=False,
                workers=1,
            )
            server = uvicorn.Server(config)
            
            # This blocks until the server stops (gracefully or via signal)
            await server.serve()
            
            # If we arrive here, the server exited "normally" (e.g. SIGTERM/SIGINT handled by uvicorn)
            # We treat this as a signal to stop the supervisor too.
            if server.should_exit:
                logger.info("Fusion instance stopped gracefully. Shutting down supervisor.")
                break
                
        except asyncio.CancelledError:
            logger.info("Supervisor loop cancelled. Exiting.")
            break
            
        except Exception as e:
            logger.critical(f"Fusion instance CRASHED: {e}", exc_info=True)
            
            restart_count += 1
            if restart_count > max_restarts:
                logger.critical(f"Max restarts ({max_restarts}) exceeded. Supervisor giving up.")
                break
            
            delay = 5.0
            logger.info(f"Restarting Fusion in {delay} seconds...")
            try:
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                break

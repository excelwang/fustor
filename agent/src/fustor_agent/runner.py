"""
Agent runner - entry point for headless operation.
"""
import asyncio
import signal
import logging
from typing import Optional, List

logger = logging.getLogger("fustor_agent")


async def run_agent(config_list: Optional[List[str]] = None):
    """
    Main entry point for running the Agent.
    
    Args:
        config_list: List of pipe config names/paths to start.
                    If None, loads from default.yaml.
    """
    from .app import App
    
    app = App(config_list=config_list)
    
    # Handle shutdown signals
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def handle_signal(sig):
        logger.info(f"Received signal {sig.name}, initiating shutdown...")
        stop_event.set()

    def handle_reload(sig):
        logger.info(f"Received signal {sig.name}, reloading configuration...")
        asyncio.create_task(app.reload_config())

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda s=sig: handle_signal(s))
    
    # Try SIGHUP if available (Unix)
    if hasattr(signal, 'SIGHUP'):
        loop.add_signal_handler(signal.SIGHUP, lambda s=signal.SIGHUP: handle_reload(s))

    try:
        await app.startup()
        logger.info("Agent started successfully. Waiting for signals...")
        
        # Keep running until signaled
        await stop_event.wait()
    except Exception as e:
        logger.critical(f"Agent runtime error: {e}", exc_info=True)
    finally:
        logger.info("Stopping agent...")
        await app.shutdown()

import asyncio
import signal
import logging
import os
from .app import App

logger = logging.getLogger("fustor_agent")

async def run_agent(config_dir: str):
    """
    Main entry point for running the Agent in headless mode.
    """
    app = App(config_dir)
    
    # Handle shutdown signals
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def handle_signal(sig):
        logger.info(f"Received signal {sig.name}, initiating shutdown...")
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda s=sig: handle_signal(s))

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

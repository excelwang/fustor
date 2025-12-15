#!/usr/bin/env python
"""
Generic daemon launcher for Fustor services.
This script is used to launch any Fustor service as a daemon.
It receives service parameters via command line arguments.
"""
import sys
import os
import logging
import importlib
import argparse
from fustor_common.paths import get_fustor_home_dir
from fustor_common.logging_config import setup_logging


def main():
    parser = argparse.ArgumentParser(description='Generic Fustor Service Daemon Launcher')
    parser.add_argument('module_path', help='Module path to import (e.g., fustor_registry.main)')
    parser.add_argument('app_var', help='Application variable name (e.g., app)')
    parser.add_argument('pid_file', help='PID file name (e.g., registry.pid)')
    parser.add_argument('log_file', help='Log file name (e.g., registry.log)')
    parser.add_argument('display_name', help='Display name for the service')
    parser.add_argument('port', type=int, help='Port to run the service on')
    parser.add_argument('host', nargs='?', default='127.0.0.1', help='Host to bind the service to (default: 127.0.0.1)')
    parser.add_argument('--reload', action='store_true', help='Enable auto-reloading for development')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Setup paths
    HOME_FUSTOR_DIR = get_fustor_home_dir()
    PID_FILE = os.path.join(HOME_FUSTOR_DIR, args.pid_file)
    LOG_FILE = os.path.join(HOME_FUSTOR_DIR, args.log_file)
    
    # Setup logging
    log_level = "DEBUG" if args.verbose else "INFO"
    setup_logging(
        log_file_path=LOG_FILE,
        base_logger_name=args.display_name.lower().replace(' ', '_') + '_daemon',
        level=log_level,
        console_output=False  # No console output for daemon
    )
    logger = logging.getLogger(args.display_name.lower().replace(' ', '_') + '_daemon')

    try:
        # Import the service module
        service_module = importlib.import_module(args.module_path)
        app = getattr(service_module, args.app_var)
        
        # Ensure directory exists and create PID file
        os.makedirs(HOME_FUSTOR_DIR, exist_ok=True)
        with open(PID_FILE, 'w') as f:
            f.write(str(os.getpid()))

        print()
        print("="*60)
        print(f"{args.display_name} (Daemon)")
        print(f"Web : http://{args.host}:{args.port}")
        print("="*60)
        print()
        
        logger.info(f"{args.display_name} daemon starting on {args.host}:{args.port}")
        
        # Import and run uvicorn
        import uvicorn
        uvicorn.run(
            app,
            host=args.host,
            port=args.port,
            log_config=None,  # Logging handled separately
            access_log=True,
            reload=args.reload,
        )
    except KeyboardInterrupt:
        logger.info(f"{args.display_name} daemon interrupted")
    except Exception as e:
        logger.critical(f"{args.display_name} daemon error: {e}", exc_info=True)
        print(f"{args.display_name} daemon error: {e}")
    finally:
        # Clean up PID file
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)
            logger.info("PID file removed")


if __name__ == "__main__":
    main()
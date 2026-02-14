import logging
import sys
from importlib.metadata import entry_points
from fastapi import FastAPI
from fustor_fusion.main import _load_management_extensions
from fustor_fusion import runtime_objects

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("verify_fusion_mgmt")

def verify_discovery():
    logger.info("Checking entry points for fustor_fusion.management_routers...")
    eps = entry_points(group="fustor_fusion.management_routers")
    found = False
    for ep in eps:
        logger.info(f"Found entry point: {ep.name} -> {ep.value}")
        if ep.name == "mgmt":
            found = True
    
    if not found:
        logger.error("fustor-fusion-mgmt entry point NOT found!")
        return False
    
    logger.info("Entry point discovery: SUCCESS")
    return True

def verify_loading():
    app = FastAPI()
    logger.info("Attempting to load management extensions...")
    _load_management_extensions(app)
    
    # Check if routes were added
    mgmt_routes = [r for r in app.routes if hasattr(r, 'path') and r.path.startswith("/api/v1/mgmt")]
    if not mgmt_routes:
        logger.error("No management routes found in app!")
        return False
    
    logger.info(f"Found {len(mgmt_routes)} management routes.")
    for r in mgmt_routes:
        logger.info(f"  Route: {r.path}")
    
    # Check if fallback registry is populated
    if runtime_objects.on_command_fallback is None:
        logger.error("runtime_objects.on_command_fallback is still None!")
        return False
    
    logger.info("on_command_fallback registry: SUCCESS")
    return True

if __name__ == "__main__":
    if verify_discovery() and verify_loading():
        logger.info("Fusion Management Split Verification: ALL OK")
        sys.exit(0)
    else:
        logger.error("Fusion Management Split Verification: FAILED")
        sys.exit(1)

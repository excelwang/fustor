"""
Dynamic loading of Fustor View Drivers.
"""
import importlib
import logging
from typing import Type
from .interfaces import ViewDriver

logger = logging.getLogger(__name__)

def load_view(driver_name: str) -> Type[ViewDriver]:
    """
    Load a ViewDriver class by driver name.
    
    Current implementation supports built-in drivers:
    - 'fs': fustor_view_fs.driver.FSViewDriver
    
    Future implementations could support external plugins via entry points.
    """
    if driver_name == "fs":
        try:
             module = importlib.import_module("fustor_view_fs.driver")
             return getattr(module, "FSViewDriver")
        except (ImportError, AttributeError) as e:
             logger.error(f"Failed to load 'fs' driver: {e}")
             raise ValueError(f"View driver 'fs' (fustor-view-fs) is not installed or invalid.")

    # Generic loading attempt (e.g. fustor_view_<driver>.driver)
    try:
        module_name = f"fustor_view_{driver_name}.driver"
        module = importlib.import_module(module_name)
        
        # Look for explicit PROVIDER_CLASS
        if hasattr(module, "PROVIDER_CLASS"):
            return module.PROVIDER_CLASS
        
        # Fallback: search for subclass of ViewDriver
        for name, obj in module.__dict__.items():
            if isinstance(obj, type) and issubclass(obj, ViewDriver) and obj is not ViewDriver:
                return obj
                
    except ImportError as e:
        logger.error(f"Failed to load generic driver '{driver_name}': {e}")
        
    raise ValueError(f"View driver '{driver_name}' not found.")

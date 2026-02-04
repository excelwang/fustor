"""
Dynamic loading of Fustor View Drivers.
"""
import importlib
import logging
from typing import Type
from .interfaces import ViewProvider

logger = logging.getLogger(__name__)

def load_view(driver_name: str) -> Type[ViewProvider]:
    """
    Load a ViewProvider class by driver name.
    
    Current implementation supports built-in drivers:
    - 'fs': fustor_view_fs.provider.FSViewProvider
    
    Future implementations could support external plugins via entry points.
    """
    if driver_name == "fs":
        try:
             module = importlib.import_module("fustor_view_fs.provider")
             return getattr(module, "FSViewProvider")
        except (ImportError, AttributeError) as e:
             logger.error(f"Failed to load 'fs' driver: {e}")
             raise ValueError(f"View driver 'fs' (fustor-view-fs) is not installed or invalid.")

    # Generic loading attempt (e.g. fustor_view_<driver>.provider)
    try:
        module_name = f"fustor_view_{driver_name}.provider"
        module = importlib.import_module(module_name)
        
        # Look for explicit PROVIDER_CLASS
        if hasattr(module, "PROVIDER_CLASS"):
            return module.PROVIDER_CLASS
        
        # Fallback: search for subclass of ViewProvider
        for name, obj in module.__dict__.items():
            if isinstance(obj, type) and issubclass(obj, ViewProvider) and obj is not ViewProvider:
                return obj
                
    except ImportError as e:
        logger.error(f"Failed to load generic driver '{driver_name}': {e}")
        
    raise ValueError(f"View driver '{driver_name}' not found.")

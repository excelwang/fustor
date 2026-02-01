"""
This module holds globally accessible runtime objects to avoid circular imports.
These objects are initialized during the application startup lifespan.
"""

from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .processing_manager import ProcessingManager
    from .runtime.pipeline_manager import PipelineManager

# Using generic type here or TYPE_CHECKING to avoid import cycle
task_manager: Optional['ProcessingManager'] = None
pipeline_manager: Optional['PipelineManager'] = None

# Global storage for active ViewManagers (keyed by datastore_id)
# This is populated at runtime as views are started.
view_managers: dict = {}
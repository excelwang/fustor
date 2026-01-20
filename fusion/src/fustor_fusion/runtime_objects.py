"""
This module holds globally accessible runtime objects to avoid circular imports.
These objects are initialized during the application startup lifespan.
"""

from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .processing_manager import ProcessingManager

# Using generic type here or TYPE_CHECKING to avoid import cycle
task_manager: Optional['ProcessingManager'] = None
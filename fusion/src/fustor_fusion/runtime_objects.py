"""
This module holds globally accessible runtime objects to avoid circular imports.
These objects are initialized during the application startup lifespan.
"""

from typing import Optional
from .processing_manager import ParserProcessingTaskManager

task_manager: Optional[ParserProcessingTaskManager] = None

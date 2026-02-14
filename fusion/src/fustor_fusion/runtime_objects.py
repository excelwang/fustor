"""
This module holds globally accessible runtime objects to avoid circular imports.
These objects are initialized during the application startup lifespan.
"""

from typing import Optional, TYPE_CHECKING, Callable

if TYPE_CHECKING:

    from .runtime.pipe_manager import PipeManager

# Using generic type here or TYPE_CHECKING to avoid import cycle
# task_manager removed
task_manager = None
pipe_manager: Optional['PipeManager'] = None

# Global storage for active ViewManagers (keyed by view_id/group_id)
# This is populated at runtime as views are started.
view_managers: dict = {}

# Mgmt Fallback registry (GAP-P0-3)
on_command_fallback: Optional[Callable] = None
"""
Fustor Event Model - DEPRECATED

This package is deprecated. Please use fustor_core.event instead.

All exports are re-exported from fustor_core.event for backward compatibility.
"""
import warnings

warnings.warn(
    "fustor_event_model is deprecated. Use fustor_core.event instead.",
    DeprecationWarning,
    stacklevel=2
)

# Re-export from fustor_core.event for backward compatibility
from fustor_core.event import EventBase, EventType, MessageSource
from fustor_core.event.base import InsertEvent, UpdateEvent, DeleteEvent

__all__ = [
    "EventBase",
    "EventType",
    "MessageSource",
    "InsertEvent",
    "UpdateEvent",
    "DeleteEvent",
]

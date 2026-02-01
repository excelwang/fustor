# src/fustor_agent/services/drivers/pusher_driver.py
"""
DEPRECATED: This module is deprecated.

Use fustor_agent.services.drivers.sender_driver instead.
The "pusher" terminology has been replaced with "sender" in V2 architecture.
"""
import warnings

warnings.warn(
    "pusher_driver module is deprecated. Use sender_driver instead.",
    DeprecationWarning,
    stacklevel=2
)

# Re-export from new module for backward compatibility
from .sender_driver import SenderDriverService as PusherDriverService

__all__ = ["PusherDriverService"]

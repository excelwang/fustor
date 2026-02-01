# src/fustor_agent/services/configs/pusher.py
"""
DEPRECATED: This module is deprecated.

Use fustor_agent.services.configs.sender instead.
The "pusher" terminology has been replaced with "sender" in V2 architecture.
"""
import warnings

warnings.warn(
    "pusher module is deprecated. Use sender instead.",
    DeprecationWarning,
    stacklevel=2
)

# Re-export from new module for backward compatibility
from .sender import SenderConfigService as PusherConfigService

__all__ = ["PusherConfigService"]
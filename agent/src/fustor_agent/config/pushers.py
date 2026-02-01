# agent/src/fustor_agent/config/pushers.py
"""
DEPRECATED: Use senders.py instead.

This module is kept for backward compatibility.
All functionality has been moved to senders.py.
"""
import warnings

warnings.warn(
    "fustor_agent.config.pushers is deprecated. Use fustor_agent.config.senders instead.",
    DeprecationWarning,
    stacklevel=2
)

# Re-export everything from senders for backward compatibility
from .senders import (
    SendersConfigLoader as PushersConfigLoader,
    senders_config as pushers_config,
    SenderConfig as PusherConfig,
)

__all__ = [
    "PushersConfigLoader",
    "pushers_config",
]

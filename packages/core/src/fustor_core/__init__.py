"""
Fustor Core - Foundation module for the Fustor data synchronization platform.

This package provides:
- Event models and types (event/)
- Pipeline abstractions (pipeline/)
- Transport abstractions (transport/)
- Time synchronization (clock/)
- Common utilities (common/)
- Driver base classes (drivers.py)
- Exception hierarchy (exceptions.py)
- Configuration models (models/)
"""

# Re-export key modules for convenience
from . import common
from . import event
from . import clock
from . import pipeline
from . import transport
from . import models

# Re-export commonly used classes at package level
from .event import EventBase, EventType, MessageSource
from .clock import LogicalClock
from .pipeline import Pipeline, PipelineState, PipelineContext, Handler
from .transport import Sender, Receiver
from .exceptions import (
    FustorException,
    ConfigError,
    NotFoundError,
    ConflictError,
    DriverError,
    StateConflictError,
    ValidationError,
)
from .drivers import SourceDriver, PusherDriver, ViewDriver

__all__ = [
    # Submodules
    "common",
    "event",
    "clock",
    "pipeline",
    "transport",
    "models",
    # Event types
    "EventBase",
    "EventType",
    "MessageSource",
    # Clock
    "LogicalClock",
    # Pipeline
    "Pipeline",
    "PipelineState",
    "PipelineContext",
    "Handler",
    # Transport
    "Sender",
    "Receiver",
    # Exceptions
    "FustorException",
    "ConfigError",
    "NotFoundError",
    "ConflictError",
    "DriverError",
    "StateConflictError",
    "ValidationError",
    # Drivers (legacy, for compatibility)
    "SourceDriver",
    "PusherDriver",
    "ViewDriver",
]

"""
Fustor Core - Foundation module for the Fustor data synchronization platform.

This package provides:
- Event models and types (event/)
- Pipe abstractions (pipe/)
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
from . import pipe
from . import transport
from . import models

# Re-export commonly used classes at package level
from .event import EventBase, EventType, MessageSource
from .clock import LogicalClock
from .pipe import FustorPipe, PipeState, PipeContext, Handler
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
from .drivers import SourceDriver, SenderDriver, ViewDriver
from .supervisor import ComponentSupervisor, ComponentState, ComponentWrapper, StartResult

__all__ = [
    # Submodules
    "common",
    "event",
    "clock",
    "pipe",
    "transport",
    "models",
    # Event types
    "EventBase",
    "EventType",
    "MessageSource",
    # Clock
    "LogicalClock",
    # Pipe
    "FustorPipe",
    "PipeState",
    "PipeContext",
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
    # Drivers
    "SourceDriver",
    "SenderDriver",
    "ViewDriver",
    # Supervisor
    "ComponentSupervisor",
    "ComponentState",
    "ComponentWrapper",
    "StartResult",
]

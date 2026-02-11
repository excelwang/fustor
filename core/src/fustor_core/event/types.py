"""
Event type definitions for Fustor.
Migrated from fustor_event_model.models
"""
from enum import Enum


class EventType(Enum):
    """Type of data change event."""
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"


class MessageSource(str, Enum):
    """
    Source type of the message, used for consistency arbitration.
    
    - REALTIME: Events from real-time monitoring (e.g., inotify, CDC)
    - SNAPSHOT: Events from initial full scan
    - AUDIT: Events from periodic consistency check
    - on_demand_job: Data found via on-demand user scan
    """
    REALTIME = "realtime"
    SNAPSHOT = "snapshot"
    AUDIT = "audit"
    on_demand_job = "on_demand_job" #TODO captive letters

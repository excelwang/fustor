from enum import Enum, Flag, auto
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field

class EventBusState(str, Enum):
    IDLE = "IDLE"
    PRODUCING = "PRODUCING"
    ERROR = "ERROR"

class PipeState(Flag):
    """
    Enumeration for the state of a pipe instance.
    """
    STOPPED = 0
    STARTING = auto()
    SNAPSHOT_SYNC = auto()
    MESSAGE_SYNC = auto()
    AUDIT_PHASE = auto()
    SENTINEL_SWEEP = auto()
    RUNNING_CONF_OUTDATE = auto()
    STOPPING = auto()
    ERROR = auto()
    RECONNECTING = auto()

class EventBusInstance(BaseModel):
    id: str
    source_name: str
    state: EventBusState
    info: str
    statistics: Dict[str, Any]

class PipeInstanceDTO(BaseModel):
    id: str
    state: PipeState
    info: str
    statistics: Dict[str, Any]
    bus_info: Optional[EventBusInstance] = None
    bus_id: Optional[str] = None
    task_id: Optional[str] = None
    current_role: Optional[str] = None


class AgentState(BaseModel):
    agent_id: str = Field(..., description="The unique identifier for the agent.")
    pipes: Dict[str, PipeInstanceDTO] = Field(
        default_factory=dict, 
        description="A dictionary of all pipes, keyed by their ID."
    )
    event_buses: Dict[str, EventBusInstance] = Field(default_factory=dict, description="A dictionary of all active event buses, keyed by their ID.")
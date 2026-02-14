from enum import Enum, Flag, auto
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field
from dataclasses import dataclass

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


@dataclass
class SessionInfo:
    """
    Information about an active session between Agent and Fusion.
    
    Using dataclass for monorepo-wide backward compatibility with positional arguments.
    """
    session_id: str
    task_id: str
    view_id: str
    role: str  # 'leader' or 'follower'
    created_at: float
    last_heartbeat: float
    can_realtime: bool = False
    source_uri: Optional[str] = None
    audit_interval_sec: Optional[float] = None
    sentinel_interval_sec: Optional[float] = None

    @property
    def pipe_id(self) -> str:
        """Deprecated alias for view_id."""
        return self.view_id

class SessionInfoDTO(BaseModel):
    """Pydantic version of SessionInfo for API responses."""
    session_id: str
    task_id: str
    view_id: str
    role: str
    created_at: float
    last_heartbeat: float
    can_realtime: bool = False
    source_uri: Optional[str] = None
    audit_interval_sec: Optional[float] = None
    sentinel_interval_sec: Optional[float] = None
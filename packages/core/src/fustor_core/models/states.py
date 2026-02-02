from enum import Enum, Flag, auto
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field

class EventBusState(str, Enum):
    IDLE = "IDLE"
    PRODUCING = "PRODUCING"
    ERROR = "ERROR"

class SyncState(Flag):
    """
    Enumeration for the state of a sync instance.
    Deprecated: Use PipelineState in fustor_core.pipeline instead.
    """
    STOPPED = 0
    STARTING = auto()
    SNAPSHOT_SYNC = auto()
    MESSAGE_SYNC = auto()
    AUDIT_SYNC = auto()
    SENTINEL_SWEEP = auto()
    RUNNING_CONF_OUTDATE = auto()
    STOPPING = auto()
    ERROR = auto()
    RECONNECTING = auto()

# Alias for terminology transition
PipelineState = SyncState

class EventBusInstance(BaseModel):
    id: str
    source_name: str
    state: EventBusState
    info: str
    statistics: Dict[str, Any]

class PipelineInstanceDTO(BaseModel):
    id: str
    state: SyncState
    info: str
    statistics: Dict[str, Any]
    bus_info: Optional[EventBusInstance] = None
    bus_id: Optional[str] = None
    task_id: Optional[str] = None
    current_role: Optional[str] = None

# Backward compatibility alias
SyncInstanceDTO = PipelineInstanceDTO


class AgentState(BaseModel):
    agent_id: str = Field(..., description="The unique identifier for the agent.")
    pipeline_tasks: Dict[str, PipelineInstanceDTO] = Field(
        default_factory=dict, 
        description="A dictionary of all pipeline tasks, keyed by their ID.",
        validation_alias='sync_tasks'
    )
    event_buses: Dict[str, EventBusInstance] = Field(default_factory=dict, description="A dictionary of all active event buses, keyed by their ID.")

    @property
    def sync_tasks(self) -> Dict[str, PipelineInstanceDTO]:
        """Deprecated property for backward compatibility."""
        return self.pipeline_tasks
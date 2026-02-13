from typing import Dict, Optional, List, Any, Protocol, Set
from dataclasses import dataclass
import asyncio

@dataclass
class SessionInfo:
    session_id: str
    view_id: str
    last_activity: float
    created_at: float
    task_id: Optional[str] = None
    allow_concurrent_push: Optional[bool] = None
    session_timeout_seconds: Optional[int] = None
    client_ip: Optional[str] = None
    source_uri: Optional[str] = None
    pending_commands: List[Dict[str, Any]] = None  # Queue of commands for the Agent
    pending_scans: Set[str] = None  # Paths currently being scanned

    def __post_init__(self):
        if self.pending_commands is None:
            self.pending_commands = []
        if self.pending_scans is None:
            self.pending_scans = set()
    can_realtime: bool = False
    agent_status: Optional[Dict[str, Any]] = None  # Cached agent status from heartbeat
    cleanup_task: Optional[asyncio.Task] = None


class SessionManagerInterface(Protocol):
    """
    Interface for managing user sessions.
    """
    async def create_session_entry(self, view_id: str, session_id: str, 
                                 task_id: Optional[str] = None, 
                                 client_ip: Optional[str] = None,
                                 allow_concurrent_push: Optional[bool] = None,
                                 session_timeout_seconds: Optional[int] = None) -> SessionInfo:
        ...

    async def keep_session_alive(self, view_id: str, session_id: str, 
                               client_ip: Optional[str] = None) -> Optional[SessionInfo]:
        ...

    async def get_session_info(self, view_id: str, session_id: str) -> Optional[SessionInfo]:
        ...

    async def get_view_sessions(self, view_id: str) -> Dict[str, SessionInfo]:
        ...

    async def remove_session(self, view_id: str, session_id: str) -> bool:
        ...

    async def cleanup_expired_sessions(self):
        ...

    async def terminate_session(self, view_id: str, session_id: str) -> bool:
        ...

    async def start_periodic_cleanup(self, interval_seconds: int = 60):
        ...

    async def stop_periodic_cleanup(self):
        ...

class ViewDriver(Protocol):
    """
    Interface for a View Driver.
    Views expose content via specific protocols (e.g. FUSE/NFS).
    """
    def __init__(self, id: str, view_id: str, **kwargs):
        ...

    async def initialize(self) -> None:
        """Initialize resources (mount points, network listeners)."""
        ...

    async def cleanup(self) -> None:
        """Release resources (unmount, stop listeners)."""
        ...
        
    async def on_session_start(self, session_id: str) -> None:
        """
        Called when a new write session starts.
        Used to clear caches or prepare for incoming data.
        """
        ...
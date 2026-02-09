# agent/tests/runtime/mocks.py
from typing import Iterator, List, Any, Dict, Tuple
from fustor_core.pipe.handler import SourceHandler
from fustor_core.pipe.sender import SenderHandler

class MockSourceHandler(SourceHandler):
    """Mock source handler for testing."""
    
    schema_name = "test"
    schema_version = "1.0"
    
    def __init__(self, events: List[Any] = None, handler_id: str = "mock-source"):
        super().__init__(handler_id, {})
        self.events = events or [{"index": i} for i in range(10)]
        self.snapshot_calls = 0
        self.message_calls = 0
        self.audit_calls = 0
    
    def get_snapshot_iterator(self, **kwargs) -> Iterator[Any]:
        self.snapshot_calls += 1
        return iter(self.events)
    
    def get_message_iterator(self, start_position: int = -1, **kwargs):
        self.message_calls += 1
        import time
        stop_event = kwargs.get("stop_event")
        
        def generator():
            # For testing purposes, we don't want an infinite loop in sync mode unless requested
            # We yield once and stop if needed
            yield from []
        
        return generator()
    
    def get_audit_iterator(self, **kwargs) -> Iterator[Any]:
        self.audit_calls += 1
        return iter([])

    def reset(self):
        """Reset all call counters."""
        self.snapshot_calls = 0
        self.message_calls = 0
        self.audit_calls = 0


class MockSenderHandler(SenderHandler):
    """Mock sender handler for testing."""
    
    schema_name = "test-sender"
    
    def __init__(self, handler_id: str = "mock-sender"):
        super().__init__(handler_id, {})
        self.session_id = "test-session"
        self.session_created = False
        self.role = "follower"
        self.batches_sent: List[List[Any]] = []
        self.batches: List[Tuple[List[Any], Dict[str, Any]]] = []
        self.heartbeat_calls = 0
        self.session_closed = False
    
    async def create_session(
        self, task_id: str, source_type: str = "test",
        session_timeout_seconds: int = 30, **kwargs
    ) -> Tuple[str, Dict[str, Any]]:
        self.session_created = True
        return self.session_id, {"role": self.role, "suggested_heartbeat_interval_seconds": 0.1}
    
    async def send_heartbeat(self, session_id: str, **kwargs) -> Dict[str, Any]:
        self.heartbeat_calls += 1
        return {"role": self.role, "session_id": session_id}
    
    async def send_batch(
        self, session_id: str, events: List[Any],
        batch_context: Dict[str, Any] = None
    ) -> Tuple[bool, Dict[str, Any]]:
        self.batches_sent.append(events.copy())
        self.batches.append((events.copy(), batch_context))
        return True, {"count": len(events)}
    
    async def close_session(self, session_id: str) -> bool:
        self.session_created = False
        self.session_closed = True
        return True
    
    async def get_latest_committed_index(self, session_id: str) -> int:
        return 0

    def reset(self):
        """Reset all call counters and storage."""
        self.session_created = False
        self.session_closed = False
        self.batches_sent.clear()
        self.batches.clear()
        self.heartbeat_calls = 0

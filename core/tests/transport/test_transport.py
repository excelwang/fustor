import pytest
from unittest.mock import MagicMock
from fustor_core.transport.receiver import Receiver, ReceiverRegistry
from fustor_core.transport.sender import Sender

class ConcreteReceiver(Receiver):
    async def start(self): pass
    async def stop(self): pass
    async def validate_credential(self, credential):
        return "pipe_1" if credential.get("key") == "valid" else None
    def register_callbacks(self, **callbacks): pass
    def register_api_key(self, api_key, pipe_id): pass

@pytest.mark.asyncio
async def test_receiver_base():
    r = ConcreteReceiver("r1", "localhost", 8080, {"admin": "secret"})
    assert r.get_address() == "localhost:8080"
    assert await r.validate_credential({"key": "valid"}) == "pipe_1"
    assert await r.validate_credential({"key": "invalid"}) is None
    
    # Hooks
    await r.on_session_created("s1", "p1")
    await r.on_session_closed("s1", "p1")
    r.mount_router(None)
    
    handler = MagicMock()
    r.set_event_handler(handler)
    assert r._event_handler is handler

def test_receiver_registry():
    ReceiverRegistry.register("test", ConcreteReceiver)
    assert "test" in ReceiverRegistry.available_drivers()
    
    r = ReceiverRegistry.create("test", receiver_id="r2", bind_host="0.0.0.0", port=90, credentials={})
    assert isinstance(r, ConcreteReceiver)
    assert r.id == "r2"
    
    with pytest.raises(KeyError):
        ReceiverRegistry.create("unknown")

class ConcreteSender(Sender):
    async def connect(self): pass
    async def create_session(self, task_id, source_type=None, session_timeout_seconds=None):
        return {"session_id": "s1"}
    async def _send_events_impl(self, events, source_type, is_end, metadata=None):
        return {"count": len(events)}
    async def heartbeat(self):
        return {"status": "ok"}

@pytest.mark.asyncio
async def test_sender_base():
    s = ConcreteSender("s1", "http://fusion:8080", {"key": "abc"})
    assert s.endpoint == "http://fusion:8080"
    
    res = await s.send_events([object(), object()], source_type="snapshot")
    assert res["count"] == 2
    
    # Test error metrics
    class ErrorSender(ConcreteSender):
        async def _send_events_impl(self, *args, **kwargs):
            raise ValueError("fail")
            
    es = ErrorSender("es1", "", {})
    with pytest.raises(ValueError):
        await es.send_events([])
        
    # Optional hooks
    assert await s.signal_audit_start() is False
    assert await s.signal_audit_end() is False
    assert await s.get_sentinel_tasks() is None
    assert await s.submit_sentinel_results({}) is True
    
    await s.close_session()
    await s.close()

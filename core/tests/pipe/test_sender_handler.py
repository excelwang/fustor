import pytest
from typing import Tuple, Dict, Any, List
from fustor_core.pipe.sender import SenderHandler
from fustor_core.event import EventBase

class ConcreteSenderHandler(SenderHandler):
    async def create_session(self, task_id, source_type, session_timeout_seconds=30, **kwargs):
        return ("s1", {"meta": "data"})
    async def send_heartbeat(self, session_id, **kwargs):
        return {"role": "leader"}
    async def send_batch(self, session_id, events, batch_context=None):
        return (True, {"sent": len(events)})
    async def close_session(self, session_id):
        return True
    async def get_latest_committed_index(self, session_id):
        return 42

@pytest.mark.asyncio
async def test_sender_handler_defaults():
    sh = ConcreteSenderHandler("sh1", {})
    
    # Test abstract implementations
    sid, meta = await sh.create_session("t1", "fs")
    assert sid == "s1"
    assert meta == {"meta": "data"}
    
    assert (await sh.send_heartbeat("s1"))["role"] == "leader"
    
    success, res = await sh.send_batch("s1", [])
    assert success is True
    
    assert await sh.close_session("s1") is True
    assert await sh.get_latest_committed_index("s1") == 42
    
    # Test base implementations
    success, msg = await sh.test_connection()
    assert success is True
    
    success, msg = await sh.check_privileges()
    assert success is True

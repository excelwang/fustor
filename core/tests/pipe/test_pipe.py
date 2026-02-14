import pytest
from fustor_core.pipe.pipe import Pipe, PipeState

class ConcretePipe(Pipe):
    async def start(self):
        self._set_state(PipeState.RUNNING)
    async def stop(self):
        self._set_state(PipeState.STOPPED)
    async def on_session_created(self, session_id, **kwargs):
        self.session_id = session_id
    async def on_session_closed(self, session_id):
        self.session_id = None

@pytest.mark.asyncio
async def test_pipe_basics():
    p = ConcretePipe("p1", {"session_timeout_seconds": 10})
    assert p.id == "p1"
    assert p.session_timeout_seconds == 10
    assert p.state == PipeState.STOPPED
    assert str(p) == "Pipe(p1, state=STOPPED)"
    
    await p.start()
    assert p.state == PipeState.RUNNING
    assert p.is_running() is True
    
    dto = p.get_dto()
    assert dto["id"] == "p1"
    assert dto["state"] == "RUNNING"
    
    await p.on_session_created("s123")
    assert p.has_active_session() is True
    assert p.session_id == "s123"
    
    await p.on_session_closed("s123")
    assert p.has_active_session() is False
    
    await p.stop()
    assert p.state == PipeState.STOPPED
    assert p.is_running() is False

@pytest.mark.asyncio
async def test_pipe_composite_states():
    p = ConcretePipe("p2", {})
    
    p._set_state(PipeState.RUNNING | PipeState.SNAPSHOT_SYNC)
    assert p.is_running() is True
    
    p._set_state(PipeState.STOPPED | PipeState.ERROR, info="failed")
    assert p.is_running() is False
    assert p.info == "failed"
    
    p._set_state(PipeState.RUNNING | PipeState.CONF_OUTDATED)
    assert p.is_outdated() is True
    assert p.is_running() is True

@pytest.mark.asyncio
async def test_pipe_restart():
    p = ConcretePipe("p3", {})
    # Manual start to set up
    await p.start()
    assert p.state == PipeState.RUNNING
    
    # Test restart
    await p.restart()
    assert p.state == PipeState.RUNNING

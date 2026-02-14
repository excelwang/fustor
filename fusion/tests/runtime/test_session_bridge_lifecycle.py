"""
Tests for SessionBridge — session lifecycle and leader promotion.
"""
import pytest
import pytest_asyncio
import asyncio
from fustor_core.pipe.handler import ViewHandler
from fustor_fusion.runtime import FusionPipe
from fustor_fusion.runtime.session_bridge import create_session_bridge


class MinimalHandler(ViewHandler):
    schema_name = "minimal"
    def __init__(self, hid="h1"):
        super().__init__(hid, {})
        self.next_role = "leader"
    async def initialize(self): pass
    async def close(self): pass
    async def process_event(self, event): pass
    async def on_session_start(self, **kwargs): pass
    async def on_session_close(self, **kwargs): pass
    async def resolve_session_role(self, session_id, **kwargs):
        from fustor_fusion.view_state_manager import view_state_manager
        view_id = "bv1" # Matches the view_id in fixture
        if self.next_role == "leader":
            is_leader = await view_state_manager.try_become_leader(view_id, session_id)
            return {"role": "leader" if is_leader else "follower"}
        return {"role": "follower"}
    def get_data_view(self, **kwargs): return {}
    def get_stats(self): return {}


@pytest_asyncio.fixture
async def pipe_with_bridge():
    from fustor_fusion.view_state_manager import view_state_manager
    from fustor_fusion.core.session_manager import session_manager
    
    # 清理
    await view_state_manager.clear_state("bv1")
    await session_manager.clear_all_sessions("bv1")

    handler = MinimalHandler(hid="bv1")
    pipe = FusionPipe(
        pipe_id="bridge-test",
        config={"view_id": "bv1", "allow_concurrent_push": True},
        view_handlers=[handler]
    )
    pipe.pipe_id = pipe.id # Inject required attribute
    await pipe.start()
    bridge = create_session_bridge(pipe)
    yield pipe, bridge
    await pipe.stop()


@pytest.mark.asyncio
async def test_bridge_create_session(pipe_with_bridge):
    """Basic session create via bridge makes first session the leader."""
    pipe, bridge = pipe_with_bridge
    await bridge.create_session(task_id="agent:pipe", session_id="s1")
    
    role = await pipe.get_session_role("s1")
    assert role == "leader"


@pytest.mark.asyncio
async def test_bridge_close_session(pipe_with_bridge):
    """Closing session via bridge works without errors."""
    pipe, bridge = pipe_with_bridge
    await bridge.create_session(task_id="agent:pipe", session_id="s1")
    result = await bridge.close_session("s1")
    assert result is True


@pytest.mark.asyncio
async def test_bridge_leader_promotion_on_close(pipe_with_bridge):
    """When leader closes, next session gets promoted to leader."""
    pipe, bridge = pipe_with_bridge
    handler = pipe.get_view_handler("bv1")
    
    await bridge.create_session(task_id="a1:p", session_id="s1")
    
    # Make next session a follower
    handler.next_role = "follower"
    await bridge.create_session(task_id="a2:p", session_id="s2")
    
    assert await pipe.get_session_role("s1") == "leader"
    assert await pipe.get_session_role("s2") == "follower"
    
    # Close leader
    # When closing, the bridge/pipe might re-elect. 
    # We set next_role back to leader so s2 can be promoted
    handler.next_role = "leader"
    await bridge.close_session("s1")
    
    # s2 should be promoted
    assert await pipe.get_session_role("s2") == "leader"


@pytest.mark.asyncio
async def test_bridge_keep_alive_returns_dict(pipe_with_bridge):
    """keep_alive returns a dict with role and status."""
    pipe, bridge = pipe_with_bridge
    handler = pipe.get_view_handler("bv1")
    handler.next_role = "leader"
    
    await bridge.create_session(task_id="a1:p", session_id="s1")
    
    result = await bridge.keep_alive("s1")
    assert isinstance(result, dict)
    assert result["status"] == "ok"
    assert result["role"] == "leader"
    assert result["commands"] == []

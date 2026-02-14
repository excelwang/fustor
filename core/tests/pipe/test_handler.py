import pytest
from unittest.mock import MagicMock
from fustor_core.pipe.handler import Handler, SourceHandler, ViewHandler
from fustor_core.event import EventBase

class ConcreteHandler(Handler):
    schema_name = "test_schema"
    schema_version = "2.0"

@pytest.mark.asyncio
async def test_handler_base():
    h = ConcreteHandler("h1", {"k": "v"})
    assert h.id == "h1"
    assert h.config == {"k": "v"}
    assert h.get_schema_info() == {"name": "test_schema", "version": "2.0"}
    
    # Hooks
    await h.initialize()
    await h.close()

class ConcreteSourceHandler(SourceHandler):
    def get_snapshot_iterator(self, **kwargs):
        return iter([])
    def get_message_iterator(self, start_position=-1, **kwargs):
        return iter([])

def test_source_handler_defaults():
    sh = ConcreteSourceHandler("sh1", {})
    assert list(sh.get_audit_iterator()) == []
    assert sh.perform_sentinel_check({}) == {}
    assert list(sh.scan_path("/tmp")) == []

class ConcreteViewHandler(ViewHandler):
    async def process_event(self, event: EventBase) -> bool:
        return True
    async def get_data_view(self, **kwargs):
        return {}

@pytest.mark.asyncio
async def test_view_handler_defaults():
    vh = ConcreteViewHandler("vh1", {})
    assert await vh.resolve_session_role("s1") == {"role": "leader"}
    
    # Hooks
    await vh.on_session_start()
    await vh.on_session_close()
    await vh.handle_audit_start()
    await vh.handle_audit_end()
    await vh.reset()

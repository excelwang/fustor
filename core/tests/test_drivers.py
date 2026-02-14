import pytest
from unittest.mock import MagicMock
from fustor_core.drivers import ViewDriver, SenderDriver, SourceDriver
from fustor_core.event.base import EventBase
from fustor_core.models.config import SourceConfig, SenderConfig

class ConcreteViewDriver(ViewDriver):
    async def process_event(self, event: EventBase) -> bool:
        return True
    
    async def get_data_view(self, **kwargs):
        return {"data": "view"}

@pytest.mark.asyncio
async def test_view_driver_defaults():
    config = {"mode": "live"}
    driver = ConcreteViewDriver(id="test_id", view_id="test_view", config=config)
    
    assert driver.id == "test_id"
    assert driver.view_id == "test_view"
    assert driver.config == config
    assert driver.requires_full_reset_on_session_close is True
    
    # Test hooks that do nothing by default
    await driver.initialize()
    await driver.on_session_start()
    await driver.on_session_close()
    await driver.handle_audit_start()
    await driver.handle_audit_end()
    await driver.reset()
    await driver.cleanup_expired_suspects()
    await driver.close()
    
    role_info = await driver.resolve_session_role("session_123")
    assert role_info == {"role": "leader"}

def test_view_driver_requires_reset():
    driver1 = ConcreteViewDriver(id="1", view_id="v1", config={"mode": "live"})
    assert driver1.requires_full_reset_on_session_close is True
    
    driver2 = ConcreteViewDriver(id="2", view_id="v2", config={"is_live": True})
    assert driver2.requires_full_reset_on_session_close is True
    
    driver3 = ConcreteViewDriver(id="3", view_id="v3", config={"mode": "persistent"})
    assert driver3.requires_full_reset_on_session_close is False
    
    driver4 = ConcreteViewDriver(id="4", view_id="v4", config=None)
    assert driver4.requires_full_reset_on_session_close is False

class ConcreteSenderDriver(SenderDriver):
    async def send_events(self, events, source_type="message", is_end=False, **kwargs):
        return {"status": "sent"}
    
    @classmethod
    async def get_needed_fields(cls, **kwargs):
        return {}

@pytest.mark.asyncio
async def test_sender_driver_defaults():
    config = MagicMock(spec=SenderConfig)
    driver = ConcreteSenderDriver(id="sender_id", config=config)
    
    assert driver.id == "sender_id"
    assert driver.config == config
    
    # Test default implementations
    assert await driver.signal_audit_start("src") is False
    assert await driver.signal_audit_end("src") is False
    assert await driver.get_sentinel_tasks() is None
    assert await driver.submit_sentinel_results({}) is True
    await driver.close()
    
    success, msg = await ConcreteSenderDriver.test_connection()
    assert success is True
    assert "not implemented" in msg
    
    success, msg = await ConcreteSenderDriver.check_privileges()
    assert success is True
    assert "not implemented" in msg

@pytest.mark.asyncio
async def test_sender_driver_abstract_methods():
    config = MagicMock(spec=SenderConfig)
    driver = ConcreteSenderDriver(id="id", config=config)
    
    with pytest.raises(NotImplementedError):
        await driver.create_session("task_1")
    
    with pytest.raises(NotImplementedError):
        await driver.heartbeat()

class ConcreteSourceDriver(SourceDriver):
    def get_snapshot_iterator(self, **kwargs):
        return iter([])
    
    def get_message_iterator(self, start_position=-1, **kwargs):
        return iter([])
    
    @classmethod
    async def get_available_fields(cls, **kwargs):
        return {}

@pytest.mark.asyncio
async def test_source_driver_defaults():
    config = MagicMock(spec=SourceConfig)
    driver = ConcreteSourceDriver(id="source_id", config=config)
    
    assert driver.id == "source_id"
    assert driver.config == config
    assert driver.require_schema_discovery is True
    assert driver.is_transient is False
    
    # Test position availability
    assert driver.is_position_available(100) is True
    assert driver.is_position_available(0) is False
    assert driver.is_position_available(-1) is False
    
    # Test optional iterators and methods
    assert list(driver.get_audit_iterator()) == []
    assert driver.perform_sentinel_check({}) == {}
    await driver.close()
    
    # Test class methods
    success, msg = await ConcreteSourceDriver.test_connection()
    assert success is True
    
    success, msg = await ConcreteSourceDriver.check_privileges()
    assert success is True
    
    success, msg = await ConcreteSourceDriver.check_runtime_params()
    assert success is True
    
    success, msg = await ConcreteSourceDriver.create_agent_user()
    assert success is True

def test_source_driver_transient():
    class TransientSource(ConcreteSourceDriver):
        @property
        def is_transient(self):
            return True
            
    config = MagicMock(spec=SourceConfig)
    driver = TransientSource(id="t", config=config)
    assert driver.is_transient is True
    assert driver.is_position_available(100) is False

# agent/tests/runtime/test_source_handler_adapter.py
"""
Tests for SourceHandlerAdapter.
"""
import pytest
from unittest.mock import MagicMock
from typing import Any, Dict, Iterator, List

from fustor_agent.runtime.source_handler_adapter import (
    SourceHandlerAdapter,
    SourceHandlerFactory,
    create_source_handler_from_config
)


class MockDriver:
    """Mock source driver for testing."""
    
    require_schema_discovery = False
    
    def __init__(self, id: str = "mock-driver", **kwargs):
        self.id = id
        self.snapshot_data = [{"id": i, "type": "snapshot"} for i in range(5)]
        self.message_data = [{"id": i, "type": "message"} for i in range(3)]
        self.audit_data = [{"id": 1, "type": "audit"}]
        self.closed = False
    
    def get_snapshot_iterator(self, **kwargs) -> Iterator[Any]:
        return iter(self.snapshot_data)
    
    def get_message_iterator(self, start_position: int = -1, **kwargs) -> Iterator[Any]:
        return iter(self.message_data)
    
    def get_audit_iterator(self, **kwargs) -> Iterator[Any]:
        return iter(self.audit_data)
    
    def perform_sentinel_check(self, task_batch: Dict[str, Any]) -> Dict[str, Any]:
        return {"verified": True, "count": len(task_batch)}
    
    def is_transient(self) -> bool:
        return True
    
    def close(self) -> None:
        self.closed = True


class MockDriverWithoutOptional:
    """Mock driver without optional methods."""
    
    def __init__(self, id: str = "basic-driver"):
        self.id = id
    
    def get_snapshot_iterator(self, **kwargs) -> Iterator[Any]:
        return iter([{"id": 1}])
    
    def get_message_iterator(self, start_position: int = -1, **kwargs) -> Iterator[Any]:
        return iter([])


@pytest.fixture
def mock_driver():
    return MockDriver()


@pytest.fixture
def adapter(mock_driver):
    return SourceHandlerAdapter(mock_driver)


class TestSourceHandlerAdapterInit:
    """Test SourceHandlerAdapter initialization."""
    
    def test_init_from_driver(self, adapter, mock_driver):
        """Adapter should initialize from driver."""
        assert adapter.id == mock_driver.id
        assert adapter.driver is mock_driver
    
    def test_schema_name_inferred_from_class(self, adapter):
        """Schema name should be inferred from driver class name."""
        # MockDriver -> "mock"
        assert adapter.schema_name == "mock"
    
    def test_schema_name_from_fsdriver(self):
        """FSDriver should have schema 'fs'."""
        class FSDriver:
            id = "test-fs"
        
        adapter = SourceHandlerAdapter(FSDriver())
        assert adapter.schema_name == "fs"


class TestSourceHandlerAdapterLifecycle:
    """Test adapter lifecycle methods."""
    
    @pytest.mark.asyncio
    async def test_initialize(self, adapter):
        """initialize() should set initialized flag."""
        await adapter.initialize()
        assert adapter._initialized is True
    
    @pytest.mark.asyncio
    async def test_initialize_idempotent(self, adapter):
        """initialize() should be idempotent."""
        await adapter.initialize()
        await adapter.initialize()
        assert adapter._initialized is True
    
    @pytest.mark.asyncio
    async def test_close_calls_driver_close(self, adapter, mock_driver):
        """close() should call driver's close method."""
        await adapter.initialize()
        await adapter.close()
        
        assert mock_driver.closed
        assert adapter._initialized is False


class TestSourceHandlerAdapterSnapshot:
    """Test snapshot iteration."""
    
    def test_get_snapshot_iterator(self, adapter, mock_driver):
        """Snapshot iterator should delegate to driver."""
        events = list(adapter.get_snapshot_iterator())
        
        assert len(events) == len(mock_driver.snapshot_data)
        assert events[0]["type"] == "snapshot"
    
    def test_snapshot_kwargs_passed(self, adapter):
        """kwargs should be passed to driver."""
        mock = MagicMock()
        mock.id = "test"
        adapter._driver = mock
        
        adapter.get_snapshot_iterator(foo="bar")
        
        mock.get_snapshot_iterator.assert_called_once_with(foo="bar")


class TestSourceHandlerAdapterMessage:
    """Test message iteration."""
    
    def test_get_message_iterator(self, adapter, mock_driver):
        """Message iterator should delegate to driver."""
        events = list(adapter.get_message_iterator(start_position=0))
        
        assert len(events) == len(mock_driver.message_data)
        assert events[0]["type"] == "message"
    
    def test_message_start_position_passed(self, adapter):
        """start_position should be passed to driver."""
        mock = MagicMock()
        mock.id = "test"
        adapter._driver = mock
        
        adapter.get_message_iterator(start_position=42)
        
        mock.get_message_iterator.assert_called_once_with(start_position=42)


class TestSourceHandlerAdapterAudit:
    """Test audit iteration."""
    
    def test_get_audit_iterator(self, adapter, mock_driver):
        """Audit iterator should delegate to driver."""
        events = list(adapter.get_audit_iterator())
        
        assert len(events) == 1
        assert events[0]["type"] == "audit"
    
    def test_audit_fallback_if_not_supported(self):
        """Should return empty iterator if driver doesn't support audit."""
        driver = MockDriverWithoutOptional()
        adapter = SourceHandlerAdapter(driver)
        
        events = list(adapter.get_audit_iterator())
        
        assert events == []


class TestSourceHandlerAdapterSentinel:
    """Test sentinel check."""
    
    def test_perform_sentinel_check(self, adapter):
        """Sentinel check should delegate to driver."""
        result = adapter.perform_sentinel_check({"file1": 123, "file2": 456})
        
        assert result["verified"] is True
        assert result["count"] == 2
    
    def test_sentinel_fallback_if_not_supported(self):
        """Should return empty dict if driver doesn't support sentinel."""
        driver = MockDriverWithoutOptional()
        adapter = SourceHandlerAdapter(driver)
        
        result = adapter.perform_sentinel_check({"test": 1})
        
        assert result == {}


class TestSourceHandlerAdapterTransient:
    """Test transient check."""
    
    def test_is_transient(self, adapter):
        """is_transient should delegate to driver."""
        assert adapter.is_transient() is True
    
    def test_transient_fallback_if_not_supported(self):
        """Should return False if driver doesn't have is_transient."""
        driver = MockDriverWithoutOptional()
        adapter = SourceHandlerAdapter(driver)
        
        assert adapter.is_transient() is False


class TestSourceHandlerFactory:
    """Test SourceHandlerFactory."""
    
    def test_create_handler(self):
        """Factory should create handler from config."""
        # Create mock driver service
        mock_driver_service = MagicMock()
        mock_driver_service._get_driver_by_type.return_value = MockDriver
        
        # Create mock config
        mock_config = MagicMock()
        mock_config.driver = "fs"
        mock_config.uri = "/data"
        
        factory = SourceHandlerFactory(mock_driver_service)
        handler = factory.create_handler(mock_config, handler_id="test-handler")
        
        assert isinstance(handler, SourceHandlerAdapter)
        assert handler.id == "test-handler"


class TestConvenienceFunction:
    """Test create_source_handler_from_config."""
    
    def test_convenience_function(self):
        """Convenience function should work correctly."""
        mock_driver_service = MagicMock()
        mock_driver_service._get_driver_by_type.return_value = MockDriver
        
        mock_config = MagicMock()
        mock_config.driver = "fs"
        
        handler = create_source_handler_from_config(
            source_config=mock_config,
            source_driver_service=mock_driver_service,
            handler_id="my-source"
        )
        
        assert isinstance(handler, SourceHandlerAdapter)
        assert handler.id == "my-source"

# fusion/tests/runtime/test_view_handler_adapter.py
"""
Tests for ViewHandler Adapters.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock
from typing import Any, Dict, List

from fustor_fusion.runtime import (
    ViewDriverAdapter,
    ViewManagerAdapter,
    create_view_handler_from_driver,
    create_view_handler_from_manager,
)


class MockViewDriver:
    """Mock ViewDriver for testing."""
    
    target_schema = "mock"
    
    def __init__(self, view_id: str = "mock-view"):
        self.view_id = view_id
        self.view_id_legacy = "1"  # Keep for legacy internal checks if any
        self.config = {"mode": "batch"}
        self.events_processed: List[Any] = []
        self.session_starts = 0
        self.session_closes = 0
        self.audit_starts = 0
        self.audit_ends = 0
        self._initialized = False
        self._closed = False
        self._reset_called = False
    
    async def initialize(self) -> None:
        self._initialized = True
    
    async def close(self) -> None:
        self._closed = True
    
    async def process_event(self, event: Any) -> bool:
        self.events_processed.append(event)
        return True
    
    async def get_data_view(self, **kwargs) -> Dict[str, Any]:
        return {
            "events_count": len(self.events_processed),
            "params": kwargs
        }
    
    async def on_session_start(self) -> None:
        self.session_starts += 1
    
    async def on_session_close(self) -> None:
        self.session_closes += 1
    
    async def handle_audit_start(self) -> None:
        self.audit_starts += 1
    
    async def handle_audit_end(self) -> None:
        self.audit_ends += 1
    
    async def reset(self) -> None:
        self._reset_called = True
        self.events_processed.clear()
    
    def get_stats(self) -> Dict[str, Any]:
        return {"processed": len(self.events_processed)}
    
    @property
    def requires_full_reset_on_session_close(self) -> bool:
        return self.config.get("mode") == "live"


class MockViewManager:
    """Mock ViewManager for testing."""
    
    def __init__(self, view_id: str = "1"):
        self.view_id = view_id
        self.driver_instances: Dict[str, MockViewDriver] = {
            "fs": MockViewDriver(view_id="fs-view"),
            "db": MockViewDriver(view_id="db-view"),
        }
        self._initialized = False
    
    async def initialize_driver_instances(self) -> None:
        self._initialized = True
        for p in self.driver_instances.values():
            await p.initialize()
    
    async def process_event(self, event: Any) -> Dict[str, Dict]:
        results = {}
        for name, driver_instance in self.driver_instances.items():
            success = await driver_instance.process_event(event)
            results[name] = {"success": success}
        return results
    
    def get_data_view(self, driver_id: str, **kwargs) -> Any:
        driver_instance = self.driver_instances.get(driver_id)
        if driver_instance:
            # Simulate sync call for testing
            return {"events_count": len(driver_instance.events_processed)}
        return None
    
    def on_session_start(self) -> None:
        for p in self.driver_instances.values():
            p.session_starts += 1
    
    def on_session_close(self) -> None:
        for p in self.driver_instances.values():
            p.session_closes += 1
    
    def get_available_driver_ids(self) -> List[str]:
        return list(self.driver_instances.keys())
    
    def get_driver_instance(self, name: str):
        return self.driver_instances.get(name)
    
    def get_aggregated_stats(self) -> Dict[str, Any]:
        return {
            name: p.get_stats()
            for name, p in self.driver_instances.items()
        }


@pytest.fixture
def mock_driver():
    return MockViewDriver()


@pytest.fixture
def mock_manager():
    return MockViewManager()


@pytest.fixture
def driver_adapter(mock_driver):
    return ViewDriverAdapter(mock_driver)


@pytest.fixture
def manager_adapter(mock_manager):
    return ViewManagerAdapter(mock_manager)


class TestViewDriverAdapterInit:
    """Test ViewDriverAdapter initialization."""
    
    def test_init_from_driver(self, driver_adapter, mock_driver):
        """Adapter should initialize from driver."""
        assert driver_adapter.id == mock_driver.view_id
        assert driver_adapter.driver is mock_driver
    
    def test_schema_name_from_driver(self, driver_adapter):
        """Schema name should come from driver."""
        assert driver_adapter.schema_name == "mock"


class TestViewDriverAdapterLifecycle:
    """Test ViewDriverAdapter lifecycle."""
    
    @pytest.mark.asyncio
    async def test_initialize(self, driver_adapter, mock_driver):
        """initialize() should call driver's initialize."""
        await driver_adapter.initialize()
        assert mock_driver._initialized
    
    @pytest.mark.asyncio
    async def test_close(self, driver_adapter, mock_driver):
        """close() should call driver's close."""
        await driver_adapter.initialize()
        await driver_adapter.close()
        assert mock_driver._closed


class TestViewDriverAdapterProcessing:
    """Test ViewDriverAdapter event processing."""
    
    @pytest.mark.asyncio
    async def test_process_event(self, driver_adapter, mock_driver):
        """process_event should delegate to driver."""
        from fustor_core.event import EventBase, EventType, MessageSource
        
        event = EventBase(
            event_type=EventType.INSERT,
            fields=["path"],
            rows=[["/test"]],
            event_schema="fs",
            table="files",
            message_source=MessageSource.REALTIME
        )
        result = await driver_adapter.process_event(event)
        
        assert result is True
        assert len(mock_driver.events_processed) == 1
    
    @pytest.mark.asyncio
    async def test_get_data_view(self, driver_adapter):
        """get_data_view should delegate to driver."""
        view = await driver_adapter.get_data_view(path="/")
        
        assert "events_count" in view
        assert view["params"]["path"] == "/"


class TestViewDriverAdapterHooks:
    """Test ViewDriverAdapter lifecycle hooks."""
    
    @pytest.mark.asyncio
    async def test_on_session_start(self, driver_adapter, mock_driver):
        """on_session_start should delegate to driver."""
        await driver_adapter.on_session_start()
        assert mock_driver.session_starts == 1
    
    @pytest.mark.asyncio
    async def test_on_session_close(self, driver_adapter, mock_driver):
        """on_session_close should delegate to driver."""
        await driver_adapter.on_session_close()
        assert mock_driver.session_closes == 1
    
    @pytest.mark.asyncio
    async def test_handle_audit_start(self, driver_adapter, mock_driver):
        """handle_audit_start should delegate to driver."""
        await driver_adapter.handle_audit_start()
        assert mock_driver.audit_starts == 1
    
    @pytest.mark.asyncio
    async def test_handle_audit_end(self, driver_adapter, mock_driver):
        """handle_audit_end should delegate to driver."""
        await driver_adapter.handle_audit_end()
        assert mock_driver.audit_ends == 1
    
    @pytest.mark.asyncio
    async def test_reset(self, driver_adapter, mock_driver):
        """reset should delegate to driver."""
        await driver_adapter.reset()
        assert mock_driver._reset_called


class TestViewDriverAdapterStats:
    """Test ViewDriverAdapter stats."""
    
    def test_get_stats(self, driver_adapter):
        """get_stats should delegate to driver."""
        stats = driver_adapter.get_stats()
        assert "processed" in stats
    
    def test_requires_full_reset(self, driver_adapter, mock_driver):
        """requires_full_reset_on_session_close should delegate."""
        assert driver_adapter.requires_full_reset_on_session_close is False
        
        mock_driver.config["mode"] = "live"
        assert driver_adapter.requires_full_reset_on_session_close is True


class TestViewManagerAdapterInit:
    """Test ViewManagerAdapter initialization."""
    
    def test_init_from_manager(self, manager_adapter, mock_manager):
        """Adapter should initialize from manager."""
        assert manager_adapter.manager is mock_manager
        assert "view-manager" in manager_adapter.id


class TestViewManagerAdapterLifecycle:
    """Test ViewManagerAdapter lifecycle."""
    
    @pytest.mark.asyncio
    async def test_initialize(self, manager_adapter, mock_manager):
        """initialize() should call manager's initialize_driver_instances."""
        await manager_adapter.initialize()
        assert mock_manager._initialized


class TestViewManagerAdapterProcessing:
    """Test ViewManagerAdapter event processing."""
    
    @pytest.mark.asyncio
    async def test_process_event(self, manager_adapter, mock_manager):
        """process_event should delegate to manager."""
        from fustor_core.event import EventBase, EventType, MessageSource
        
        event = EventBase(
            event_type=EventType.INSERT,
            fields=["path"],
            rows=[["/test"]],
            event_schema="fs",
            table="files",
            message_source=MessageSource.REALTIME
        )
        result = await manager_adapter.process_event(event)
        
        assert result is True  # At least one driver instance succeeded
        assert len(mock_manager.driver_instances["fs"].events_processed) == 1
        assert len(mock_manager.driver_instances["db"].events_processed) == 1
    
    @pytest.mark.asyncio
    async def test_get_data_view_with_driver(self, manager_adapter):
        """get_data_view should query specific driver instance."""
        view = await manager_adapter.get_data_view(driver_id="fs")
        
        assert "events_count" in view
    
    @pytest.mark.asyncio
    async def test_get_data_view_without_driver(self, manager_adapter):
        """get_data_view without driver instance ID should list driver instances."""
        view = await manager_adapter.get_data_view()
        
        assert "driver_instances" in view
        assert "fs" in view["driver_instances"]


class TestViewManagerAdapterStats:
    """Test ViewManagerAdapter stats and driver_instances."""
    
    def test_get_stats(self, manager_adapter):
        """get_stats should return aggregated stats."""
        stats = manager_adapter.get_stats()
        assert "fs" in stats
        assert "db" in stats
    
    def test_get_available_driver_ids(self, manager_adapter):
        """get_available_driver_ids should list all driver instances."""
        driver_ids = manager_adapter.get_available_driver_ids()
        assert "fs" in driver_ids
        assert "db" in driver_ids
    
    def test_get_driver_instance(self, manager_adapter, mock_manager):
        """get_driver_instance should return specific driver instance."""
        driver_instance = manager_adapter.get_driver_instance("fs")
        assert driver_instance is mock_manager.driver_instances["fs"]


class TestConvenienceFunctions:
    """Test convenience functions."""
    
    def test_create_view_handler_from_driver(self, mock_driver):
        """Convenience function should work for driver."""
        handler = create_view_handler_from_driver(mock_driver)
        assert isinstance(handler, ViewDriverAdapter)
        assert handler.driver is mock_driver
    
    def test_create_view_handler_from_manager(self, mock_manager):
        """Convenience function should work for manager."""
        handler = create_view_handler_from_manager(mock_manager)
        assert isinstance(handler, ViewManagerAdapter)
        assert handler.manager is mock_manager

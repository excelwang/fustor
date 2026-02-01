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
    
    def __init__(self, view_id: str = "mock-view", datastore_id: str = "1"):
        self.view_id = view_id
        self.datastore_id = datastore_id
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
    
    def __init__(self, datastore_id: str = "1"):
        self.datastore_id = datastore_id
        self.providers: Dict[str, MockViewDriver] = {
            "fs": MockViewDriver(view_id="fs-view"),
            "db": MockViewDriver(view_id="db-view"),
        }
        self._initialized = False
    
    async def initialize_providers(self) -> None:
        self._initialized = True
        for p in self.providers.values():
            await p.initialize()
    
    async def process_event(self, event: Any) -> Dict[str, Dict]:
        results = {}
        for name, provider in self.providers.items():
            success = await provider.process_event(event)
            results[name] = {"success": success}
        return results
    
    def get_data_view(self, provider_name: str, **kwargs) -> Any:
        provider = self.providers.get(provider_name)
        if provider:
            # Simulate sync call for testing
            return {"events_count": len(provider.events_processed)}
        return None
    
    def on_session_start(self) -> None:
        for p in self.providers.values():
            p.session_starts += 1
    
    def on_session_close(self) -> None:
        for p in self.providers.values():
            p.session_closes += 1
    
    def get_available_providers(self) -> List[str]:
        return list(self.providers.keys())
    
    def get_provider(self, name: str):
        return self.providers.get(name)
    
    def get_aggregated_stats(self) -> Dict[str, Any]:
        return {
            name: p.get_stats()
            for name, p in self.providers.items()
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
        """initialize() should call manager's initialize_providers."""
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
        
        assert result is True  # At least one provider succeeded
        assert len(mock_manager.providers["fs"].events_processed) == 1
        assert len(mock_manager.providers["db"].events_processed) == 1
    
    @pytest.mark.asyncio
    async def test_get_data_view_with_provider(self, manager_adapter):
        """get_data_view should query specific provider."""
        view = await manager_adapter.get_data_view(provider="fs")
        
        assert "events_count" in view
    
    @pytest.mark.asyncio
    async def test_get_data_view_without_provider(self, manager_adapter):
        """get_data_view without provider should list providers."""
        view = await manager_adapter.get_data_view()
        
        assert "providers" in view
        assert "fs" in view["providers"]


class TestViewManagerAdapterStats:
    """Test ViewManagerAdapter stats and providers."""
    
    def test_get_stats(self, manager_adapter):
        """get_stats should return aggregated stats."""
        stats = manager_adapter.get_stats()
        assert "fs" in stats
        assert "db" in stats
    
    def test_get_available_providers(self, manager_adapter):
        """get_available_providers should list all providers."""
        providers = manager_adapter.get_available_providers()
        assert "fs" in providers
        assert "db" in providers
    
    def test_get_provider(self, manager_adapter, mock_manager):
        """get_provider should return specific provider."""
        provider = manager_adapter.get_provider("fs")
        assert provider is mock_manager.providers["fs"]


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

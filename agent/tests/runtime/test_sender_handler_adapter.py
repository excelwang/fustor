# agent/tests/runtime/test_sender_handler_adapter.py
"""
Tests for SenderHandlerAdapter.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock
from typing import Any, Dict, List, Optional

from fustor_core.transport import Sender
from fustor_agent.runtime.sender_handler_adapter import (
    SenderHandlerAdapter,
    SenderHandlerFactory,
    create_sender_handler_from_config
)


class MockSender(Sender):
    """Mock Sender for testing."""
    
    def __init__(self, sender_id: str = "mock", **kwargs):
        super().__init__(
            sender_id=sender_id,
            endpoint="http://mock:8080",
            credential={"key": "test-key"},
            config=kwargs.get("config", {})
        )
        self.connected = False
        self.session_created = False
        self.events_sent: List[List[Any]] = []
        self.heartbeats_sent = 0
        self.role = "follower"
    
    async def connect(self) -> None:
        self.connected = True
    
    async def create_session(
        self,
        task_id: str,
        source_type: Optional[str] = None,
        session_timeout_seconds: Optional[int] = None
    ) -> Dict[str, Any]:
        self.session_created = True
        self.session_id = f"sess-{task_id}"
        return {
            "session_id": self.session_id,
            "role": self.role,
            "session_timeout_seconds": session_timeout_seconds or 30
        }
    
    async def _send_events_impl(
        self, 
        events: List[Any], 
        source_type: str = "message",
        is_end: bool = False
    ) -> Dict[str, Any]:
        self.events_sent.append((events, source_type, is_end))
        return {"success": True, "count": len(events)}
    
    async def heartbeat(self) -> Dict[str, Any]:
        self.heartbeats_sent += 1
        return {"role": self.role, "status": "ok"}
    
    async def close_session(self) -> None:
        self.session_created = False
        self.session_id = None
    
    async def close(self) -> None:
        self.connected = False


@pytest.fixture
def mock_sender():
    return MockSender()


@pytest.fixture
def adapter(mock_sender):
    return SenderHandlerAdapter(mock_sender)


class TestSenderHandlerAdapterInit:
    """Test SenderHandlerAdapter initialization."""
    
    def test_init_from_sender(self, adapter, mock_sender):
        """Adapter should initialize from sender."""
        assert adapter.id == mock_sender.id
        assert adapter.sender is mock_sender
    
    def test_schema_name_default(self, adapter):
        """Default schema name should be set."""
        assert adapter.schema_name == "sender-adapter"


class TestSenderHandlerAdapterLifecycle:
    """Test adapter lifecycle methods."""
    
    @pytest.mark.asyncio
    async def test_initialize_connects_sender(self, adapter, mock_sender):
        """initialize() should connect the underlying sender."""
        assert not mock_sender.connected
        
        await adapter.initialize()
        
        assert mock_sender.connected
    
    @pytest.mark.asyncio
    async def test_initialize_idempotent(self, adapter, mock_sender):
        """initialize() should be idempotent."""
        await adapter.initialize()
        await adapter.initialize()
        
        assert mock_sender.connected
    
    @pytest.mark.asyncio
    async def test_close_disconnects_sender(self, adapter, mock_sender):
        """close() should close the underlying sender."""
        await adapter.initialize()
        await adapter.close()
        
        assert not mock_sender.connected


class TestSenderHandlerAdapterSession:
    """Test session management."""
    
    @pytest.mark.asyncio
    async def test_create_session(self, adapter, mock_sender):
        """create_session should delegate to sender."""
        session_id, metadata = await adapter.create_session(
            task_id="test-task",
            source_type="fs",
            session_timeout_seconds=60
        )
        
        assert session_id == "sess-test-task"
        assert metadata["role"] == "follower"
        assert metadata["source_type"] == "fs"
        assert mock_sender.connected  # Should auto-connect
    
    @pytest.mark.asyncio
    async def test_close_session(self, adapter, mock_sender):
        """close_session should delegate to sender."""
        await adapter.create_session("test", "fs")
        
        result = await adapter.close_session("sess-test")
        
        assert result is True
        assert not mock_sender.session_created


class TestSenderHandlerAdapterHeartbeat:
    """Test heartbeat functionality."""
    
    @pytest.mark.asyncio
    async def test_send_heartbeat(self, adapter, mock_sender):
        """send_heartbeat should delegate to sender."""
        await adapter.create_session("test", "fs")
        
        response = await adapter.send_heartbeat("sess-test")
        
        assert mock_sender.heartbeats_sent == 1
        assert response["role"] == "follower"
        assert response["session_id"] == "sess-test"
    
    @pytest.mark.asyncio
    async def test_heartbeat_returns_role(self, adapter, mock_sender):
        """Heartbeat should return current role."""
        mock_sender.role = "leader"
        await adapter.create_session("test", "fs")
        
        response = await adapter.send_heartbeat("sess-test")
        
        assert response["role"] == "leader"


class TestSenderHandlerAdapterBatch:
    """Test batch sending."""
    
    @pytest.mark.asyncio
    async def test_send_batch_message_sync(self, adapter, mock_sender):
        """send_batch should map message sync phase correctly."""
        await adapter.create_session("test", "fs")
        
        events = [{"id": 1}, {"id": 2}]
        success, response = await adapter.send_batch(
            session_id="sess-test",
            events=events,
            batch_context={"phase": "realtime"}
        )
        
        assert success
        assert len(mock_sender.events_sent) == 1
        sent_events, source_type, is_end = mock_sender.events_sent[0]
        assert sent_events == events
        assert source_type == "message"
        assert is_end is False
    
    @pytest.mark.asyncio
    async def test_send_batch_SNAPSHOT_SYNC(self, adapter, mock_sender):
        """send_batch should map snapshot sync phase correctly."""
        await adapter.create_session("test", "fs")
        
        events = [{"id": 1}]
        success, _ = await adapter.send_batch(
            session_id="sess-test",
            events=events,
            batch_context={"phase": "snapshot", "is_final": True}
        )
        
        assert success
        _, source_type, is_end = mock_sender.events_sent[0]
        assert source_type == "snapshot"
        assert is_end is True
    
    @pytest.mark.asyncio
    async def test_send_batch_audit_sync(self, adapter, mock_sender):
        """send_batch should map audit phase correctly."""
        await adapter.create_session("test", "fs")
        
        events = [{"id": 1}]
        success, _ = await adapter.send_batch(
            session_id="sess-test",
            events=events,
            batch_context={"phase": "audit"}
        )
        
        assert success
        _, source_type, _ = mock_sender.events_sent[0]
        assert source_type == "audit"

    @pytest.mark.asyncio
    async def test_send_batch_error_sync(self, adapter, mock_sender):
        """send_batch should propagate generic exceptions."""
        mock_sender._send_events_impl = AsyncMock(side_effect=RuntimeError("Network failure"))
        
        with pytest.raises(RuntimeError, match="Network failure"):
            await adapter.send_batch("sess-test", [{"id": 1}])

    @pytest.mark.asyncio
    async def test_send_batch_session_obsolete_error(self, adapter, mock_sender):
        """send_batch should propagate SessionObsoletedError (important for pipeline)."""
        from fustor_core.exceptions import SessionObsoletedError
        mock_sender._send_events_impl = AsyncMock(side_effect=SessionObsoletedError("Expired"))
        
        with pytest.raises(SessionObsoletedError, match="Expired"):
            await adapter.send_batch("sess-test", [{"id": 1}])


class TestSenderHandlerAdapterConnection:
    """Test connection testing."""
    
    @pytest.mark.asyncio
    async def test_test_connection_success(self, adapter, mock_sender):
        """test_connection should return success when connect works."""
        success, message = await adapter.test_connection()
        
        assert success
        assert "successful" in message.lower()
        assert mock_sender.connected
    
    @pytest.mark.asyncio
    async def test_test_connection_failure(self, adapter, mock_sender):
        """test_connection should return failure on error."""
        async def fail_connect():
            raise ConnectionError("Network error")
        
        mock_sender.connect = fail_connect
        
        success, message = await adapter.test_connection()
        
        assert not success
        assert "Network error" in message


class TestSenderHandlerFactory:
    """Test SenderHandlerFactory."""
    
    def test_create_handler(self):
        """Factory should create handler from config."""
        # Create mock driver service
        mock_driver_service = MagicMock()
        mock_driver_service._get_driver_by_type.return_value = MockSender
        
        # Create mock config
        mock_config = MagicMock()
        mock_config.driver = "http"
        mock_config.endpoint = "http://fusion:8080"
        mock_config.credential.model_dump.return_value = {"key": "test-key"}
        mock_config.batch_size = 100
        mock_config.driver_params = {}
        
        factory = SenderHandlerFactory(mock_driver_service)
        handler = factory.create_handler(mock_config, handler_id="test-handler")
        
        assert isinstance(handler, SenderHandlerAdapter)
        assert handler.id == "test-handler"

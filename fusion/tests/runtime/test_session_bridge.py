# fusion/tests/runtime/test_session_bridge.py
"""
Tests for PipelineSessionBridge.
"""
import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch

from fustor_fusion.runtime import (
    FusionPipeline,
    PipelineSessionBridge,
    create_session_bridge,
)


@pytest.fixture
def mock_pipeline():
    """Create a mock FusionPipeline."""
    pipeline = MagicMock(spec=FusionPipeline)
    pipeline.datastore_id = "1"
    pipeline._active_sessions = {}
    pipeline._leader_session = None
    
    # Mock async methods
    pipeline.on_session_created = AsyncMock()
    pipeline.on_session_closed = AsyncMock()
    
    # Mock get_session_role
    async def get_role(session_id):
        session = pipeline._active_sessions.get(session_id)
        return session.get("role", "unknown") if session else "unknown"
    
    pipeline.get_session_role = get_role
    
    # Mock get_session_info - returns session info with session_id added, or None if not found
    async def get_session_info(session_id):
        session = pipeline._active_sessions.get(session_id)
        if session:
            return {**session, "session_id": session_id}
        return None
    
    pipeline.get_session_info = get_session_info
    
    async def get_all_sessions():
        return {sid: {**s, "session_id": sid} for sid, s in pipeline._active_sessions.items()}
    pipeline.get_all_sessions = get_all_sessions
    
    return pipeline


@pytest.fixture
def mock_session_manager():
    """Create a mock SessionManager."""
    manager = MagicMock()
    manager.create_session_entry = AsyncMock()
    manager.keep_session_alive = AsyncMock()
    manager.terminate_session = AsyncMock()
    manager.remove_session = AsyncMock()
    manager.get_session_info = AsyncMock(return_value=None)
    return manager


@pytest.fixture
def session_bridge(mock_pipeline, mock_session_manager):
    """Create a PipelineSessionBridge."""
    return PipelineSessionBridge(mock_pipeline, mock_session_manager)


class TestPipelineSessionBridgeInit:
    """Test bridge initialization."""
    
    def test_init(self, mock_pipeline, mock_session_manager):
        """Bridge should initialize with pipeline and session manager."""
        bridge = PipelineSessionBridge(mock_pipeline, mock_session_manager)
        
        assert bridge._pipeline is mock_pipeline
        assert bridge._session_manager is mock_session_manager
        assert bridge._session_datastore_map == {}


class TestSessionCreation:
    """Test session creation."""
    
    @pytest.mark.asyncio
    async def test_create_session(self, session_bridge, mock_pipeline, mock_session_manager):
        """create_session should create in both systems."""
        # Wrap everything in a patch for datastore_state_manager to avoid side effects
        with patch("fustor_fusion.datastore_state_manager.datastore_state_manager.try_become_leader", AsyncMock(return_value=True)), \
             patch("fustor_fusion.datastore_state_manager.datastore_state_manager.set_authoritative_session", AsyncMock()), \
             patch("fustor_fusion.datastore_state_manager.datastore_state_manager.lock_for_session", AsyncMock()):
            
            # Simulate pipeline setting role
            async def set_role(session_id, **kwargs):
                mock_pipeline._active_sessions[session_id] = {"role": "leader"}
            
            mock_pipeline.on_session_created = set_role
            
            result = await session_bridge.create_session(
                task_id="agent-1:sync-1",
                client_ip="192.168.1.1",
                session_timeout_seconds=60
            )
            
            # Verify session manager was called
            mock_session_manager.create_session_entry.assert_called_once()
            call_args = mock_session_manager.create_session_entry.call_args
            assert call_args.kwargs["view_id"] == "1"
            assert call_args.kwargs["task_id"] == "agent-1:sync-1"
            assert call_args.kwargs["client_ip"] == "192.168.1.1"
            
            # Verify result
            assert "session_id" in result
            assert result["timeout_seconds"] == 60
    
    @pytest.mark.asyncio
    async def test_create_session_returns_role(self, session_bridge, mock_pipeline):
        """create_session should return the role from pipeline."""
        async def set_role(session_id, **kwargs):
            mock_pipeline._active_sessions[session_id] = {"role": "follower"}
        
        mock_pipeline.on_session_created = set_role
        
        result = await session_bridge.create_session(task_id="agent-1:sync-1")
        
        assert result["role"] == "follower"


class TestSessionKeepAlive:
    """Test keep alive (heartbeat)."""
    
    @pytest.mark.asyncio
    async def test_keep_alive(self, session_bridge, mock_pipeline, mock_session_manager):
        """keep_alive should update both systems."""
        # First create a session
        async def set_role(session_id, **kwargs):
            mock_pipeline._active_sessions[session_id] = {"role": "leader"}
        
        mock_pipeline.on_session_created = set_role
        
        create_result = await session_bridge.create_session(task_id="agent:sync")
        session_id = create_result["session_id"]
        
        # Keep alive
        result = await session_bridge.keep_alive(session_id, client_ip="192.168.1.1")
        
        # Verify session manager was called
        mock_session_manager.keep_session_alive.assert_called_once()
        
        # Verify result
        assert result["session_id"] == session_id
        assert result["role"] == "leader"


class TestSessionClose:
    """Test session closure."""
    
    @pytest.mark.asyncio
    async def test_close_session(self, session_bridge, mock_pipeline, mock_session_manager):
        """close_session should close in both systems."""
        with patch("fustor_fusion.datastore_state_manager.datastore_state_manager.unlock_for_session", AsyncMock()), \
             patch("fustor_fusion.datastore_state_manager.datastore_state_manager.release_leader", AsyncMock()):
            # First create a session
            async def set_role(session_id, **kwargs):
                mock_pipeline._active_sessions[session_id] = {"role": "leader"}
            
            mock_pipeline.on_session_created = set_role
            mock_pipeline.on_session_closed = AsyncMock()
            
            # Initialize map
            session_bridge._session_datastore_map["sess-123"] = "1"
            
            # Close session
            result = await session_bridge.close_session("sess-123")
            
            # Verify session manager was called (terminate_session is used now)
            mock_session_manager.terminate_session.assert_called_once_with(
                datastore_id="1",
                session_id="sess-123"
            )
            
            # Verify pipeline was called
            mock_pipeline.on_session_closed.assert_called_once_with("sess-123")
            
            assert result is True


class TestGetSessionInfo:
    """Test session info retrieval."""
    
    @pytest.mark.asyncio
    async def test_get_session_info_from_pipeline(self, session_bridge, mock_pipeline):
        """get_session_info should get info from pipeline first."""
        mock_pipeline._active_sessions["sess-1"] = {
            "role": "leader",
            "task_id": "agent:sync"
        }
        
        info = await session_bridge.get_session_info("sess-1")
        
        assert info["role"] == "leader"
        assert info["session_id"] == "sess-1"
    
    @pytest.mark.asyncio
    async def test_get_session_info_fallback_to_legacy(self, session_bridge, mock_session_manager):
        """get_session_info should fallback to session manager."""
        session_bridge._session_datastore_map["sess-1"] = "1"
        mock_session_info = MagicMock()
        mock_session_info.session_id = "sess-1"
        mock_session_info.task_id = "old"
        mock_session_info.client_ip = "1.2.3.4"
        
        mock_session_manager.get_session_info.return_value = mock_session_info
        
        info = await session_bridge.get_session_info("sess-1")
        
        assert info["task_id"] == "old"
        assert info["session_id"] == "sess-1"
        mock_session_manager.get_session_info.assert_called_once_with("1", "sess-1")
    
    @pytest.mark.asyncio
    async def test_get_session_info_not_found(self, session_bridge):
        """get_session_info should return None if not found."""
        info = await session_bridge.get_session_info("nonexistent")
        assert info is None


class TestConvenienceFunction:
    """Test create_session_bridge convenience function."""
    
    def test_create_session_bridge_with_explicit_manager(self, mock_pipeline, mock_session_manager):
        """create_session_bridge should work with explicit session manager."""
        bridge = create_session_bridge(mock_pipeline, mock_session_manager)
        
        assert isinstance(bridge, PipelineSessionBridge)
        assert bridge._session_manager is mock_session_manager

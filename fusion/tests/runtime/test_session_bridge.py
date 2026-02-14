# fusion/tests/runtime/test_session_bridge.py
"""
Tests for PipeSessionBridge.
"""
import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch

from fustor_fusion.runtime import (
    FusionPipe,
    PipeSessionBridge,
    create_session_bridge,
)


@pytest.fixture
def mock_pipe():
    """Create a mock FusionPipe."""
    pipe = MagicMock(spec=FusionPipe)
    pipe.view_id = "1"
    pipe.view_ids = ["1"]
    pipe._active_sessions = {}
    pipe._leader_session = None
    pipe.config = {"session_timeout_seconds": 30}
    
    # Mock async methods
    pipe.on_session_created = AsyncMock()
    pipe.on_session_closed = AsyncMock()
    
    # Mock get_session_role
    async def get_role(session_id):
        session = pipe._active_sessions.get(session_id)
        return session.get("role", "unknown") if session else "unknown"
    
    pipe.get_session_role = get_role
    
    # Mock get_session_info - returns session info with session_id added, or None if not found
    async def get_session_info(session_id):
        session = pipe._active_sessions.get(session_id)
        if session:
            return {**session, "session_id": session_id}
        return None
    
    pipe.get_session_info = get_session_info
    
    async def get_all_sessions():
        return {sid: {**s, "session_id": sid} for sid, s in pipe._active_sessions.items()}
    pipe.get_all_sessions = get_all_sessions
    
    return pipe


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
def session_bridge(mock_pipe, mock_session_manager):
    """Create a PipeSessionBridge."""
    return PipeSessionBridge(mock_pipe, mock_session_manager)


class TestPipeSessionBridgeInit:
    """Test bridge initialization."""
    
    def test_init(self, mock_pipe, mock_session_manager):
        """Bridge should initialize with pipe and session manager."""
        bridge = PipeSessionBridge(mock_pipe, mock_session_manager)
        
        assert bridge._pipe is mock_pipe
        assert bridge._session_manager is mock_session_manager

    @pytest.mark.asyncio
    async def test_close_session(self, session_bridge, mock_pipe, mock_session_manager):
        """close_session should close in both systems."""
        with patch("fustor_fusion.view_state_manager.view_state_manager.unlock_for_session", AsyncMock()), \
             patch("fustor_fusion.view_state_manager.view_state_manager.release_leader", AsyncMock()):
            # First create a session
            async def set_role(session_id, **kwargs):
                mock_pipe._active_sessions[session_id] = {"role": "leader"}
            
            mock_pipe.on_session_created = set_role
            mock_pipe.on_session_closed = AsyncMock()
            
            # Close session
            result = await session_bridge.close_session("sess-123")
            
            # Verify session manager was called (terminate_session is used now)
            mock_session_manager.terminate_session.assert_called_once_with(
                view_id="1",
                session_id="sess-123"
            )
            
            # Verify pipe was called
            mock_pipe.on_session_closed.assert_called_once_with("sess-123")
            
            assert result is True

    @pytest.mark.asyncio
    async def test_get_session_info_from_pipe(self, session_bridge, mock_pipe):
        """get_session_info should get info from pipe."""
        mock_pipe._active_sessions["sess-1"] = {
            "role": "leader",
            "task_id": "agent:sync"
        }
        
        info = await session_bridge.get_session_info("sess-1")
        
        assert info["role"] == "leader"
        assert info["session_id"] == "sess-1"

    @pytest.mark.asyncio
    async def test_get_session_info_not_found(self, session_bridge):
        """get_session_info should return None if not found."""
        info = await session_bridge.get_session_info("nonexistent")
        assert info is None


class TestConvenienceFunction:
    """Test create_session_bridge convenience function."""
    
    def test_create_session_bridge_with_explicit_manager(self, mock_pipe, mock_session_manager):
        """create_session_bridge should work with explicit session manager."""
        bridge = create_session_bridge(mock_pipe, mock_session_manager)
        
        assert isinstance(bridge, PipeSessionBridge)
        assert bridge._session_manager is mock_session_manager

"""
Test cases for session management with multiple servers to prevent the 409 conflict issue
"""
import asyncio
import uuid
from datetime import datetime
from unittest.mock import AsyncMock, Mock, patch
from dataclasses import dataclass

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from fustor_fusion.api.session import create_session, _should_allow_new_session
from fustor_fusion.core.session_manager import session_manager
from fustor_fusion.view_state_manager import view_state_manager


class MockRequest:
    def __init__(self, client_host="127.0.0.1"):
        self.client = Mock()
        self.client.host = client_host


def make_session_config(allow_concurrent_push=False, session_timeout_seconds=30):
    """Create a mock session config dict."""
    return {
        "allow_concurrent_push": allow_concurrent_push,
        "session_timeout_seconds": session_timeout_seconds,
    }


@pytest.mark.asyncio
async def test_session_creation_multiple_servers():
    """
    Test that multiple servers can create sessions without 409 errors when using different task IDs
    """
    await session_manager.cleanup_expired_sessions()
    view_state_manager._states.clear()
    
    view_id = "1"
    config = make_session_config(allow_concurrent_push=False, session_timeout_seconds=1)
    
    with patch('fustor_fusion.api.session._get_session_config', return_value=config):
        payload1 = type('CreateSessionPayload', (), {})()
        payload1.task_id = "task_server1"
        
        request1 = MockRequest(client_host="192.168.1.10")
        
        result1 = await create_session(payload1, request1, view_id)
        session_id1 = result1["session_id"]
        
        assert session_id1 is not None
        assert await view_state_manager.is_locked_by_session(view_id, session_id1)
        
        await asyncio.sleep(1.5)
        
        payload2 = type('CreateSessionPayload', (), {})()
        payload2.task_id = "task_server2"
        
        request2 = MockRequest(client_host="192.168.1.11")
        
        result2 = await create_session(payload2, request2, view_id)
        session_id2 = result2["session_id"]
        
        assert session_id2 is not None
        assert session_id1 != session_id2
        assert await view_state_manager.is_locked_by_session(view_id, session_id2)
        assert not await view_state_manager.is_locked_by_session(view_id, session_id1)


@pytest.mark.asyncio
async def test_session_creation_same_task_id():
    """
    Test that sessions with the same task_id are properly rejected when concurrent push is not allowed
    """
    await session_manager.cleanup_expired_sessions()
    view_state_manager._states.clear()
    
    view_id = "2"
    config = make_session_config(allow_concurrent_push=False, session_timeout_seconds=30)
    
    with patch('fustor_fusion.api.session._get_session_config', return_value=config):
        payload1 = type('CreateSessionPayload', (), {})()
        payload1.task_id = "same_task"
        
        request1 = MockRequest(client_host="192.168.1.12")
        
        result1 = await create_session(payload1, request1, view_id)
        session_id1 = result1["session_id"]
        
        assert session_id1 is not None
        
        payload2 = type('CreateSessionPayload', (), {})()
        payload2.task_id = "same_task"
        
        request2 = MockRequest(client_host="192.168.1.13")
        
        with pytest.raises(Exception) as exc_info:
            await create_session(payload2, request2, view_id)
        
        assert hasattr(exc_info.value, 'status_code')
        assert exc_info.value.status_code == 409


@pytest.mark.asyncio
async def test_session_creation_different_task_id():
    """
    Test that sessions with different task IDs are rejected when concurrent push is not allowed
    """
    await session_manager.cleanup_expired_sessions()
    view_state_manager._states.clear()
    
    view_id = "3"
    config = make_session_config(allow_concurrent_push=False, session_timeout_seconds=30)
    
    with patch('fustor_fusion.api.session._get_session_config', return_value=config):
        payload1 = type('CreateSessionPayload', (), {})()
        payload1.task_id = "different_task_1"
        
        request1 = MockRequest(client_host="192.168.1.14")
        
        result1 = await create_session(payload1, request1, view_id)
        session_id1 = result1["session_id"]
        
        assert session_id1 is not None
        
        payload2 = type('CreateSessionPayload', (), {})()
        payload2.task_id = "different_task_2"
        
        request2 = MockRequest(client_host="192.168.1.15")
        
        with pytest.raises(Exception) as exc_info:
            await create_session(payload2, request2, view_id)
        
        assert hasattr(exc_info.value, 'status_code')
        assert exc_info.value.status_code == 409


@pytest.mark.asyncio
async def test_concurrent_push_allowed():
    """
    Test that multiple sessions are allowed when concurrent push is enabled
    """
    await session_manager.cleanup_expired_sessions()
    view_state_manager._states.clear()
    
    view_id = "4"
    config = make_session_config(allow_concurrent_push=True, session_timeout_seconds=30)
    
    with patch('fustor_fusion.api.session._get_session_config', return_value=config):
        payload1 = type('CreateSessionPayload', (), {})()
        payload1.task_id = "concurrent_task_1"
        
        request1 = MockRequest(client_host="192.168.1.16")
        
        result1 = await create_session(payload1, request1, view_id)
        session_id1 = result1["session_id"]
        
        assert session_id1 is not None
        
        payload2 = type('CreateSessionPayload', (), {})()
        payload2.task_id = "concurrent_task_2"
        
        request2 = MockRequest(client_host="192.168.1.17")
        
        result2 = await create_session(payload2, request2, view_id)
        session_id2 = result2["session_id"]
        
        assert session_id2 is not None
        assert session_id1 != session_id2


@pytest.mark.asyncio
async def test_same_task_id_with_concurrent_push():
    """
    Test that same task IDs are rejected even when concurrent push is enabled
    """
    await session_manager.cleanup_expired_sessions()
    view_state_manager._states.clear()
    
    view_id = "5"
    config = make_session_config(allow_concurrent_push=True, session_timeout_seconds=30)
    
    with patch('fustor_fusion.api.session._get_session_config', return_value=config):
        payload1 = type('CreateSessionPayload', (), {})()
        payload1.task_id = "repeated_task"
        
        request1 = MockRequest(client_host="192.168.1.18")
        
        result1 = await create_session(payload1, request1, view_id)
        session_id1 = result1["session_id"]
        
        assert session_id1 is not None
        
        payload2 = type('CreateSessionPayload', (), {})()
        payload2.task_id = "repeated_task"
        
        request2 = MockRequest(client_host="192.168.1.19")
        
        with pytest.raises(Exception) as exc_info:
            await create_session(payload2, request2, view_id)
        
        assert hasattr(exc_info.value, 'status_code')
        assert exc_info.value.status_code == 409


@pytest.mark.asyncio
async def test_stale_lock_handling():
    """
    Test the handling of stale locks where view is locked by a session not in session manager
    """
    await session_manager.cleanup_expired_sessions()
    view_state_manager._states.clear()
    
    view_id = "6"
    config = make_session_config(allow_concurrent_push=False, session_timeout_seconds=30)
    
    with patch('fustor_fusion.api.session._get_session_config', return_value=config):
        stale_session_id = str(uuid.uuid4())
        await view_state_manager.lock_for_session(view_id, stale_session_id)
        
        assert await view_state_manager.is_locked_by_session(view_id, stale_session_id)
        
        payload = type('CreateSessionPayload', (), {})()
        payload.task_id = "new_task"
        
        request = MockRequest(client_host="192.168.1.20")
        
        result = await create_session(payload, request, view_id)
        new_session_id = result["session_id"]
        
        assert new_session_id is not None
        assert await view_state_manager.is_locked_by_session(view_id, new_session_id)
        assert not await view_state_manager.is_locked_by_session(view_id, stale_session_id)
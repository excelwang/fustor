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
from fustor_fusion.datastore_state_manager import datastore_state_manager


@dataclass
class DatastoreModel:
    id: int
    name: str
    type: str
    allow_concurrent_push: bool
    session_timeout_seconds: int


class MockRequest:
    def __init__(self, client_host="127.0.0.1"):
        self.client = Mock()
        self.client.host = client_host


@pytest.mark.asyncio
async def test_session_creation_multiple_servers():
    """
    Test that multiple servers can create sessions without 409 errors when using different task IDs
    """
    # Reset managers before test
    await session_manager.cleanup_expired_sessions()
    # Clear datastore states
    datastore_state_manager._states.clear()
    
    datastore_id = 1
    datastore = DatastoreModel(
        id=datastore_id,
        name="test_datastore",
        type="submit",
        allow_concurrent_push=False,  # Default configuration that was causing the issue
        session_timeout_seconds=1  # Short timeout for testing
    )
    
    with patch('fustor_fusion.api.session.datastore_config_cache.get_datastore_config', return_value=datastore):
        # Server 1 creates a session
        payload1 = type('CreateSessionPayload', (), {})()
        payload1.task_id = "task_server1"
        
        request1 = MockRequest(client_host="192.168.1.10")
        
        result1 = await create_session(payload1, request1, datastore_id)
        session_id1 = result1["session_id"]
        
        assert session_id1 is not None
        # Verify that the datastore is locked by this session
        assert await datastore_state_manager.is_locked_by_session(datastore_id, session_id1)
        
        # Wait for the session to timeout
        await asyncio.sleep(1.5)  # Wait longer than the timeout
        
        # At this point, session1 should have expired and been removed from session manager
        # but the lock should also be released
        
        # Server 2 should now be able to create a session with a different task_id
        payload2 = type('CreateSessionPayload', (), {})()
        payload2.task_id = "task_server2"
        
        request2 = MockRequest(client_host="192.168.1.11")
        
        # This should not raise a 409 error anymore
        result2 = await create_session(payload2, request2, datastore_id)
        session_id2 = result2["session_id"]
        
        assert session_id2 is not None
        assert session_id1 != session_id2
        # Verify that the datastore is now locked by the new session
        assert await datastore_state_manager.is_locked_by_session(datastore_id, session_id2)
        # And not by the old session
        assert not await datastore_state_manager.is_locked_by_session(datastore_id, session_id1)


@pytest.mark.asyncio
async def test_session_creation_same_task_id():
    """
    Test that sessions with the same task_id are properly rejected when concurrent push is not allowed
    """
    # Reset managers before test
    await session_manager.cleanup_expired_sessions()
    # Clear datastore states
    datastore_state_manager._states.clear()
    
    datastore_id = 2
    datastore = DatastoreModel(
        id=datastore_id,
        name="test_datastore2",
        type="submit",
        allow_concurrent_push=False,
        session_timeout_seconds=30  # Longer timeout
    )
    
    with patch('fustor_fusion.api.session.datastore_config_cache.get_datastore_config', return_value=datastore):
        # Server 1 creates a session
        payload1 = type('CreateSessionPayload', (), {})()
        payload1.task_id = "same_task"
        
        request1 = MockRequest(client_host="192.168.1.12")
        
        result1 = await create_session(payload1, request1, datastore_id)
        session_id1 = result1["session_id"]
        
        assert session_id1 is not None
        
        # Server 2 tries to create a session with the same task_id
        # This should raise a 409 error
        payload2 = type('CreateSessionPayload', (), {})()
        payload2.task_id = "same_task"  # Same task ID
        
        request2 = MockRequest(client_host="192.168.1.13")
        
        with pytest.raises(Exception) as exc_info:
            await create_session(payload2, request2, datastore_id)
        
        # Check that the exception has the expected 409 status
        assert hasattr(exc_info.value, 'status_code')
        assert exc_info.value.status_code == 409


@pytest.mark.asyncio
async def test_session_creation_different_task_id():
    """
    Test that sessions with different task IDs are allowed when concurrent push is not allowed
    """
    # Reset managers before test
    await session_manager.cleanup_expired_sessions()
    # Clear datastore states
    datastore_state_manager._states.clear()
    
    datastore_id = 3
    datastore = DatastoreModel(
        id=datastore_id,
        name="test_datastore3",
        type="submit",
        allow_concurrent_push=False,
        session_timeout_seconds=30
    )
    
    with patch('fustor_fusion.api.session.datastore_config_cache.get_datastore_config', return_value=datastore):
        # Server 1 creates a session
        payload1 = type('CreateSessionPayload', (), {})()
        payload1.task_id = "different_task_1"
        
        request1 = MockRequest(client_host="192.168.1.14")
        
        result1 = await create_session(payload1, request1, datastore_id)
        session_id1 = result1["session_id"]
        
        assert session_id1 is not None
        
        # Server 2 tries to create a session with a different task_id
        # This should be allowed
        payload2 = type('CreateSessionPayload', (), {})()
        payload2.task_id = "different_task_2"  # Different task ID
        
        request2 = MockRequest(client_host="192.168.1.15")
        
        # Since the first session is still active and allow_concurrent_push is False,
        # this should raise a 409 error
        with pytest.raises(Exception) as exc_info:
            await create_session(payload2, request2, datastore_id)
        
        # Check that the exception has the expected 409 status
        assert hasattr(exc_info.value, 'status_code')
        assert exc_info.value.status_code == 409


@pytest.mark.asyncio
async def test_concurrent_push_allowed():
    """
    Test that multiple sessions are allowed when concurrent push is enabled
    """
    # Reset managers before test
    await session_manager.cleanup_expired_sessions()
    # Clear datastore states
    datastore_state_manager._states.clear()
    
    datastore_id = 4
    datastore = DatastoreModel(
        id=datastore_id,
        name="test_datastore4",
        type="submit",
        allow_concurrent_push=True,  # Allow concurrent push
        session_timeout_seconds=30
    )
    
    with patch('fustor_fusion.api.session.datastore_config_cache.get_datastore_config', return_value=datastore):
        # Server 1 creates a session
        payload1 = type('CreateSessionPayload', (), {})()
        payload1.task_id = "concurrent_task_1"
        
        request1 = MockRequest(client_host="192.168.1.16")
        
        result1 = await create_session(payload1, request1, datastore_id)
        session_id1 = result1["session_id"]
        
        assert session_id1 is not None
        
        # Server 2 tries to create a session with a different task_id
        # This should be allowed since concurrent push is enabled
        payload2 = type('CreateSessionPayload', (), {})()
        payload2.task_id = "concurrent_task_2"  # Different task ID
        
        request2 = MockRequest(client_host="192.168.1.17")
        
        result2 = await create_session(payload2, request2, datastore_id)
        session_id2 = result2["session_id"]
        
        assert session_id2 is not None
        assert session_id1 != session_id2


@pytest.mark.asyncio
async def test_same_task_id_with_concurrent_push():
    """
    Test that same task IDs are rejected even when concurrent push is enabled
    """
    # Reset managers before test
    await session_manager.cleanup_expired_sessions()
    # Clear datastore states
    datastore_state_manager._states.clear()
    
    datastore_id = 5
    datastore = DatastoreModel(
        id=datastore_id,
        name="test_datastore5",
        type="submit",
        allow_concurrent_push=True,  # Allow concurrent push
        session_timeout_seconds=30
    )
    
    with patch('fustor_fusion.api.session.datastore_config_cache.get_datastore_config', return_value=datastore):
        # Server 1 creates a session
        payload1 = type('CreateSessionPayload', (), {})()
        payload1.task_id = "repeated_task"
        
        request1 = MockRequest(client_host="192.168.1.18")
        
        result1 = await create_session(payload1, request1, datastore_id)
        session_id1 = result1["session_id"]
        
        assert session_id1 is not None
        
        # Server 2 tries to create a session with the same task_id
        # This should still be rejected even with concurrent push enabled
        payload2 = type('CreateSessionPayload', (), {})()
        payload2.task_id = "repeated_task"  # Same task ID
        
        request2 = MockRequest(client_host="192.168.1.19")
        
        with pytest.raises(Exception) as exc_info:
            await create_session(payload2, request2, datastore_id)
        
        # Check that the exception has the expected 409 status
        assert hasattr(exc_info.value, 'status_code')
        assert exc_info.value.status_code == 409


@pytest.mark.asyncio
async def test_stale_lock_handling():
    """
    Test the handling of stale locks where datastore is locked by a session not in session manager
    """
    # Reset managers before test
    await session_manager.cleanup_expired_sessions()
    # Clear datastore states
    datastore_state_manager._states.clear()
    
    datastore_id = 6
    datastore = DatastoreModel(
        id=datastore_id,
        name="test_datastore6",
        type="submit",
        allow_concurrent_push=False,
        session_timeout_seconds=30
    )
    
    with patch('fustor_fusion.api.session.datastore_config_cache.get_datastore_config', return_value=datastore):
        # Manually create a lock in the datastore state manager for a session that doesn't exist in session manager
        stale_session_id = str(uuid.uuid4())
        await datastore_state_manager.lock_for_session(datastore_id, stale_session_id)
        
        # Verify the datastore is locked by this stale session
        assert await datastore_state_manager.is_locked_by_session(datastore_id, stale_session_id)
        
        # Create a new session, which should detect the stale lock and handle it
        payload = type('CreateSessionPayload', (), {})()
        payload.task_id = "new_task"
        
        request = MockRequest(client_host="192.168.1.20")
        
        # This should succeed by automatically unlocking the stale lock
        result = await create_session(payload, request, datastore_id)
        new_session_id = result["session_id"]
        
        assert new_session_id is not None
        # The datastore should now be locked by the new session, not the stale one
        assert await datastore_state_manager.is_locked_by_session(datastore_id, new_session_id)
        assert not await datastore_state_manager.is_locked_by_session(datastore_id, stale_session_id)
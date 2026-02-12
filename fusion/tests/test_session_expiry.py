"""
Tests for SessionManager.cleanup_expired_sessions.
"""
import pytest
import time
from unittest.mock import patch, AsyncMock
from fustor_fusion.core.session_manager import SessionManager


@pytest.fixture
def sm():
    return SessionManager(default_session_timeout=1)


@pytest.mark.asyncio
async def test_expired_sessions_are_removed(sm):
    """Sessions past their timeout are cleaned up."""
    await sm.create_session_entry("v1", "s1", session_timeout_seconds=1)
    assert await sm.get_session_info("v1", "s1") is not None

    # Simulate time passing beyond timeout
    si = (await sm.get_view_sessions("v1"))["s1"]
    si.last_activity = time.monotonic() - 10  # force expired

    # Patch external managers to avoid import issues
    with patch('fustor_fusion.core.session_manager.SessionManager._terminate_session_internal',
               new_callable=AsyncMock, return_value=True) as mock_terminate:
        await sm.cleanup_expired_sessions()
        mock_terminate.assert_called_once_with("v1", "s1", "expired")


@pytest.mark.asyncio
async def test_active_sessions_not_removed(sm):
    """Sessions within timeout are not cleaned up."""
    await sm.create_session_entry("v1", "s1", session_timeout_seconds=60)
    
    await sm.cleanup_expired_sessions()
    
    # Session should still exist
    assert await sm.get_session_info("v1", "s1") is not None


@pytest.mark.asyncio
async def test_cleanup_multiple_views(sm):
    """Cleanup scans sessions across different views."""
    await sm.create_session_entry("v1", "s1", session_timeout_seconds=1)
    await sm.create_session_entry("v2", "s2", session_timeout_seconds=1)
    await sm.create_session_entry("v2", "s3", session_timeout_seconds=60)  # long timeout

    # Expire s1 and s2
    for vid, sid in [("v1", "s1"), ("v2", "s2")]:
        sessions = await sm.get_view_sessions(vid)
        if sid in sessions:
            sessions[sid].last_activity = time.monotonic() - 10

    with patch('fustor_fusion.core.session_manager.SessionManager._terminate_session_internal',
               new_callable=AsyncMock, return_value=True) as mock_terminate:
        await sm.cleanup_expired_sessions()
        assert mock_terminate.call_count == 2
        # s3 should NOT be terminated
        called_sids = {call.args[1] for call in mock_terminate.call_args_list}
        assert "s1" in called_sids
        assert "s2" in called_sids
        assert "s3" not in called_sids

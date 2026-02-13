import pytest
import pytest_asyncio
import asyncio
import time
from unittest.mock import MagicMock, AsyncMock, patch
from fustor_fusion.core.session_manager import SessionManager

class TestSessionLifecycle:
    """
    Test suite for SessionManager lifecycle events:
    - Job creation/completion
    - Session expiration
    - Leader election triggering
    """

    @pytest_asyncio.fixture
    async def manager(self):
        mgr = SessionManager(default_session_timeout=1)
        yield mgr
        # Cleanup
        await mgr.stop_periodic_cleanup()

    @pytest.mark.asyncio
    async def test_agent_job_lifecycle(self, manager):
        """Verify AgentJob creation and completion flow."""
        view_id = "view-job"
        sessions = ["s1", "s2"]
        
        # 0. Create sessions so they count as "active"
        await manager.create_session_entry(view_id, "s1")
        await manager.create_session_entry(view_id, "s2")
        
        # 1. Create Job
        job_id = await manager.create_agent_job(view_id, "/path/target", sessions)
        assert job_id
        assert job_id in manager._agent_jobs
        job = manager._agent_jobs[job_id]
        assert job.status == "RUNNING"
        assert job.expected_sessions == set(sessions)
        
        # 2. Complete for s1
        await manager.complete_agent_job(view_id, "s1", "/path/target")
        assert "s1" in job.completed_sessions
        assert job.status == "RUNNING"
        
        # 3. Complete for s2 (Final)
        await manager.complete_agent_job(view_id, "s2", "/path/target")
        assert "s2" in job.completed_sessions
        assert job.status == "COMPLETED"
        assert job.completed_at is not None

    @pytest.mark.asyncio
    async def test_session_expiration(self, manager):
        """Verify sessions are cleaned up after timeout."""
        view_id = "view-exp"
        session_id = "sess-exp"
        
        # Mock external dependencies to avoid import errors during termination
        mock_vsm = AsyncMock()
        mock_vsm.is_leader.return_value = False
        
        with patch.dict('sys.modules', {
            'fustor_fusion.view_state_manager': MagicMock(view_state_manager=mock_vsm),
            'fustor_fusion.view_manager.manager': MagicMock(),
        }):
            # 1. Create session with 0.1s timeout
            await manager.create_session_entry(view_id, session_id, session_timeout_seconds=0.1)
            assert session_id in manager._sessions[view_id]
            
            # 2. Wait for expiration
            await asyncio.sleep(0.2)
            
            # 3. Trigger cleanup
            await manager.cleanup_expired_sessions()
            
            # 4. Verify removal
            if view_id in manager._sessions:
                assert session_id not in manager._sessions[view_id]

    @pytest.mark.asyncio
    async def test_leader_promotion_trigger(self, manager):
        """Verify termination triggers leader promotion for remaining sessions."""
        view_id = "view-elect"
        leader_id = "s-leader"
        follower_id = "s-follower"
        
        # Setup mocks
        mock_vsm = AsyncMock()
        mock_vsm.is_leader.return_value = True # The one being removed IS leader
        mock_vsm.try_become_leader.return_value = True # Follower accepts promotion
        
        with patch.dict('sys.modules', {
            'fustor_fusion.view_state_manager': MagicMock(view_state_manager=mock_vsm),
            'fustor_fusion.view_manager.manager': MagicMock(),
        }):
            # 1. Create two sessions
            await manager.create_session_entry(view_id, leader_id)
            await manager.create_session_entry(view_id, follower_id)
            
            # 2. Terminate Leader
            await manager.terminate_session(view_id, leader_id)
            
            # 3. Verify VSM calls
            # Check leader released
            mock_vsm.release_leader.assert_awaited_with(view_id, leader_id)
            
            # Check promotion attempted for follower
            mock_vsm.try_become_leader.assert_awaited_with(view_id, follower_id)
            mock_vsm.set_authoritative_session.assert_awaited_with(view_id, follower_id)

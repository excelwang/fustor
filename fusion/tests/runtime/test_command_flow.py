import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock
from fustor_fusion.core.session_manager import SessionManager
from fustor_view_fs.driver import FSViewDriver
from fustor_fusion_sdk.interfaces import SessionInfo

@pytest.mark.asyncio
async def test_fusion_command_queue_flow():
    # 1. Setup SessionManager
    sm = SessionManager()
    view_id = "view-1"
    session_id = "session-1"
    
    # Create a session
    await sm.create_session_entry(view_id, session_id, task_id="task-1")
    
    # 2. Setup Driver
    driver = FSViewDriver("driver-1", view_id, config={})
    # Mock arbitrator to return our session as leader
    driver.arbitrator = MagicMock()
    driver.arbitrator.get_leader_session_id = AsyncMock(return_value=session_id)
    
    # Patch session_manager used by driver (it imports the global one)
    # We need to make sure the driver uses OUR sm instance or we use the global one.
    # The driver does `from fustor_fusion.core.session_manager import session_manager` inside the method.
    # So we should patch that import or verify using the global instance.
    
    from fustor_fusion.core.session_manager import session_manager as global_sm
    # Reset global sm for test isolation if needed, but easier to just use it.
    await global_sm.clear_all_sessions(view_id)
    await global_sm.create_session_entry(view_id, session_id, task_id="task-1")
    
    # 3. Trigger Scan
    path = "/data/logs"
    success = await driver.trigger_realtime_scan(path, recursive=True)
    assert success is True
    
    # 4. Verify Command is Queued (Internal state check)
    session = await global_sm.get_session_info(view_id, session_id)
    assert session is not None
    assert len(session.pending_commands) == 1
    cmd = session.pending_commands[0]
    assert cmd["type"] == "scan"
    assert cmd["path"] == path
    
    # 4.1 Verify Scan Pending Status
    assert await global_sm.has_pending_scan(view_id, path) is True
    assert session.pending_scans is not None
    assert path in session.pending_scans
    
    # 5. Verify Command Retrieval via Heatbeat (keep_session_alive)
    alive, commands = await global_sm.keep_session_alive(view_id, session_id)
    assert alive is True
    assert len(commands) == 1
    assert commands[0]["type"] == "scan"
    
    # 6. Verify Queue Cleared but Scan Pending remains
    session = await global_sm.get_session_info(view_id, session_id)
    assert len(session.pending_commands) == 0
    assert await global_sm.has_pending_scan(view_id, path) is True
    
    # 7. Simulate Scan Completion
    await global_sm.complete_scan(view_id, session_id, path)
    
    # 8. Verify Scan Pending is Cleared
    assert await global_sm.has_pending_scan(view_id, path) is False
    assert path not in session.pending_scans
    
    # 9. cleanup
    await global_sm.remove_session(view_id, session_id)

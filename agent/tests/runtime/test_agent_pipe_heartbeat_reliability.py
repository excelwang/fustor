import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from fustor_agent.runtime.agent_pipe import AgentPipe
from fustor_core.exceptions import SessionObsoletedError
from .mocks import MockSourceHandler, MockSenderHandler

@pytest.fixture
def pipe_config():
    return {
        "error_retry_interval": 0.01,
        "max_consecutive_errors": 2,
        "backoff_multiplier": 2.0,
        "max_backoff_seconds": 1.0,
        "role_check_interval": 0.01,
        "control_loop_interval": 0.01
    }

@pytest.mark.asyncio
async def test_heartbeat_failure_backoff(mock_source, mock_sender, pipe_config):
    """Verify that transient heartbeat failures cause backoff but don't stop the pipe."""
    # Setup: Succeed once then fail
    mock_sender.send_heartbeat = AsyncMock(side_effect=[
        {"role": "leader"},
        RuntimeError("Transient error"),
        RuntimeError("Transient error"),
        {"role": "leader"}
    ])
    
    pipe = AgentPipe(
        "hb-test", "agent:test", pipe_config,
        mock_source, mock_sender
    )
    
    # Manually start session with very short timeout to get fast heartbeat
    await pipe.on_session_created("test-session", role="leader", session_timeout_seconds=0.1)
    
    hb_task = asyncio.create_task(pipe._run_heartbeat_loop())
    
    try:
        # Wait for failures
        await asyncio.sleep(0.3)
        
        # Verify it skipped/failed but continued
        assert mock_sender.send_heartbeat.call_count >= 3
        # Should have incremented error counter
        assert pipe._consecutive_errors >= 2
    finally:
        hb_task.cancel()
        await pipe.stop()

@pytest.mark.asyncio
async def test_heartbeat_session_obsolete_recovery(mock_source, mock_sender, pipe_config):
    """Verify that SessionObsoletedError in heartbeat triggers session reset."""
    hb_called = asyncio.Event()
    
    async def mock_hb(session_id, **kwargs):
        hb_called.set()
        raise SessionObsoletedError("Session expired")
        
    mock_sender.send_heartbeat = mock_hb
    
    pipe = AgentPipe(
        "hb-fatal", "agent:test", pipe_config,
        mock_source, mock_sender
    )
    
    await pipe.on_session_created("sess-old", role="leader", session_timeout_seconds=0.1)
    
    hb_task = asyncio.create_task(pipe._run_heartbeat_loop())
    
    try:
        await asyncio.wait_for(hb_called.wait(), timeout=1.0)
        await asyncio.sleep(0.05)
        
        # Should have cleared session
        assert pipe.session_id is None
        assert pipe.current_role is None
    finally:
        hb_task.cancel()
        await pipe.stop()

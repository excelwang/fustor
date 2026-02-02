import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from fustor_agent.runtime.agent_pipeline import AgentPipeline
from fustor_core.exceptions import SessionObsoletedError
from .mocks import MockSourceHandler, MockSenderHandler

@pytest.fixture
def pipeline_config():
    return {
        "heartbeat_interval_sec": 0.05,
        "error_retry_interval": 0.01,
        "max_consecutive_errors": 2,
        "backoff_multiplier": 2.0,
        "max_backoff_seconds": 1.0,
        "role_check_interval": 0.01,
        "control_loop_interval": 0.01
    }

@pytest.mark.asyncio
async def test_heartbeat_failure_backoff(mock_source, mock_sender, pipeline_config):
    """Verify that transient heartbeat failures cause backoff but don't stop the pipeline."""
    # Setup: Succeed once then fail
    mock_sender.send_heartbeat = AsyncMock(side_effect=[
        {"role": "leader"},
        RuntimeError("Transient error"),
        RuntimeError("Transient error"),
        {"role": "leader"}
    ])
    
    pipeline = AgentPipeline(
        "hb-test", "agent:test", pipeline_config,
        mock_source, mock_sender
    )
    
    # Manually start session
    pipeline.session_id = "test-session"
    pipeline.current_role = "leader"
    
    hb_task = asyncio.create_task(pipeline._run_heartbeat_loop())
    
    try:
        # Wait for failures
        await asyncio.sleep(0.3)
        
        # Verify it skipped/failed but continued
        assert mock_sender.send_heartbeat.call_count >= 3
        # Should have incremented error counter
        assert pipeline._consecutive_errors >= 2
    finally:
        hb_task.cancel()
        await pipeline.stop()

@pytest.mark.asyncio
async def test_heartbeat_session_obsolete_recovery(mock_source, mock_sender, pipeline_config):
    """Verify that SessionObsoletedError in heartbeat triggers session reset."""
    hb_called = asyncio.Event()
    
    async def mock_hb(session_id):
        hb_called.set()
        raise SessionObsoletedError("Session expired")
        
    mock_sender.send_heartbeat = mock_hb
    
    pipeline = AgentPipeline(
        "hb-fatal", "agent:test", pipeline_config,
        mock_source, mock_sender
    )
    
    pipeline.session_id = "sess-old"
    pipeline.current_role = "leader"
    
    hb_task = asyncio.create_task(pipeline._run_heartbeat_loop())
    
    try:
        await asyncio.wait_for(hb_called.wait(), timeout=1.0)
        await asyncio.sleep(0.05)
        
        # Should have cleared session
        assert pipeline.session_id is None
        assert pipeline.current_role is None
    finally:
        hb_task.cancel()
        await pipeline.stop()


import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from fustor_core.pipe import PipeState
from fustor_agent.runtime.agent_pipe import AgentPipe
from fustor_core.pipe.handler import SourceHandler
from fustor_core.pipe.sender import SenderHandler

@pytest.fixture
def mock_source():
    handler = MagicMock(spec=SourceHandler)
    handler.initialize = AsyncMock()
    handler.close = AsyncMock()
    handler.schema_name = "test-schema"
    return handler

@pytest.fixture
def mock_sender():
    handler = MagicMock(spec=SenderHandler)
    handler.initialize = AsyncMock()
    handler.close = AsyncMock()
    handler.create_session = AsyncMock(return_value=("sess-123", {"role": "leader"}))
    handler.send_heartbeat = AsyncMock(return_value={"role": "leader"})
    handler.close_session = AsyncMock()
    return handler

@pytest.fixture
def reliability_config():
    return {
        "heartbeat_interval_sec": 0.01,
        "audit_interval_sec": 0.01,
        "sentinel_interval_sec": 0.01,
        "error_retry_interval": 0.01,
        "backoff_multiplier": 2.0,
        "max_backoff_seconds": 0.1,
        "max_consecutive_errors": 3,
        "batch_size": 10
    }

def test_backoff_calculation():
    pipe = AgentPipe("test", "t1", {
        "error_retry_interval": 1.0, 
        "backoff_multiplier": 2.0, 
        "max_backoff_seconds": 10.0
    }, MagicMock(), MagicMock())
    
    assert pipe._calculate_backoff(0) == 0.0
    assert pipe._calculate_backoff(1) == 1.0
    assert pipe._calculate_backoff(2) == 2.0
    assert pipe._calculate_backoff(3) == 4.0
    assert pipe._calculate_backoff(10) == 10.0 # Maxed out

def test_handle_error_counter(mock_source, mock_sender, reliability_config):
    pipe = AgentPipe("test", "t1", reliability_config, mock_source, mock_sender)
    
    backoff = pipe._handle_error(Exception("Test 1"), "test-loop")
    assert pipe._consecutive_errors == 1
    assert backoff == 0.01
    
    backoff = pipe._handle_error(Exception("Test 2"), "test-loop")
    assert pipe._consecutive_errors == 2
    assert backoff == 0.02
    
    backoff = pipe._handle_error(Exception("Test 3"), "test-loop")
    assert pipe._consecutive_errors == 3
    assert backoff == 0.04

@pytest.mark.asyncio
async def test_heartbeat_loop_uses_backoff(mock_source, mock_sender, reliability_config, mocker):
    mock_sender.send_heartbeat.side_effect = Exception("HB error")
    pipe = AgentPipe("test", "t1", reliability_config, mock_source, mock_sender)
    pipe.session_id = "sess-1"
    
    # Mock sleep to capture backoff value
    mock_sleep = mocker.patch("asyncio.sleep", AsyncMock())
    spy_handle = mocker.spy(pipe, "_handle_error")
    
    # We want to run exactly one iteration
    # To do this safely, we can make the second iteration exit
    async def side_effect_stop_loop(*args, **kwargs):
        pipe.session_id = None
        return None
    mock_sleep.side_effect = side_effect_stop_loop

    await pipe._run_heartbeat_loop()
    
    assert spy_handle.called
    assert pipe._consecutive_errors >= 1
    # Check that sleep was called with at least 0.01 (the backoff for 1st error)
    # The actual sleep is max(interval, backoff)
    assert mock_sleep.call_args[0][0] >= 0.01

@pytest.mark.asyncio
async def test_audit_loop_uses_backoff(mock_source, mock_sender, reliability_config, mocker):
    # Simulating leader role so it enters the try block
    pipe = AgentPipe("test", "t1", reliability_config, mock_source, mock_sender)
    pipe.current_role = "leader"
    pipe._set_state(PipeState.RUNNING)
    
    # We need to mock _run_audit_sync to avoid side effects
    mocker.patch.object(pipe, "_run_audit_sync", AsyncMock())
    
    # Mock sleep to fail the task execution or just skip waiting
    # We force an error in sleep or by some other means
    mock_sleep = mocker.patch("asyncio.sleep", AsyncMock())
    
    # Let's make the FIRST sleep (initial delay) fail
    mock_sleep.side_effect = [Exception("Sleep Err"), None]
    
    spy_handle = mocker.spy(pipe, "_handle_error")
    
    # Use a function side effect to handle repeated calls
    loop_count = 0
    async def sleep_se(delay):
        nonlocal loop_count
        if delay == reliability_config["audit_interval_sec"]:
            loop_count += 1
            if loop_count == 1:
                raise Exception("Sample Error")
            else:
                pipe._set_state(PipeState.STOPPED)
        return None

    mock_sleep.side_effect = sleep_se

    await pipe._run_audit_loop()
    
    assert spy_handle.called
    assert pipe._consecutive_errors >= 1

def test_alert_threshold(mock_source, mock_sender, reliability_config, caplog):
    pipe = AgentPipe("test", "t1", reliability_config, mock_source, mock_sender)
    
    # 1. Error 1 & 2 -> no warning
    pipe._handle_error(Exception("E1"), "test")
    pipe._handle_error(Exception("E2"), "test")
    assert "reached threshold" not in caplog.text
    
    # 3. Error 3 -> warning logged
    pipe._handle_error(Exception("E3"), "test")
    assert "reached threshold" in caplog.text

@pytest.mark.asyncio
async def test_heartbeat_recovery_resets_counter(mock_source, mock_sender, reliability_config, mocker):
    pipe = AgentPipe("test", "t1", reliability_config, mock_source, mock_sender)
    pipe.session_id = "sess-1"
    pipe._consecutive_errors = 5
    
    # Mock sleep to exit after one successful HB
    mock_sleep = mocker.patch("asyncio.sleep", AsyncMock())
    async def stop_loop(*args):
        pipe.session_id = None
        return None
    mock_sleep.side_effect = stop_loop
    
    await pipe._run_heartbeat_loop()
    
    assert pipe._consecutive_errors == 0

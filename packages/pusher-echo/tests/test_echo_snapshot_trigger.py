"""
Test cases to verify that the echo pusher can trigger snapshot sync.
This test simulates the scenario where the echo pusher returns snapshot_needed=True
for echo tasks, which should trigger the _run_snapshot_sync method in the sync instance.
"""
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from fustor_pusher_echo import EchoDriver
from fustor_core.models.config import PusherConfig, PasswdCredential
from fustor_core.event import UpdateEvent

@pytest.mark.asyncio
async def test_echo_pusher_requests_snapshot_on_first_push():
    """Test that echo pusher requests snapshot on the first push."""
    # 1. Arrange
    config = PusherConfig(
        driver="echo", 
        endpoint="dummy",  # Required field
        credential=PasswdCredential(user="test")
    )
    driver = EchoDriver("test-echo", config)
    
    # Simulate events
    events = [UpdateEvent(event_schema="test", table="test", rows=[{"id": 1, "name": "test"}], fields=["id", "name"])]

    # 2. Act
    result = await driver.push(events, task_id="echo-sync-fs", agent_id="test-agent")

    # 3. Assert
    assert result == {"snapshot_needed": True}


@pytest.mark.asyncio
async def test_echo_pusher_requests_snapshot_on_first_push_for_any_task():
    """Test that echo pusher requests snapshot on the first push, regardless of task name."""
    # 1. Arrange
    config = PusherConfig(
        driver="echo", 
        endpoint="dummy",  # Required field
        credential=PasswdCredential(user="test")
    )
    driver = EchoDriver("test-echo", config)
    
    # Simulate events
    events = [UpdateEvent(event_schema="test", table="test", rows=[{"id": 1, "name": "test"}], fields=["id", "name"])]

    # 2. Act
    result = await driver.push(events, task_id="some-other-task", agent_id="test-agent")

    # 3. Assert
    assert result == {"snapshot_needed": True}


@pytest.mark.asyncio
async def test_echo_pusher_requests_snapshot_on_first_push_with_missing_task_id():
    """Test that echo pusher requests snapshot on the first push even when task_id is not provided."""
    # 1. Arrange
    config = PusherConfig(
        driver="echo", 
        endpoint="dummy",  # Required field
        credential=PasswdCredential(user="test")
    )
    driver = EchoDriver("test-echo", config)
    
    # Simulate events
    events = [UpdateEvent(event_schema="test", table="test", rows=[{"id": 1, "name": "test"}], fields=["id", "name"])]

    # 2. Act
    result = await driver.push(events, agent_id="test-agent")  # No task_id provided

    # 3. Assert
    assert result == {"snapshot_needed": True}


@pytest.mark.asyncio
async def test_echo_pusher_logs_properly():
    """Test that echo pusher still logs properly while triggering snapshots."""
    import logging
    from io import StringIO
    
    # 1. Arrange
    config = PusherConfig(
        driver="echo", 
        endpoint="dummy",  # Required field
        credential=PasswdCredential(user="test")
    )
    driver = EchoDriver("test-echo", config)
    
    # Capture logs
    log_stream = StringIO()
    handler = logging.StreamHandler(log_stream)
    logger = logging.getLogger(f"fustor_agent.pusher.echo.test-echo")
    logger.addHandler(handler)
    logger.setLevel(logging.INFO) # Ensure INFO logs are captured
    events = [UpdateEvent(event_schema="test", table="test", rows=[{"id": 1, "name": "test"}], fields=["id", "name"])]

    # 2. Act
    result = await driver.push(events, task_id="echo-sync-fs", agent_id="test-agent", is_snapshot_end=True)

    # 3. Assert
    assert result == {"snapshot_needed": True}
    log_output = log_stream.getvalue()
    assert "[EchoPusher]" in log_output
    assert "Agent: test-agent" in log_output
    assert "Task: echo-sync-fs" in log_output
    assert "本批次: 1条" in log_output
    assert "累计: 1条" in log_output
    assert "Flags: SNAPSHOT_END" in log_output
    assert "First event data" in log_output
    
    # Cleanup
    logger.removeHandler(handler)


@pytest.mark.asyncio
async def test_echo_pusher_maintains_statistics():
    """Test that echo pusher maintains cumulative statistics while triggering snapshots."""
    # 1. Arrange
    config = PusherConfig(
        driver="echo", 
        endpoint="dummy",  # Required field
        credential=PasswdCredential(user="test")
    )
    driver = EchoDriver("test-stats", config)
    
    # First batch of events
    events1 = [UpdateEvent(event_schema="test", table="test", rows=[{"id": 1}, {"id": 2}], fields=["id"])]
    
    # Second batch of events
    events2 = [UpdateEvent(event_schema="test", table="test", rows=[{"id": 3}], fields=["id"])]

    # 2. Act
    result1 = await driver.push(events1, task_id="echo-task-1", agent_id="test-agent")
    result2 = await driver.push(events2, task_id="echo-task-2", agent_id="test-agent")

    # 3. Assert
    # First call should return snapshot_needed=True (echo-task-1 starts with "echo")
    assert result1 == {"snapshot_needed": True}
    
    # Second call should return snapshot_needed=False because the trigger is one-time
    assert result2 == {"snapshot_needed": False}
    
    # Statistics should be cumulative (2 from first batch + 1 from second batch = 3 total)
    assert driver.total_rows == 3
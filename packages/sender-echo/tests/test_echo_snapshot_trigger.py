"""
Test cases to verify that the echo sender can trigger snapshot sync.
This test simulates the scenario where the echo sender returns snapshot_needed=True
for echo tasks, which should trigger the _run_message_sync method in the pipeline instance.
"""
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from fustor_sender_echo import EchoDriver
from fustor_core.models.config import SenderConfig, PasswdCredential
from fustor_core.event import UpdateEvent

@pytest.mark.asyncio
async def test_echo_sender_requests_snapshot_on_first_push():
    """Test that echo sender requests snapshot on the first push."""
    # 1. Arrange
    credential = {"user": "test"}
    config = {"batch_size": 10}
    driver = EchoDriver("test-echo", "http://localhost", credential, config)
    
    # Simulate events
    events = [UpdateEvent(event_schema="test", table="test", rows=[{"id": 1, "name": "test"}], fields=["id", "name"])]

    # 2. Act
    result = await driver.send_events(events, source_type="snapshot")

    # 3. Assert
    assert result == {"success": True, "snapshot_needed": True}


@pytest.mark.asyncio
async def test_echo_sender_requests_snapshot_on_first_push_for_any_task():
    """Test that echo sender requests snapshot on the first push, regardless of task name."""
    # 1. Arrange
    credential = {"user": "test"}
    config = {"batch_size": 10}
    driver = EchoDriver("test-echo", "http://localhost", credential, config)
    
    # Simulate events
    events = [UpdateEvent(event_schema="test", table="test", rows=[{"id": 1, "name": "test"}], fields=["id", "name"])]

    # 2. Act
    result = await driver.send_events(events, source_type="realtime")

    # 3. Assert
    assert result == {"success": True, "snapshot_needed": True}


@pytest.mark.asyncio
async def test_echo_sender_requests_snapshot_on_first_push_with_missing_task_id():
    """Test that echo sender requests snapshot on the first push even when task_id is not provided."""
    # 1. Arrange
    credential = {"user": "test"}
    config = {"batch_size": 10}
    driver = EchoDriver("test-echo", "http://localhost", credential, config)
    
    # Simulate events
    events = [UpdateEvent(event_schema="test", table="test", rows=[{"id": 1, "name": "test"}], fields=["id", "name"])]

    # 2. Act
    result = await driver.send_events(events)  # Use defaults

    # 3. Assert
    assert result == {"success": True, "snapshot_needed": True}


@pytest.mark.asyncio
async def test_echo_sender_logs_properly():
    """Test that echo sender still logs properly while triggering snapshots."""
    import logging
    from io import StringIO
    
    # 1. Arrange
    credential = {"user": "test"}
    config = {"batch_size": 10}
    driver = EchoDriver("test-echo", "http://localhost", credential, config)
    
    # Capture logs
    log_stream = StringIO()
    handler = logging.StreamHandler(log_stream)
    logger = logging.getLogger(f"fustor_agent.sender.echo.test-echo")
    logger.addHandler(handler)
    logger.setLevel(logging.INFO) # Ensure INFO logs are captured
    events = [UpdateEvent(event_schema="test", table="test", rows=[{"id": 1, "name": "test"}], fields=["id", "name"])]

    # 2. Act
    result = await driver.send_events(events, source_type="snapshot", is_end=True)

    # 3. Assert
    assert result == {"success": True, "snapshot_needed": True}
    log_output = log_stream.getvalue()
    assert "[EchoSender]" in log_output
    assert "[SNAPSHOT]" in log_output
    assert "Task: test-echo" in log_output
    assert "本批次: 1条" in log_output
    assert "累计: 1条" in log_output
    assert "Flags: END" in log_output
    assert "First event data" in log_output
    
    # Cleanup
    logger.removeHandler(handler)


@pytest.mark.asyncio
async def test_echo_sender_maintains_statistics():
    """Test that echo sender maintains cumulative statistics while triggering snapshots."""
    # 1. Arrange
    credential = {"user": "test"}
    config = {"batch_size": 10}
    driver = EchoDriver("test-stats", "http://localhost", credential, config)
    
    # First batch of events
    events1 = [UpdateEvent(event_schema="test", table="test", rows=[{"id": 1}, {"id": 2}], fields=["id"])]
    
    # Second batch of events
    events2 = [UpdateEvent(event_schema="test", table="test", rows=[{"id": 3}], fields=["id"])]

    # 2. Act
    result1 = await driver.send_events(events1, source_type="realtime")
    result2 = await driver.send_events(events2, source_type="realtime")

    # 3. Assert
    # First call should return snapshot_needed=True
    assert result1 == {"success": True, "snapshot_needed": True}
    
    # Second call should return snapshot_needed=False because the trigger is one-time
    assert result2 == {"success": True, "snapshot_needed": False}
    
    # Statistics should be cumulative (2 from first batch + 1 from second batch = 3 total)
    assert driver.total_rows == 3
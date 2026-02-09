"""
Integration test to verify that the echo sender can trigger snapshot sync.
"""
import asyncio
import pytest
from fustor_sender_echo import EchoDriver
from fustor_core.event import UpdateEvent

@pytest.mark.asyncio
async def test_echo_pipe_instance_triggers_snapshot():
    """Integration test to verify echo sender triggers snapshot."""
    # 1. Arrange - Create configurations
    credential = {"user": "echo-user"}
    config = {"batch_size": 100}
    
    # Create echo sender and verify it returns snapshot_needed=True
    echo_driver = EchoDriver("echo-sender", "http://localhost", credential, config)
    
    # Mock an event to simulate push
    mock_events = [UpdateEvent(event_schema="test", table="files", rows=[{"file_path": "/tmp/test.txt", "size": 100}], fields=["file_path", "size"])]    
    
    # send_events should return snapshot_needed=True on first call
    result = await echo_driver.send_events(mock_events, source_type="realtime")
    
    # Verify that snapshot sync would be triggered
    assert result == {"success": True, "snapshot_needed": True}, "Echo sender should request snapshot on first call"


@pytest.mark.asyncio
async def test_snapshot_trigger_once_and_only_once():
    """
    Tests that the EchoSender triggers a snapshot on the first push, and not on subsequent pushes.
    """
    credential = {"user": "test"}
    config = {"batch_size": 10}
    driver = EchoDriver("test-driver", "http://localhost", credential, config)
    events = [UpdateEvent(event_schema="test", table="test", rows=[{"id": 1}], fields=["id"])]
    
    # First push should trigger a snapshot
    result1 = await driver.send_events(events, source_type="realtime")
    assert result1 == {"success": True, "snapshot_needed": True}, "Should trigger snapshot on the first push"

    # Second push should NOT trigger a snapshot
    result2 = await driver.send_events(events, source_type="realtime")
    assert result2 == {"success": True, "snapshot_needed": False}, "Should NOT trigger snapshot on the second push"
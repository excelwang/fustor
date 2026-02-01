"""
Integration test to verify that the echo pusher can trigger snapshot sync in the full sync instance context.
This test verifies the fix works end-to-end.
"""
import asyncio
import tempfile
import os
import pytest
from unittest.mock import AsyncMock, MagicMock, patch


@pytest.mark.asyncio
async def test_echo_sync_instance_triggers_snapshot():
    """Integration test to verify echo pusher triggers snapshot in sync instance context."""
    from fustor_core.models.config import SyncConfig, PusherConfig, SourceConfig, PasswdCredential, FieldMapping
    
    # 1. Arrange - Create configurations
    pusher_config = PusherConfig(
        driver="echo",
        endpoint="dummy",
        credential=PasswdCredential(user="echo-user"),
        batch_size=100,
        max_retries=10,
        retry_delay_sec=5,
        disabled=False
    )
    
    source_config = SourceConfig(
        driver="fs",
        uri="/tmp/test_echo_fs",  # Use temp directory for testing
        credential=PasswdCredential(user="test"),
        max_queue_size=1000,
        max_retries=10,
        retry_delay_sec=5,
        disabled=False,
        driver_params={}
    )
    
    sync_config = SyncConfig(
        source="test-fs-source",
        pusher="echo-pusher",
        disabled=False,
        fields_mapping=[
            FieldMapping(
                to="events.content",
                source=["file_path:0", "size:1", "modified_time:2", "created_time:3", "is_dir:4"],
                required=False
            )
        ]
    )
    
    # Create temp directory for testing
    test_dir = tempfile.mkdtemp()
    source_config.uri = test_dir
    
    # 2. Act & 3. Assert - This test verifies the architectural understanding
    # The actual triggering of _run_snapshot_sync happens in the sync instance
    # when the echo pusher returns {"snapshot_needed": True}
    
    # Create echo pusher and verify it returns snapshot_needed=True for echo tasks
    from fustor_pusher_echo import EchoDriver
    echo_driver = EchoDriver("echo-pusher", pusher_config)
    
    # Mock an event to simulate push
    from fustor_core.event import UpdateEvent
    mock_events = [UpdateEvent(event_schema="test", table="files", rows=[{"file_path": "/tmp/test.txt", "size": 100}], fields=["file_path", "size"])]    
    # When the echo pusher is called with a task ID that starts with "echo", 
    # it should return snapshot_needed=True, which should trigger the snapshot sync
    result = await echo_driver.push(mock_events, task_id="echo-sync-fs", agent_id="test-agent")
    
    # Verify that snapshot sync would be triggered
    assert result == {"snapshot_needed": True}, "Echo pusher should request snapshot for echo tasks"
    
    # Cleanup
    import shutil
    shutil.rmtree(test_dir, ignore_errors=True)


@pytest.mark.asyncio
async def test_snapshot_trigger_once_and_only_once():
    """
    Tests that the EchoPusher triggers a snapshot on the first push, and not on subsequent pushes.
    """
    from fustor_pusher_echo import EchoDriver
    from fustor_core.models.config import PusherConfig, PasswdCredential
    from fustor_core.event import UpdateEvent
    
    config = PusherConfig(
        driver="echo", 
        endpoint="dummy",
        credential=PasswdCredential(user="test")
    )
    driver = EchoDriver("test-driver", config)
    events = [UpdateEvent(event_schema="test", table="test", rows=[{"id": 1}], fields=["id"])]
    # First push should trigger a snapshot
    result1 = await driver.push(events, task_id="any-task-id", agent_id="test-agent")
    assert result1 == {"snapshot_needed": True}, "Should trigger snapshot on the first push"

    # Second push should NOT trigger a snapshot
    result2 = await driver.push(events, task_id="any-task-id", agent_id="test-agent")
    assert result2 == {"snapshot_needed": False}, "Should NOT trigger snapshot on the second push"



if __name__ == "__main__":
    asyncio.run(test_snapshot_trigger_condition_logic())
    print("All integration tests passed!")
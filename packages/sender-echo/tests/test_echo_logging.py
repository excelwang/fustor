"""
Test cases specifically for echo pusher logging functionality.
This test verifies the echo pusher can write to logs for various event types.
"""
import pytest
import logging
from fustor_sender_echo import EchoDriver
from fustor_core.models.config import SenderConfig, PasswdCredential
from fustor_core.event import UpdateEvent, DeleteEvent, InsertEvent


@pytest.mark.asyncio
async def test_echo_driver_logs_update_events(caplog):
    """Test that echo driver properly logs UpdateEvent data."""
    # 1. Arrange
    credential = {"user": "test"}
    config = {"batch_size": 10}
    driver = EchoDriver("test-update-echo", "http://localhost", credential, config)
    
    # Create UpdateEvent similar to what fs would generate
    update_events = [
        UpdateEvent(
            event_schema="/home/test",
            table="files", 
            rows=[
                {
                    "file_path": "/home/test/file1.txt",
                    "size": 1024,
                    "modified_time": 1700000000.0,
                    "created_time": 1699999000.0,
                    "is_dir": False
                },
                {
                    "file_path": "/home/test/subdir",
                    "size": 4096,
                    "modified_time": 1700000100.0,
                    "created_time": 1699999100.0,
                    "is_dir": True
                }
            ],
            fields=["file_path", "size", "modified_time", "created_time", "is_dir"]
        )
    ]

    # 2. Act & 3. Assert - Check logging
    with caplog.at_level(logging.INFO):
        result = await driver.send_events(update_events, source_type="realtime")
        
        # Verify logging content
        assert "[EchoSender]" in caplog.text
        assert "Task: test-update-echo" in caplog.text
    assert result == {"success": True, "snapshot_needed": True}


@pytest.mark.asyncio
async def test_echo_driver_logs_delete_events(caplog):
    """Test that echo driver properly logs DeleteEvent data."""
    # 1. Arrange
    credential = {"user": "test"}
    config = {"batch_size": 10}
    driver = EchoDriver("test-delete-echo", "http://localhost", credential, config)
    
    # Create DeleteEvent similar to what fs would generate
    delete_events = [
        DeleteEvent(
            event_schema="/home/test",
            table="files", 
            rows=[
                {"file_path": "/home/test/old_file.txt"},
                {"file_path": "/home/test/old_dir"}
            ],
            fields=["file_path"]
        )
    ]

    # 2. Act & 3. Assert - Check logging
    with caplog.at_level(logging.INFO):
        result = await driver.send_events(delete_events, source_type="realtime")
        
        # Verify logging content
        assert "[EchoSender]" in caplog.text
        assert "Task: test-delete-echo" in caplog.text
        assert "本批次: 2条" in caplog.text  # 2 rows in the delete event
    assert result == {"success": True, "snapshot_needed": True}


@pytest.mark.asyncio
async def test_echo_driver_logs_with_snapshot_end_flag(caplog):
    """Test that echo driver properly logs with snapshot end flag."""
    # 1. Arrange
    credential = {"user": "test"}
    config = {"batch_size": 10}
    driver = EchoDriver("test-snapshot-echo", "http://localhost", credential, config)
    
    events = [
        UpdateEvent(
            event_schema="test_schema",
            table="test_table", 
            rows=[{"id": 1, "name": "test"}],
            fields=["id", "name"]
        )
    ]

    # 2. Act & 3. Assert - Check logging with end flag
    with caplog.at_level(logging.INFO):
        result = await driver.send_events(
            events, 
            source_type="realtime", 
            is_end=True
        )
        
        # Verify logging content includes end flag
        assert "[EchoSender]" in caplog.text
        assert "Task: test-snapshot-echo" in caplog.text
        assert "本批次: 1条" in caplog.text
        assert "累计: 1条" in caplog.text
        assert "Flags: END" in caplog.text
        assert result == {"success": True, "snapshot_needed": True}


@pytest.mark.asyncio
async def test_echo_driver_logs_with_multiple_flags(caplog):
    """Test that echo driver properly logs with multiple control flags."""
    # 1. Arrange
    credential = {"user": "test"}
    config = {"batch_size": 10}
    driver = EchoDriver("test-flags-echo", "http://localhost", credential, config)
    
    events = [
        InsertEvent(
            event_schema="test_schema",
            table="test_table", 
            rows=[{"id": 1, "name": "test"}],
            fields=["id", "name"]
        )
    ]

    # 2. Act & 3. Assert - Check logging with end flag
    with caplog.at_level(logging.INFO):
        result = await driver.send_events(
            events, 
            source_type="realtime", 
            is_end=True
        )
        
        # Verify logging content includes flag
        assert "[EchoSender]" in caplog.text
        assert "Flags: END" in caplog.text
        assert result == {"success": True, "snapshot_needed": True}


@pytest.mark.asyncio
async def test_echo_driver_logs_first_event_data(caplog):
    """Test that echo driver logs the first event's data in JSON format."""
    # 1. Arrange
    credential = {"user": "test"}
    config = {"batch_size": 10}
    driver = EchoDriver("test-data-echo", "http://localhost", credential, config)
    
    # Create an event with structured data
    events = [
        UpdateEvent(
            event_schema="test_schema",
            table="files", 
            rows=[
                {
                    "file_path": "/home/test/file.txt",
                    "size": 2048,
                    "modified_time": 1700000000.0,
                    "created_time": 1699999000.0,
                    "is_dir": False
                },
                {
                    "file_path": "/home/test/another.txt",
                    "size": 4096,
                    "modified_time": 1700000100.0,
                    "created_time": 1699999100.0,
                    "is_dir": False
                }
            ],
            fields=["file_path", "size", "modified_time", "created_time", "is_dir"]
        )
    ]

    # 2. Act & 3. Assert - Check that first event data is logged
    with caplog.at_level(logging.INFO):
        await driver.send_events(events, source_type="realtime")
        
        # Verify that the first event's data appears in logs (in JSON format)
        assert "/home/test/file.txt" in caplog.text  # First event's file path
        assert "2048" in caplog.text  # First event's size
        assert "false" in caplog.text  # First event's is_dir value (JSON format)
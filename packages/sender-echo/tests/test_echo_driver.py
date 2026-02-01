import pytest
import json
import logging
from io import StringIO
from fustor_sender_echo import EchoDriver
from fustor_core.models.config import SenderConfig, PasswdCredential
from fustor_core.event import InsertEvent

@pytest.mark.asyncio
async def test_echo_driver_push(caplog):
    """Tests the send_events method of the EchoDriver conforms to the new interface."""
    # 1. Arrange
    credential = {"user": "test"}
    config = {"batch_size": 10}
    driver = EchoDriver("test-echo-id", "http://localhost", credential, config)
    events = [InsertEvent(event_schema="test_schema", table="test_table", rows=[{"id": 1, "msg": "hello"}], fields=["id", "msg"])]
    
    # 2. Act & 3. Assert - Check logging
    with caplog.at_level(logging.INFO):
        result = await driver.send_events(events, source_type="realtime", is_end=True)
        
        # Check for the summary output in logs
        assert "[EchoSender]" in caplog.text
        assert "[REALTIME]" in caplog.text
        assert "Task: test-echo-id" in caplog.text
        assert "本批次: 1条" in caplog.text
        assert "累计: 1条" in caplog.text
        assert "Flags: END" in caplog.text
        
    # Check the result dictionary
    assert result == {"success": True, "snapshot_needed": True}

@pytest.mark.asyncio
async def test_echo_driver_get_needed_fields():
    """Tests the get_needed_fields class method."""
    # 1. Arrange & 2. Act
    fields = await EchoDriver.get_needed_fields()

    # 3. Assert
    assert fields == {}

@pytest.mark.asyncio
async def test_echo_driver_get_wizard_steps():
    """Tests the get_wizard_steps class method."""
    # 1. Arrange & 2. Act
    wizard = await EchoDriver.get_wizard_steps()

    # 3. Assert
    assert "steps" in wizard
    assert wizard["steps"][0]["step_id"] == "confirmation"

@pytest.mark.asyncio
async def test_echo_driver_cumulative_push(caplog):
    """Tests that the driver correctly accumulates row counts over multiple pushes."""
    # 1. Arrange
    credential = {"user": "test"}
    config = {"batch_size": 10}
    driver = EchoDriver("test-echo-id", "http://localhost", credential, config)

    # First batch
    events1 = [
        InsertEvent(event_schema="test_schema", table="files", rows=[{"id": 1}, {"id": 2}], fields=["id"]),
    ]
    
    # Second batch
    events2 = [
        InsertEvent(event_schema="test_schema", table="files", rows=[{"id": 3}], fields=["id"])
    ]

    # Clear any previous log records
    caplog.clear()

    # 2. Act & 3. Assert - First push
    with caplog.at_level(logging.INFO):
        result1 = await driver.send_events(events1)
        
        # Check the logs for first push
        assert "本批次: 2条" in caplog.text
        assert "累计: 2条" in caplog.text
        assert result1 == {"success": True, "snapshot_needed": True}

        # Clear logs for second push
        caplog.clear()
        
        # 2. Act & 3. Assert - Second push
        result2 = await driver.send_events(events2)
        
        # Check the logs for second push
        assert "本批次: 1条" in caplog.text
        assert "累计: 3条" in caplog.text  # 2 (from first) + 1 (from second)
        assert result2 == {"success": True, "snapshot_needed": False}

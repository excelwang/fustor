import pytest
import json
import logging
from io import StringIO
from fustor_pusher_echo import EchoDriver
from fustor_core.models.config import PusherConfig, PasswdCredential
from fustor_core.models.event import InsertEvent

@pytest.mark.asyncio
async def test_echo_driver_push(caplog):
    """Tests the push method of the EchoDriver conforms to the new interface."""
    # 1. Arrange
    config = PusherConfig(driver="echo", endpoint="", credential=PasswdCredential(user="test"))
    driver = EchoDriver("test-echo-id", config)
    events = [InsertEvent(event_schema="test_schema", table="test_table", rows=[{"id": 1, "msg": "hello"}])]
    # 2. Act & 3. Assert - Check logging
    with caplog.at_level(logging.INFO):
        result = await driver.push(events, agent_id="agent-1", task_id="task-1", is_snapshot_end=True)
        
        # Check for the new summary output in logs
        assert "[EchoPusher]" in caplog.text
        assert "Agent: agent-1" in caplog.text
        assert "Task: task-1" in caplog.text
        assert "本批次: 1条" in caplog.text
        assert "累计: 1条" in caplog.text
        assert "Flags: SNAPSHOT_END" in caplog.text
        
    # Check the new result dictionary
    assert result == {"snapshot_needed": True}

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
    config = PusherConfig(driver="echo", endpoint="", credential=PasswdCredential(user="test"))
    driver = EchoDriver("test-echo-id", config)

    # First batch
    events1 = [
        InsertEvent(event_schema="test_schema", table="files", rows=[{"id": 1}, {"id": 2}]),    ]
    
    # Second batch
    events2 = [
        InsertEvent(event_schema="test_schema", table="files", rows=[{"id": 3}])
    ]

    # Clear any previous log records
    caplog.clear()

    # 2. Act & 3. Assert - First push
    with caplog.at_level(logging.INFO):
        result1 = await driver.push(events1)
        
        # Check the logs for first push
        assert "本批次: 2条" in caplog.text
        assert "累计: 2条" in caplog.text
        assert result1 == {"snapshot_needed": True}

        # Clear logs for second push
        caplog.clear()
        
        # 2. Act & 3. Assert - Second push
        result2 = await driver.push(events2)
        
        # Check the logs for second push
        assert "本批次: 1条" in caplog.text
        assert "累计: 3条" in caplog.text  # 2 (from first) + 1 (from second)
        assert result2 == {"snapshot_needed": False}

import asyncio
import pytest
from pathlib import Path
import time

from fustor_agent.runtime.sync import SyncInstance, SyncState
from fustor_agent.services.drivers.source_driver import SourceDriverService
from fustor_agent.services.drivers.pusher_driver import PusherDriverService
from fustor_agent.services.instances.bus import EventBusService
from fustor_core.models.config import SyncConfig, SourceConfig, PusherConfig, PasswdCredential, FieldMapping

# This fixture sets up the configuration for the integration tests.
@pytest.fixture
def integration_configs(tmp_path: Path):
    # Use the tmp_path fixture provided by pytest for the fs driver
    source_config = SourceConfig(
        driver="fs", 
        uri=str(tmp_path),
        credential=PasswdCredential(user="test")
    )
    # Configure the echo driver
    pusher_config = PusherConfig(
        driver="echo", 
        endpoint="", 
        credential=PasswdCredential(user="test"),
        batch_size=10
    )
    # Configure the sync task with field mappings
    sync_config = SyncConfig(
        source="test_source", 
        pusher="test_pusher",
        fields_mapping=[
            FieldMapping(to="target.file_path", source=["fs.files.file_path:0"]),
            FieldMapping(to="target.size", source=["fs.files.size:0"])
        ]
    )
    return sync_config, source_config, pusher_config

import logging

@pytest.mark.asyncio
async def test_snapshot_flow_with_real_drivers(integration_configs, tmp_path: Path, caplog):
    """Integration test for the snapshot flow using fs and echo drivers."""
    sync_config, source_config, pusher_config = integration_configs

    # --- Use real services instead of mocks ---
    sds = SourceDriverService()
    pds = PusherDriverService()
    bus_service = EventBusService(source_configs={"test_source": source_config}, source_driver_service=sds)

    # --- Setup the test condition ---
    test_file = tmp_path / "test1.txt"
    test_file.write_text("hello")

    pusher_schema = {
        "properties": {
            "target.file_path": {"type": "string"},
            "target.size": {"type": "integer"}
        }
    }

    # --- Instantiate the SyncInstance with real drivers ---
    sync_instance = SyncInstance(
        id="test_sync", 
        agent_id="test-agent", 
        config=sync_config, 
        source_config=source_config,
        pusher_config=pusher_config, 
        bus_service=bus_service,
        pusher_driver_service=pds, 
        source_driver_service=sds,
        pusher_schema=pusher_schema
    )

    # --- Act & Assert ---
    with caplog.at_level(logging.INFO):
        await sync_instance._run_snapshot_sync()
        
        # Check that the data was pushed once
        assert "本批次: 2条; 累计: 2条" in caplog.text
        # Check that the final end signal was sent (with 0 rows in its batch)
        assert "本批次: 0条; 累计: 2条 | Flags: SNAPSHOT_END" in caplog.text

@pytest.mark.asyncio
async def test_message_sync_flow_with_real_drivers(integration_configs, tmp_path: Path, caplog):
    """Integration test for the message sync flow using fs and echo drivers."""
    sync_config, source_config, pusher_config = integration_configs
    start_position = int(time.time() * 1000)

    # --- Use real services instead of mocks ---
    sds = SourceDriverService()
    pds = PusherDriverService()
    bus_service = EventBusService(source_configs={"test_source": source_config}, source_driver_service=sds)

    # The pusher schema is needed for field mapping
    pusher_schema = {
        "properties": {
            "target.file_path": {"type": "string"},
            "target.size": {"type": "integer"}
        }
    }

    # --- Instantiate the SyncInstance with real drivers ---
    sync_instance = SyncInstance(
        id="test_sync", 
        agent_id="test-agent", 
        config=sync_config, 
        source_config=source_config,
        pusher_config=pusher_config, 
        bus_service=bus_service,
        pusher_driver_service=pds, 
        source_driver_service=sds,
        pusher_schema=pusher_schema
    )

    # --- Act & Assert ---
    with caplog.at_level(logging.INFO):
        # Run the message sync loop in a cancellable task
        sync_instance.state = SyncState.MESSAGE_SYNC
        task = asyncio.create_task(sync_instance._run_message_sync(start_position))
        
        # Allow the loop to start and the file watcher to initialize
        await asyncio.sleep(0.5)

        # Create a new file to trigger a realtime event
        test_file = tmp_path / "test2.txt"
        test_file.write_text("world")

        # Allow time for the event to be processed
        await asyncio.sleep(0.5)

        # Cleanly stop the task
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        # --- Assert ---
        assert "[EchoPusher]" in caplog.text
        assert "Agent: N/A" in caplog.text
        assert "Task: test_sync" in caplog.text
        assert f'"{str(test_file)}"' in caplog.text
        assert '"size": 5' in caplog.text

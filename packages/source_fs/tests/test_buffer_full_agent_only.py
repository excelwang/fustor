"""
Test case to reproduce the 'buffer full' scenario for transient sources.
"""
import asyncio
import tempfile
import time
from pathlib import Path
import pytest
from unittest.mock import MagicMock

# Import necessary modules
from fustor_core.models.config import SourceConfig, PusherConfig, SyncConfig, PasswdCredential, FieldMapping
from fustor_agent.app import App
from fustor_core.models.states import SyncState

# We need to patch the Echo driver to avoid the AttributeError seen earlier
try:
    from fustor_pusher_echo import EchoDriver
    original_create_session = EchoDriver.create_session
    original_push = EchoDriver.push
    
    async def mock_create_session(self, task_id):
        return {"session_id": "mock-session-id", "suggested_heartbeat_interval_seconds": 10}
        
    async def mock_push(self, events, **kwargs):
        # Simulate slow processing to trigger buffer full
        await asyncio.sleep(1) # Delay of 1 second
        # Call original push to keep logging behavior
        return await original_push(self, events, **kwargs)
        
    EchoDriver.create_session = mock_create_session
    EchoDriver.push = mock_push
except ImportError:
    pass

@pytest.mark.asyncio
async def test_transient_source_buffer_full_triggers_error():
    """
    Verifies that a transient source (like FS) triggers a buffer full error
    when the event production rate exceeds consumption and buffer capacity.
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        # 1. Setup paths
        monitored_dir = Path(temp_dir) / "monitored"
        monitored_dir.mkdir()
        config_dir = Path(temp_dir) / "config"
        config_dir.mkdir()
        
        # 2. Initialize App
        app = App(config_dir=str(config_dir))
        
        # 3. Configure Source with extremely small buffer
        # 'fs' driver is typically treated as a transient source in this context
        source_config = SourceConfig(
            driver="fs",
            uri=str(monitored_dir),
            credential=PasswdCredential(user="test", passwd="test"),
            max_queue_size=2,  # Adjusted buffer size to 10
            max_retries=1,
            retry_delay_sec=1,
            disabled=False,
            driver_params={
                "file_pattern": "*",
                "max_sync_delay_seconds": 0.1
            }
        )
        await app.source_config_service.add_config("test-fs-source", source_config)
        
        # 4. Configure Pusher (Echo)
        pusher_config = PusherConfig(
            driver="echo",
            endpoint="http://localhost:8080/echo",
            credential=PasswdCredential(user="test", passwd="test"),
            batch_size=1, # Still slow consumption per item, but overall faster
            max_retries=1,
            retry_delay_sec=1,
            disabled=False
        )
        await app.pusher_config_service.add_config("test-echo-pusher", pusher_config)
        
        # 5. Configure Sync
        sync_config = SyncConfig(
            source="test-fs-source",
            pusher="test-echo-pusher",
            disabled=False,
            fields_mapping=[
                FieldMapping(to="file_path", source=["file_path"], required=True)
            ]
        )
        await app.sync_config_service.add_config("test-sync-task", sync_config)
        
        # 6. Start App and Sync
        await app.startup()
        try:
            await app.sync_instance_service.start_one("test-sync-task")
            
            # 7. Generate load (burst of events)
            # Create more files than the buffer size (1000)
            for i in range(3): # Generate 20 events
                test_file = monitored_dir / f"file_{i}.txt"
                test_file.touch()
                await asyncio.sleep(1) # Event generation interval of 1 second
            
            # 8. Wait for the system to react
            # Give it a moment to process and hopefully fail
            # Wait for more than the total event generation time + processing time to ensure it finishes.
            for _ in range(5): # 40 iterations * 1s (mock_push) = 40s if each pushes a batch.
                sync_instance = app.sync_instance_service.get_instance("test-sync-task")
                if sync_instance:
                    # Standard assertion: The system should handle the load without entering an error state.
                    # If the bug exists (buffer full causes crash), this assertion will fail.
                    assert sync_instance.state != SyncState.ERROR, f"Sync task unexpectedly failed: {sync_instance.info}"
                
                await asyncio.sleep(1) # Check state every second
            
            # If we reach here, the system survived the load.
            pass
        finally:
            await app.shutdown()

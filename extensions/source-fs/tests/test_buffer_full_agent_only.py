"""
Test case to reproduce the 'buffer full' scenario for transient sources.
"""
import asyncio
import tempfile
import time
from pathlib import Path
import pytest
from unittest.mock import MagicMock
import logging # Added import

# Import necessary modules
from fustor_core.models.config import SourceConfig, SenderConfig, PipeConfig, PasswdCredential, FieldMapping
from fustor_agent.app import App
from fustor_core.models.states import PipeState

@pytest.mark.xfail(reason="Expected to fail: Buffer full bug reproduction")
@pytest.mark.asyncio
async def test_transient_source_buffer_full_triggers_error(caplog):
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
        
        try: # Outer try-finally for app shutdown
            await app.startup()
            
            # 3. Configure Source with extremely small buffer to force overflow
            source_config = SourceConfig(
                driver="fs",
                uri=str(monitored_dir),
                credential=PasswdCredential(user="test", passwd="test"),
                max_queue_size=2,  # Force buffer overflow
                max_retries=1,
                retry_delay_sec=1,
                disabled=False,
                driver_params={
                    "file_pattern": "*",
                    "max_sync_delay_seconds": 0.1
                }
            )
            await app.source_config_service.add_config("test-fs-source", source_config)
            
            # 4. Configure Sender (Echo)
            sender_config = SenderConfig(
                driver="echo",
                uri="http://localhost:8080/echo",
                credential=PasswdCredential(user="test", passwd="test"),
                batch_size=1, # Slow consumer effectively
                max_retries=1,
                retry_delay_sec=1,
                disabled=False
            )
            await app.sender_config_service.add_config("test-echo-sender", sender_config)
            
            # 5. Configure Sync
            pipe_config = PipeConfig(
                source="test-fs-source",
                sender="test-echo-sender",
                disabled=False,
                fields_mapping=[
                    FieldMapping(to="file_path", source=["path"], required=True)
                ]
            )
            await app.pipe_config_service.add_config("test-sync-task", pipe_config)
            
            # Use caplog to check for the specific error message
            with caplog.at_level(logging.ERROR):
                await app.pipe_instance_service.start_one("test-sync-task")
                
                # 7. Generate load (burst of events)
                # Create more files than the buffer size
                for i in range(10): # Generate 10 events quickly
                    test_file = monitored_dir / f"file_{i}.txt"
                    test_file.touch()
                    # No sleep here, we want to flood the buffer
                
                # 8. Wait for the system to react and enter ERROR state
                error_state_found = False
                for _ in range(50): # 50 iterations * 0.1s check = 5s max wait
                    pipe_instance = app.pipe_instance_service.get_instance("test-sync-task")
                    if pipe_instance and pipe_instance.state == PipeState.ERROR:
                        error_state_found = True
                        # Assert the log message content here as part of the bug reproduction check
                        buffer_full_log_found = False
                        for record in caplog.records:
                            if record.levelno == logging.ERROR and "Event buffer is filled with no extra space left!" in record.message:
                                buffer_full_log_found = True
                                break
                        assert buffer_full_log_found, f"Buffer full error message with suggestion not found in logs after ERROR state detected. Logs: {caplog.text}"
                        break
                    await asyncio.sleep(0.1) 
                
                # This assertion will fail if the bug exists (system enters ERROR state)
                assert not error_state_found, f"Sync task entered ERROR state unexpectedly (Bug Reproduced): {pipe_instance.info}"
                
        finally:
            await app.shutdown()
import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch
from fustor_agent.runtime.agent_pipe import AgentPipe
from fustor_source_fs.driver import FSDriver
from fustor_core.pipe.sender import SenderHandler

@pytest.mark.asyncio
async def test_agent_command_execution():
    # 1. Mock Handlers
    mock_source = MagicMock(spec=FSDriver)
    mock_source.scan_path = MagicMock(return_value=iter([])) # Returns empty iterator
    
    mock_sender = AsyncMock(spec=SenderHandler)
    mock_sender.send_heartbeat.return_value = {
        "status":"ok", 
        "role":"leader", 
        "commands": [
            {"type": "scan", "path": "/test/path", "recursive": True}
        ]
    }
    
    # 2. Initialize Pipe
    config = {
        "heartbeat_interval_sec": 0.1,
        "control_loop_interval": 0.1
    }
    pipe = AgentPipe("pipe-1", "task-1", config, mock_source, mock_sender)
    pipe.session_id = "sess-1"
    pipe.current_role = "leader"
    
    # 3. Inject Command Processing (Manually call _handle_commands to avoid starting full loop)
    commands = [{"type": "scan", "path": "/test/path", "recursive": True}]
    await pipe._handle_commands(commands)
    
    # 4. Verify Source Scan Triggered
    # Since _handle_command_scan creates a background task, we need to wait briefly
    await asyncio.sleep(0.1)
    
    mock_source.scan_path.assert_called_once_with("/test/path", recursive=True)
    
    # 5. Verify Sender was called (even with empty iterator, map_batch might be called)
    # If iterator is empty, send_batch might not be called if batch size not reached and flush not triggered?
    # In _run_on_demand_scan:
    # for event in iterator: ...
    # if batch: ...
    # if iterator empty, nothing happens except log.
    
    # Let's mock iterator to yield one event
    event = MagicMock()
    mock_source.scan_path.return_value = iter([event])
    
    # Run again
    await pipe._handle_commands(commands)
    await asyncio.sleep(0.1)
    
    assert mock_source.scan_path.call_count == 2
    assert mock_sender.send_batch.called

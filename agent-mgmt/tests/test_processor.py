import pytest
import asyncio
import os
import signal
from unittest.mock import MagicMock, AsyncMock, patch
from fustor_agent_mgmt.processor import CommandProcessor

class MockPipe:
    def __init__(self):
        self.id = "test-pipe"
        self.session_id = "sess-123"
        self.batch_size = 10
        self.source_handler = MagicMock()
        self.sender_handler = AsyncMock()
        self.stop = AsyncMock()
        
    def map_batch(self, batch):
        return batch

@pytest.mark.asyncio
async def test_handle_command_scan():
    processor = CommandProcessor()
    pipe = MockPipe()
    pipe.source_handler.scan_path = MagicMock(return_value=[{"path": "/a"}, {"path": "/b"}])
    
    cmd = {"type": "scan", "path": "/test", "job_id": "job-1"}
    await processor.process_commands(pipe, [cmd])
    
    # Wait a bit for the background task
    await asyncio.sleep(0.1)
    
    pipe.source_handler.scan_path.assert_called_once_with("/test", recursive=True)
    # 1 batch of events + 1 job_complete signal
    assert pipe.sender_handler.send_batch.call_count == 2
    
    # Verify job_complete signal
    last_call = pipe.sender_handler.send_batch.call_args_list[-1]
    assert last_call[0][2]["phase"] == "job_complete"
    assert last_call[0][2]["metadata"]["job_id"] == "job-1"

@pytest.mark.asyncio
async def test_handle_command_stop_pipe():
    processor = CommandProcessor()
    pipe = MockPipe()
    
    # Wrong agent_pipe_id -> Ignore
    await processor.process_commands(pipe, [{"type": "stop_pipe", "agent_pipe_id": "other"}])
    pipe.stop.assert_not_called()
    
    # Correct agent_pipe_id -> Stop
    await processor.process_commands(pipe, [{"type": "stop_pipe", "agent_pipe_id": "test-pipe"}])
    await asyncio.sleep(0.1) # Background task
    pipe.stop.assert_called_once()

@pytest.mark.asyncio
async def test_handle_command_update_config(tmp_path):
    processor = CommandProcessor()
    pipe = MockPipe()
    config_dir = tmp_path / "agent-config"
    config_dir.mkdir()
    default_yaml = config_dir / "default.yaml"
    default_yaml.write_text("old: config")
    
    with patch("fustor_core.common.get_fustor_home_dir", return_value=tmp_path):
        with patch("os.kill") as mock_kill:
            # Valid config
            valid_yaml = "agent_id: test\nsources: {s1: {driver: fs, uri: /tmp}}\nsenders: {f1: {driver: fusion, uri: http://f}}\npipes: {p1: {source: s1, sender: f1}}"
            cmd = {"type": "update_config", "config_yaml": valid_yaml, "filename": "default.yaml"}
            
            processor._handle_command_update_config(pipe, cmd)
            
            assert default_yaml.read_text() == valid_yaml
            assert (config_dir / "default.yaml.bak").exists()
            mock_kill.assert_called_once_with(os.getpid(), signal.SIGHUP)

@pytest.mark.asyncio
async def test_handle_command_report_config(tmp_path):
    processor = CommandProcessor()
    pipe = MockPipe()
    config_dir = tmp_path / "agent-config"
    config_dir.mkdir()
    (config_dir / "default.yaml").write_text("hello: world")
    
    with patch("fustor_core.common.get_fustor_home_dir", return_value=tmp_path):
        processor._handle_command_report_config(pipe, {"filename": "default.yaml"})
        await asyncio.sleep(0.1)
        
        pipe.sender_handler.send_batch.assert_called_once()
        args = pipe.sender_handler.send_batch.call_args[0]
        assert args[2]["phase"] == "config_report"
        assert args[2]["metadata"]["config_yaml"] == "hello: world"

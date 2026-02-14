import asyncio
import pytest
import os
import signal
import yaml
import shutil
from unittest.mock import MagicMock, AsyncMock, patch
from pathlib import Path

from fustor_agent.runtime.agent_pipe import AgentPipe
from fustor_core.pipe.pipe import PipeState

@pytest.fixture(autouse=True)
def mock_os_kill():
    with patch("os.kill") as m:
        yield m

@pytest.fixture
def mock_pipe():
    config = {
        "source": "src",
        "sender": "push",
        "batch_size": 2
    }
    source_h = MagicMock()
    sender_h = AsyncMock()
    # AgentPipe will load CommandProcessor if installed (which it is since we ran uv sync)
    pipe = AgentPipe("test-pipe", config, source_h, sender_h)
    pipe.session_id = "s123"
    pipe.state = PipeState.RUNNING
    return pipe

@pytest.mark.asyncio
async def test_handle_unknown_command(mock_pipe, caplog):
    """测试未知命令处理"""
    await mock_pipe._handle_commands([{"type": "unknown_action"}])
    assert "Unknown command type 'unknown_action'" in caplog.text

@pytest.mark.asyncio
async def test_handle_commands_delegation(mock_pipe):
    """测试命令分发到 mgmt 扩展"""
    # Replace mgmt with a mock
    mock_mgmt = AsyncMock()
    mock_pipe._mgmt = mock_mgmt
    
    cmds = [{"type": "scan", "path": "/tmp"}]
    await mock_pipe._handle_commands(cmds)
    
    mock_mgmt.process_commands.assert_called_once_with(mock_pipe, cmds)

@pytest.mark.asyncio
async def test_handle_command_scan_via_mgmt(mock_pipe):
    """集成测试：_handle_commands 触发 mgmt.process_commands"""
    # Force real mgmt if available
    assert mock_pipe._mgmt is not None
    
    mock_pipe.source_handler.scan_path.return_value = [{"id": "e1"}]
    
    cmd = {"type": "scan", "path": "/tmp", "job_id": "j1"}
    await mock_pipe._handle_commands([cmd])
    
    # Wait for background task
    await asyncio.sleep(0.1)
    
    # Verify it reached source handler
    mock_pipe.source_handler.scan_path.assert_called_once()
    assert mock_pipe.sender_handler.send_batch.call_count >= 2

@pytest.mark.asyncio
async def test_handle_command_reload_via_mgmt(mock_pipe):
    """集成测试：远程重启"""
    assert mock_pipe._mgmt is not None
    with patch("os.kill") as mock_kill, patch("os.getpid", return_value=999):
        await mock_pipe._handle_commands([{"type": "reload_config"}])
        mock_kill.assert_called_with(999, signal.SIGHUP)

@pytest.mark.asyncio
async def test_handle_command_stop_via_mgmt(mock_pipe):
    """集成测试：停止 Pipe"""
    assert mock_pipe._mgmt is not None
    with patch.object(mock_pipe, "stop", new_callable=AsyncMock) as mock_stop:
        await mock_pipe._handle_commands([{"type": "stop_pipe", "agent_pipe_id": "test-pipe"}])
        await asyncio.sleep(0.1)
        mock_stop.assert_called_once()

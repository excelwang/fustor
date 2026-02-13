import pytest
import asyncio
import os
import signal
from unittest.mock import MagicMock, patch, AsyncMock
from fustor_agent.runtime.agent_pipe import AgentPipe
from fustor_core.pipe import PipeState


@pytest.fixture
def mock_handlers():
    source = MagicMock()
    sender = MagicMock()
    sender.send_batch = AsyncMock()
    sender.send_heartbeat = AsyncMock(return_value={"role": "leader"})
    return source, sender


@pytest.fixture
def pipe(mock_handlers):
    source, sender = mock_handlers
    p = AgentPipe(
        pipe_id="pipe1",
        task_id="agent1:pipe1",
        config={"source": "s1", "sender": "sn1"},
        source_handler=source,
        sender_handler=sender
    )
    return p


@pytest.mark.asyncio
async def test_handle_reload_command(pipe):
    """Test reload_config command sends SIGHUP."""
    with patch("os.kill") as mock_kill:
        await pipe._handle_commands([{"type": "reload_config"}])
        mock_kill.assert_called_once_with(os.getpid(), signal.SIGHUP)


@pytest.mark.asyncio
async def test_handle_stop_pipe_command(pipe):
    """Test stop_pipe command stops the pipe if ID matches."""
    pipe.stop = AsyncMock()
    
    # ID mismatch
    await pipe._handle_commands([{"type": "stop_pipe", "pipe_id": "other"}])
    pipe.stop.assert_not_called()
    
    # ID match
    await pipe._handle_commands([{"type": "stop_pipe", "pipe_id": "pipe1"}])
    # It uses asyncio.create_task(self.stop()), so we need to wait a bit
    await asyncio.sleep(0.1)
    pipe.stop.assert_called_once()


@pytest.mark.asyncio
async def test_handle_update_config_command(pipe, tmp_path):
    """Test update_config command writes file and reloads."""
    # Mock get_fustor_home_dir to use tmp_path
    with patch("fustor_core.common.get_fustor_home_dir", return_value=tmp_path), \
         patch("os.kill") as mock_kill:
        
        config_dir = tmp_path / "agent-config"
        config_dir.mkdir()
        config_file = config_dir / "default.yaml"
        config_file.write_text("old: config")
        
        yaml_content = "new: configuration\nagent_id: agent1"
        await pipe._handle_commands([{
            "type": "update_config",
            "config_yaml": yaml_content,
            "filename": "default.yaml"
        }])
        
        # Verify file written
        assert config_file.read_text() == yaml_content
        # Verify backup created
        assert (config_dir / "default.yaml.bak").exists()
        # Verify reload triggered
        mock_kill.assert_called_once_with(os.getpid(), signal.SIGHUP)


@pytest.mark.asyncio
async def test_handle_upgrade_command(pipe):
    """Test upgrade_agent command flow."""
    with patch("subprocess.run") as mock_run, \
         patch("os.execv") as mock_execv, \
         patch("fustor_agent.__version__", "0.8.17"):
        
        # Mock successful pip install
        mock_run.side_effect = [
            MagicMock(returncode=0), # pip install
            MagicMock(returncode=0, stdout="1.0.0") # verification
        ]
        
        await pipe._handle_commands([{
            "type": "upgrade_agent",
            "version": "1.0.0"
        }])
        
        assert mock_run.call_count == 2
        mock_execv.assert_called_once()


@pytest.mark.asyncio
async def test_build_agent_status(pipe):
    """Test status includes version and uptime."""
    status = pipe._build_agent_status()
    assert status["agent_id"] == "agent1"
    assert "version" in status
    assert "uptime_seconds" in status
    assert status["pipe_id"] == "pipe1"

import pytest
import asyncio
import os
import shutil
from unittest.mock import MagicMock, AsyncMock, patch
from fustor_agent.app import App
from fustor_agent.config.unified import agent_config
import socket

@pytest.fixture
def mock_config_dir(tmp_path):
    config_dir = tmp_path / "agent-config"
    config_dir.mkdir()
    
    with open(config_dir / "default.yaml", "w") as f:
        f.write("""
agent_id: "test-agent"
sources:
  src1:
    driver: fs
    uri: "/tmp"
senders:
  snd1:
    driver: fusion
    uri: "http://localhost"
pipes:
  pipe1:
    source: src1
    sender: snd1
""")
    
    with open(config_dir / "other.yaml", "w") as f:
        f.write("""
pipes:
  pipe2:
    source: src1
    sender: snd1
""")
    
    return config_dir

@pytest.mark.asyncio
async def test_app_initialization_no_agent_id(tmp_path):
    config_dir = tmp_path / "empty-config"
    config_dir.mkdir()
    
    with patch("fustor_agent.app.get_fustor_home_dir", return_value=tmp_path):
        with patch.object(agent_config, "dir", config_dir):
            with patch("socket.gethostname", return_value="win"): # Mock hostname to "win"
                # Ensure no agent_id is set in the mocked config initially
                mock_agent_config_loader = MagicMock(spec=agent_config.__class__)
                mock_agent_config_loader.get_all_sources.return_value = {}
                mock_agent_config_loader.get_all_senders.return_value = {}
                mock_agent_config_loader.get_all_pipes.return_value = {}
                mock_agent_config_loader.agent_id = None # Explicitly set to None
                mock_agent_config_loader.reload.return_value = None
                mock_agent_config_loader.fs_scan_workers = 4 # Default value needed by pipe_instance_service init

                with patch("fustor_agent.app.agent_config", new=mock_agent_config_loader):
                    # App should initialize without error and default agent_id
                    app = App()
                    # Assert that agent_id falls back to the mocked hostname
                    assert app.agent_id == "win"
                    assert app.config_loader.agent_id == "win"
                    # Also verify it was synced back to the loader
                    assert mock_agent_config_loader.agent_id == "win"

@pytest.mark.asyncio
async def test_app_resolve_target_pipes(mock_config_dir, tmp_path):
    with patch("fustor_agent.app.get_fustor_home_dir", return_value=tmp_path):
        with patch.object(agent_config, "dir", mock_config_dir):
            # Default: only from default.yaml
            app = App()
            assert app._target_pipe_ids == ["pipe1"]
            
            # Explicit pipe ID
            app2 = App(config_list=["pipe2"])
            assert "pipe2" in app2._target_pipe_ids
            
            # Explicit file
            app3 = App(config_list=["other.yaml"])
            assert "pipe2" in app3._target_pipe_ids

@pytest.mark.asyncio
async def test_app_startup_shutdown(mock_config_dir, tmp_path):
    with patch("fustor_agent.app.get_fustor_home_dir", return_value=tmp_path):
        with patch.object(agent_config, "dir", mock_config_dir):
            app = App()
            
            # Mock drivers and instances to avoid actual IO
            app.source_driver_service = MagicMock()
            app.sender_driver_service = MagicMock()
            
            mock_bus = MagicMock()
            mock_bus.source_driver_instance = MagicMock()
            app.event_bus_service = AsyncMock()
            app.event_bus_service.get_or_create_bus_for_subscriber.return_value = (mock_bus, 0)
            
            mock_pipe = AsyncMock()
            mock_pipe.id = "pipe1" # Mock attribute access
            mock_pipe.state = MagicMock() # Mock state
            mock_pipe.info = "" # Mock info
            mock_pipe.task_id = "test-agent:pipe1" # Mock task_id
            mock_pipe.bus = MagicMock(id="mock-bus-id") # Mock bus attribute

            with patch("fustor_agent.app.SourceHandlerAdapter"), patch("fustor_agent.app.SenderHandlerAdapter"):
                with patch("fustor_agent.runtime.agent_pipe.AgentPipe", return_value=mock_pipe):
                    await app.startup()
                    assert "pipe1" in app.pipe_runtime
                    mock_pipe.start.assert_called_once()
                    
                    await app.shutdown()
                    assert "pipe1" not in app.pipe_runtime
                    mock_pipe.stop.assert_called_once()

@pytest.mark.asyncio
async def test_app_reload_config(mock_config_dir, tmp_path):
    with patch("fustor_agent.app.get_fustor_home_dir", return_value=tmp_path):
        with patch.object(agent_config, "dir", mock_config_dir):
            app = App()
            app.source_driver_service = MagicMock()
            app.sender_driver_service = MagicMock()
            
            mock_bus = MagicMock()
            mock_bus.source_driver_instance = MagicMock()
            app.event_bus_service = AsyncMock()
            app.event_bus_service.get_or_create_bus_for_subscriber.return_value = (mock_bus, 0)
            
            mock_pipe1 = AsyncMock()
            mock_pipe1.id = "pipe1"
            mock_pipe1.state = MagicMock()
            mock_pipe1.info = ""
            mock_pipe1.task_id = "test-agent:pipe1"
            mock_pipe1.bus = MagicMock(id="mock-bus-id-1")

            mock_pipe2 = AsyncMock()
            mock_pipe2.id = "pipe2"
            mock_pipe2.state = MagicMock()
            mock_pipe2.info = ""
            mock_pipe2.task_id = "test-agent:pipe2"
            mock_pipe2.bus = MagicMock(id="mock-bus-id-2")
            
            def pipe_side_effect(pipe_id, **kwargs):
                if pipe_id == "pipe1": return mock_pipe1
                if pipe_id == "pipe2": return mock_pipe2
                return AsyncMock()

            with patch("fustor_agent.app.SourceHandlerAdapter"), patch("fustor_agent.app.SenderHandlerAdapter"):
                with patch("fustor_agent.runtime.agent_pipe.AgentPipe", side_effect=pipe_side_effect):
                    await app.startup()
                    assert "pipe1" in app.pipe_runtime
                    
                    with patch.object(agent_config, "get_diff", return_value={"added": {"pipe2"}, "removed": {"pipe1"}}):
                        await app.reload_config()
                        
                        assert "pipe1" not in app.pipe_runtime
                        assert "pipe2" in app.pipe_runtime
                        mock_pipe1.stop.assert_called_once()
                        mock_pipe2.start.assert_called_once()

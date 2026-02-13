import asyncio
import pytest
import signal
import os
from unittest.mock import MagicMock, AsyncMock, patch

from fustor_agent.runner import run_agent

@pytest.fixture
def mock_app_instance():
    """Fixture for a mocked App instance."""
    mock_app = MagicMock()
    mock_app.startup = AsyncMock()
    mock_app.shutdown = AsyncMock()
    mock_app.reload_config = AsyncMock()
    return mock_app

@pytest.mark.asyncio
async def test_run_agent_signal_handling(mock_app_instance):
    """
    Test that run_agent properly handles SIGINT/SIGTERM for shutdown
    and SIGHUP for reload.
    """
    with patch("fustor_agent.app.App", return_value=mock_app_instance) as MockAppClass: # Corrected patch target
        # Create a task to run the agent
        agent_task = asyncio.create_task(run_agent())
        
        # Give the agent some time to start up
        await asyncio.sleep(0.1)
        mock_app_instance.startup.assert_called_once()
        
        # Simulate SIGINT (or SIGTERM)
        os.kill(os.getpid(), signal.SIGINT)
        
        # Wait for the agent to shut down
        await agent_task
        
        mock_app_instance.shutdown.assert_called_once()
        assert mock_app_instance.reload_config.call_count == 0

@pytest.mark.asyncio
async def test_run_agent_sighup_handling(mock_app_instance):
    """
    Test that run_agent properly handles SIGHUP for config reload.
    """
    if not hasattr(signal, 'SIGHUP'):
        pytest.skip("SIGHUP is not available on this platform (e.g., Windows)")

    with patch("fustor_agent.app.App", return_value=mock_app_instance) as MockAppClass: # Corrected patch target
        # Create a task to run the agent
        agent_task = asyncio.create_task(run_agent())
        
        # Give the agent some time to start up
        await asyncio.sleep(0.1)
        mock_app_instance.startup.assert_called_once()
        
        # Simulate SIGHUP
        os.kill(os.getpid(), signal.SIGHUP)
        
        # Give time for reload task to be created and run
        await asyncio.sleep(0.1)
        mock_app_instance.reload_config.assert_called_once()
        
        # Simulate SIGINT to allow graceful exit
        os.kill(os.getpid(), signal.SIGINT)
        await agent_task
        mock_app_instance.shutdown.assert_called_once()

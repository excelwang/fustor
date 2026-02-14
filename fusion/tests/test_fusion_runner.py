import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch
from fustor_fusion.runner import run_fusion

@pytest.mark.asyncio
async def test_supervisor_restarts_on_crash():
    # Mock uvicorn.Server
    with patch("fustor_fusion.runner.uvicorn.Server") as MockServer, \
         patch("fustor_fusion.runner.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        
        # Setup mock instance
        server_instance = AsyncMock()
        MockServer.return_value = server_instance
        
        # Scenario:
        # 1st run: crashes
        # 2nd run: crashes
        # 3rd run: stops gracefully (so test finishes)
        
        async def mock_serve_side_effect():
             if MockServer.call_count <= 2:
                 raise RuntimeError("Crash!")
             else:
                 # 3rd time: success
                 server_instance.should_exit = True
                 return

        server_instance.serve.side_effect = mock_serve_side_effect
        
        # Initially false, but for 3rd run we want logic to see True to exit loop
        # We handle this by setting side_effect above which sets it before returning
        server_instance.should_exit = False 
        
        await run_fusion("localhost", 8000)
        
        assert MockServer.call_count == 3
        # Should have slept twice
        assert mock_sleep.call_count == 2

@pytest.mark.asyncio
async def test_supervisor_exits_on_graceful_stop():
    with patch("fustor_fusion.runner.uvicorn.Server") as MockServer:
        server_instance = AsyncMock()
        MockServer.return_value = server_instance
        
        # 1st run: success and exit requested (mimics CTRL+C handling inside uviorn)
        server_instance.should_exit = True
        
        await run_fusion("localhost", 8000)
        
        assert MockServer.call_count == 1

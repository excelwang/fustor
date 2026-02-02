# agent/tests/runtime/conftest.py
import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock
from fustor_core.pipeline import PipelineState
from .mocks import MockSourceHandler, MockSenderHandler

@pytest.fixture
def mock_source():
    ms = MockSourceHandler()
    # Make message iterator block so it doesn't end immediately
    async def mock_aiter_msg():
        while True:
            await asyncio.sleep(0.1)
            yield {"index": 999}
    ms.get_message_iterator = MagicMock(return_value=mock_aiter_msg())
    return ms

@pytest.fixture
def mock_sender():
    return MockSenderHandler()

@pytest.fixture
def pipeline_config():
    return {
        "batch_size": 5,
        "heartbeat_interval_sec": 0.01,
        "audit_interval_sec": 0.05,
        "sentinel_interval_sec": 0,
    }

@pytest.fixture(autouse=True)
def fast_pipeline_intervals():
    """
    Fixture to speed up tests by shortening intervals in AgentPipeline.
    Automatically resets them after each test.
    """
    from fustor_agent.runtime.agent_pipeline import AgentPipeline
    
    # Save original values
    orig_control = AgentPipeline.CONTROL_LOOP_INTERVAL
    orig_role = AgentPipeline.ROLE_CHECK_INTERVAL
    orig_follower = AgentPipeline.FOLLOWER_STANDBY_INTERVAL
    orig_error = AgentPipeline.ERROR_RETRY_INTERVAL
    
    # Set fast values
    AgentPipeline.CONTROL_LOOP_INTERVAL = 0.01
    AgentPipeline.ROLE_CHECK_INTERVAL = 0.01
    AgentPipeline.FOLLOWER_STANDBY_INTERVAL = 0.01
    AgentPipeline.ERROR_RETRY_INTERVAL = 0.01
    
    yield
    
    # Restore original values
    AgentPipeline.CONTROL_LOOP_INTERVAL = orig_control
    AgentPipeline.ROLE_CHECK_INTERVAL = orig_role
    AgentPipeline.FOLLOWER_STANDBY_INTERVAL = orig_follower
    AgentPipeline.ERROR_RETRY_INTERVAL = orig_error

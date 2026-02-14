# agent/tests/runtime/conftest.py
import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock
from fustor_core.pipe import PipeState
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
def pipe_config():
    return {
        "batch_size": 5,
        "audit_interval_sec": 0.5,
        "sentinel_interval_sec": 0,
        "control_loop_interval": 0.1,
        "role_check_interval": 0.1,
        "follower_standby_interval": 0.1,
        "error_retry_interval": 0.1,
        "data_supervisor_interval": 0.1,
        "session_timeout_seconds": 0.3,
    }

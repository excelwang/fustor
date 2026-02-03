# it/fixtures/fusion.py
"""
Fusion client and configuration fixtures for integration tests.
"""
import sys
import time
import pytest
import logging
from pathlib import Path

# Ensure parent directory is in path
_fixtures_dir = Path(__file__).parent
_it_dir = _fixtures_dir.parent
if str(_it_dir) not in sys.path:
    sys.path.insert(0, str(_it_dir))

from utils import FusionClient
from .constants import EXTREME_TIMEOUT, POLL_INTERVAL

logger = logging.getLogger("fustor_test")


@pytest.fixture(scope="session")
def test_view() -> dict:
    """Return static test view info."""
    return {
        "id": "integration-test-ds",
        "name": "integration-test-ds",
        "allow_concurrent_push": True,
        "session_timeout_seconds": 3
    }

# Backward compatibility alias
test_view = test_view


@pytest.fixture(scope="session")
def test_api_key(test_view) -> dict:
    """Return static API key info."""
    return {
        "key": "test-api-key-123",
        "view_id": "integration-test-ds",
        "name": "integration-test-key"
    }


@pytest.fixture(scope="session")
def fusion_client(docker_env, test_api_key) -> FusionClient:
    """
    Create Fusion client with API key.
    
    Waits for Fusion to become ready and sync its configuration cache.
    """
    client = FusionClient(base_url="http://localhost:18102", view_id="integration-test-ds")
    client.set_api_key(test_api_key["key"])
    
    # Wait for Fusion to be ready to accept requests
    logger.info("Waiting for Fusion to become ready and sync cache...")
    start_wait = time.time()
    while time.time() - start_wait < EXTREME_TIMEOUT:
        try:
            client.get_sessions()
            logger.info("Fusion ready and API key synced")
            break
        except Exception:
            time.sleep(POLL_INTERVAL)
    else:
        raise RuntimeError(f"Fusion did not become ready within {EXTREME_TIMEOUT} seconds")
    
    return client

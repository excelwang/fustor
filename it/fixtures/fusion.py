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

logger = logging.getLogger("fustor_test")


@pytest.fixture(scope="session")
def test_datastore() -> dict:
    """Return static test datastore info."""
    return {
        "id": "integration-test-ds",
        "name": "integration-test-ds",
        "allow_concurrent_push": True,
        "session_timeout_seconds": 3
    }


@pytest.fixture(scope="session")
def test_api_key(test_datastore) -> dict:
    """Return static API key info."""
    return {
        "key": "test-api-key-123",
        "datastore_id": "integration-test-ds",
        "name": "integration-test-key"
    }


@pytest.fixture(scope="session")
def fusion_client(docker_env, test_api_key) -> FusionClient:
    """
    Create Fusion client with API key.
    
    Waits for Fusion to become ready and sync its configuration cache.
    """
    client = FusionClient(base_url="http://localhost:18102", view_id="test-fs")
    client.set_api_key(test_api_key["key"])
    
    # Wait for Fusion to be ready to accept requests
    logger.info("Waiting for Fusion to become ready and sync cache...")
    for i in range(60):
        try:
            client.get_sessions()
            logger.info(f"Fusion ready and API key synced after {i+1} seconds")
            break
        except Exception:
            time.sleep(1)
    else:
        raise RuntimeError("Fusion did not become ready within 60 seconds")
    
    return client

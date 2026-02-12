# tests/e2e/fixtures/fusion.py
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
from .constants import EXTREME_TIMEOUT, POLL_INTERVAL, TEST_VIEW_ID, TEST_API_KEY, TEST_QUERY_KEY

logger = logging.getLogger("fustor_test")


@pytest.fixture(scope="session")
def test_view() -> dict:
    """Return static test view info."""
    return {
        "id": TEST_VIEW_ID,
        "name": TEST_VIEW_ID
    }

# Backward compatibility alias
test_view = test_view


@pytest.fixture(scope="session")
def test_api_key(test_view) -> dict:
    """Return static API key info (Ingestion Key)."""
    return {
        "key": TEST_API_KEY,
        "view_id": TEST_VIEW_ID,
        "name": "integration-test-ingestion-key"
    }

@pytest.fixture(scope="session")
def test_query_key(test_view) -> dict:
    """Return static Query Key info."""
    return {
        "key": TEST_QUERY_KEY,
        "view_id": TEST_VIEW_ID,
        "name": "integration-test-query-key"
    }


@pytest.fixture(scope="session")
def fusion_client(docker_env, test_query_key) -> FusionClient:
    """
    Create Fusion client with Query API key.
    
    Waits for Fusion to become ready and sync its configuration cache.
    """
    client = FusionClient(base_url="http://localhost:18102", view_id=TEST_VIEW_ID)
    client.set_api_key(test_query_key["key"])
    
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

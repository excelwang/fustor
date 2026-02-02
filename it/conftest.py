# it/conftest.py
"""
Pytest configuration for NFS multi-mount consistency integration tests.

This conftest imports modular fixtures from the fixtures/ package.
For implementation details, see:
- fixtures/docker.py: Docker environment management
- fixtures/fusion.py: Fusion client and configuration
- fixtures/agents.py: Agent setup and configuration
- fixtures/leadership.py: Leadership management and audit control
"""
import os
import sys
import time
import pytest
import logging
from pathlib import Path

# Add it/ directory to path for imports
_it_dir = Path(__file__).parent
if str(_it_dir) not in sys.path:
    sys.path.insert(0, str(_it_dir))

from utils import docker_manager, FusionClient

# ============================================================================
# Logging Setup
# ============================================================================
logger = logging.getLogger("fustor_test")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# ============================================================================
from fixtures.constants import TEST_TIMEOUT, CONTAINER_CLIENT_A, CONTAINER_CLIENT_B, CONTAINER_CLIENT_C, MOUNT_POINT, AUDIT_INTERVAL

# Log architecture status
logger.info("🚀 Integration tests running in V2 AgentPipeline mode")


# ============================================================================
# Import Modular Fixtures
# ============================================================================
# Note: pytest_plugins doesn't work well with relative imports in packages,
# so we re-export fixtures here for pytest to discover them.

from fixtures.docker import docker_env, clean_shared_dir
from fixtures.fusion import test_datastore, test_api_key, fusion_client
from fixtures.agents import setup_agents
from fixtures.leadership import wait_for_audit, reset_leadership


# ============================================================================
# Additional Convenience Fixtures
# ============================================================================

@pytest.fixture(autouse=True)
def reset_fusion_state(fusion_client):
    """
    Reset Fusion parser state before each test case to ensure isolation.
    """
    try:
        fusion_client.reset()
    except Exception as e:
        logger.debug(f"Fusion reset skipped or failed: {e}")
    yield


# ============================================================================
# Re-export ensure_agent_running for tests that need it directly
# ============================================================================
from fixtures.agents import ensure_agent_running

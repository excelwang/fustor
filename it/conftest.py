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
from fixtures.constants import TEST_TIMEOUT, CONTAINER_CLIENT_A, CONTAINER_CLIENT_B, CONTAINER_CLIENT_C, CONTAINER_FUSION, CONTAINER_NFS_SERVER, MOUNT_POINT, AUDIT_INTERVAL

# Log architecture status
logger.info("🚀 Integration tests running in V2 AgentPipe mode")


# ============================================================================
# Import Modular Fixtures
# ============================================================================
# Note: pytest_plugins doesn't work well with relative imports in packages,
# so we re-export fixtures here for pytest to discover them.

from fixtures.docker import docker_env, clean_shared_dir
from fixtures.fusion import test_view, test_view, test_api_key, fusion_client
from fixtures.agents import setup_agents
from fixtures.leadership import wait_for_audit, reset_leadership


# ============================================================================
# Additional Convenience Fixtures
# ============================================================================

@pytest.fixture(autouse=True)
def reset_fusion_state(fusion_client, clean_shared_dir):
    """
    Aggressively reset environment before each test:
    1. Kill all agents in ALL containers (and wait for death)
    2. Reset Fusion state via API (clears sessions, views)
    3. Verify no stale sessions remain
    """
    containers = [CONTAINER_CLIENT_A, CONTAINER_CLIENT_B, CONTAINER_CLIENT_C]
    
    # 1. Kill agents and clean up local state in ALL containers
    for container in containers:
        docker_manager.cleanup_agent_state(container)
    
    # Small delay to ensure all agent processes are fully dead
    # and their last heartbeats/requests have been processed
    time.sleep(1.0)
    
    # 2. Reset Fusion state (AFTER agents are dead, so no re-registration)
    try:
        fusion_client.reset()
        
        # Wait for all stale sessions to vanish
        logger.debug("Waiting for sessions to vanish after reset...")
        for _ in range(15):
            sessions = fusion_client.get_sessions()
            if not sessions:
                break
            logger.debug(f"Still {len(sessions)} sessions remaining, waiting...")
            time.sleep(1)
        else:
            remaining = fusion_client.get_sessions()
            if remaining:
                logger.warning(f"Sessions still exist after reset wait: {[s.get('agent_id') for s in remaining]}")
        
        # Wait for View to be READY (Initial snapshot complete)
        time.sleep(1.0)
        max_retries = 30
        for i in range(max_retries):
            try:
                fusion_client.get_stats()
                break
            except Exception:
                if i == max_retries - 1:
                    logger.warning("View failed to become ready after reset (timeout)")
                time.sleep(1)
    except Exception as e:
        logger.debug(f"Fusion reset failed: {e}")
    
    # 3. Clear logs
    for container in containers + [CONTAINER_FUSION]:
        try:
            log_path = "/root/.fustor/agent.log" if "client" in container else "/root/.fustor/fusion.log"
            docker_manager.exec_in_container(container, ["sh", "-c", f"> {log_path}"], timeout=5)
        except Exception:
            pass

    yield


# ============================================================================
# Re-export ensure_agent_running for tests that need it directly
# ============================================================================
from fixtures.agents import ensure_agent_running

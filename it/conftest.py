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
logger.info("🚀 Integration tests running in V2 AgentPipeline mode")


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
def reset_fusion_state(fusion_client):
    """
    Aggressively reset environment before each test:
    1. Kill all agents in containers
    2. Clean up agent pid/state files
    3. Reset Fusion state via API
    """
    containers = [CONTAINER_CLIENT_A, CONTAINER_CLIENT_B, CONTAINER_CLIENT_C]
    
    # 1. Kill agents and clean up local state
    for container in containers:
        try:
            # Force kill any running agents
            docker_manager.exec_in_container(container, ["pkill", "-9", "-f", "fustor-agent"], timeout=10)
            # Remove state files
            docker_manager.exec_in_container(container, ["rm", "-f", "/root/.fustor/agent.pid"], timeout=5)
            docker_manager.exec_in_container(container, ["rm", "-f", "/root/.fustor/agent-state.json"], timeout=5)
        except Exception:
            pass
            
    # 2. Clean Monitor Directory (Shared Mount)
    logger.info("Cleaning shared directory before test...")
    for container in [CONTAINER_CLIENT_A, CONTAINER_CLIENT_B, CONTAINER_CLIENT_C]:
        try:
            # removing * is safer than removing the mount point itself
            docker_manager.exec_in_container(container, ["sh", "-c", f"rm -rf {MOUNT_POINT}/*"], timeout=20)
        except Exception:
            pass
            
    # Also clean from NFS server directly to be sure
    try:
        docker_manager.exec_in_container(CONTAINER_NFS_SERVER, ["sh", "-c", "rm -rf /exports/*"], timeout=20)
    except Exception:
        pass

    # 3. Reset Fusion state
    try:
        fusion_client.reset()
        
        # RESTORED: Wait for View to be READY (Initial snapshot complete)
        # We need to ensure the system is stable before the next test starts.
        # Otherwise, early requests (like Sentinel polling) might hit 503s.
        time.sleep(1.0) # Give it a breath
        max_retries = 100
        for i in range(max_retries):
            try:
                # Check if View is accessible (any call, e.g., stats)
                fusion_client.get_tree(path="/", max_depth=1)
                break
            except Exception:
                if i == max_retries - 1:
                    logger.warning("View failed to become ready after reset (timeout)")
                time.sleep(1)
    except Exception as e:
        logger.debug(f"Fusion reset failed: {e}")
    
    # 4. Clear logs
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

# it/fixtures/leadership.py
"""
Leadership management and audit control fixtures for integration tests.
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

from utils import docker_manager
from fixtures.agents import (
    CONTAINER_CLIENT_A, 
    CONTAINER_CLIENT_B,
    AUDIT_INTERVAL,
    ensure_agent_running
)

logger = logging.getLogger("fustor_test")


@pytest.fixture
def wait_for_audit():
    """
    Return a function that waits for audit cycle to complete.
    
    Usage:
        def test_something(wait_for_audit):
            # ... make changes ...
            wait_for_audit()  # Wait for default audit interval
            # ... assert results ...
    """
    def _wait(seconds: int = AUDIT_INTERVAL + 1):
        time.sleep(seconds)
    return _wait


@pytest.fixture
def reset_leadership(fusion_client, setup_agents, docker_env):
    """
    Ensure Agent A is Leader and Agent B is Follower before test.
    
    This fixture resets the cluster state if previous tests messed it up.
    It's useful for tests that depend on specific leadership configuration.
    """
    api_key = setup_agents["api_key"]
    datastore_id = setup_agents["datastore_id"]
    
    # Check current state
    sessions = fusion_client.get_sessions()
    leader = next((s for s in sessions if s.get("role") == "leader"), None)
    
    is_clean = False
    if leader and "client-a" in leader.get("agent_id", ""):
        # Agent A is leader. Check Agent B presence.
        agent_b = next((s for s in sessions if "client-b" in s.get("agent_id", "")), None)
        if agent_b:
            is_clean = True
            
    if is_clean:
        logger.info("Cluster state is clean (A=Leader, B=Follower). Skipping reset.")
        return

    logger.warning("Cluster state dirty. Forcing leadership reset...")
    
    # Force reset: Stop everyone
    for container in [CONTAINER_CLIENT_A, CONTAINER_CLIENT_B]:
        try:
            docker_manager.exec_in_container(container, ["sh", "-c", "pkill -CONT -f fustor-agent || true"])
            docker_manager.exec_in_container(container, ["pkill", "-9", "-f", "fustor-agent"])
        except Exception:
            pass

    # Wait for sessions to vanish
    logger.info("Waiting for stale sessions to expire...")
    start_cleanup = time.time()
    while time.time() - start_cleanup < 6:
        if not fusion_client.get_sessions():
            break
        time.sleep(0.5)
        
    # Restart A first
    logger.info("Restarting Agent A...")
    ensure_agent_running(CONTAINER_CLIENT_A, api_key, datastore_id)
    
    # Wait for A to become Leader
    logger.info("Waiting for Agent A to be Leader...")
    start_wait = time.time()
    while time.time() - start_wait < 30:
        sessions = fusion_client.get_sessions()
        leader = next((s for s in sessions if s.get("role") == "leader"), None)
        if leader and "client-a" in leader.get("agent_id", ""):
            break
        time.sleep(0.5)
        
    # Restart B
    logger.info("Restarting Agent B...")
    ensure_agent_running(CONTAINER_CLIENT_B, api_key, datastore_id)
    
    # Wait for B
    time.sleep(1)
    logger.info("Leadership reset complete.")

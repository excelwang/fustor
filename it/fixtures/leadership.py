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
    ensure_agent_running,
    MOUNT_POINT
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


@pytest.fixture(scope="function")
def leader_follower_agents(setup_agents, fusion_client):
    """
    Ensure we have a stable leader and follower agent set up.
    
    Returns:
        dict: {
            "leader": container_id_of_leader,
            "follower": container_id_of_follower
        }
    """
    api_key = setup_agents["api_key"]
    view_id = setup_agents["view_id"]
    client_A = CONTAINER_CLIENT_A
    client_B = CONTAINER_CLIENT_B

    # Check current state
    sessions = fusion_client.get_sessions()
    leader = next((s for s in sessions if s.get("role") == "leader"), None)
    
    is_clean = False
    if leader and "client-a" in leader.get("agent_id", ""):
        # Agent A is leader. Check Agent B presence.
        agent_b = next((s for s in sessions if "client-b" in s.get("agent_id", "")), None)
        if agent_b and agent_b.get("role") == "follower":
            is_clean = True
            
    if is_clean:
        logger.info("Cluster state is clean (A=Leader, B=Follower). Skipping reset.")
        return {
            "leader": client_A,
            "follower": client_B
        }

    logger.warning("Cluster state dirty. Forcing leadership reset...")
    
    # Force reset: Stop everyone
    for container in [client_A, client_B]:
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
        
    # 1. Start Client A
    logger.info("Restarting Agent A...")
    ensure_agent_running(CONTAINER_CLIENT_A, api_key, view_id)
    
    # Wait for A to become leader
    time.sleep(2)

    # 2. Start Client B
    logger.info("Restarting Agent B...")
    ensure_agent_running(CONTAINER_CLIENT_B, api_key, view_id)
    
    # Wait for B
    time.sleep(1)
    logger.info("Leadership reset complete.")
    
    return {
        "leader": client_A,
        "follower": client_B
    }

@pytest.fixture
def reset_leadership(setup_agents, fusion_client):
    """
    Fixture to manually trigger a leadership reset.
    """
    api_key = setup_agents["api_key"]
    view_id = setup_agents["view_id"]
    
    async def _reset():
        logger.warning("Forcing leadership reset via fixture...")
        # Stop everyone
        for container in [CONTAINER_CLIENT_A, CONTAINER_CLIENT_B]:
            docker_manager.exec_in_container(container, ["pkill", "-9", "-f", "fustor-agent"])
        
        # Wait for sessions to vanish
        start_cleanup = time.time()
        while time.time() - start_cleanup < 6:
            if not fusion_client.get_sessions():
                break
            time.sleep(0.5)
            
        # Restart A then B
        ensure_agent_running(CONTAINER_CLIENT_A, api_key, view_id)
        time.sleep(2)
        ensure_agent_running(CONTAINER_CLIENT_B, api_key, view_id)
        logger.info("Leadership reset via fixture complete.")
        
    return _reset

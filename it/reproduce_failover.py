
import pytest
import time
import logging
from it.utils import docker_manager
from it.conftest import CONTAINER_CLIENT_A, CONTAINER_CLIENT_B
from it.fixtures.constants import MEDIUM_TIMEOUT, POLL_INTERVAL

logger = logging.getLogger("fustor_test")

def test_manual_failover_check(fusion_client, setup_agents):
    """
    Manual verification of failover logic.
    """
    # 1. Verify A is leader
    sessions = fusion_client.get_sessions()
    leader = next((s for s in sessions if s.get("role") == "leader"), None)
    assert leader and "client-a" in leader.get("agent_id", ""), f"A should be leader, got {leader}"
    
    logger.info(f"Stopping {CONTAINER_CLIENT_A}...")
    docker_manager.stop_container(CONTAINER_CLIENT_A)
    
    # 2. Wait for session expiry
    logger.info("Waiting for session expiry...")
    start = time.time()
    new_leader = None
    
    while time.time() - start < 30: # Hardcoded 30s wait
        sessions = fusion_client.get_sessions()
        # Check if A is gone
        a_session = next((s for s in sessions if "client-a" in s.get("agent_id", "")), None)
        
        # Check if B is leader
        b_session = next((s for s in sessions if "client-b" in s.get("agent_id", "")), None)
        
        if not a_session:
             logger.info("Agent A session gone.")
        else:
             logger.info(f"Agent A session still present: {a_session.get('session_id')}")

        if b_session and b_session.get("role") == "leader":
            new_leader = b_session
            logger.info("Agent B promoted to Leader!")
            break
            
        time.sleep(1)
        
    if not new_leader:
        logger.error(f"Failover failed. Final sessions: {sessions}")
        
    # restore
    docker_manager.start_container(CONTAINER_CLIENT_A)
    setup_agents["ensure_agent_running"](CONTAINER_CLIENT_A, setup_agents["api_key"], setup_agents["view_id"])
    
    assert new_leader is not None

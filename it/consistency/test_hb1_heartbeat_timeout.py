# it/consistency/test_hb1_heartbeat_timeout.py
"""
Test HB1: Heartbeat Timeout - Agent recovers after session expires naturally.

验证 Agent 在长时间由于网络或其他原因无法发送心跳，导致会话在 Fusion 端超时后，
能够检测到会话过期并自动重新创建会话。
"""
import time
import pytest
import logging

from ..utils import docker_manager
from ..fixtures.constants import (
    CONTAINER_CLIENT_A,
    SESSION_TIMEOUT,
    SESSION_VANISH_TIMEOUT,
    MEDIUM_TIMEOUT,
    POLL_INTERVAL
)

logger = logging.getLogger(__name__)

class TestHeartbeatTimeout:
    """Test Agent's ability to recover from naturally expired sessions."""

    def test_agent_recovers_after_timeout(
        self,
        setup_agents,
        fusion_client
    ):
        """
        Scenario:
          1. Agent A is running with an active session.
          2. Pause Agent A container to stop heartbeats.
          3. Wait for session timeout (3s + buffer).
          4. Unpause Agent A container.
          5. Verify Agent A detects session loss and creates a new one.
        """
        logger.info("Starting heartbeat timeout recovery test")
        
        # 1. Get current Agent A session
        sessions = fusion_client.get_sessions()
        agent_a = next((s for s in sessions if "client-a" in s.get("agent_id", "")), None)
        assert agent_a is not None, "Agent A must have a session initially"
        
        old_session_id = agent_a["session_id"]
        logger.info(f"Initial session ID: {old_session_id}")
        
        # 2. Pause Agent A to stop everything (including heartbeats)
        logger.info(f"Pausing container {CONTAINER_CLIENT_A}...")
        docker_manager.exec_in_container(CONTAINER_CLIENT_A, ["sh", "-c", "kill -STOP $(cat /root/.fustor/agent.pid)"])
        
        # 3. Wait for session timeout in Fusion
        # Fusion timeout is SESSION_TIMEOUT. Wait SESSION_VANISH_TIMEOUT to be safe.
        logger.info(f"Waiting {SESSION_VANISH_TIMEOUT}s for session to expire in Fusion...")
        time.sleep(SESSION_VANISH_TIMEOUT)
        
        # Verify session is gone from Fusion's perspective
        sessions_after = fusion_client.get_sessions()
        if old_session_id in [s["session_id"] for s in sessions_after]:
            logger.warning(f"Session {old_session_id} still exists in Fusion. Waiting a bit more...")
            time.sleep(SESSION_VANISH_TIMEOUT)
            sessions_after = fusion_client.get_sessions()
            
        assert old_session_id not in [s["session_id"] for s in sessions_after], "Session should have expired"
        
        # 4. Resume Agent A
        logger.info(f"Resuming container {CONTAINER_CLIENT_A}...")
        docker_manager.exec_in_container(CONTAINER_CLIENT_A, ["sh", "-c", "kill -CONT $(cat /root/.fustor/agent.pid)"])
        
        # 5. Wait for Agent A to detect error and recover
        logger.info("Waiting for Agent A to detect timeout and recover...")
        
        start_wait = time.time()
        new_session_id = None
        
        while time.time() - start_wait < MEDIUM_TIMEOUT:
            sessions = fusion_client.get_sessions()
            agent_a_sessions = [s for s in sessions if "client-a" in s.get("agent_id", "")]
            if agent_a_sessions:
                new_session_id = agent_a_sessions[0]["session_id"]
                if new_session_id != old_session_id:
                    logger.info(f"Agent A recovered with new session ID: {new_session_id}")
                    break
            time.sleep(POLL_INTERVAL)
            
        assert new_session_id is not None, "Agent A did not create a new session after timeout"
        assert new_session_id != old_session_id, "Agent A should have a DIFFERENT session ID"
        
        logger.info("✅ Heartbeat timeout recovery verified successfully")

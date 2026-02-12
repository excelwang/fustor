"""
Test E1: Leader failover when leader crashes.

验证当 Leader Agent 宕机后，Follower 接管成为新 Leader。
参考文档: CONSISTENCY_DESIGN.md - Section 3.3 (Follower 在 Leader 会话超时后可升级为 Leader)
"""
import pytest
import time

from ..utils import docker_manager
from ..conftest import CONTAINER_CLIENT_A, CONTAINER_CLIENT_B, MOUNT_POINT
from ..fixtures.constants import SHORT_TIMEOUT, MEDIUM_TIMEOUT, EXTREME_TIMEOUT, POLL_INTERVAL, SESSION_VANISH_TIMEOUT


class TestLeaderFailover:
    """Test leader failover when leader agent crashes."""

    def test_follower_becomes_leader_after_crash(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        reset_leadership
    ):
        """
        场景:
          1. Agent A 是 Leader，Agent B 是 Follower
          2. Agent A 的容器停止（模拟崩溃）
          3. Fusion 检测到 Leader 会话超时
          4. Agent B 升级为新 Leader
        预期:
          - Agent B 成为 Leader
          - Agent B 获得 Snapshot/Audit 权限
        """
        # Verify initial state: A is leader, B is follower
        sessions = fusion_client.get_sessions()
        
        leader_session = None
        follower_session = None
        for s in sessions:
            aid = s.get("agent_id", "")
            if aid.startswith("client-a"):
                leader_session = s
            elif aid.startswith("client-b"):
                follower_session = s
        
        assert leader_session is not None, "Agent A session should exist"
        assert leader_session.get("role") == "leader", "Agent A should be leader initially"
        
        if follower_session:
            assert follower_session.get("role") == "follower", "Agent B should be follower initially"
        
        # Stop Agent A container immediately (simulate crash, no grace period)
        docker_manager.stop_container(CONTAINER_CLIENT_A, timeout=0)
        
        try:
            # Poll for Agent B to become the new leader
            # Session timeout is 5s (from docker-compose), cleanup runs every 1s
            # So we need to wait at most ~6-7s, but use MEDIUM_TIMEOUT for safety
            timeout_wait = MEDIUM_TIMEOUT
            print(f"Waiting up to {timeout_wait}s for leader failover...")
            
            new_leader = None
            start = time.time()
            while time.time() - start < timeout_wait:
                sessions_after = fusion_client.get_sessions()
                for s in sessions_after:
                    if s.get("role") == "leader" and s.get("agent_id", "").startswith("client-b"):
                        new_leader = s
                        break
                if new_leader:
                    break
                time.sleep(1.0)
            
            assert new_leader is not None, \
                f"Agent B should become leader within {timeout_wait}s. Sessions: {fusion_client.get_sessions()}"
            
            # Verify new leader has proper permissions
            assert new_leader.get("can_snapshot") is True, \
                "New leader should have snapshot permission"
            assert new_leader.get("can_audit") is True, \
                "New leader should have audit permission"
            
        finally:
            # Restart Agent A container and agent process for other tests
            docker_manager.start_container(CONTAINER_CLIENT_A)
            setup_agents["ensure_agent_running"](
                CONTAINER_CLIENT_A, 
                setup_agents["api_key"], 
                setup_agents["view_id"]
            )
            time.sleep(SHORT_TIMEOUT)  # Wait for restart

    def test_failover_preserves_data_integrity(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir
    ):
        """
        场景: Leader 故障转移后，数据完整性得到保持
        """
        import os
        test_file = f"/mnt/shared/failover_data_test_{int(time.time()*1000)}.txt"
        test_file_rel = "/" + os.path.relpath(test_file, MOUNT_POINT)
        
        # Create file before failover
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_A,
            test_file,
            content="data before failover"
        )
        
        # Wait for sync
        found = fusion_client.wait_for_file_in_tree(test_file_rel, timeout=MEDIUM_TIMEOUT)
        assert found is not None
        
        # Stop leader
        docker_manager.stop_container(CONTAINER_CLIENT_A, timeout=0)
        
        # Diagnostic: Check if B can see the file
        output = docker_manager.exec_in_container(CONTAINER_CLIENT_B, ["ls", "-l", MOUNT_POINT])
        print(f"File listing from B after A stop: {output}")
        
        try:
            # Wait for failover (Session timeout + buffer)
            time.sleep(MEDIUM_TIMEOUT)

            # Wait for Snapshot (Readiness) restoration
            # New leader must complete initial snapshot sync phase
            start_wait = time.time()
            ready = False
            while time.time() - start_wait < EXTREME_TIMEOUT:
                try:
                    stats = fusion_client.get_stats()
                    ready = True # If get_stats succeeds, readiness check passed
                    break
                except Exception:
                    time.sleep(POLL_INTERVAL)
            
            if not ready:
                pytest.fail("View failed to become ready after failover (New Leader Snapshot timed out)")
            
            # Data should still be accessible
            # After failover, Agent B should perform Audit and report the missing file
            # or at least not delete it if it was already synced.
            # Wait for the file to be present in Fusion's tree (giving it time for Agent B audit)
            found_after = fusion_client.wait_for_file_in_tree(
                file_path=test_file_rel,
                timeout=EXTREME_TIMEOUT  # Allow time for Agent B promotion + Audit cycle
            )
            
            assert found_after is not None, \
                f"Data should be preserved after leader failover. Tree: {fusion_client.get_tree(path='/', max_depth=-1)}"
            
        finally:
            docker_manager.start_container(CONTAINER_CLIENT_A)
            setup_agents["ensure_agent_running"](
                CONTAINER_CLIENT_A, 
                setup_agents["api_key"], 
                setup_agents["view_id"]
            )
            time.sleep(SHORT_TIMEOUT)

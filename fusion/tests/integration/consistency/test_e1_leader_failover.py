"""
Test E1: Leader failover when leader crashes.

验证当 Leader Agent 宕机后，Follower 接管成为新 Leader。
参考文档: CONSISTENCY_DESIGN.md - Section 3.3 (Follower 在 Leader 会话超时后可升级为 Leader)
"""
import pytest
import time

from ..utils import docker_manager
from ..conftest import CONTAINER_CLIENT_A, CONTAINER_CLIENT_B, MOUNT_POINT


class TestLeaderFailover:
    """Test leader failover when leader agent crashes."""

    def test_follower_becomes_leader_after_crash(
        self,
        docker_env,
        fusion_client,
        setup_agents
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
            if aid.startswith("agent-a"):
                leader_session = s
            elif aid.startswith("agent-b"):
                follower_session = s
        
        assert leader_session is not None, "Agent A session should exist"
        assert leader_session.get("role") == "leader", "Agent A should be leader initially"
        
        if follower_session:
            assert follower_session.get("role") == "follower", "Agent B should be follower initially"
        
        # Stop Agent A container (simulate crash)
        docker_manager.stop_container(CONTAINER_CLIENT_A)
        
        try:
            # Wait for session timeout (typically 30-60 seconds)
            # The exact time depends on the configured timeout
            timeout_wait = 15  # Wait longer than session timeout (configured as 10s)
            print(f"Waiting {timeout_wait}s for leader session timeout...")
            time.sleep(timeout_wait)
            
            # Check that Agent B is now the leader
            sessions_after = fusion_client.get_sessions()
            
            new_leader = None
            for s in sessions_after:
                if s.get("role") == "leader":
                    new_leader = s
                    break
            
            assert new_leader is not None, "A new leader should be elected"
            assert new_leader.get("agent_id", "").startswith("agent-b"), \
                f"Agent B should become leader, got {new_leader.get('agent_id')}"
            
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
                setup_agents["datastore_id"]
            )
            time.sleep(10)  # Wait for restart

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
        test_file = f"/mnt/shared/failover_data_test_{int(time.time()*1000)}.txt"
        
        # Create file before failover
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_A,
            test_file,
            content="data before failover"
        )
        
        # Wait for sync
        found = fusion_client.wait_for_file_in_tree(test_file, timeout=15)
        assert found is not None
        
        # Stop leader
        docker_manager.stop_container(CONTAINER_CLIENT_A)
        
        # Diagnostic: Check if B can see the file
        output = docker_manager.exec_in_container(CONTAINER_CLIENT_B, ["ls", "-l", MOUNT_POINT])
        print(f"File listing from B after A stop: {output}")
        
        try:
            # Wait for failover (Session timeout 10s + buffer)
            time.sleep(15)
            
            # Data should still be accessible
            # After failover, Agent B should perform Audit and report the missing file
            # or at least not delete it if it was already synced.
            # Wait for the file to be present in Fusion's tree (giving it time for Agent B audit)
            found_after = fusion_client.wait_for_file_in_tree(
                file_path=test_file,
                timeout=60  # Allow time for Agent B promotion + Audit cycle
            )
            
            assert found_after is not None, \
                f"Data should be preserved after leader failover. Tree: {fusion_client.get_tree(path='/', max_depth=-1)}"
            
        finally:
            docker_manager.start_container(CONTAINER_CLIENT_A)
            setup_agents["ensure_agent_running"](
                CONTAINER_CLIENT_A, 
                setup_agents["api_key"], 
                setup_agents["datastore_id"]
            )
            time.sleep(10)

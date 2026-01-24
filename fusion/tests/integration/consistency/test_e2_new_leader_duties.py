"""
Test E2: New Leader resumes Snapshot/Audit duties.

验证新 Leader 接管后，恢复 Snapshot/Audit 职责。
参考文档: CONSISTENCY_DESIGN.md - Section 3 (Leader/Follower 选举)
"""
import pytest
import time

from ..utils import docker_manager
from ..conftest import (
    CONTAINER_CLIENT_A, CONTAINER_CLIENT_B, CONTAINER_CLIENT_C, MOUNT_POINT
)


class TestNewLeaderResumesDuties:
    """Test that new leader resumes snapshot/audit after failover."""

    def test_new_leader_performs_audit(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        场景:
          1. Agent A 是 Leader
          2. Agent A 宕机，Agent B 升级为 Leader
          3. 无 Agent 客户端 C 创建文件
          4. Agent B（新 Leader）执行 Audit，发现盲区文件
        预期:
          - 新 Leader 正常执行 Audit
          - 盲区文件被发现
        """
        # Stop Agent A to trigger failover
        docker_manager.stop_container(CONTAINER_CLIENT_A)
        
        try:
            # Wait for failover
            print("Waiting for leader failover...")
            time.sleep(90)
            
            # Verify B is now leader
            sessions = fusion_client.get_sessions()
            new_leader = None
            for s in sessions:
                if s.get("role") == "leader":
                    new_leader = s
                    break
            
            assert new_leader is not None, "New leader should exist"
            assert new_leader.get("agent_id") == "agent-b", \
                "Agent B should be the new leader"
            
            # Create file from blind-spot
            test_file = f"{MOUNT_POINT}/new_leader_audit_test.txt"
            docker_manager.create_file_in_container(
                CONTAINER_CLIENT_C,
                test_file,
                content="blind spot file for new leader"
            )
            
            # Wait for new leader's Audit
            wait_for_audit()
            
            # File should be discovered by new leader's Audit
            found = fusion_client.wait_for_file_in_tree(test_file, timeout=10)
            
            assert found is not None, \
                "New leader should discover blind-spot file via Audit"
            
            # Verify it's marked as agent_missing
            flags = fusion_client.check_file_flags(test_file)
            assert flags["agent_missing"] is True
            
        finally:
            docker_manager.start_container(CONTAINER_CLIENT_A)
            time.sleep(10)

    def test_new_leader_performs_snapshot(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir
    ):
        """
        场景: 验证新 Leader 执行 Snapshot 初始同步
        """
        # First, create some files for the new leader to snapshot
        test_dir = f"{MOUNT_POINT}/snapshot_resume_test"
        docker_manager.exec_in_container(
            CONTAINER_CLIENT_B,
            ["mkdir", "-p", test_dir]
        )
        
        test_files = [
            f"{test_dir}/file_{i}.txt" for i in range(3)
        ]
        for f in test_files:
            docker_manager.create_file_in_container(
                CONTAINER_CLIENT_B,
                f,
                content=f"content for {f}"
            )
        
        # Wait for initial sync
        time.sleep(5)
        
        # Stop Agent A
        docker_manager.stop_container(CONTAINER_CLIENT_A)
        
        try:
            # Wait for failover
            time.sleep(90)
            
            # Create new file while B is leader
            new_file = f"{test_dir}/after_failover.txt"
            docker_manager.create_file_in_container(
                CONTAINER_CLIENT_B,
                new_file,
                content="created after failover"
            )
            
            # New leader B should sync this via realtime
            found = fusion_client.wait_for_file_in_tree(new_file, timeout=15)
            
            assert found is not None, \
                "New leader should sync files via realtime"
            
        finally:
            docker_manager.start_container(CONTAINER_CLIENT_A)
            time.sleep(10)

    def test_original_leader_becomes_follower_on_return(
        self,
        docker_env,
        fusion_client,
        setup_agents
    ):
        """
        场景: 原 Leader 重新上线后，应该成为 Follower（不抢占）
        """
        # Stop A
        docker_manager.stop_container(CONTAINER_CLIENT_A)
        
        try:
            # Wait for failover
            time.sleep(90)
            
            # Restart A
            docker_manager.start_container(CONTAINER_CLIENT_A)
            time.sleep(15)
            
            # Check roles
            sessions = fusion_client.get_sessions()
            
            roles = {}
            for s in sessions:
                roles[s.get("agent_id")] = s.get("role")
            
            # B should remain leader
            assert roles.get("agent-b") == "leader", \
                "Agent B should remain leader"
            
            # A should be follower
            assert roles.get("agent-a") == "follower", \
                "Returning Agent A should become follower"
            
        finally:
            # Final cleanup - restart to restore original state if needed
            pass

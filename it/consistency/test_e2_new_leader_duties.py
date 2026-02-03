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
from ..fixtures.constants import (
    SHORT_TIMEOUT,
    MEDIUM_TIMEOUT,
    LONG_TIMEOUT,
    EXTREME_TIMEOUT,
    POLL_INTERVAL,
    STRESS_DELAY,
    INGESTION_DELAY
)
import logging
logger = logging.getLogger(__name__)


class TestNewLeaderResumesDuties:
    """Test that new leader resumes snapshot/audit after failover."""

    def test_new_leader_performs_audit(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit,
        reset_leadership
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
            # Wait for failover (Session timeout + buffer)
            time.sleep(MEDIUM_TIMEOUT)
            
            # Wait for Snapshot (Readiness) restoration
            start_wait = time.time()
            ready = False
            while time.time() - start_wait < EXTREME_TIMEOUT:
                try:
                    fusion_client.get_stats()
                    ready = True 
                    break
                except Exception:
                    time.sleep(POLL_INTERVAL)
            if not ready:
                pytest.fail("View failed to become ready after failover in test_new_leader_performs_audit")
            
            # Verify B is now leader
            logger.info("Waiting for Agent B to become leader...")
            new_leader = None
            start_poll = time.time()
            while time.time() - start_poll < EXTREME_TIMEOUT:
                sessions = fusion_client.get_sessions()
                for s in sessions:
                    if s.get("role") == "leader" and s.get("agent_id", "").startswith("client-b"):
                        new_leader = s
                        break
                if new_leader:
                    break
                time.sleep(POLL_INTERVAL)
            
            assert new_leader is not None, "New leader should exist"
            assert new_leader.get("agent_id", "").startswith("client-b"), \
                "Agent B should be the new leader"
            
            # Create file from blind-spot
            test_file = f"{MOUNT_POINT}/new_leader_audit_test_{int(time.time()*1000)}.txt"
            docker_manager.create_file_in_container(
                CONTAINER_CLIENT_C,
                test_file,
                content="blind spot file for new leader"
            )
            
            # Wait for new leader's Audit to find the file
            # Use marker file approach for more reliable audit cycle detection
            marker_file = f"{MOUNT_POINT}/audit_marker_e2_{int(time.time()*1000)}.txt"
            docker_manager.create_file_in_container(CONTAINER_CLIENT_C, marker_file, content="marker")
            time.sleep(STRESS_DELAY)
            assert fusion_client.wait_for_file_in_tree(marker_file, timeout=LONG_TIMEOUT) is not None
            
            # File should be discovered by new leader's Audit
            found = fusion_client.wait_for_file_in_tree(test_file, timeout=MEDIUM_TIMEOUT)
            
            assert found is not None, \
                "New leader should discover blind-spot file via Audit"
            
            # Poll for agent_missing flag (may need an additional audit cycle)
            start = time.time()
            flag_set = False
            while time.time() - start < LONG_TIMEOUT:
                flags = fusion_client.check_file_flags(test_file)
                if flags["agent_missing"] is True:
                    flag_set = True
                    break
                time.sleep(INGESTION_DELAY)
            
            assert flag_set, "Blind-spot file should be marked agent_missing"
            
        finally:
            docker_manager.start_container(CONTAINER_CLIENT_A)
            setup_agents["ensure_agent_running"](
                CONTAINER_CLIENT_A, 
                setup_agents["api_key"], 
                setup_agents["view_id"]
            )
            time.sleep(SHORT_TIMEOUT)


    def test_new_leader_performs_snapshot(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        reset_leadership
    ):
        """
        场景: 验证新 Leader 执行 Snapshot 初始同步
        """
        # First, create some files for the new leader to snapshot
        test_dir = f"{MOUNT_POINT}/snapshot_resume_test_{int(time.time()*1000)}"
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
        time.sleep(STRESS_DELAY)
        
        # Stop Agent A
        docker_manager.stop_container(CONTAINER_CLIENT_A)
        
        try:
            # Wait for failover (Session timeout + buffer)
            time.sleep(MEDIUM_TIMEOUT)

            # Wait for Snapshot (Readiness) restoration
            start_wait = time.time()
            ready = False
            while time.time() - start_wait < EXTREME_TIMEOUT:
                try:
                    fusion_client.get_stats()
                    ready = True 
                    break
                except Exception:
                    time.sleep(POLL_INTERVAL)
            if not ready:
                pytest.fail("View failed to become ready after failover in test_new_leader_performs_snapshot")
            
            # Create new file while B is leader
            new_file = f"{test_dir}/after_failover.txt"
            docker_manager.create_file_in_container(
                CONTAINER_CLIENT_B,
                new_file,
                content="created after failover"
            )
            
            # New leader B should sync this via realtime
            found = fusion_client.wait_for_file_in_tree(new_file, timeout=MEDIUM_TIMEOUT)
            
            assert found is not None, \
                f"New leader should sync files via realtime, got sessions: {fusion_client.get_sessions()}"
            
        finally:
            docker_manager.start_container(CONTAINER_CLIENT_A)
            setup_agents["ensure_agent_running"](
                CONTAINER_CLIENT_A, 
                setup_agents["api_key"], 
                setup_agents["view_id"]
            )
            time.sleep(SHORT_TIMEOUT)


    def test_original_leader_becomes_follower_on_return(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        reset_leadership
    ):
        """
        场景: 原 Leader 重新上线后，应该成为 Follower（不抢占）
        """
        # Stop A
        docker_manager.stop_container(CONTAINER_CLIENT_A)
        
        try:
            # Wait for failover (Session timeout 10s + buffer)
            time.sleep(MEDIUM_TIMEOUT)
            
            # Restart A
            docker_manager.start_container(CONTAINER_CLIENT_A)
            setup_agents["ensure_agent_running"](
                CONTAINER_CLIENT_A, 
                setup_agents["api_key"], 
                setup_agents["view_id"]
            )
            # Wait for A to register new session
            time.sleep(MEDIUM_TIMEOUT)
            
            # Check roles
            sessions = fusion_client.get_sessions()
            logger.info(f"Checking roles for returning Agent A. Sessions: {sessions}")
            
            roles = {}
            for s in sessions:
                aid = s.get("agent_id", "")
                role = s.get("role")
                if aid.startswith("client-a"):
                    # For agent-a, we might have a stale session. 
                    # Prefer leader if multiple exist, or the one with "can_snapshot"
                    if roles.get("client-a") != "leader":
                        roles["client-a"] = role
                elif aid.startswith("client-b"):
                    if roles.get("client-b") != "leader":
                        roles["client-b"] = role
            
            logger.info(f"Inferred roles: {roles}")
            # B should remain leader
            assert roles.get("client-b") == "leader", \
                f"Agent B should remain leader, got roles: {roles}, sessions: {sessions}"
            
            # A should be follower
            assert roles.get("client-a") == "follower", \
                f"Returning Agent A should become follower, got roles: {roles}, sessions: {sessions}"
            
        finally:
            # Final cleanup - restart to restore original state if needed
            pass

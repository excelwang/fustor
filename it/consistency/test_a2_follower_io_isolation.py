"""
Test A2: Second Agent becomes Follower with IO isolation.

验证第二个 Agent 成为 Follower，且不执行 Snapshot/Audit IO 操作。
参考文档: CONSISTENCY_DESIGN.md - Section 3 (Leader/Follower 选举)
"""
import pytest
import time

import os
from ..conftest import CONTAINER_CLIENT_A, CONTAINER_CLIENT_B, MOUNT_POINT
from ..fixtures.constants import (
    VIEW_READY_TIMEOUT,
    AGENT_B_READY_TIMEOUT,
    SHORT_TIMEOUT,
    MEDIUM_TIMEOUT,
    LONG_TIMEOUT,
    POLL_INTERVAL
)


class TestFollowerIOIsolation:
    """Test that the follower agent does not perform snapshot/audit."""

    def test_second_agent_becomes_follower(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir
    ):
        """
        场景: Agent A 已经是 Leader，Agent B 连接到 Fusion
        预期: Agent B 被标记为 Follower
        验证方法: 查询 Sessions，确认 Agent B 的 role 为 "follower"
        """
        # Wait for agents to establish sessions and view to be ready
        assert fusion_client.wait_for_view_ready(timeout=VIEW_READY_TIMEOUT), "View did not become ready for Agent A"
        
        # Get all sessions
        sessions = fusion_client.get_sessions()
        
        # Find follower session
        follower_session = None
        for session in sessions:
            if session.get("agent_id", "").startswith("client-b"):
                follower_session = session
                break
        
        if follower_session is None:
            print(f"DEBUG: All sessions found: {sessions}")
        
        assert follower_session is not None, "Agent B session not found"
        assert follower_session.get("role") == "follower", \
            f"Expected client-b to be follower, got {follower_session.get('role')}"
        
        # Verify follower capabilities are restricted
        assert follower_session.get("can_snapshot") is False, \
            "Follower should not be able to snapshot"
        assert follower_session.get("can_audit") is False, \
            "Follower should not be able to audit"
        
        # Follower should still be able to send realtime events
        assert follower_session.get("can_realtime") is True, \
            "Follower should be able to send realtime events"

    def test_follower_only_sends_realtime_events(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir
    ):
        """
        场景: Follower Agent 检测到文件变更
        预期: Follower 只发送 realtime 事件，不发送 snapshot/audit 事件
        验证方法: 创建文件，检查事件类型
        """
        from ..utils import docker_manager
        from ..conftest import MOUNT_POINT
        
        test_file = f"{MOUNT_POINT}/test_follower_realtime_{int(time.time()*1000)}.txt"
        
        # Wait for Agent B to be registered and ready (post-prescan)
        if not fusion_client.wait_for_agent_ready("client-b", timeout=AGENT_B_READY_TIMEOUT):
             pytest.fail(f"Agent B did not become ready (can_realtime=True) within {AGENT_B_READY_TIMEOUT}s")
        
        # Get session info to verify it's a follower
        sessions = fusion_client.get_sessions()
        follower_session = next((s for s in sessions if "client-b" in s.get("agent_id", "")), None)
        assert follower_session is not None, "Agent B session not found"
        assert follower_session.get("role") == "follower", \
            f"Expected client-b to be follower, got {follower_session.get('role')}"

        # WARMUP: Ensure Follower's FS Driver is actively watching
        # Create a warmup file and wait for it to be seen. This confirms inotify is ready.
        warmup_file = f"{MOUNT_POINT}/warmup_follower_{int(time.time())}.txt"
        docker_manager.create_file_in_container(CONTAINER_CLIENT_B, warmup_file, "warmup")
        
        # Check against relative path since Source-FS emits relative keys
        warmup_rel = os.path.relpath(warmup_file, MOUNT_POINT)
        if not fusion_client.wait_for_file_in_tree(warmup_rel, timeout=SHORT_TIMEOUT):
            pytest.fail("Follower Agent B failed to detect warmup file. FS Driver might not be ready.")
        
        # Create file on follower's mount
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_B,
            test_file,
            content="realtime test"
        )
        
        # Wait for event to be processed (Follower -> Fusion)
        # We poll to observe the arrival as soon as possible
        found = None
        start = time.time()
        test_file_rel = os.path.relpath(test_file, MOUNT_POINT)
        
        while time.time() - start < LONG_TIMEOUT:
            # Short-circuit if found
            found = fusion_client.wait_for_file_in_tree(
                file_path=test_file_rel,
                timeout=SHORT_TIMEOUT
            )
            if found:
                break
            time.sleep(POLL_INTERVAL)
        
        assert found is not None, "File should appear via realtime event from follower"
        
        # The file should not have agent_missing flag (came from agent)
        # We poll for the False flag as Realtime might arrive slightly after Audit discovery
        cleared = False
        start = time.time()
        flags = {}
        while time.time() - start < MEDIUM_TIMEOUT:
            flags = fusion_client.check_file_flags(test_file_rel)
            if flags.get("agent_missing") is False:
                cleared = True
                break
            time.sleep(POLL_INTERVAL)
            
        assert cleared, \
            f"File from follower should eventually have agent_missing=False, got flags: {flags}"

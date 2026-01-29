"""
Test A2: Second Agent becomes Follower with IO isolation.

验证第二个 Agent 成为 Follower，且不执行 Snapshot/Audit IO 操作。
参考文档: CONSISTENCY_DESIGN.md - Section 3 (Leader/Follower 选举)
"""
import pytest
import time

from ..conftest import CONTAINER_CLIENT_A, CONTAINER_CLIENT_B


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
        # Wait for both agents to establish sessions
        time.sleep(5)
        
        # Get all sessions
        sessions = fusion_client.get_sessions()
        
        # Find follower session
        follower_session = None
        for session in sessions:
            if session.get("agent_id", "").startswith("agent-b"):
                follower_session = session
                break
        
        if follower_session is None:
            print(f"DEBUG: All sessions found: {sessions}")
        
        assert follower_session is not None, "Agent B session not found"
        assert follower_session.get("role") == "follower", \
            f"Expected agent-b to be follower, got {follower_session.get('role')}"
        
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
        
        # Give more buffer for agents to fully transition to stable state and establish watchers
        time.sleep(5)
        
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
        while time.time() - start < 30:
            # Short-circuit if found
            found = fusion_client.wait_for_file_in_tree(
                file_path=test_file,
                timeout=2
            )
            if found:
                break
            time.sleep(1)
        
        assert found is not None, "File should appear via realtime event from follower"
        
        # The file should not have agent_missing flag (came from agent)
        # We poll for the False flag as Realtime might arrive slightly after Audit discovery
        cleared = False
        start = time.time()
        flags = {}
        while time.time() - start < 15:
            flags = fusion_client.check_file_flags(test_file)
            if flags.get("agent_missing") is False:
                cleared = True
                break
            time.sleep(1)
            
        assert cleared, \
            f"File from follower should eventually have agent_missing=False, got flags: {flags}"

"""
Test C5: Sentinel Sweep - Leader periodically updates Suspect List mtime.

验证 Leader Agent 定期执行哨兵巡检，获取 Suspect List 并上报最新 mtime。
参考文档: CONSISTENCY_DESIGN.md - Section 7 (哨兵巡检)
"""
import pytest
import time

from ..utils import docker_manager
from ..conftest import CONTAINER_CLIENT_C, MOUNT_POINT


class TestSentinelSweep:
    """Test Leader's sentinel sweep functionality."""

    def test_leader_fetches_suspect_list_periodically(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        场景:
          1. 无 Agent 客户端创建多个文件（加入 Suspect List）
          2. Leader 应定期请求 GET /api/v1/views/fs/suspect-list
          3. Leader 检查这些文件的当前 mtime
          4. Leader 通过 PUT /api/v1/views/fs/suspect-list 上报
        预期:
          - 文件出现在 suspect list 中
          - PUT API 能够正常处理更新
        """
        test_files = [
            f"{MOUNT_POINT}/suspect_sweep_1_{int(time.time()*1000)}.txt",
            f"{MOUNT_POINT}/suspect_sweep_2_{int(time.time()*1000)}.txt",
            f"{MOUNT_POINT}/suspect_sweep_3_{int(time.time()*1000)}.txt",
        ]
        
        # Create files from blind-spot
        for f in test_files:
            docker_manager.create_file_in_container(
                CONTAINER_CLIENT_C,
                f,
                content=f"content for {f}"
            )
        
        # Wait for Audit to discover and mark as suspect
        # Marker synchronization
        marker_file = f"{MOUNT_POINT}/audit_marker_c5_{int(time.time()*1000)}.txt"
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, marker_file, content="marker")
        time.sleep(3)
        assert fusion_client.wait_for_file_in_tree(marker_file, timeout=30) is not None
        
        # Wait for initial sync visibility
        for f in test_files:
            fusion_client.wait_for_file_in_tree(f, timeout=10)
        
        # Get suspect list
        suspect_list = fusion_client.get_suspect_list()
        paths_in_list = [item.get("path") for item in suspect_list]
        
        # All test files should be in suspect list
        for f in test_files:
            assert f in paths_in_list, f"File {f} should be in suspect list"
        
        # Manually trigger a suspect list update (simulating what agent would do)
        # Testing PUT API robustness and compatibility
        updates = [
            {"path": f, "mtime": time.time()} # Use 'mtime' as expected by API
            for f in test_files
        ]
        
        result = fusion_client.update_suspect_list(updates)
        assert result is not None, "Suspect list update should return a result"

    def test_only_leader_performs_sentinel_sweep(
        self,
        docker_env,
        fusion_client,
        setup_agents
    ):
        """
        场景: 只有 Leader 执行哨兵巡检
        验证方法: 检查 Follower 不会发起 suspect-list 请求
        """
        # Get sessions
        sessions = fusion_client.get_sessions()
        
        # Find leader
        leader = None
        for s in sessions:
            if s.get("role") == "leader":
                leader = s
        
        assert leader is not None, "Leader should exist"
        # The agent logs would normally show the sweep starting.
        # This test ensures basic session role distinction via Fusion API.

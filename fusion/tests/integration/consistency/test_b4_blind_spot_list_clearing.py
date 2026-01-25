"""
Test B4: Blind-spot list cleared at each Audit start.

验证每轮 Audit 开始时，旧的盲区名单被清空。
参考文档: CONSISTENCY_DESIGN.md - Section 4.4 (清空时机: 每轮 Audit 开始时清空)
"""
import pytest
import time

from ..utils import docker_manager
from ..conftest import CONTAINER_CLIENT_C, MOUNT_POINT


class TestBlindSpotListClearing:
    """Test that blind-spot list is cleared at each audit start."""

    def test_blind_spot_list_cleared_on_new_audit(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        场景:
          1. 创建盲区文件 A，等待 Audit 发现
          2. 确认文件 A 在 blind-spot list 中
          3. 删除文件 A，等待下一轮 Audit
          4. 新 Audit 开始时，旧的 blind-spot list 被清空
          5. 文件 A 不再在 blind-spot list 中（已删除）
        """
        test_file_a = f"{MOUNT_POINT}/blind_clear_test_a_{int(time.time()*1000)}.txt"
        test_file_b = f"{MOUNT_POINT}/blind_clear_test_b_{int(time.time()*1000)}.txt"
        
        # Step 1: Create file A from blind-spot client
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_C,
            test_file_a,
            content="file A for clearing test"
        )
        
        # Use a marker file to detect Audit completion
        marker_file = f"{MOUNT_POINT}/audit_marker_b4_1_{int(time.time())}.txt"
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, marker_file, content="marker")
        # NFS Lag: wait for kernel to refresh dir listing
        time.sleep(3)
        
        # Wait for marker to appear in Fusion (at least one audit cycle completed)
        assert fusion_client.wait_for_file_in_tree(marker_file, timeout=30) is not None, \
            f"Audit marker file {marker_file} should be discovered by Audit scan within 30s"
        
        # Verify A is in blind-spot list
        blind_list = fusion_client.get_blind_spot_list()
        paths = [item.get("path") for item in blind_list if item.get("type") == "file"]
        assert test_file_a in paths, "File A should be in blind-spot list"
        
        # Step 3: Delete file A, create new file B
        docker_manager.delete_file_in_container(CONTAINER_CLIENT_C, test_file_a)
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_C,
            test_file_b,
            content="file B for clearing test"
        )
        
        # Step 4 & 5: Wait for next Audit cycle and verify list
        marker_file_2 = f"{MOUNT_POINT}/audit_marker_b4_2_{int(time.time())}.txt"
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, marker_file_2, content="marker")
        time.sleep(3) # NFS cache
        assert fusion_client.wait_for_file_in_tree(marker_file_2, timeout=30) is not None
        
        blind_list_2 = fusion_client.get_blind_spot_list()
        paths_2 = [item.get("path") for item in blind_list_2 if item.get("type") == "file"]
        # We want A to be gone AND B to be present
        assert test_file_a not in paths_2, f"File A should be cleared from list. Current paths: {paths_2}"
        assert test_file_b in paths_2, f"File B should be in the new list. Current paths: {paths_2}"

    def test_agent_missing_flag_cleared_after_realtime_update(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        场景: 盲区文件被 Agent 客户端重新 touch，agent_missing 标记应被清除
        参考: CONSISTENCY_DESIGN.md - Section 5.1 (Realtime 从 Blind-spot List 移除)
        """
        from ..conftest import CONTAINER_CLIENT_A
        
        test_file = f"{MOUNT_POINT}/blind_flag_clear_test_{int(time.time()*1000)}.txt"
        
        # Create file from blind-spot
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_C,
            test_file,
            content="blind spot file"
        )
        # Use marker file to detect Audit completion
        marker_file = f"{MOUNT_POINT}/audit_marker_b4_flag_{int(time.time())}.txt"
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, marker_file, content="marker")
        time.sleep(3) # NFS cache delay
        assert fusion_client.wait_for_file_in_tree(marker_file, timeout=30) is not None
        
        # Verify agent_missing is set
        assert fusion_client.wait_for_flag(test_file, "agent_missing", True, timeout=10), \
            "File should eventually be marked agent_missing"
        
        # Touch file from Agent client (triggers realtime update)
        docker_manager.touch_file(CONTAINER_CLIENT_A, test_file)
        
        # Wait for realtime update (poll for flag cleared)
        assert fusion_client.wait_for_flag(test_file, "agent_missing", False, timeout=10), \
            "agent_missing flag should be cleared after realtime update"

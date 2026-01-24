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
        test_file_a = f"{MOUNT_POINT}/blind_clear_test_a.txt"
        test_file_b = f"{MOUNT_POINT}/blind_clear_test_b.txt"
        
        # Step 1: Create file A from blind-spot client
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_C,
            test_file_a,
            content="file A for clearing test"
        )
        
        # Wait for Audit to discover file A
        wait_for_audit()
        
        # Step 2: Verify A is in blind-spot list
        blind_list_1 = fusion_client.get_blind_spot_list()
        paths_1 = [item.get("path") for item in blind_list_1 if item.get("type") == "file"]
        assert test_file_a in paths_1, "File A should be in blind-spot list"
        
        # Step 3: Delete file A, create new file B
        docker_manager.delete_file_in_container(CONTAINER_CLIENT_C, test_file_a)
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_C,
            test_file_b,
            content="file B for clearing test"
        )
        
        # Step 4: Wait for next Audit cycle
        wait_for_audit()
        
        # Step 5: Get new blind-spot list
        blind_list_2 = fusion_client.get_blind_spot_list()
        # Filter for file type only (ignore tracked deletions)
        paths_2 = [item.get("path") for item in blind_list_2 if item.get("type") == "file"]
        
        # File A should NOT be in the new list (list was cleared, A no longer exists)
        assert test_file_a not in paths_2, \
            "File A should not be in blind-spot list after next Audit (list cleared)"
        
        # File B SHOULD be in the new list
        assert test_file_b in paths_2, \
            "File B should be in the new blind-spot list"

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
        
        test_file = f"{MOUNT_POINT}/blind_flag_clear_test.txt"
        
        # Create file from blind-spot
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_C,
            test_file,
            content="blind spot file"
        )
        
        # Wait for Audit to discover
        wait_for_audit()
        
        # Verify agent_missing is set
        flags = fusion_client.check_file_flags(test_file)
        assert flags["agent_missing"] is True, "File should be marked agent_missing"
        
        # Touch file from Agent client (triggers realtime update)
        docker_manager.touch_file(CONTAINER_CLIENT_A, test_file)
        
        # Wait for realtime event to be processed
        time.sleep(3)
        
        # Check flag is cleared
        flags_after = fusion_client.check_file_flags(test_file)
        assert flags_after["agent_missing"] is False, \
            "agent_missing flag should be cleared after realtime update"

"""
Test C4: Realtime update removes file from Suspect List.

验证收到 Realtime Update 事件后，文件从 Suspect List 中移除。
参考文档: CONSISTENCY_DESIGN.md - Section 4.3 (淘汰: 收到 Realtime Update 后移除)
            & Section 5.1 (Realtime INSERT/UPDATE 从 Suspect List 移除)
"""
import pytest
import time

from ..utils import docker_manager
from ..conftest import CONTAINER_CLIENT_A, CONTAINER_CLIENT_C, MOUNT_POINT


class TestRealtimeRemovesSuspect:
    """Test that realtime update clears suspect status."""

    def test_realtime_update_clears_suspect(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        场景:
          1. 无 Agent 客户端创建文件（Audit 发现加入 Suspect List）
          2. 对文件由 Agent 客户端进行明确的修改（触发 Realtime Update）
          3. Realtime Update 事件处理后，文件从 Suspect List 移除
        预期:
          - 收到 Realtime Update 后，integrity_suspect 标记被清除
        """
        test_file = f"{MOUNT_POINT}/realtime_clear_suspect_{int(time.time()*1000)}.txt"
        
        # Step 1: Create file from blind-spot
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_C,
            test_file,
            content="initial content"
        )
        
        # Wait for Audit to discover and mark as suspect
        wait_for_audit()
        fusion_client.wait_for_file_in_tree(test_file, timeout=10)
        
        # Verify file is suspect
        flags_initial = fusion_client.check_file_flags(test_file)
        assert flags_initial["integrity_suspect"] is True, \
            "New file from Audit should be marked as suspect"
        
        # Step 2: Wait a moment, then modify file from Agent A (trigger Realtime Update)
        time.sleep(2)
        docker_manager.modify_file_in_container(
            CONTAINER_CLIENT_A,
            test_file,
            append_content="additional content via realtime"
        )
        
        # Step 3: Wait for Realtime event to be processed
        time.sleep(5)
        
        # Step 4: Check that suspect flag is cleared
        flags_after = fusion_client.check_file_flags(test_file)
        assert flags_after["integrity_suspect"] is False, \
            "Suspect flag should be cleared after Realtime UPDATE"
        
        # File should be removed from suspect list
        suspect_list = fusion_client.get_suspect_list()
        paths = [item.get("path") for item in suspect_list]
        assert test_file not in paths, \
            "File should be removed from suspect list after Realtime UPDATE"

    def test_realtime_insert_clears_prior_suspect(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        场景: 盲区文件被 Agent 重新创建（INSERT），应清除 suspect 状态
        """
        test_file = f"{MOUNT_POINT}/realtime_insert_clear_{int(time.time()*1000)}.txt"
        
        # Create from blind-spot (will be suspect + agent_missing)
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_C,
            test_file,
            content="blind spot content"
        )
        
        wait_for_audit()
        fusion_client.wait_for_file_in_tree(test_file, timeout=10)
        
        # Verify flags
        flags = fusion_client.check_file_flags(test_file)
        assert flags["integrity_suspect"] is True
        
        # Delete and recreate from Agent (simulates clear INSERT)
        docker_manager.delete_file_in_container(CONTAINER_CLIENT_A, test_file)
        time.sleep(1)
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_A,
            test_file,
            content="recreated from agent"
        )
        
        # Wait for realtime events
        time.sleep(5)
        
        # After Agent INSERT, suspect should be cleared
        flags_after = fusion_client.check_file_flags(test_file)
        assert flags_after["agent_missing"] is False, \
            "agent_missing should be cleared after Agent INSERT"
        assert flags_after["integrity_suspect"] is False, \
            "integrity_suspect should be cleared after Agent INSERT/UPDATE"

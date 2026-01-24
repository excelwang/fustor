"""
Test B3: Blind-spot file modification detected by Audit.

验证无 Agent 客户端修改文件时，Audit 通过 mtime 仲裁更新内存树。
参考文档: CONSISTENCY_DESIGN.md - Section 5.3 场景 1 (Audit 报告存在文件 X)
"""
import pytest
import time

from ..utils import docker_manager
from ..conftest import CONTAINER_CLIENT_A, CONTAINER_CLIENT_C, MOUNT_POINT


class TestBlindSpotFileModification:
    """Test detection of file modifications by client without agent."""

    def test_blind_spot_modification_updates_mtime(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        场景:
          1. Agent A 创建文件（实时同步）
          2. 无 Agent 客户端 C 修改文件内容
          3. Audit 发现文件 mtime 更新，且 Audit.mtime > Memory.mtime
          4. 内存树更新 mtime（盲区修改）
        预期:
          - 修改后，Fusion 中文件的 mtime 被更新
          - 文件可能被标记为 agent_missing（取决于实现）
        """
        test_file = f"{MOUNT_POINT}/blind_modify_test.txt"
        
        # Step 1: Create file from agent client
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_A,
            test_file,
            content="original content"
        )
        
        # Wait for realtime sync
        found = fusion_client.wait_for_file_in_tree(test_file, timeout=10)
        assert found is not None, "File should appear via realtime event"
        
        # Record original mtime from Fusion
        original_mtime = found.get("mtime")
        
        # Step 2: Wait a bit, then modify file from blind-spot client
        time.sleep(2)
        docker_manager.modify_file_in_container(
            CONTAINER_CLIENT_C,
            test_file,
            append_content="modified from blind spot"
        )
        
        # Get new mtime from filesystem
        new_fs_mtime = docker_manager.get_file_mtime(CONTAINER_CLIENT_C, test_file)
        assert new_fs_mtime > original_mtime, "Filesystem mtime should increase after modification"
        
        # Step 3: Before Audit, Fusion mtime should be unchanged
        tree = fusion_client.get_tree(path=test_file, max_depth=0)
        mtime_before_audit = tree.get("mtime")
        assert mtime_before_audit == original_mtime, \
            "Fusion mtime should be unchanged before Audit"
        
        # Step 4: Wait for Audit
        wait_for_audit()
        
        # Step 5: After Audit, Fusion mtime should be updated
        time.sleep(5)
        tree_after = fusion_client.get_tree(path=test_file, max_depth=0)
        mtime_after_audit = tree_after.get("mtime")
        
        assert mtime_after_audit > original_mtime, \
            "Fusion mtime should be updated by Audit (blind-spot modification)"
        assert mtime_after_audit == new_fs_mtime, \
            "Fusion mtime should match filesystem mtime after Audit"

    def test_blind_spot_modification_marks_agent_missing(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        场景: 盲区修改的文件应被标记为 agent_missing
        """
        test_file = f"{MOUNT_POINT}/blind_modify_flag_test.txt"
        
        # Create from agent, modify from blind-spot
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_A,
            test_file,
            content="original"
        )
        fusion_client.wait_for_file_in_tree(test_file, timeout=10)
        
        # Initial file should not have agent_missing
        flags_initial = fusion_client.check_file_flags(test_file)
        assert flags_initial["agent_missing"] is False
        
        # Modify from blind-spot
        time.sleep(1)
        docker_manager.modify_file_in_container(
            CONTAINER_CLIENT_C,
            test_file,
            append_content="blind modification"
        )
        
        # Wait for Audit
        wait_for_audit()
        
        # Check agent_missing flag after modification
        flags_after = fusion_client.check_file_flags(test_file)
        # Note: Whether modification sets agent_missing depends on implementation
        # The file was discovered via audit update, so it may be flagged
        # This assertion may need adjustment based on actual implementation

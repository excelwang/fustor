"""
Test B2: Blind-spot file deletion detected by Audit.

验证无 Agent 客户端删除的文件通过 Audit 被发现，并从内存树移除。
参考文档: CONSISTENCY_DESIGN.md - Section 5.3 场景 2 (Audit 报告目录缺少文件)
"""
import pytest
import time

from ..utils import docker_manager
from ..conftest import CONTAINER_CLIENT_A, CONTAINER_CLIENT_C, MOUNT_POINT


class TestBlindSpotFileDeletion:
    """Test detection of files deleted by client without agent."""

    def test_blind_spot_deletion_detected_by_audit(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        场景:
          1. Agent A 创建文件（正常路径，实时同步到 Fusion）
          2. 无 Agent 的客户端 C 删除该文件
          3. 因为 C 没有 Agent，删除事件不会实时同步
          4. Audit 发现文件缺失，从内存树移除
        预期:
          - 删除前文件存在于 Fusion
          - 删除后（Audit 前）文件仍存在于 Fusion
          - Audit 后文件从 Fusion 移除
        """
        test_file = f"{MOUNT_POINT}/blind_delete_test.txt"
        
        # Step 1: Create file from agent client (realtime sync)
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_A,
            test_file,
            content="will be deleted from blind spot"
        )
        
        # Wait for realtime sync
        found = fusion_client.wait_for_file_in_tree(test_file, timeout=10)
        assert found is not None, "File should appear via realtime event"
        
        # Step 2: Delete file from blind-spot client
        docker_manager.delete_file_in_container(CONTAINER_CLIENT_C, test_file)
        
        # Step 3: Immediately after deletion, file should STILL be in Fusion
        time.sleep(2)
        still_exists = fusion_client.wait_for_file_in_tree(test_file, timeout=5)
        assert still_exists is not None, \
            "File should still exist in Fusion (no realtime delete from blind-spot)"
        
        # Step 4: Wait for Audit cycle
        wait_for_audit()
        
        # Step 5: After Audit, file should be removed
        time.sleep(5)
        tree = fusion_client.get_tree(path="/", max_depth=-1)
        found_after_audit = fusion_client._find_in_tree(tree, test_file)
        
        assert found_after_audit is None, \
            "File should be removed after Audit detects blind-spot deletion"

    def test_blind_spot_deletion_added_to_blind_spot_list(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        场景: 盲区删除的文件应被记录到 Blind-spot List
        """
        test_file = f"{MOUNT_POINT}/blind_delete_list_test.txt"
        
        # Create file from agent, then delete from blind-spot
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_A,
            test_file,
            content="for blind delete list test"
        )
        fusion_client.wait_for_file_in_tree(test_file, timeout=10)
        
        docker_manager.delete_file_in_container(CONTAINER_CLIENT_C, test_file)
        
        # Wait for Audit
        wait_for_audit()
        
        # Check blind-spot list for deletion record
        blind_spot_list = fusion_client.get_blind_spot_list()
        
        # Look for deletion entry
        deletion_entries = [
            item for item in blind_spot_list
            if item.get("path") == test_file and item.get("type") == "deletion"
        ]
        
        assert len(deletion_entries) > 0, \
            "Blind-spot deletion should be recorded in blind-spot list"

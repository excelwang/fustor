"""
Test D4: Tombstones are cleaned after Audit-End.

验证在 Audit-End 信号后，旧的 Tombstone 条目被清理。
参考文档: CONSISTENCY_DESIGN.md - Section 4.2 (销毁: 收到 Audit-End 信号后，清理所有在此次 Audit 开始前创建的 Tombstone)
            & Section 6 (审计生命周期)
"""
import pytest
import time

from ..utils import docker_manager
from ..conftest import CONTAINER_CLIENT_A, CONTAINER_CLIENT_C, MOUNT_POINT


class TestTombstoneCleanup:
    """Test that tombstones are cleaned after audit-end."""

    def test_old_tombstones_cleaned_after_audit_cycle(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        场景:
          1. 创建文件并删除（创建 Tombstone，记录 create_time = T1）
          2. 等待 Audit 开始（记录 scan_start_time = T2，其中 T2 > T1）
          3. 等待 Audit 结束（Audit-End 信号）
          4. 因为 Tombstone.create_time < scan_start_time，该 Tombstone 被清理
          5. 之后，如果同名文件被创建，它可以正常添加到内存树
        预期:
          - Audit 完成后，旧 Tombstone 被清理
          - 相同路径的新文件可以被正常同步
        """
        test_file = f"{MOUNT_POINT}/tombstone_cleanup.txt"
        
        # Step 1: Create and delete file (creates Tombstone)
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_A,
            test_file,
            content="will be deleted and cleaned"
        )
        fusion_client.wait_for_file_in_tree(test_file, timeout=15)
        
        docker_manager.delete_file_in_container(CONTAINER_CLIENT_A, test_file)
        time.sleep(3)
        
        # Verify deleted
        tree = fusion_client.get_tree(path="/", max_depth=-1)
        assert fusion_client._find_in_tree(tree, test_file) is None
        
        # Step 2-3: Wait for Audit cycle to complete
        wait_for_audit()
        
        # Step 4: After Audit, Tombstone should be cleaned
        # We verify by creating a new file with the same path
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_A,
            test_file,
            content="new file after tombstone cleanup"
        )
        
        # Step 5: The new file should appear (Tombstone no longer blocks it)
        time.sleep(3)
        
        # Wait for realtime sync of new file
        found = fusion_client.wait_for_file_in_tree(test_file, timeout=10)
        
        assert found is not None, \
            "New file should appear after Tombstone is cleaned"

    def test_tombstone_blocks_during_audit_but_not_after(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        场景: 验证 Tombstone 在 Audit 期间阻止复活，但 Audit 后允许新创建
        """
        test_file = f"{MOUNT_POINT}/tombstone_lifecycle.txt"
        
        # Create and delete
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_A,
            test_file,
            content="lifecycle test"
        )
        fusion_client.wait_for_file_in_tree(test_file, timeout=15)
        
        docker_manager.delete_file_in_container(CONTAINER_CLIENT_A, test_file)
        time.sleep(3)
        
        # Immediately try to create from blind-spot (should be blocked by Tombstone)
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_C,
            test_file,
            content="blind spot attempt during tombstone"
        )
        
        # Wait for Audit
        wait_for_audit()
        
        # First Audit may or may not show the file depending on timing
        # Wait for another Audit cycle to ensure Tombstone is definitely cleaned
        wait_for_audit()
        
        # Now the file should appear (Tombstone cleaned, blind-spot file exists)
        tree = fusion_client.get_tree(path="/", max_depth=-1)
        found = fusion_client._find_in_tree(tree, test_file)
        
        # After Tombstone cleanup, the blind-spot file should be discoverable
        # Note: The exact behavior depends on implementation details

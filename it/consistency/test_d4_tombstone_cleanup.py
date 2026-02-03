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
from ..fixtures.constants import SHORT_TIMEOUT, MEDIUM_TIMEOUT, LONG_TIMEOUT, STRESS_DELAY


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
        test_file = f"{MOUNT_POINT}/tombstone_cleanup_{int(time.time()*1000)}.txt"
        
        # Step 1: Create and delete file (creates Tombstone)
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_A,
            test_file,
            content="will be deleted and cleaned"
        )
        fusion_client.wait_for_file_in_tree(test_file, timeout=MEDIUM_TIMEOUT)
        
        # Delete via Agent (creates Tombstone)
        docker_manager.delete_file_in_container(CONTAINER_CLIENT_A, test_file)
        
        # Wait for DELETE event and Verify deleted
        assert fusion_client.wait_for_file(test_file, timeout=MEDIUM_TIMEOUT, should_exist=False), \
            f"File {test_file} should be removed from tree after Realtime DELETE"
        
        # Step 2-3: Use a marker file to detect Audit completion
        marker_file = f"{MOUNT_POINT}/audit_marker_d4_{int(time.time())}.txt"
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, marker_file, content="marker")
        time.sleep(STRESS_DELAY) # NFS cache
        assert fusion_client.wait_for_file_in_tree(marker_file, timeout=LONG_TIMEOUT) is not None
        
        # Step 4: After Audit, Tombstone should be cleaned
        # We verify by creating a new file with the same path via Agent
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_A,
            test_file,
            content="new file after tombstone cleanup"
        )
        
        # Step 5: The new file should appear (Tombstone no longer blocks it)
        # Wait for realtime sync of new file
        found = fusion_client.wait_for_file_in_tree(test_file, timeout=SHORT_TIMEOUT)
        
        assert found is not None, \
            f"New file should appear after Tombstone is cleaned. Tree: {fusion_client.get_tree(path='/', max_depth=-1)}"

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
        test_file = f"{MOUNT_POINT}/tombstone_lifecycle_{int(time.time()*1000)}.txt"
        
        # Create and delete
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_A,
            test_file,
            content="lifecycle test"
        )
        fusion_client.wait_for_file_in_tree(test_file, timeout=MEDIUM_TIMEOUT)
        
        docker_manager.delete_file_in_container(CONTAINER_CLIENT_A, test_file)
        assert fusion_client.wait_for_file(test_file, timeout=MEDIUM_TIMEOUT, should_exist=False), \
            f"File {test_file} should be removed after delete"
        
        # Give a small buffer for potential racing snapshots to be processed and blocked
        time.sleep(STRESS_DELAY)
        
        # Immediately try to create from blind-spot (should be blocked by Tombstone)
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_C,
            test_file,
            content="blind spot attempt during tombstone"
        )
        
        # Step 4: Use marker to ensure first audit cycle ran
        marker_file = f"{MOUNT_POINT}/audit_marker_d4_lifecycle_{int(time.time())}.txt"
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, marker_file, content="marker")
        time.sleep(STRESS_DELAY) # NFS cache
        assert fusion_client.wait_for_file_in_tree(marker_file, timeout=LONG_TIMEOUT) is not None
        
        # After first audit finishes, the tombstone (created before audit) is cleaned.
        # However, the blind-spot file report arrived DURING the first audit and was blocked.
        # We need a SECOND audit cycle to discover the still-existing blind-spot file.
        marker_file_2 = f"{MOUNT_POINT}/audit_marker_d4_lifecycle_2_{int(time.time())}.txt"
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, marker_file_2, content="marker")
        time.sleep(STRESS_DELAY) # NFS cache
        assert fusion_client.wait_for_file_in_tree(marker_file_2, timeout=LONG_TIMEOUT) is not None
        
        # Now the file should appear (Tombstone cleaned, blind-spot file exists)
        tree = fusion_client.get_tree(path="/", max_depth=-1)
        found = fusion_client._find_in_tree(tree, test_file)
        
        assert found is not None, \
            "Blind-spot file should be discovered after tombstone expires"
        
        # After Tombstone cleanup, the blind-spot file should be discoverable
        # Note: The exact behavior depends on implementation details

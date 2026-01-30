"""
Test D2: Snapshot does not resurrect tombstoned files.

验证在 Tombstone 中的文件不会被 Snapshot 事件复活。
参考文档: CONSISTENCY_DESIGN.md - Section 5.2 (Snapshot 消息处理: if X in Tombstone → 丢弃)
"""
import pytest
import time

from ..utils import docker_manager
from ..conftest import CONTAINER_CLIENT_A, CONTAINER_CLIENT_B, MOUNT_POINT


class TestSnapshotTombstoneProtection:
    """Test that snapshot does not resurrect tombstoned files."""

    def test_snapshot_discards_tombstoned_file(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir
    ):
        """
        场景:
          1. Agent A 创建文件（Realtime INSERT）
          2. Agent A 删除文件（Realtime DELETE → 创建 Tombstone）
          3. Agent B 的 Snapshot 扫描仍然看到该文件（NFS 缓存延迟）
          4. Agent B 发送 Snapshot 事件
          5. Fusion 检测到文件在 Tombstone 中，丢弃该事件
        预期:
          - 文件不会在删除后重新出现
        """
        test_file = f"{MOUNT_POINT}/snapshot_tombstone_test_{int(time.time()*1000)}.txt"
        
        # Step 1: Create file via Agent A
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_A,
            test_file,
            content="file for tombstone test"
        )
        
        # Wait for realtime sync
        found = fusion_client.wait_for_file_in_tree(test_file, timeout=15)
        assert found is not None, "File should be synced"
        
        # Step 2: Delete via Agent A (creates Tombstone)
        docker_manager.delete_file_in_container(CONTAINER_CLIENT_A, test_file)
        
        # Wait for DELETE event and Verify file is gone
        removed = fusion_client.wait_for_file(test_file, timeout=20, should_exist=False)
        assert removed, "File should be deleted"
        
        # Step 3: Now, if Agent B's snapshot sees the file (due to NFS cache)
        # and sends a snapshot event, it should be discarded.
        # We simulate this by waiting for a snapshot cycle.
        time.sleep(10)  # Wait for potential snapshot from B
        
        # Step 4: Verify file is still gone (not resurrected)
        tree_after = fusion_client.get_tree(path="/", max_depth=-1)
        found_after = fusion_client._find_in_tree(tree_after, test_file)
        
        assert found_after is None, \
            "Tombstoned file should NOT be resurrected by Snapshot"

    def test_rapid_create_delete_no_resurrection(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir
    ):
        """
        场景: 快速创建删除场景，验证 Tombstone 保护生效
        """
        test_files = [
            f"{MOUNT_POINT}/rapid_{int(time.time()*1000)}_{i}.txt" for i in range(5)
        ]
        
        # Rapidly create and delete files
        for f in test_files:
            docker_manager.create_file_in_container(
                CONTAINER_CLIENT_A,
                f,
                content="rapid test"
            )
        
        # Wait for sync
        time.sleep(5)
        
        # Delete all
        for f in test_files:
            docker_manager.delete_file_in_container(CONTAINER_CLIENT_A, f)
        
        # Wait for delete events and potential snapshot
        time.sleep(15)
        
        # All files should remain deleted
        tree = fusion_client.get_tree(path="/", max_depth=-1)
        for f in test_files:
            assert fusion_client._find_in_tree(tree, f) is None, \
                f"File {f} should not be resurrected"

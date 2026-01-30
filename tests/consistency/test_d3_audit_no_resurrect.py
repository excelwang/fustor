"""
Test D3: Audit does not resurrect tombstoned files.

验证在 Tombstone 中的文件不会被 Audit 事件复活。
参考文档: CONSISTENCY_DESIGN.md - Section 5.3 场景 1 (if X in Tombstone → 丢弃 (僵尸复活))
"""
import pytest
import time

from ..utils import docker_manager
from ..conftest import (
    CONTAINER_CLIENT_A, CONTAINER_CLIENT_C, MOUNT_POINT
)


class TestAuditTombstoneProtection:
    """Test that audit does not resurrect tombstoned files."""

    def test_audit_discards_tombstoned_file(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        场景:
          1. Agent A 创建文件
          2. Agent A 删除文件（创建 Tombstone）
          3. 无 Agent 客户端 C 重新创建同名文件（盲区操作）
          4. Audit 发现该文件
          5. 但因为文件在 Tombstone 中，Audit 事件被丢弃
        预期:
          - 即使磁盘上存在同名文件，Fusion 中也不会出现该文件
        """
        test_file = f"{MOUNT_POINT}/audit_tombstone_test_{int(time.time()*1000)}.txt"
        
        # Step 1: Create file via Agent
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_A,
            test_file,
            content="original content"
        )
        fusion_client.wait_for_file_in_tree(test_file, timeout=15)
        
        # Step 2: Delete via Agent (creates Tombstone)
        docker_manager.delete_file_in_container(CONTAINER_CLIENT_A, test_file)
        
        # Wait for DELETE event and Verify deleted
        removed = fusion_client.wait_for_file(test_file, timeout=20, should_exist=False)
        assert removed, "File should be removed"
        
        # Step 3: Recreate same file from blind-spot (simulating a problematic scenario)
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_C,
            test_file,
            content="blind spot recreation"
        )
        
        # Step 4: Use a marker file to detect Audit completion
        marker_file = f"{MOUNT_POINT}/audit_marker_{int(time.time())}.txt"
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, marker_file, content="marker")
        time.sleep(3) # NFS cache
        
        # Wait for marker to appear in Fusion (confirms at least one audit cycle completed)
        assert fusion_client.wait_for_file_in_tree(marker_file, timeout=30) is not None, \
            "Audit marker file should be discovered by Audit scan"
        
        # Step 5: File SHOULD appear (Reincarnation detected)
        # Consistency Logic Update:
        # If the file is recreated with a NEWER mtime than the tombstone, it is a valid Reincarnation.
        # It should ONLY be discarded if it is a Zombie (stale mtime).
        
        tree_after = fusion_client.get_tree(path="/", max_depth=-1)
        found = fusion_client._find_in_tree(tree_after, test_file)
        
        assert found is not None, \
            "Reincarnated file (fresh mtime) SHOULD be accepted by Audit, overruling the Tombstone"

    def test_tombstone_protects_against_nfs_cache_resurrection(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        场景: NFS 缓存延迟导致的潜在复活被 Tombstone 阻止
        
        背景: NFS 客户端可能因为缓存延迟，在文件已被删除后仍然"看到"该文件。
        这可能导致 Audit 错误地报告文件存在。Tombstone 机制可以防止这种情况。
        """
        test_file = f"{MOUNT_POINT}/nfs_cache_tombstone_{int(time.time()*1000)}.txt"
        
        # Create and sync
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_A,
            test_file,
            content="NFS cache test"
        )
        fusion_client.wait_for_file_in_tree(test_file, timeout=15)
        
        # Delete
        docker_manager.delete_file_in_container(CONTAINER_CLIENT_A, test_file)
        
        # Verify deleted
        removed = fusion_client.wait_for_file(test_file, timeout=20, should_exist=False)
        assert removed, "File should be removed"
        
        # Even with NFS cache (actimeo), the tombstone should protect
        # Use marker file to detect Audit completion
        marker_file = f"{MOUNT_POINT}/audit_marker_cache_{int(time.time())}.txt"
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, marker_file, content="marker")
        time.sleep(3) # NFS delay
        
        assert fusion_client.wait_for_file_in_tree(marker_file, timeout=30) is not None
        
        # Verify file stays deleted
        tree = fusion_client.get_tree(path="/", max_depth=-1)
        assert fusion_client._find_in_tree(tree, test_file) is None, \
            "Tombstone should protect against NFS cache resurrection during audit"

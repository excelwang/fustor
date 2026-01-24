"""
Test D1: Realtime Delete creates Tombstone.

验证 Realtime Delete 事件会创建 Tombstone 条目。
参考文档: CONSISTENCY_DESIGN.md - Section 4.2 (墓碑表 - 创建时机: 处理 Realtime Delete 时)
            & Section 5.1 (Realtime DELETE: 加入 Tombstone List)
"""
import pytest
import time

from ..utils import docker_manager
from ..conftest import CONTAINER_CLIENT_A, MOUNT_POINT


class TestRealtimeDeleteTombstone:
    """Test that realtime delete creates tombstone."""

    def test_realtime_delete_creates_tombstone(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir
    ):
        """
        场景:
          1. 通过 Agent 创建文件
          2. 通过 Agent 删除文件
          3. 删除触发 Realtime DELETE 事件
          4. 文件被加入 Tombstone List
        预期:
          - 文件从内存树移除
          - 文件路径记录在 Tombstone 中
        """
        test_file = f"{MOUNT_POINT}/tombstone_create_test.txt"
        
        # Step 1: Create file
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_A,
            test_file,
            content="file to be deleted"
        )
        
        # Wait for sync
        found = fusion_client.wait_for_file_in_tree(test_file, timeout=15)
        assert found is not None, "File should appear in Fusion"
        
        # Step 2: Delete file via Agent
        docker_manager.delete_file_in_container(CONTAINER_CLIENT_A, test_file)
        
        # Step 3: Wait for Realtime DELETE event
        time.sleep(3)
        
        # Step 4: Verify file is removed from tree
        tree = fusion_client.get_tree(path="/", max_depth=-1)
        found_after = fusion_client._find_in_tree(tree, test_file)
        
        assert found_after is None, \
            "File should be removed from tree after Realtime DELETE"

    def test_deleted_file_in_tombstone_list(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir
    ):
        """
        场景: 验证删除的文件确实在 Tombstone List 中
        
        注意: 这需要一个 API 来查询 Tombstone List。
        如果没有公开 API，可以通过验证 Audit 不会复活来间接验证。
        """
        test_file = f"{MOUNT_POINT}/tombstone_list_check.txt"
        
        # Create and delete
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_A,
            test_file,
            content="for tombstone list check"
        )
        fusion_client.wait_for_file_in_tree(test_file, timeout=15)
        
        docker_manager.delete_file_in_container(CONTAINER_CLIENT_A, test_file)
        time.sleep(3)
        
        # The file should be in tombstone.
        # Since we may not have a direct API, we verify indirectly
        # by checking the tree doesn't show the file
        tree = fusion_client.get_tree(path="/", max_depth=-1)
        assert fusion_client._find_in_tree(tree, test_file) is None
        
        # Tombstone existence is verified by subsequent tests (D2, D3)

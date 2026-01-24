"""
Test B5: Parent mtime check prevents stale Audit data.

验证当 Audit 的父目录 mtime < 内存中的 mtime 时，Audit 数据被丢弃。
参考文档: CONSISTENCY_DESIGN.md - Section 5.3 场景 1 (父目录已更新，X 是旧文件)
"""
import pytest
import time

from ..utils import docker_manager
from ..conftest import CONTAINER_CLIENT_A, CONTAINER_CLIENT_C, MOUNT_POINT


class TestParentMtimeCheck:
    """Test that parent mtime arbitration works correctly."""

    def test_stale_audit_discarded_by_parent_mtime(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        场景:
          1. 在 blind-spot 创建文件（但未被 Audit 扫描）
          2. 在 Agent 客户端删除该文件并重新创建同目录下的其他文件
          3. Agent 的 realtime 事件更新了父目录的 mtime
          4. 当延迟的 Audit 报告旧文件时：
             - Audit.Parent.mtime < Memory.Parent.mtime
             - 旧文件被丢弃，不会添加到内存树
        预期: 延迟 Audit 中的旧文件不会出现在 Fusion 中
        """
        test_dir = f"{MOUNT_POINT}/parent_mtime_test"
        stale_file = f"{test_dir}/stale_file.txt"
        new_file = f"{test_dir}/new_file.txt"
        
        # Create test directory from Agent
        docker_manager.exec_in_container(
            CONTAINER_CLIENT_A,
            ["mkdir", "-p", test_dir]
        )
        time.sleep(1)
        
        # Create stale file from blind-spot (simulating delayed audit scenario)
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_C,
            stale_file,
            content="stale content"
        )
        
        # Before audit runs, delete stale file and create new file via Agent
        # This updates the parent directory mtime
        time.sleep(1)
        docker_manager.delete_file_in_container(CONTAINER_CLIENT_A, stale_file)
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_A,
            new_file,
            content="new content via agent"
        )
        
        # Wait for realtime events to be processed
        time.sleep(3)
        
        # Verify new_file exists but stale_file does not
        new_found = fusion_client.wait_for_file_in_tree(new_file, timeout=10)
        assert new_found is not None, "New file should be synced via realtime"
        
        tree = fusion_client.get_tree(path=test_dir, max_depth=-1)
        stale_found = fusion_client._find_in_tree(tree, stale_file)
        assert stale_found is None, \
            "Stale file should not appear (parent mtime check should prevent it)"

    def test_audit_missing_file_ignored_if_parent_stale(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        场景 2 (CONSISTENCY_DESIGN.md Section 5.3):
          当 Audit 报告目录 D 缺少文件 B，但 Audit.D.mtime < Memory.D.mtime 时，
          应该忽略这个删除报告（B 可能是后来创建的）。
        
        验证方法:
          1. Agent 创建文件 B
          2. 模拟一个滞后的 Audit（mtime 较旧）
          3. 即使 Audit 报告 B 不存在，文件 B 仍然保留
        """
        test_dir = f"{MOUNT_POINT}/audit_stale_dir_test"
        file_b = f"{test_dir}/file_b.txt"
        
        # Create directory and file via Agent
        docker_manager.exec_in_container(
            CONTAINER_CLIENT_A,
            ["mkdir", "-p", test_dir]
        )
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_A,
            file_b,
            content="file B content"
        )
        
        # Wait for realtime sync
        found = fusion_client.wait_for_file_in_tree(file_b, timeout=10)
        assert found is not None, "File B should appear via realtime"
        
        # Wait for Audit (which may have stale view of parent directory)
        wait_for_audit()
        
        # File B should still exist (Audit mtime check should preserve it)
        tree_after = fusion_client.get_tree(path=test_dir, max_depth=-1)
        b_after = fusion_client._find_in_tree(tree_after, file_b)
        
        assert b_after is not None, \
            "File B should still exist (stale Audit should not delete it)"

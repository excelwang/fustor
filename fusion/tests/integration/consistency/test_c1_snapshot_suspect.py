"""
Test C1: Snapshot/Audit triggers Suspect marking for recent files.

验证 Snapshot/Audit 同步的新文件如果 mtime 距今 < 10 分钟，会被标记为 integrity_suspect。
参考文档: CONSISTENCY_DESIGN.md - Section 4.3 (可疑名单) & Section 5.2/5.3
"""
import pytest
import time

from ..utils import docker_manager
from ..conftest import CONTAINER_CLIENT_C, MOUNT_POINT


class TestSnapshotTriggersSuspect:
    """Test that audit marks recent files as suspect."""

    def test_recent_file_marked_as_suspect(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        场景:
          1. 无 Agent 客户端创建文件（mtime 是当前时间）
          2. Audit 发现该文件，发现其 mtime 距今 < 10 分钟
          3. 该文件被标记为 integrity_suspect: true
        预期:
          - 文件在 Audit 后，带有 integrity_suspect 标记
        """
        test_file = f"{MOUNT_POINT}/snapshot_suspect_test_{int(time.time()*1000)}.txt"
        
        # Create file from blind-spot
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_C,
            test_file,
            content="recent file for suspect test"
        )
        
        # Wait for Audit to discover and mark as suspect
        wait_for_audit()
        
        # File should appear
        found = fusion_client.wait_for_file_in_tree(test_file, timeout=10)
        assert found is not None, "File should be discovered by Audit"
        
        # Check integrity_suspect flag
        flags = fusion_client.check_file_flags(test_file)
        assert flags["integrity_suspect"] is True, \
            "Recent file (mtime < 10 min) should be marked as integrity_suspect"

    def test_suspect_list_contains_recent_file(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        场景: 刚创建的文件应出现在 Suspect List 中
        """
        test_file = f"{MOUNT_POINT}/suspect_list_test_{int(time.time()*1000)}.txt"
        
        # Create file from blind-spot
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_C,
            test_file,
            content="file for suspect list"
        )
        
        # Wait for Audit
        wait_for_audit()
        fusion_client.wait_for_file_in_tree(test_file, timeout=10)
        
        # Get suspect list
        suspect_list = fusion_client.get_suspect_list()
        
        # File should be in the list
        paths_in_list = [item.get("path") for item in suspect_list]
        assert test_file in paths_in_list, \
            f"File {test_file} should be in suspect list"

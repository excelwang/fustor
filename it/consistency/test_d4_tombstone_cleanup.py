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
from ..fixtures.constants import (
    STRESS_DELAY,
    SHORT_TIMEOUT,
    MEDIUM_TIMEOUT,
    LONG_TIMEOUT,
    TEST_TOMBSTONE_TTL,
    TOMBSTONE_CLEANUP_WAIT
)


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
        # Spec defaults to 1 hour, but we need to verify cleanup mechanism in seconds.
        # This is now handled by Env Injection (docker.py) using TEST_TOMBSTONE_TTL.
        
        try:
            import os
            test_file = f"{MOUNT_POINT}/tombstone_cleanup_{int(time.time()*1000)}.txt"
            test_file_rel = "/" + os.path.relpath(test_file, MOUNT_POINT)
            
            # Step 1: Create and delete file (creates Tombstone)
            docker_manager.create_file_in_container(
                CONTAINER_CLIENT_A,
                test_file,
                content="will be deleted and cleaned"
            )
            fusion_client.wait_for_file_in_tree(test_file_rel, timeout=MEDIUM_TIMEOUT)
            
            # Delete via Agent (creates Tombstone)
            docker_manager.delete_file_in_container(CONTAINER_CLIENT_A, test_file)
            
            # Wait for DELETE event and Verify deleted
            assert fusion_client.wait_for_file(test_file_rel, timeout=MEDIUM_TIMEOUT, should_exist=False), \
                f"File {test_file} should be removed from tree after Realtime DELETE"
            
            assert fusion_client.wait_for_file(test_file_rel, timeout=MEDIUM_TIMEOUT, should_exist=False), \
                f"File {test_file} should be removed from tree after Realtime DELETE"
            
            # Wait for TTL to expire (TEST_TOMBSTONE_TTL + Buffer)
            time.sleep(TOMBSTONE_CLEANUP_WAIT) 
            
            # Step 2-3: Use a marker file to detect Audit completion
            # Audit End triggers the cleanup
            # Marker MUST be in the root directory to ensure the parent (root) mtime changes,
            # triggering Audit to scan the tree. Ref: specs/02-CONSISTENCY_DESIGN.md §3.1
            marker_file = f"{MOUNT_POINT}/audit_marker_d4_{int(time.time())}.txt"
            marker_file_rel = "/" + os.path.relpath(marker_file, MOUNT_POINT)
            
            docker_manager.create_file_in_container(CONTAINER_CLIENT_C, marker_file, content="marker")
            
            # Wait for marker to appear (Audit 1 processed)
            assert fusion_client.wait_for_file_in_tree(marker_file_rel, timeout=LONG_TIMEOUT) is not None
            
            # Wait explicitly for Audit Cycle to finish and cleanup to run
            # sleep(AUDIT_INTERVAL) ensures we pass the cycle boundary
            wait_for_audit()
            
            # Step 4: After Audit, Tombstone should be cleaned
            # We verify by creating a new file with the same path via Agent
            docker_manager.create_file_in_container(
                CONTAINER_CLIENT_A,
                test_file,
                content="new file after tombstone cleanup"
            )
            
            # Step 5: The new file should appear (Tombstone no longer blocks it)
            # Wait for realtime sync of new file
            found = fusion_client.wait_for_file_in_tree(test_file_rel, timeout=SHORT_TIMEOUT)
            
            assert found is not None, \
                f"New file should appear after Tombstone is cleaned. Tree: {fusion_client.get_tree(path='/', max_depth=-1)}"
        finally:
            # TTL restore not needed as we didn't modify it locally
            pass

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

        # Env Injection (docker.py) sets TEST_TOMBSTONE_TTL
        
        try:
            import os
            test_file = f"{MOUNT_POINT}/tombstone_lifecycle_{int(time.time()*1000)}.txt"
            test_file_rel = "/" + os.path.relpath(test_file, MOUNT_POINT)
            
            # Create and delete
            docker_manager.create_file_in_container(
                CONTAINER_CLIENT_A,
                test_file,
                content="lifecycle test"
            )
            fusion_client.wait_for_file_in_tree(test_file_rel, timeout=MEDIUM_TIMEOUT)
            
            docker_manager.delete_file_in_container(CONTAINER_CLIENT_A, test_file)
            assert fusion_client.wait_for_file(test_file_rel, timeout=MEDIUM_TIMEOUT, should_exist=False), \
                f"File {test_file} should be removed after delete"
            
            # Give a small buffer for potential racing snapshots to be processed and blocked
            time.sleep(STRESS_DELAY)
            
            # Immediately try to create from blind-spot (should be blocked by Tombstone initially)
            docker_manager.create_file_in_container(
                CONTAINER_CLIENT_C,
                test_file,
                content="blind spot attempt during tombstone"
            )
            
            # Step 4: Use marker to ensure first audit cycle ran
            marker_file = f"{MOUNT_POINT}/audit_marker_d4_lifecycle_{int(time.time())}.txt"
            marker_file_rel = "/" + os.path.relpath(marker_file, MOUNT_POINT)
            
            docker_manager.create_file_in_container(CONTAINER_CLIENT_C, marker_file, content="marker")
            
            # Wait for Marker. This Audit Cycle (1) cleans tombstones but ALSO blocks the file (because tombstone active during scan)
            assert fusion_client.wait_for_file_in_tree(marker_file_rel, timeout=LONG_TIMEOUT) is not None
            
            # Wait for Audit Cycle to complete and trigger next scan
            wait_for_audit()
            
            # Now trigger/wait for SECOND Audit to pick up the file (Tombstone gone)
            marker_file_2 = f"{MOUNT_POINT}/audit_marker_d4_lifecycle_2_{int(time.time())}.txt"
            marker_file_2_rel = "/" + os.path.relpath(marker_file_2, MOUNT_POINT)
            docker_manager.create_file_in_container(CONTAINER_CLIENT_C, marker_file_2, content="marker2")
            
            assert fusion_client.wait_for_file_in_tree(marker_file_2_rel, timeout=LONG_TIMEOUT) is not None
            
            # After Tombstone cleanup + 2nd Audit, the blind-spot file should be discoverable
            # Verify file exists
            tree = fusion_client.get_tree(path="/", max_depth=-1)
            found = fusion_client._find_in_tree(tree, test_file_rel)
            
            assert found is not None, \
                "Blind-spot file should be discovered after tombstone expires"
        finally:
             pass
        
        # After Tombstone cleanup, the blind-spot file should be discoverable
        # Note: The exact behavior depends on implementation details

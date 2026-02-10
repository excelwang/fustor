"""
Test D4: Tombstones are cleaned after Audit-End.

验证在 Audit-End 信号后，旧的 Tombstone 条目被清理。
参考文档: CONSISTENCY_DESIGN.md - Section 4.2 (销毁: 收到 Audit-End 信号后，清理所有在此次 Audit 开始前创建的 Tombstone)
            & Section 6 (审计生命周期)

注意: NFS 环境下，inotify 只能检测到本地 Client 上的文件 I/O。
      Client C 上的文件操作不会被 Client A/B 的 source-fs inotify 监听到。
      因此所有文件操作使用 Client A（有 inotify），靠 inotify 实现实时检测。
"""
import pytest
import time

from ..utils import docker_manager
from ..conftest import CONTAINER_CLIENT_A, MOUNT_POINT
from ..fixtures.constants import (
    SHORT_TIMEOUT,
    MEDIUM_TIMEOUT,
    LONG_TIMEOUT,
    TOMBSTONE_CLEANUP_WAIT,
    AUDIT_INTERVAL,
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
          1. 在 Client A 创建文件，等待出现在 Tree 中
          2. 在 Client A 删除文件（通过 inotify 实时检测 → 创建 Tombstone）
          3. 等待 Tombstone TTL 过期
          4. 等待 Audit 完成（Audit-End 清理过期 Tombstone）
          5. 重新创建同路径文件（通过 inotify 实时检测）
          6. 验证文件可以正常出现（Tombstone 不再阻挡）
        预期:
          - Audit 完成后，旧 Tombstone 被清理
          - 相同路径的新文件可以被正常同步
        """
        import os
        test_file = f"{MOUNT_POINT}/tombstone_cleanup_{int(time.time()*1000)}.txt"
        test_file_rel = "/" + os.path.relpath(test_file, MOUNT_POINT)

        # Step 1: Create file on Client A (inotify detects → realtime event)
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_A,
            test_file,
            content="will be deleted and cleaned"
        )
        assert fusion_client.wait_for_file_in_tree(test_file_rel, timeout=MEDIUM_TIMEOUT), \
            f"File {test_file_rel} should appear in tree after creation on Client A"

        # Step 2: Delete file on Client A (inotify detects → realtime DELETE → Tombstone created)
        docker_manager.delete_file_in_container(CONTAINER_CLIENT_A, test_file)

        assert fusion_client.wait_for_file_not_in_tree(test_file_rel, timeout=MEDIUM_TIMEOUT), \
            f"File {test_file_rel} should be removed from tree after delete"

        # Step 3: Wait for Tombstone TTL to expire
        time.sleep(TOMBSTONE_CLEANUP_WAIT)

        # Step 4: Wait for Audit cycle to complete (Audit-End triggers Tombstone cleanup)
        # Use a marker file on Client A (inotify detects it quickly)
        marker_file = f"{MOUNT_POINT}/audit_marker_d4_{int(time.time())}.txt"
        marker_file_rel = "/" + os.path.relpath(marker_file, MOUNT_POINT)

        docker_manager.create_file_in_container(CONTAINER_CLIENT_A, marker_file, content="marker")
        assert fusion_client.wait_for_file_in_tree(marker_file_rel, timeout=LONG_TIMEOUT) is not None, \
            f"Audit marker {marker_file_rel} should appear in tree"

        # Wait for an Audit cycle to ensure Audit-End runs and cleans the Tombstone
        wait_for_audit()

        # Step 5: Recreate file with the same path on Client A
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_A,
            test_file,
            content="new file after tombstone cleanup"
        )

        # Step 6: Verify the new file appears (Tombstone no longer blocks it)
        # Use LONG_TIMEOUT to account for potential audit cycle timing
        found = fusion_client.wait_for_file_in_tree(test_file_rel, timeout=LONG_TIMEOUT)

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
        场景: 验证 Tombstone 在活跃期间阻止文件复活，Audit-End 清理后允许新创建。

        流程:
          1. 在 Client A 创建文件 → inotify → 出现在 Tree
          2. 在 Client A 删除文件 → inotify → Tombstone 创建
          3. 等待 Tombstone TTL 过期 + 至少两个 Audit 周期
          4. 重新创建文件 → Tombstone 已被清理 → 文件出现
        """
        import os
        test_file = f"{MOUNT_POINT}/tombstone_lifecycle_{int(time.time()*1000)}.txt"
        test_file_rel = "/" + os.path.relpath(test_file, MOUNT_POINT)

        # Step 1: Create file on Client A
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_A,
            test_file,
            content="lifecycle test"
        )
        assert fusion_client.wait_for_file_in_tree(test_file_rel, timeout=MEDIUM_TIMEOUT), \
            f"File {test_file_rel} should appear in tree"

        # Step 2: Delete file on Client A → Tombstone created
        docker_manager.delete_file_in_container(CONTAINER_CLIENT_A, test_file)
        assert fusion_client.wait_for_file_not_in_tree(test_file_rel, timeout=MEDIUM_TIMEOUT), \
            f"File {test_file_rel} should be removed after delete"

        # Step 3: Wait for Tombstone TTL to expire
        time.sleep(TOMBSTONE_CLEANUP_WAIT)

        # Step 4: Use marker to trigger and detect Audit cycle completion
        marker_file = f"{MOUNT_POINT}/audit_marker_d4_lifecycle_{int(time.time())}.txt"
        marker_file_rel = "/" + os.path.relpath(marker_file, MOUNT_POINT)

        docker_manager.create_file_in_container(CONTAINER_CLIENT_A, marker_file, content="marker")
        assert fusion_client.wait_for_file_in_tree(marker_file_rel, timeout=LONG_TIMEOUT) is not None, \
            f"Audit marker {marker_file_rel} should appear"

        # Wait for Audit Cycle to complete (ensures Audit-End cleans the Tombstone)
        wait_for_audit()

        # Step 5: Trigger second Audit to confirm cleanup
        marker_file_2 = f"{MOUNT_POINT}/audit_marker_d4_lifecycle_2_{int(time.time())}.txt"
        marker_file_2_rel = "/" + os.path.relpath(marker_file_2, MOUNT_POINT)
        docker_manager.create_file_in_container(CONTAINER_CLIENT_A, marker_file_2, content="marker2")

        assert fusion_client.wait_for_file_in_tree(marker_file_2_rel, timeout=LONG_TIMEOUT) is not None, \
            f"Second audit marker {marker_file_2_rel} should appear"

        # Step 6: Recreate the file on Client A after Tombstone cleanup
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_A,
            test_file,
            content="new lifecycle file after cleanup"
        )

        # Step 7: Verify file appears (Tombstone has been cleaned by Audit-End)
        found = fusion_client.wait_for_file_in_tree(test_file_rel, timeout=LONG_TIMEOUT)

        assert found is not None, \
            f"File should be discoverable after tombstone expires. Tree: {fusion_client.get_tree(path='/', max_depth=-1)}"

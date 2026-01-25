"""
Test B3: Blind-spot file modification detected by Audit.

验证无 Agent 客户端修改文件时，Audit 通过 mtime 仲裁更新内存树。
参考文档: CONSISTENCY_DESIGN.md - Section 5.3 场景 1 (Audit 报告存在文件 X)
"""
import pytest
import time

from ..utils import docker_manager
from ..conftest import CONTAINER_CLIENT_A, CONTAINER_CLIENT_C, MOUNT_POINT


class TestBlindSpotFileModification:
    """Test detection of file modifications by client without agent."""

    def test_blind_spot_modification_updates_mtime(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """场景: 盲区修改更新 mtime"""
        test_file = f"{MOUNT_POINT}/blind_modify_test_{int(time.time()*1000)}.txt"
        
        # Step 1: Create file from agent client
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_A,
            test_file,
            content="original content"
        )
        
        # Wait for realtime sync
        found = fusion_client.wait_for_file_in_tree(test_file, timeout=10)
        assert found is not None, "File should appear via realtime event"
        
        # Record original mtime from Fusion
        original_mtime = found.get("mtime")
        
        # Step 2: Wait a bit, then modify file from blind-spot client
        time.sleep(2)
        docker_manager.modify_file_in_container(
            CONTAINER_CLIENT_C,
            test_file,
            append_content="modified from blind spot"
        )
        
        # Get new mtime from filesystem
        new_fs_mtime = docker_manager.get_file_mtime(CONTAINER_CLIENT_C, test_file)
        assert new_fs_mtime > original_mtime, "Filesystem mtime should increase after modification"
        
        # Step 3: Before Audit, Fusion mtime should be unchanged
        tree = fusion_client.get_tree(path=test_file, max_depth=0)
        mtime_before_audit = tree.get("mtime")
        assert mtime_before_audit == original_mtime, \
            "Fusion mtime should be unchanged before Audit"
        
        # Step 4: Use a marker file to detect Audit completion
        marker_file = f"{MOUNT_POINT}/audit_marker_b3_{int(time.time()*1000)}.txt"
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, marker_file, content="marker")
        time.sleep(3) # NFS cache
        assert fusion_client.wait_for_file_in_tree(marker_file, timeout=30) is not None
        
        # Step 5: After Audit, Fusion mtime should be updated
        # Poll for mtime update
        start = time.time()
        success = False
        mtime_after_audit = 0
        while time.time() - start < 15:
            tree_after = fusion_client.get_tree(path=test_file, max_depth=0)
            mtime_after_audit = tree_after.get("mtime")
            if abs(mtime_after_audit - new_fs_mtime) < 0.001:
                success = True
                break
            time.sleep(1)
        
        assert success, \
            f"Fusion mtime should match filesystem mtime {new_fs_mtime} after Audit. Got {mtime_after_audit}"

    def test_blind_spot_modification_marks_agent_missing(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """场景: 盲区修改的文件标记为 agent_missing"""
        test_file = f"{MOUNT_POINT}/blind_modify_flag_test_{int(time.time()*1000)}.txt"
        
        # Create from agent, modify from blind-spot
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_A,
            test_file,
            content="original"
        )
        fusion_client.wait_for_file_in_tree(test_file, timeout=20)
        
        # Initial file should not have agent_missing
        flags_initial = fusion_client.check_file_flags(test_file)
        assert flags_initial["agent_missing"] is False
        
        # Modify from blind-spot
        time.sleep(2)
        docker_manager.modify_file_in_container(
            CONTAINER_CLIENT_C,
            test_file,
            append_content="blind modification"
        )
        
        # Use marker file to detect Audit completion
        marker_file = f"{MOUNT_POINT}/audit_marker_b3_flag_{int(time.time()*1000)}.txt"
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, marker_file, content="marker")
        time.sleep(3) # NFS cache
        assert fusion_client.wait_for_file_in_tree(marker_file, timeout=30) is not None
        
        # Check agent_missing flag after modification
        assert fusion_client.wait_for_flag(test_file, "agent_missing", True, timeout=15), \
            "agent_missing flag should be set after blind modification"

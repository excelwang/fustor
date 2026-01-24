"""
Test B1: Blind-spot file creation detected by Audit.

验证无 Agent 客户端创建的文件通过 Audit 被发现，并标记为 agent_missing。
参考文档: CONSISTENCY_DESIGN.md - Section 4.4 (盲区名单) & Section 5.3 (Audit 消息处理)
"""
import pytest
import time

from ..utils import docker_manager
from ..conftest import CONTAINER_CLIENT_A, CONTAINER_CLIENT_C, MOUNT_POINT


class TestBlindSpotFileCreation:
    """Test detection of files created by client without agent."""

    def test_blind_spot_file_discovered_by_audit(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        场景: 无 Agent 的客户端 C 创建文件
        预期:
          1. 文件在初始时不会出现在 Fusion 中（无实时事件）
          2. Audit 扫描后，文件被发现并添加到内存树
          3. 文件被标记为 agent_missing: true
        """
        test_file = f"{MOUNT_POINT}/blind_spot_created.txt"
        
        # Step 0: Wait for initial agent sync to be definitely complete
        # We use a synchronization marker
        sync_marker = f"{MOUNT_POINT}/sync_marker_{int(time.time())}.txt"
        docker_manager.create_file_in_container(CONTAINER_CLIENT_A, sync_marker, content="ready")
        assert fusion_client.wait_for_file_in_tree(sync_marker, timeout=30) is not None
        
        # Step 1: Create file on client without agent
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_C,
            test_file,
            content="created from blind spot"
        )
        
        # Step 2: Immediately after creation, file should NOT be in Fusion
        time.sleep(2)
        tree = fusion_client.get_tree(path="/", max_depth=-1)
        found_immediately = fusion_client._find_in_tree(tree, test_file)
        assert found_immediately is None, \
            "File should NOT appear immediately (no realtime event from blind-spot client)"
        
        # Step 3 & 4: Use a marker file to detect Audit completion
        marker_file = f"{MOUNT_POINT}/audit_marker_b1_{int(time.time())}.txt"
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, marker_file, content="marker")
        time.sleep(7) # NFS cache delay
        
        # Wait for marker to appear in Fusion (at least one audit cycle completed)
        assert fusion_client.wait_for_file_in_tree(marker_file, timeout=120) is not None
        
        # Now check if the original blind-spot file was discovered
        found_after_audit = fusion_client.wait_for_file_in_tree(test_file, timeout=10)
        assert found_after_audit is not None, \
            f"File {test_file} should be discovered by the Audit scan"
        
        # Step 5: Verify agent_missing flag is set
        assert fusion_client.wait_for_flag(test_file, "agent_missing", True, timeout=10), \
            f"Blind-spot file {test_file} should be marked with agent_missing: true. Tree node: {found_after_audit}"

    def test_blind_spot_file_added_to_blind_spot_list(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        场景: 无 Agent 客户端创建的文件
        预期: 文件被添加到 Blind-spot List
        """
        test_file = f"{MOUNT_POINT}/blind_list_test.txt"
        
        # Wait for initial sync
        sync_marker = f"{MOUNT_POINT}/sync_marker_list_{int(time.time())}.txt"
        docker_manager.create_file_in_container(CONTAINER_CLIENT_A, sync_marker, content="ready")
        assert fusion_client.wait_for_file_in_tree(sync_marker, timeout=30) is not None
        
        # Create file from blind-spot client
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_C,
            test_file,
            content="blind list test"
        )
        
        # Use marker file to detect Audit completion
        marker_file = f"{MOUNT_POINT}/audit_marker_b1_list_{int(time.time())}.txt"
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, marker_file, content="marker")
        time.sleep(7) # NFS cache delay
        assert fusion_client.wait_for_file_in_tree(marker_file, timeout=120) is not None
        
        # Check blind-spot list for file (poll to be safe)
        start = time.time()
        found = False
        while time.time() - start < 15:
            blind_spot_list = fusion_client.get_blind_spot_list()
            paths_in_list = [item.get("path") for item in blind_spot_list if item.get("type") == "file"]
            if test_file in paths_in_list:
                found = True
                break
            time.sleep(1)
            
        assert found, f"File {test_file} should be in blind-spot list. List: {blind_spot_list}"

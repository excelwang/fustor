"""
Test B1: Blind-spot file creation detected by Audit.

验证无 Agent 客户端创建的文件通过 Audit 被发现，并标记为 agent_missing。
参考文档: CONSISTENCY_DESIGN.md - Section 4.4 (盲区名单) & Section 5.3 (Audit 消息处理)
"""
import pytest
import time

from ..utils import docker_manager
from ..conftest import CONTAINER_CLIENT_C, MOUNT_POINT


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
        
        # Step 3: Wait for Audit cycle to complete
        wait_for_audit()
        
        # Step 4: After Audit, file should be discovered
        found_after_audit = fusion_client.wait_for_file_in_tree(
            file_path=test_file,
            timeout=10
        )
        assert found_after_audit is not None, \
            "File should be discovered by Audit scan"
        
        # Step 5: Verify agent_missing flag is set
        flags = fusion_client.check_file_flags(test_file)
        assert flags["agent_missing"] is True, \
            "Blind-spot file should be marked with agent_missing: true"

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
        
        # Create file from blind-spot client
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_C,
            test_file,
            content="blind list test"
        )
        
        # Wait for Audit
        wait_for_audit()
        
        # Get blind-spot list
        blind_spot_list = fusion_client.get_blind_spot_list()
        
        # File should be in the list
        paths_in_list = [item.get("path") for item in blind_spot_list if item.get("type") == "file"]
        assert test_file in paths_in_list, \
            f"File {test_file} should be in blind-spot list"

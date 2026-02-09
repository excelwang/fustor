"""
Test B1: Blind-spot file creation detected by Audit.

验证无 Agent 客户端创建的文件通过 Audit 被发现，并标记为 agent_missing。
参考文档: CONSISTENCY_DESIGN.md - Section 4.4 (盲区名单) & Section 5.3 (Audit 消息处理)
"""
import pytest
import time

import os
from ..utils import docker_manager
from ..conftest import CONTAINER_CLIENT_A, CONTAINER_CLIENT_C, MOUNT_POINT
from ..fixtures.constants import (
    INGESTION_DELAY,
    SHORT_TIMEOUT,
    MEDIUM_TIMEOUT,
    EXTREME_TIMEOUT,
    POLL_INTERVAL
)


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
        """场景: 盲区发现的新文件"""
        test_file = f"{MOUNT_POINT}/blind_spot_created_{int(time.time()*1000)}.txt"
        
        # Step 1: Create file on client without agent
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_C,
            test_file,
            content="created from blind spot"
        )
        
        # Step 2: Check if file appeared
        time.sleep(INGESTION_DELAY)
        # Check presence first
        try:
            test_file_rel = os.path.relpath(test_file, MOUNT_POINT)
            tree = fusion_client.get_tree(path=test_file_rel, max_depth=0)
            found_immediately = tree.get("path") == test_file_rel
        except Exception as e:
            # If 404 or other error, it means not found immediately
            print(f"DEBUG: File not found immediately: {e}")
            found_immediately = False
        
        if found_immediately:
            # Re-fetch flags to ensure currency
            print(f"DEBUG: File found immediately. Waiting for agent_missing=True...")
            assert fusion_client.wait_for_flag(test_file_rel, "agent_missing", True, timeout=SHORT_TIMEOUT), \
                f"If found immediately, it must be blind spot (agent_missing=True) eventually."
        
        # Wait for Audit completion
        wait_for_audit()
        
        # Now check if the original blind-spot file was discovered
        test_file_rel = os.path.relpath(test_file, MOUNT_POINT)
        found_after_audit = fusion_client.wait_for_file_in_tree(test_file_rel, timeout=SHORT_TIMEOUT)
        assert found_after_audit is not None, \
            f"File {test_file} should be discovered by the Audit scan"
        
        # Step 5: Verify agent_missing flag is set
        assert fusion_client.wait_for_flag(test_file_rel, "agent_missing", True, timeout=SHORT_TIMEOUT), \
            f"Blind-spot file {test_file} should be marked with agent_missing: true. Tree node: {found_after_audit}"

    def test_blind_spot_file_added_to_blind_spot_list(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """场景: 盲区发现的文件加入列表"""
        test_file = f"{MOUNT_POINT}/blind_list_test_{int(time.time()*1000)}.txt"
        
        # Create file from blind-spot client
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_C,
            test_file,
            content="blind list test"
        )
        
        # Wait for Audit completion
        wait_for_audit()
        
        # Check blind-spot list for file (poll to be safe)
        start = time.time()
        found = False
        test_file_rel = os.path.relpath(test_file, MOUNT_POINT)
        while time.time() - start < MEDIUM_TIMEOUT:
            blind_spot_list = fusion_client.get_blind_spot_list()
            paths_in_list = [item.get("path") for item in blind_spot_list if item.get("type") == "file"]
            if test_file_rel in paths_in_list:
                found = True
                break
            time.sleep(POLL_INTERVAL)
            
        assert found, f"File {test_file} should be in blind-spot list. List: {blind_spot_list}"

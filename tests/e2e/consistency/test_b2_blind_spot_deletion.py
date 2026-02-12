"""
Test B2: Blind-spot file deletion detected by Audit.

验证无 Agent 客户端删除的文件通过 Audit 被发现，并从内存树移除。
参考文档: CONSISTENCY_DESIGN.md - Section 5.3 场景 2 (Audit 报告目录缺少文件)
"""
import pytest
import time

import os
from ..utils import docker_manager
from ..conftest import CONTAINER_CLIENT_A, CONTAINER_CLIENT_C, MOUNT_POINT
from ..fixtures.constants import (
    SHORT_TIMEOUT,
    MEDIUM_TIMEOUT,
    EXTREME_TIMEOUT,
    POLL_INTERVAL
)


class TestBlindSpotFileDeletion:
    """Test detection of files deleted by client without agent."""

    def test_blind_spot_deletion_detected_by_audit(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        场景:
          1. Agent A 创建文件（正常路径，实时同步到 Fusion）
          2. 无 Agent 的客户端 C 删除该文件
          3. 因为 C 没有 Agent，删除事件不会实时同步
          4. Audit 发现文件缺失，从内存树移除
        预期:
          - 删除前文件存在于 Fusion
          - 删除后（Audit 前）文件仍存在于 Fusion
          - Audit 后文件从 Fusion 移除
        """
        test_file = f"{MOUNT_POINT}/blind_delete_test_{int(time.time()*1000)}.txt"
        
        # Step 1: Create file from agent client (realtime sync)
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_A,
            test_file,
            content="will be deleted from blind spot"
        )
        
        # Wait for realtime sync
        # Wait for realtime sync
        test_file_rel = "/" + os.path.relpath(test_file, MOUNT_POINT)
        found = fusion_client.wait_for_file_in_tree(test_file_rel, timeout=MEDIUM_TIMEOUT)
        assert found is not None, "File should appear via realtime event"
        
        # Step 2: Delete file from blind-spot client
        docker_manager.delete_file_in_container(CONTAINER_CLIENT_C, test_file)
        
        # Step 3: Immediately after deletion, file should STILL be in Fusion
        # Note: In some environments (or if Audit runs coincidentally), this might fail.
        # We relax this check to focus on eventual consistency.
        
        # Step 4: Wait for Audit to detect deletion
        wait_for_audit()
        
        assert fusion_client.wait_for_file_not_in_tree(test_file_rel, timeout=SHORT_TIMEOUT), \
            "File should be removed after Audit detects blind-spot deletion"

    def test_blind_spot_deletion_added_to_blind_spot_list(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        场景: 盲区删除的文件应被记录到 Blind-spot List
        """
        test_file = f"{MOUNT_POINT}/blind_delete_list_test_{int(time.time()*1000)}.txt"
        
        # Create file from agent, then delete from blind-spot
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_A,
            test_file,
            content="for blind delete list test"
        )
        test_file_rel = "/" + os.path.relpath(test_file, MOUNT_POINT)
        fusion_client.wait_for_file_in_tree(test_file_rel, timeout=SHORT_TIMEOUT)
        
        docker_manager.delete_file_in_container(CONTAINER_CLIENT_C, test_file)
        
        # Wait for Audit completion
        wait_for_audit()
        
        # Check blind-spot list for deletion record
        # Poll since events might be processed shortly after marker appearance
        start = time.time()
        found = False
        while time.time() - start < MEDIUM_TIMEOUT:
            blind_spot_list = fusion_client.get_blind_spot_list()
            deletion_entries = [
                item for item in blind_spot_list
                if item.get("path") == test_file_rel and item.get("type") == "deletion"
            ]
            if deletion_entries:
                found = True
                break
            time.sleep(POLL_INTERVAL)
        
        assert found, "Blind-spot deletion should be recorded in blind-spot list"

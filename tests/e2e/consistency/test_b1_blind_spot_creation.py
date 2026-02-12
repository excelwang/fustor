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
        import datetime
        print(f"\n[DEBUG] TEST BODY START at {datetime.datetime.utcnow().isoformat()} UTC, time.time()={time.time()}")
        
        # Use a subdirectory to ensure directory mtime change is isolated and noticeable
        test_dir = f"{MOUNT_POINT}/blind_subdir_{int(time.time())}"
        docker_manager.exec_in_container(CONTAINER_CLIENT_C, ["mkdir", "-p", test_dir])
        
        test_file = f"{test_dir}/blind_spot_created_{int(time.time()*1000)}.txt"
        print(f"[DEBUG] Creating blind-spot file: {test_file}")
        test_file_rel = "/" + os.path.relpath(test_file, MOUNT_POINT)
        
        # Step 1: Create file on client without agent
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_C,
            test_file,
            content="created from blind spot"
        )
        print(f"[DEBUG] File created on Client C at {datetime.datetime.utcnow().isoformat()} UTC")
        
        # Verify file exists on Client C
        check_result = docker_manager.exec_in_container(CONTAINER_CLIENT_C, ["ls", "-la", test_file])
        print(f"[DEBUG] File on Client C: {check_result}")
        
        # Verify file exists on Agent A's mount (check NFS propagation)
        print(f"[DEBUG] Waiting for file to be visible on Agent A (NFS propagation)...")
        visible_on_a = False
        for _ in range(20): # ~10s wait
            check_a = docker_manager.exec_in_container(CONTAINER_CLIENT_A, ["ls", "-la", test_file])
            if check_a.returncode == 0:
                visible_on_a = True
                print(f"[DEBUG] File visible on Agent A: {check_a.stdout.strip()}")
                break
            time.sleep(0.5)
        
        assert visible_on_a, f"File {test_file} did not become visible on Agent A within timeout"
        
        # Step 1.1: Create a trigger file to ensure directory mtime change is noticed
        # This prevents Smart Audit from skipping the scan if it already scanned the dir recently.
        trigger_file = f"{MOUNT_POINT}/audit_trigger_{int(time.time()*1000)}.txt"
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, trigger_file, "trigger")
        print(f"[DEBUG] Trigger file created at {datetime.datetime.utcnow().isoformat()} UTC")
        
        # IMPORTANT: Wait for Agent A to see the changes via NFS actimeo
        print(f"[DEBUG] Waiting {INGESTION_DELAY}s for NFS propagation to Agent A...")
        time.sleep(INGESTION_DELAY)

        # Check dir mtime from Agent A's perspective
        try:
            dir_stat = docker_manager.exec_in_container(CONTAINER_CLIENT_A, ["stat", MOUNT_POINT])
            print(f"[DEBUG] Agent A dir stat: {dir_stat.stdout}")
            file_stat = docker_manager.exec_in_container(CONTAINER_CLIENT_A, ["ls", "-la", test_file])
            print(f"[DEBUG] Agent A file check: {file_stat.stdout}")
        except Exception as e:
            print(f"[DEBUG] Agent A check failed: {e}")
        
        # Debug: check audit stats before waiting
        try:
            stats = fusion_client.get_stats()
            print(f"[DEBUG] Stats before audit wait: audit_cycle_count={stats.get('audit_cycle_count')}, is_auditing={stats.get('is_auditing')}")
        except Exception as e:
            print(f"[DEBUG] get_stats failed: {e}")

        # Step 2: Wait for Audit completion
        wait_for_audit()
        
        # Now check if the original blind-spot file was discovered
        found_after_audit = fusion_client.wait_for_file_in_tree(test_file_rel, timeout=SHORT_TIMEOUT)
        
        if not found_after_audit:
             # Debug: dump the tree
             try:
                 tree_dump = fusion_client.get_tree(path="/", max_depth=2)
                 print(f"DEBUG TREE DUMP: {tree_dump}")
             except Exception as e:
                 print(f"DEBUG TREE DUMP FAILED: {e}")

        assert found_after_audit is not None, \
            f"File {test_file_rel} should be discovered by the Audit scan"
        
        # Step 5: Verify agent_missing flag is set
        assert fusion_client.wait_for_flag(test_file_rel, "agent_missing", True, timeout=SHORT_TIMEOUT), \
            f"Blind-spot file {test_file_rel} should be marked with agent_missing: true. Tree node: {found_after_audit}"

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
        test_file_rel = "/" + os.path.relpath(test_file, MOUNT_POINT)
        while time.time() - start < MEDIUM_TIMEOUT:
            blind_spot_list = fusion_client.get_blind_spot_list()
            paths_in_list = [item.get("path") for item in blind_spot_list if item.get("type") == "file"]
            if test_file_rel in paths_in_list:
                found = True
                break
            time.sleep(POLL_INTERVAL)
            
        assert found, f"File {test_file_rel} should be in blind-spot list. List: {blind_spot_list}"

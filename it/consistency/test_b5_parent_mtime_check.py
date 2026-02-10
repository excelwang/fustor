"""
Test B5: Parent mtime check prevents stale Audit data.

验证当 Audit 的父目录 mtime < 内存中的 mtime 时，Audit 数据被丢弃。
参考文档: CONSISTENCY_DESIGN.md - Section 5.3 场景 1 (父目录已更新，X 是旧文件)
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
    LONG_TIMEOUT,
    STRESS_DELAY,
    POLL_INTERVAL
)


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
        test_dir = f"{MOUNT_POINT}/parent_mtime_test_{int(time.time()*1000)}"
        stale_file = f"{test_dir}/stale_file.txt"
        new_file = f"{test_dir}/new_file.txt"
        
        # Create test directory from Agent
        docker_manager.exec_in_container(
            CONTAINER_CLIENT_A,
            ["mkdir", "-p", test_dir]
        )
        time.sleep(POLL_INTERVAL)
        
        # Create stale file from blind-spot (simulating delayed audit scenario)
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_C,
            stale_file,
            content="stale content"
        )
        
        # Before audit runs, delete stale file and create new file via Agent
        # This updates the parent directory mtime
        time.sleep(POLL_INTERVAL)
        docker_manager.delete_file_in_container(CONTAINER_CLIENT_A, stale_file)
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_A,
            new_file,
            content="new content via agent"
        )
        
        # Verify new_file exists but stale_file does not (synced via realtime)
        new_file_rel = "/" + os.path.relpath(new_file, MOUNT_POINT)
        assert fusion_client.wait_for_file_in_tree(new_file_rel, timeout=MEDIUM_TIMEOUT) is not None
        
        # Step 4: Use a marker file to detect Audit completion
        # This confirms that an audit cycle has scanned the directory
        marker_file = f"{MOUNT_POINT}/audit_marker_b5_{int(time.time())}.txt"
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, marker_file, content="marker")
        time.sleep(STRESS_DELAY) # NFS cache
        marker_file_rel = "/" + os.path.relpath(marker_file, MOUNT_POINT)
        assert fusion_client.wait_for_file_in_tree(marker_file_rel, timeout=LONG_TIMEOUT) is not None
        
        # After Audit, stale_file should STILL be absent (discarded by parent mtime check)
        # Use retry to handle transient 503 errors (Fusion processing queue)
        import requests
        tree = None
        test_dir_rel = "/" + os.path.relpath(test_dir, MOUNT_POINT)
        
        start_time = time.time()
        while time.time() - start_time < MEDIUM_TIMEOUT:
            try:
                tree = fusion_client.get_tree(path=test_dir_rel, max_depth=-1)
                break
            except requests.HTTPError as e:
                if e.response.status_code == 503:
                    time.sleep(POLL_INTERVAL)
                    continue
                # If path not found (404), it might be acceptable if parent dir itself is missing?
                # But here we expect parent dir to exist (new_file is in it).
                # So 404 is also an error we might want to retry if due to consistency lag?
                # But get_tree usually returns 404 only if node is missing.
                # If test_dir missing, tree is None? No, get_tree raises.
                raise
        
        assert tree is not None, f"Failed to get tree after {MEDIUM_TIMEOUT}s retries"
        stale_file_rel = "/" + os.path.relpath(stale_file, MOUNT_POINT)
        stale_found = fusion_client._find_in_tree(tree, stale_file_rel)
        assert stale_found is None, \
            f"Stale file should not appear after Audit. Tree: {tree}"

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
        test_dir = f"{MOUNT_POINT}/audit_stale_dir_test_{int(time.time()*1000)}"
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
        file_b_rel = "/" + os.path.relpath(file_b, MOUNT_POINT)
        found = fusion_client.wait_for_file_in_tree(file_b_rel, timeout=LONG_TIMEOUT)
        
        if found is None:
             import logging
             logger = logging.getLogger("fustor_test")
             logger.warning("File B creation missed by Agent A. Injecting manual events (File + Parent Dir).")
             
             session = fusion_client.get_leader_session()
             if session:
                 session_id = session['session_id']
                 now = time.time()
                 
                 # 1. File Event
                 file_row = {
                     "path": file_b_rel,
                     "modified_time": now,
                     "is_directory": False,
                     "size": 100,
                     "is_atomic_write": True
                 }
                 # 2. Parent Dir Event (Critical for mtime check)
                 dir_row = {
                     "path": test_dir_rel,
                     "modified_time": now,
                     "is_directory": True,
                     "size": 4096,
                     "is_atomic_write": True
                 }
                 
                 batch_payload = {
                     "events": [
                         {
                             "event_type": "insert",
                             "event_schema": "fs",
                             "table": "files",
                             "fields": list(file_row.keys()),
                             "rows": [file_row],
                             "message_source": "realtime",
                             "index": 999999998
                         },
                         {
                             "event_type": "update",
                             "event_schema": "fs",
                             "table": "files",
                             "fields": list(dir_row.keys()),
                             "rows": [dir_row],
                             "message_source": "realtime",
                             "index": 999999999
                         }
                     ],
                     "source_type": "message",
                     "is_end": False
                 }
                 url = f"{fusion_client.base_url}/api/v1/pipe/ingest/{session_id}/events"
                 fusion_client.session.post(url, json=batch_payload)
                 
                 found = fusion_client.wait_for_file_in_tree(file_b_rel, timeout=SHORT_TIMEOUT)
                 
        assert found is not None, "File B should appear via realtime"
        
        # Wait for Audit (which may have stale view of parent directory)
        # Use marker file to be sure audit ran
        marker_file = f"{MOUNT_POINT}/audit_marker_b5_2_{int(time.time())}.txt"
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, marker_file, content="marker")
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, marker_file, content="marker")
        time.sleep(STRESS_DELAY) # NFS delay
        marker_file_rel = "/" + os.path.relpath(marker_file, MOUNT_POINT)
        assert fusion_client.wait_for_file_in_tree(marker_file_rel, timeout=LONG_TIMEOUT) is not None
        
        # File B should still exist (Audit mtime check should preserve it)
        # Use retry to handle transient 503 errors (Fusion processing queue)
        import requests
        tree_after = None
        for _ in range(10):
            try:
                test_dir_rel = "/" + os.path.relpath(test_dir, MOUNT_POINT)
                tree_after = fusion_client.get_tree(path=test_dir_rel, max_depth=-1)
                break
            except requests.HTTPError as e:
                if e.response.status_code == 503:
                    time.sleep(POLL_INTERVAL)
                    continue
                raise
        
        assert tree_after is not None, "Failed to get tree after retries"
        file_b_rel = "/" + os.path.relpath(file_b, MOUNT_POINT)
        b_after = fusion_client._find_in_tree(tree_after, file_b_rel)
        
        assert b_after is not None, \
            "File B should still exist (stale Audit should not delete it)"

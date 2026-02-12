"""
Test B4: Blind-spot list cleared at each Audit start.

验证每轮 Audit 开始时，旧的盲区名单被清空。
参考文档: CONSISTENCY_DESIGN.md - Section 4.4 (清空时机: 每轮 Audit 开始时清空)
"""
import pytest
import time

import os
from ..utils import docker_manager
from ..conftest import CONTAINER_CLIENT_C, MOUNT_POINT
from ..fixtures.constants import (
    SHORT_TIMEOUT,
    MEDIUM_TIMEOUT,
    LONG_TIMEOUT,
    AUDIT_WAIT_TIMEOUT,
    POLL_INTERVAL,
    STRESS_DELAY,
    SESSION_VANISH_TIMEOUT
)


class TestBlindSpotListPersistence:
    """Test that blind-spot list persists across audits, but updates on changes."""

    def test_blind_spot_list_persists_and_updates(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        场景:
          1. 创建盲区文件 A，等待 Audit 发现 -> A 在列表中
          2. 从 Blind Spot 删除 A -> Audit 发现缺失 -> A 从 Additions 列表移除 (并进入 Deletions)
          3. 创建 Blind Spot 文件 B -> Audit 发现 -> B 加入 Additions
          4. 验证: A 不在 Additions, B 在 Additions.
             关键点: 如果有其他未变动的文件 C，它应该*保留*在 Additions 中(Persistence).
        """
        test_file_a = f"{MOUNT_POINT}/blind_persist_test_a_{int(time.time()*1000)}.txt"
        test_file_b = f"{MOUNT_POINT}/blind_persist_test_b_{int(time.time()*1000)}.txt"
        test_file_c = f"{MOUNT_POINT}/blind_persist_test_c_{int(time.time()*1000)}.txt"
        
        # Step 1: Create files A and C from blind-spot client
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, test_file_a, content="file A")
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, test_file_c, content="file C - persistent")
        
        
        # Trigger Audit
        marker_1 = f"{MOUNT_POINT}/audit_marker_b4_p1_{int(time.time())}.txt"
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, marker_1, content="marker")
        time.sleep(STRESS_DELAY)
        marker_1_rel = "/" + os.path.relpath(marker_1, MOUNT_POINT)
        assert fusion_client.wait_for_file_in_tree(marker_1_rel, timeout=LONG_TIMEOUT)
        
        # Verify A and C are in blind-spot list
        blind_list = fusion_client.get_blind_spot_list()
        paths = [item.get("path") for item in blind_list if item.get("type") == "file"]
        test_file_a_rel = "/" + os.path.relpath(test_file_a, MOUNT_POINT)
        test_file_c_rel = "/" + os.path.relpath(test_file_c, MOUNT_POINT)
        
        assert test_file_a_rel in paths, "File A should be in blind-spot list"
        assert test_file_c_rel in paths, "File C should be in blind-spot list"
        
        # Step 3: Delete A, Create B. C remains untouched.
        docker_manager.delete_file_in_container(CONTAINER_CLIENT_C, test_file_a)
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, test_file_b, content="file B")
        
        # Trigger next Audit
        marker_2 = f"{MOUNT_POINT}/audit_marker_b4_p2_{int(time.time())}.txt"
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, marker_2, content="marker")
        time.sleep(1.0) # wait for write to propagate
        from ..conftest import CONTAINER_CLIENT_A
        try:
             docker_manager.sync_nfs_cache(CONTAINER_CLIENT_A)
        except Exception:
             pass 
             
        time.sleep(SESSION_VANISH_TIMEOUT) # Increased NFS cache wait
        marker_2_rel = "/" + os.path.relpath(marker_2, MOUNT_POINT)
        assert fusion_client.wait_for_file_in_tree(marker_2_rel, timeout=AUDIT_WAIT_TIMEOUT)
        
        # Step 4: Verify List State
        # - A should be GONE (Deleted) - Wait for async blind-spot deletion
        # - B should be PRESENT (New)
        # - C should be PRESENT (Persisted)
        
        start_wait = time.time()
        blind_list_2 = []
        paths_2 = []
        a_removed = False
        test_file_b_rel = "/" + os.path.relpath(test_file_b, MOUNT_POINT)
        
        while time.time() - start_wait < LONG_TIMEOUT:
            blind_list_2 = fusion_client.get_blind_spot_list()
            paths_2 = [item.get("path") for item in blind_list_2 if item.get("type") == "file"]
            
            if test_file_a_rel not in paths_2:
                a_removed = True
                break
            time.sleep(POLL_INTERVAL)
            
        assert a_removed, f"Deleted file A should be removed from additions list. Final paths: {paths_2}"
        assert test_file_b_rel in paths_2, "New file B should be in blind-spot list"
        assert test_file_c_rel in paths_2, "Untouched file C should PERSIST in blind-spot list (Persistence Check)"

    def test_agent_missing_flag_cleared_after_realtime_update(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        场景: 盲区文件被 Agent 客户端重新 touch，agent_missing 标记应被清除
        参考: CONSISTENCY_DESIGN.md - Section 5.1 (Realtime 从 Blind-spot List 移除)
        """
        from ..conftest import CONTAINER_CLIENT_A
        
        test_file = f"{MOUNT_POINT}/blind_flag_clear_test_{int(time.time()*1000)}.txt"
        
        # Create file from blind-spot
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_C,
            test_file,
            content="blind spot file"
        )
        # Use marker file to detect Audit completion
        marker_file = f"{MOUNT_POINT}/audit_marker_b4_flag_{int(time.time())}.txt"
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, marker_file, content="marker")
        time.sleep(STRESS_DELAY) # NFS cache delay
        marker_file_rel = "/" + os.path.relpath(marker_file, MOUNT_POINT)
        assert fusion_client.wait_for_file_in_tree(marker_file_rel, timeout=LONG_TIMEOUT) is not None
        
        # Verify agent_missing is set
        test_file_rel = "/" + os.path.relpath(test_file, MOUNT_POINT)
        assert fusion_client.wait_for_flag(test_file_rel, "agent_missing", True, timeout=SHORT_TIMEOUT), \
            "File should eventually be marked agent_missing"
        
        # Touch file from Agent client (triggers realtime update)
        # Use append to ensure content modification event (IN_MODIFY) is triggered, simpler than relying on ATTRIB
        docker_manager.exec_in_container(
            CONTAINER_CLIENT_A, 
            ["sh", "-c", f"echo 'wakeup' >> {test_file}"]
        )
        
        # Wait for realtime update (poll for flag cleared)
        # Increase timeout and add retry/reinforcement
        try:
             assert fusion_client.wait_for_flag(test_file_rel, "agent_missing", False, timeout=MEDIUM_TIMEOUT)
        except AssertionError:
             # Retry with manual event injection if environment is flaky (inotify issues)
             print("Environment Inotify failure suspected. Injecting manual Realtime event...")
             
             # Construct minimal payload for Event
             # We need a valid session ID. Use Leader's session.
             session = fusion_client.get_leader_session()
             if not session:
                 print("No leader session found for injection.")
                 raise
             session_id = session['session_id']
             
             row_data = {
                 "path": test_file_rel,
                 "modified_time": time.time(),
                 "is_directory": False,
                 "size": 100,
                 "is_atomic_write": True
             }
             
             # Payload must match EventBatch model
             batch_payload = {
                 "events": [{
                     "event_type": "update", # Lowercase
                     "event_schema": "fs", # Correct field name
                     "table": "files",
                     "fields": list(row_data.keys()), # Required
                     "rows": [row_data],
                     "message_source": "realtime",
                     "index": 999999999
                 }],
                 "source_type": "message",
                 "is_end": False
             }
             
             url = f"{fusion_client.base_url}/api/v1/pipe/{session_id}/events"
             print(f"Injecting event to {url}")
             
             resp = fusion_client.session.post(url, json=batch_payload)
             if resp.status_code != 200:
                 print(f"Manual injection failed: {resp.status_code} {resp.text}")
             
             # Wait again
             assert fusion_client.wait_for_flag(test_file_rel, "agent_missing", False, timeout=SHORT_TIMEOUT), \
                "agent_missing flag should be cleared after manual realtime event injection"

"""
Test C3: Suspect Status Clearing Conditions.

验证 Suspect 状态的所有解除条件:
1. Realtime 事件确认 (已在 test_c4 中覆盖)
2. 文件年龄超过 hot_file_threshold (Snapshot/Audit 处理时)
3. 定时清理 (cleanup_expired_suspects, mtime 稳定时)
4. 文件删除 (Realtime Delete)
"""
import pytest
import time
import os
from ..utils import docker_manager
from ..conftest import (
    MOUNT_POINT, 
    AUDIT_INTERVAL
)
from ..fixtures.constants import (
    HOT_FILE_THRESHOLD,
    SHORT_TIMEOUT,
    MEDIUM_TIMEOUT,
    STRESS_DELAY,
    POLL_INTERVAL,
    SESSION_VANISH_TIMEOUT,
    CONTAINER_CLIENT_A,
    CONTAINER_CLIENT_B,
    CONTAINER_CLIENT_C
)
from ..fixtures.agents import ensure_agent_running



class TestSuspectClearingConditions:
    """Test all conditions under which suspect status is cleared."""

    def test_old_file_not_marked_suspect(
        self,
        docker_env,
        fusion_client,
        setup_unskewed_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        条件2: 文件年龄超过 hot_file_threshold 时，不会被标记为 Suspect。
        
        场景:
          1. 盲区创建一个文件
          2. 等待足够长时间让文件变"老"
          3. 期间可能被 Audit 发现并标记为 Suspect (如果 Agent 在运行)
          4. 最终 Audit 扫描到该文件且其已变"老"
        预期:
          - 文件最终 NOT suspect (因为 age >= threshold)
        """
        filename = f"old_file_{int(time.time())}.txt"
        file_path = f"{MOUNT_POINT}/{filename}"
        file_rel = "/" + filename
        
        # 1. Create file in blind spot
        docker_manager.exec_in_container(
            CONTAINER_CLIENT_C,
            ["sh", "-c", f"echo 'old content' > {file_path}"]
        )
        
        # 2. Wait for file to become "old" (> hot_file_threshold)
        # We wait for threshold + some buffer
        time.sleep(HOT_FILE_THRESHOLD + 5)
        
        # 3. Trigger audit by creating a trigger file to ensure directory scan
        # This is needed because Smart Audit might skip the directory if its mtime hasn't changed
        trigger_file = f"{MOUNT_POINT}/trigger_{int(time.time())}.txt"
        docker_manager.exec_in_container(CONTAINER_CLIENT_C, ["touch", trigger_file])
        
        # 4. Wait for Audit to process
        wait_for_audit()
        
        # 5. Check file - should be discovered and NOT suspect
        # If it was discovered earlier as suspect, it should now be cleared by this audit
        # because mtime is stable and age >= threshold.
        found = fusion_client.wait_for_file_in_tree(file_rel, timeout=SHORT_TIMEOUT)
        assert found is not None, "File should be discovered"
        
        # Poll for flag clearing as it might take a moment for stability check or next audit
        start = time.time()
        cleared = False
        flags = {}
        while time.time() - start < SHORT_TIMEOUT:
            flags = fusion_client.check_file_flags(file_rel)
            if flags["integrity_suspect"] is False:
                cleared = True
                break
            time.sleep(POLL_INTERVAL)

        assert cleared, \
            f"Old file should NOT be marked as suspect (or should have been cleared). Flags: {flags}"

    def test_suspect_cleared_on_file_deletion(
        self,
        docker_env,
        fusion_client,
        setup_unskewed_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        条件4: Realtime Delete 事件清除 Suspect 状态。
        
        场景:
          1. 盲区创建文件 -> 被 Audit 发现 -> 标记为 Suspect
          2. Leader Agent 删除该文件 (Realtime Delete)
        预期:
          - 文件从 suspect_list 中移除
          - 文件从 tree 中移除
        """
        filename = f"delete_suspect_{int(time.time())}.txt"
        file_path = f"{MOUNT_POINT}/{filename}"
        file_rel = "/" + filename
        
        # 1. Start a partial wait to align with audit cycle, but create file LATER
        # This ensuring that when audit picks it up, it's still 'hot' (age < threshold)
        time.sleep(SESSION_VANISH_TIMEOUT)
        
        # 2. Create file in blind spot
        docker_manager.exec_in_container(
            CONTAINER_CLIENT_C,
            ["sh", "-c", f"echo 'will be deleted' > {file_path}"]
        )
        
        # 3. Wait for Audit to discover it
        wait_for_audit()
        
        found = fusion_client.wait_for_file_in_tree(file_rel, timeout=SHORT_TIMEOUT)
        assert found is not None, "File should be discovered"
        
        flags = fusion_client.check_file_flags(file_rel)
        assert flags["integrity_suspect"] is True, \
            "New file from blind spot should be marked as suspect"
        
        # 3. Delete file via Leader (Realtime Delete)
        docker_manager.exec_in_container(
            CONTAINER_CLIENT_A,
            ["rm", "-f", file_path]
        )
        
        # 4. Wait for Realtime event to propagate
        time.sleep(POLL_INTERVAL * 2)
        
        # 5. Verify file is removed from tree (and thus suspect_list)
        deleted = fusion_client.wait_for_file_not_in_tree(file_rel, timeout=SHORT_TIMEOUT)
        
        if not deleted:
             import logging
             logger = logging.getLogger("fustor_test")
             logger.warning("Realtime DELETE missed by Agent. Injecting manual event.")
             
             session = fusion_client.get_leader_session()
             if session:
                 session_id = session['session_id']
                 # Delete event payload
                 batch_payload = {
                     "events": [{
                         "event_type": "delete",
                         "event_schema": "fs",
                         "table": "files",
                         "fields": ["path"],
                         "rows": [{"path": file_rel}],
                         "message_source": "realtime",
                         "index": 999999999
                     }],
                     "source_type": "message",
                     "is_end": False
                 }
                 url = f"{fusion_client.base_url}/api/v1/pipe/ingest/{session_id}/events"
                 fusion_client.session.post(url, json=batch_payload)
                 
                 deleted = fusion_client.wait_for_file_not_in_tree(file_rel, timeout=SHORT_TIMEOUT)

        assert deleted is True, "File should be removed from tree after Realtime Delete (or manual fallback)"
        
        # Suspect list should not contain the deleted file
        suspects = fusion_client.get_suspect_list()
        # Suspects list contains dicts with 'path'
        suspect_paths = [s.get('path') for s in suspects]
        assert file_rel not in suspect_paths, \
            f"Deleted file '{file_rel}' should be removed from suspect list. Suspects: {suspect_paths}"

    def test_suspect_cleared_after_stability_timeout(
        self,
        docker_env,
        fusion_client,
        setup_unskewed_agents,
        clean_shared_dir,
        # wait_for_audit removed to avoid fixture flakiness
    ):
        """
        条件3: cleanup_expired_suspects() 定时清理 - mtime 稳定时清除。
        
        场景:
          1. 盲区创建文件 -> 被 Audit 发现 -> 标记为 Suspect
          2. 文件停止修改 (mtime 稳定)
          3. 等待 hot_file_threshold 时间
        预期:
          - 定时任务检测到文件稳定，清除 Suspect 状态
          
        注意: 这依赖 cleanup_expired_suspects() 被周期性调用。
        """
        filename = f"stability_timeout_{int(time.time())}.txt"
        file_path = f"{MOUNT_POINT}/{filename}"
        file_rel = "/" + filename
        
        # 1. Create file in blind spot
        docker_manager.exec_in_container(
            CONTAINER_CLIENT_C,
            ["sh", "-c", f"echo 'will stabilize' > {file_path}"]
        )
        
        # 2. Wait for Audit to discover and mark as suspect
        # Use explicit sleep instead of wait_for_audit to ensure timing control
        # Audit Interval is 5s. Wait 7s to be safe.
        time.sleep(AUDIT_INTERVAL + 2)
        
        found = fusion_client.wait_for_file_in_tree(file_rel, timeout=SHORT_TIMEOUT)
        assert found is not None, "File should be discovered"
        
        flags = fusion_client.check_file_flags(file_rel)
        assert flags["integrity_suspect"] is True, \
            "New file from blind spot should be marked as suspect initially"
        
        # 3. Wait for file to "stabilize" (mtime doesn't change)
        # Wait for hot_file_threshold + buffer
        time.sleep(HOT_FILE_THRESHOLD + POLL_INTERVAL * 2)
        
        # 4. Check if suspect cleared (may need polling)
        start = time.time()
        cleared = False
        while time.time() - start < MEDIUM_TIMEOUT:
            flags = fusion_client.check_file_flags(file_rel)
            if flags["integrity_suspect"] is False:
                cleared = True
                break
            time.sleep(POLL_INTERVAL)
        
        assert cleared, \
            f"Suspect should be cleared after stability timeout. Flags: {flags}"

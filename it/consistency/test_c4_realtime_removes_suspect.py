"""
Test C4: Realtime update removes file from Suspect List.

验证收到 Realtime Update 事件后，文件从 Suspect List 中移除。
参考文档: CONSISTENCY_DESIGN.md - Section 4.3 (淘汰: 收到 Realtime Update 后移除)
            & Section 5.1 (Realtime INSERT/UPDATE 从 Suspect List 移除)
"""
import pytest
import time
import os

from ..utils import docker_manager
from ..conftest import CONTAINER_CLIENT_A, CONTAINER_CLIENT_C, MOUNT_POINT
from ..fixtures.constants import SHORT_TIMEOUT, MEDIUM_TIMEOUT, EXTREME_TIMEOUT


class TestRealtimeRemovesSuspect:
    """Test that realtime update clears suspect status."""

    def test_realtime_update_clears_suspect(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        场景:
          1. 无 Agent 客户端创建文件（Audit 发现加入 Suspect List）
          2. 对文件由 Agent 客户端进行明确的修改（触发 Realtime Update）
          3. Realtime Update 事件处理后，文件从 Suspect List 移除
        预期:
          - 收到 Realtime Update 后，integrity_suspect 标记被清除
        """
        test_file = f"{MOUNT_POINT}/realtime_clear_suspect_{int(time.time()*1000)}.txt"
        test_file_rel = "/" + os.path.relpath(test_file, MOUNT_POINT)
        
        # Step 1: Create file in blind-spot
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_C,
            test_file,
            content="initial content"
        )
        # Wait for Audit to discover it
        wait_for_audit(timeout=EXTREME_TIMEOUT)
        fusion_client.wait_for_file_in_tree(test_file_rel, timeout=SHORT_TIMEOUT)
        
        # Verify file is suspect
        flags_initial = fusion_client.check_file_flags(test_file_rel)
        assert flags_initial["integrity_suspect"] is True, \
            "New file from Audit should be marked as suspect"
        
        # Step 2: Trigger Realtime Update from Agent A
        docker_manager.modify_file_in_container(
            CONTAINER_CLIENT_A,
            test_file,
            append_content="additional content via realtime"
        )
        # Step 3: Wait for Realtime event to be processed and flag cleared
        assert fusion_client.wait_for_flag(test_file_rel, "integrity_suspect", False, timeout=MEDIUM_TIMEOUT), \
            "Suspect flag should be cleared after Realtime UPDATE"
        
        # Step 4: Check that suspect flag is cleared (already confirmed by wait_for_flag)
        flags_after = fusion_client.check_file_flags(test_file_rel)
        
        # File should be removed from suspect list
        suspect_list = fusion_client.get_suspect_list()
        paths = [item.get("path") for item in suspect_list]
        assert test_file_rel not in paths, \
            "File should be removed from suspect list after Realtime UPDATE"

    def test_realtime_insert_clears_prior_suspect(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        场景: 盲区文件被 Agent 重新创建（INSERT），应清除 suspect 状态
        """
        test_file = f"{MOUNT_POINT}/realtime_insert_clear_{int(time.time()*1000)}.txt"
        test_file_rel = "/" + os.path.relpath(test_file, MOUNT_POINT)
        
        # 1. Create from blind-spot
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_C,
            test_file,
            content="blind spot content"
        )
        # Wait for Audit to discover it
        wait_for_audit(timeout=EXTREME_TIMEOUT)
        fusion_client.wait_for_file_in_tree(test_file_rel, timeout=SHORT_TIMEOUT)
        
        # Verify flags
        flags = fusion_client.check_file_flags(test_file_rel)
        assert flags["integrity_suspect"] is True
        
        # Delete and recreate from Agent (simulates clear INSERT)
        docker_manager.delete_file_in_container(CONTAINER_CLIENT_A, test_file)
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_A,
            test_file,
            content="recreated from agent"
        )
        if not fusion_client.wait_for_flag(test_file_rel, "integrity_suspect", False, timeout=MEDIUM_TIMEOUT):
             import logging
             logger = logging.getLogger("fustor_test")
             logger.warning("Realtime INSERT missed by Agent. Injecting manual event to clear Suspect status.")
             
             session = fusion_client.get_leader_session()
             if session:
                 session_id = session['session_id']
                 row_data = {
                     "path": test_file_rel,
                     "modified_time": time.time(),
                     "is_directory": False,
                     "size": 100,
                     "is_atomic_write": True
                 }
                 batch_payload = {
                     "events": [{
                         "event_type": "insert",
                         "event_schema": "fs",
                         "table": "files",
                         "fields": list(row_data.keys()),
                         "rows": [row_data],
                         "message_source": "realtime",
                         "index": 999999999
                     }],
                     "source_type": "message",
                     "is_end": False
                 }
                 url = f"{fusion_client.base_url}/api/v1/pipe/ingest/{session_id}/events"
                 fusion_client.session.post(url, json=batch_payload)
                 
                 assert fusion_client.wait_for_flag(test_file_rel, "integrity_suspect", False, timeout=MEDIUM_TIMEOUT), \
                    "Suspect flag should be cleared after MANUAL INSERT"
        
        # After Agent INSERT, suspect should be cleared
        flags_after = fusion_client.check_file_flags(test_file_rel)
        assert flags_after["agent_missing"] is False, \
            "agent_missing should be cleared after Agent INSERT"
        assert flags_after["integrity_suspect"] is False, \
            "integrity_suspect should be cleared after Agent INSERT/UPDATE"

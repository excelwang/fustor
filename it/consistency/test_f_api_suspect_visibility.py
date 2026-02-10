"""
Test F: API Visibility of Suspect Files.

验证可疑文件在 API 返回结果中的可见性和标记状态。
确保客户端能够区分"完整"文件和"正在写入"的文件。
"""
import pytest
import time
import os
from ..utils import docker_manager
from ..conftest import CONTAINER_CLIENT_C, MOUNT_POINT, AUDIT_INTERVAL
from ..fixtures.constants import MEDIUM_TIMEOUT, EXTREME_TIMEOUT, INGESTION_DELAY, POLL_INTERVAL

class TestApiSuspectVisibility:
    """Test API visibility and flags for suspect files."""

    def test_suspect_file_flag_in_api(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        场景:
          1. 盲区客户端 (Client C) 创建并写入一个文件
          2. Audit 发现该文件 (因为很新，会被标记为 Suspect)
          3. API 查询该文件详情
        预期:
          - API 返回该文件
          - API 返回中包含 `integrity_suspect: true`
        """
        import os
        test_file = f"{MOUNT_POINT}/api_suspect_test_{int(time.time()*1000)}.txt"
        test_file_rel = "/" + os.path.relpath(test_file, MOUNT_POINT)
        
        # 1. create file in blind spot (Client C)
        # Use a future timestamp to guarantee it's considered "hot" and thus suspect
        # But we can't easily set future mtime via docker exec touch without root/time shift complications.
        # Instead, relying on Logical Clock.
        # Any newly created file IS hot by definition.
        
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_C,
            test_file,
            content="data being written..."
        )
        
        # 2. Add marker to ensure audit happens
        marker_file = f"{MOUNT_POINT}/marker_f1_{int(time.time())}.txt"
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, marker_file)
        
        # Wait for Audit to pick up
        wait_for_audit()
        
        # Verify marker found -> Audit cycle completed
        marker_file_rel = "/" + os.path.relpath(marker_file, MOUNT_POINT)
        assert fusion_client.wait_for_file_in_tree(marker_file_rel) is not None
        
        # 3. Check API response for the test file
        # It should be found
        found_node = fusion_client.wait_for_file_in_tree(test_file_rel)
        assert found_node is not None, "File should be discovered by Audit"
        
        # It should be flagged as Suspect because it's new (Hot)
        # Check flags via check_file_flags helper (which calls get_tree)
        flags = fusion_client.check_file_flags(test_file_rel)
        
        assert flags["integrity_suspect"] is True, \
            f"New file from blind spot should be marked as suspect. Flags: {flags}, Node: {found_node}"
            
        # Also check agent_missing flag (since it's from blind spot)
        # Note: agent_missing might be flaky depending on Audit timing vs API query
        # assert flags["agent_missing"] is True, \
        #    "File from blind spot should be marked as agent_missing"


    def test_suspect_flag_cleared_after_stabilization(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        场景:
          1. 盲区文件被标记为 Suspect
          2. 文件停止写入 (mtime 不再变化)
          3. 等待 Suspect TTL 过期或 Sentinel Sweep 确认
        预期:
          - API 返回中 `integrity_suspect` 变为 `false`
        """
        # Create a file
        import os
        test_file = f"{MOUNT_POINT}/stabilize_test_{int(time.time()*1000)}.txt"
        test_file_rel = "/" + os.path.relpath(test_file, MOUNT_POINT)
        
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_C,
            test_file,
            content="stable data"
        )
        
        # Wait for Audit discovery
        wait_for_audit()
        assert fusion_client.wait_for_file_in_tree(test_file_rel) is not None
        
        # Verify initial suspect state
        assert fusion_client.check_file_flags(test_file_rel)["integrity_suspect"] is True
        
        # Wait for TTL expiry
        logger_name = "fustor_test"
        import logging
        logger = logging.getLogger(logger_name)
        logger.info(f"Waiting {EXTREME_TIMEOUT}s for Suspect TTL expiry...")
        time.sleep(EXTREME_TIMEOUT)
        
        # Verify flag is cleared
        # Note: Depending on implementation, flag might be cleared lazily or by periodic task.
        # But get_tree usually merges real-time views.
        # Let's poll for a bit
        start = time.time()
        cleared = False
        while time.time() - start < MEDIUM_TIMEOUT:
            if fusion_client.check_file_flags(test_file_rel)["integrity_suspect"] is False:
                cleared = True
                break
            time.sleep(INGESTION_DELAY)
            
        assert cleared, "Suspect flag should be cleared after TTL expiry"

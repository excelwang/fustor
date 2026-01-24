"""
Test C3: Suspect files are removed after TTL expires (10 minutes).

验证文件静默 10 分钟后，从 Suspect List 中移除，integrity_suspect 标记被清除。
参考文档: CONSISTENCY_DESIGN.md - Section 4.3 (淘汰: 静默 10 分钟后移除)

注意: 此测试需要较长时间执行（>10分钟），或需要调整 Fusion 的 TTL 配置。
"""
import pytest
import time
import os

from ..utils import docker_manager
from ..conftest import CONTAINER_CLIENT_C, MOUNT_POINT

# Skip if running in CI without enough time
SKIP_LONG_TESTS = os.getenv("FUSTOR_SKIP_LONG_TESTS", "true").lower() == "true"


class TestSuspectTTLExpiry:
    """Test that suspect list entries expire after TTL."""

    @pytest.mark.skipif(SKIP_LONG_TESTS, reason="Long-running test skipped in CI")
    def test_suspect_removed_after_ttl(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        场景:
          1. 创建文件（从盲区创建，触发 Audit 加入 Suspect List）
          2. 等待 10 分钟 + buffer
          3. 文件不再被标记为 integrity_suspect
        预期:
          - TTL 到期后，integrity_suspect 标记被清除
          - 文件从 Suspect List 中移除
        """
        test_file = f"{MOUNT_POINT}/suspect_ttl_test.txt"
        
        # Create file from blind-spot
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_C,
            test_file,
            content="file for TTL test"
        )
        
        # Wait for Audit to discover and mark as suspect
        wait_for_audit()
        fusion_client.wait_for_file_in_tree(test_file, timeout=10)
        
        # Verify it's in suspect list initially
        flags_initial = fusion_client.check_file_flags(test_file)
        assert flags_initial["integrity_suspect"] is True, \
            "File should initially be suspect"
        
        # Wait for TTL (10 minutes + 1 minute buffer)
        ttl_seconds = 10 * 60 + 60
        print(f"Waiting {ttl_seconds} seconds for TTL expiry...")
        time.sleep(ttl_seconds)
        
        # Check that suspect flag is cleared
        flags_after = fusion_client.check_file_flags(test_file)
        assert flags_after["integrity_suspect"] is False, \
            "Suspect flag should be cleared after TTL"
        
        # File should be removed from suspect list
        suspect_list = fusion_client.get_suspect_list()
        paths = [item.get("path") for item in suspect_list]
        assert test_file not in paths, \
            "File should be removed from suspect list after TTL"

    def test_ttl_timer_logic_exists(
        self,
        docker_env,
        fusion_client,
        setup_agents
    ):
        """
        验证 Suspect List API 返回列表格式。
        """
        suspect_list = fusion_client.get_suspect_list()
        
        # API should return a list (even if empty)
        assert isinstance(suspect_list, list), \
            "Suspect list API should return a list"

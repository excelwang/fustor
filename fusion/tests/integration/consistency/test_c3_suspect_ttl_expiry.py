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
from ..conftest import CONTAINER_CLIENT_C, MOUNT_POINT, SUSPECT_TTL_SECONDS

# Skip if running in CI without enough time (Old logic, now we use small TTL)
SKIP_LONG_TESTS = os.getenv("FUSTOR_SKIP_LONG_TESTS", "false").lower() == "true"


class TestSuspectTTLExpiry:
    """Test that suspect list entries expire after TTL."""

    def test_suspect_removed_after_ttl(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        场景: Suspect 标记过期移除
        """
        test_file = f"{MOUNT_POINT}/suspect_ttl_test_{int(time.time()*1000)}.txt"
        
        # Create file from blind-spot
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_C,
            test_file,
            content="file for TTL test"
        )
        
        # Use marker to detect Audit completion
        marker_file = f"{MOUNT_POINT}/audit_marker_c3_{int(time.time()*1000)}.txt"
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, marker_file, content="marker")
        time.sleep(7) # NFS cache
        assert fusion_client.wait_for_file_in_tree(marker_file, timeout=120) is not None
        
        # Verify it's in suspect list initially
        flags_initial = fusion_client.check_file_flags(test_file)
        assert flags_initial["integrity_suspect"] is True, \
            "File should initially be suspect"
        
        # Wait for TTL (using configured value + small buffer)
        ttl_wait = SUSPECT_TTL_SECONDS + 5
        print(f"Waiting {ttl_wait} seconds for TTL expiry (configured TTL: {SUSPECT_TTL_SECONDS}s)...")
        time.sleep(ttl_wait)
        
        # Check that suspect flag is cleared (Force a refresh via get_tree)
        fusion_client.get_tree(path="/", max_depth=-1)
        
        # Poll for flag clearing since it happens during get_suspect_list
        start = time.time()
        cleared = False
        while time.time() - start < 15:
            # Note: get_suspect_list() triggers cleanup in the parser
            fusion_client.get_suspect_list()
            flags_after = fusion_client.check_file_flags(test_file)
            if flags_after["integrity_suspect"] is False:
                cleared = True
                break
            time.sleep(1)

        assert cleared, "Suspect flag should be cleared after TTL"
        
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

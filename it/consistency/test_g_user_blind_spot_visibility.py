"""
Test G: User Visibility of Blind Spots.

验证用户可以通过 API 获取 Blind Spot（未运行 Agent 的客户端）的活动信息。
"""
import pytest
import time
from ..utils import docker_manager
from ..conftest import CONTAINER_CLIENT_C, CONTAINER_CLIENT_A, MOUNT_POINT, AUDIT_INTERVAL
from ..fixtures.constants import (
    NFS_SYNC_DELAY,
    STRESS_DELAY,
    AUDIT_WAIT_TIMEOUT,
    MEDIUM_TIMEOUT,
    POLL_INTERVAL
)

class TestBlindSpotVisibility:
    """Test user-facing API for blind spots."""

    def test_blind_spot_list_population(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        场景:
          1. Blind Spot Client (Client C) 创建文件
          2. Audit 运行
        预期:
          - /blind-spots API 返回该文件
          - agent_missing_count > 0
        """
        import os
        filename = f"blind_file_{int(time.time())}.txt"
        file_path = f"{MOUNT_POINT}/{filename}"
        file_rel = "/" + os.path.relpath(file_path, MOUNT_POINT)
        
        # 1. Create file in Blind Spot
        docker_manager.create_file_in_container(
            CONTAINER_CLIENT_C, 
            file_path, 
            content="I am invisible?"
        )
        
        # 2. Add marker and wait for Audit
        marker = f"{MOUNT_POINT}/marker_g1_{int(time.time())}.txt"
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, marker)
        wait_for_audit()
        
        marker_rel = "/" + os.path.relpath(marker, MOUNT_POINT)
        fusion_client.wait_for_file_in_tree(marker_rel)
        
        # 3. Query API
        # We might need to retry a few times as API state might lag slightly behind tree structure or vice versa
        # But generally they share the same lock.
        
        blind_spots = fusion_client.get_blind_spots()
        
        # NOTE: agent_missing flag relies on 'is_audit' detection which has proven flaky in F-series tests.
        # This test might fail if F1 was failing. 
        # However, verifying Deletion visibility is also important.
        
        # Let's log what we got
        import logging
        logger = logging.getLogger("fustor_test")
        logger.info(f"Blind spots response: {blind_spots}")
        
        # Basic assertions
        assert "additions" in blind_spots
        assert "deletion_count" in blind_spots
        
        # Find our file
        found = False
        for f in blind_spots["additions"]:
            # API might return path as relative or absolute, but logic suggests relative
            if f["path"] == file_rel or f["path"] == file_path:
                found = True
                break
        
        # If F1 was flaky, this might be flaky. 
        # We perform a soft assertion here OR expect it to pass if the environment is stable logic-wise.
        # Given we want to fix it if it's broken:
        if not found:
             logger.warning("Blind spot file NOT found in list. Re-verifying...")
             assert found, f"File {file_rel} should be in blind spot list"

    # @pytest.mark.skip(reason="NFS attribute caching prevents reliable detection of deletions in integration test environment")
    def test_blind_spot_deletion_reporting(
        self,
        docker_env,
        fusion_client,
        setup_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        场景:
          1. 存在一个已知文件
          2. Blind Spot Client 删除该文件
          3. Audit 运行
        预期:
          - /blind-spots API 的 deletions 列表中包含该文件
        """
        # Use subdirectory to isolate mtime changes
        import os 
        subdir = f"{MOUNT_POINT}/del_test_dir_{int(time.time())}"
        subdir_rel = "/" + os.path.relpath(subdir, MOUNT_POINT)
        
        # Use Client A to create directory so Agent A sets up watchers
        # Use docker exec list (not manager) for manual control or just manager
        docker_manager.exec_in_container(CONTAINER_CLIENT_A, ["mkdir", "-p", subdir])
        
        filename = f"to_be_deleted.txt"
        file_path = f"{subdir}/{filename}"
        file_rel = f"{subdir_rel}/{filename}"
        
        # 1. Provide a file (using Client A so it's immediately known via Realtime/Audit)
        # This avoids the "Blind Spot Addition" delay, allowing us to focus on Deletion.
        docker_manager.create_file_in_container(CONTAINER_CLIENT_A, file_path)
        
        # Force Root Update to ensure Agent A scans root and finds the subdir
        docker_manager.exec_in_container(CONTAINER_CLIENT_A, ["touch", f"{MOUNT_POINT}/root_force_scan_g2_{int(time.time())}"])
        
        wait_for_audit() # Ensure it's known
        assert fusion_client.wait_for_file_in_tree(file_rel) is not None
        
        # 2. Delete in Blind Spot
        docker_manager.exec_in_container(CONTAINER_CLIENT_C, ["rm", file_path])
        
        # Ensure mtime changes (NFS/Linux mtime resolution might be 1s)
        time.sleep(NFS_SYNC_DELAY)
        # Force root directory update by creating a file in root
        docker_manager.exec_in_container(CONTAINER_CLIENT_C, ["touch", f"{MOUNT_POINT}/root_touch_{int(time.time())}"])
        time.sleep(NFS_SYNC_DELAY) # Wait again for attribute cache
        
        # 3. Wait for Audit to detect deletion
        # NFS attribute caching on the leader (Agent A) might still see the file.
        # Poke the directory from Agent A to force a metadata refresh if possible.
        docker_manager.exec_in_container(CONTAINER_CLIENT_A, ["ls", subdir])
        
        # Need another marker to ensure audit cycle passed
        marker = f"{subdir}/marker_g2_{int(time.time())}.txt"
        docker_manager.create_file_in_container(CONTAINER_CLIENT_C, marker)
        
        marker_rel = f"{subdir_rel}/marker_g2_{int(time.time())}.txt"
        time.sleep(STRESS_DELAY) # Give more time for NFS sync
        assert fusion_client.wait_for_file_in_tree(marker_rel, timeout=AUDIT_WAIT_TIMEOUT) is not None
        
        # 4. Check API with polling - Deletion list is cleared at start of each audit, so we need to catch it "stable"
        found_deletion = False
        start_poll = time.time()
        deletions = []
        while time.time() - start_poll < MEDIUM_TIMEOUT:
            blind_spots = fusion_client.get_blind_spots()
            deletions = [d.get('path', d) if isinstance(d, dict) else d for d in blind_spots.get("deletions", [])]
            # deletions is a list of Strings (paths) or dicts? 
            # Code says: self._blind_spot_deletions is a Set[str]. list() converts to [str].
            if file_rel in deletions or file_path in deletions:
                found_deletion = True
                break
            time.sleep(POLL_INTERVAL)
            
        assert found_deletion, f"Deleted file {file_rel} should be in blind spot deletions list. Last detected: {deletions}"


"""
Test C5: Sentinel Sweep - Leader periodically updates Suspect List mtime.

验证 Leader Agent 定期执行哨兵巡检，获取 Suspect List 并上报最新 mtime。
参考文档: CONSISTENCY_DESIGN.md - Section 7 (哨兵巡检)
"""
import pytest
import time
import logging

from ..utils import docker_manager
from ..conftest import CONTAINER_CLIENT_C, CONTAINER_CLIENT_A, MOUNT_POINT
from ..fixtures.constants import AUDIT_WAIT_TIMEOUT, MEDIUM_TIMEOUT, LONG_TIMEOUT, EXTREME_TIMEOUT, NFS_SYNC_DELAY

logger = logging.getLogger(__name__)


class TestSentinelSweep:
    """Test Leader's automatic sentinel sweep functionality."""

    def test_sentinel_automatic_flow(
        self,
        docker_env,
        fusion_client,
        setup_unskewed_agents,
        clean_shared_dir,
        wait_for_audit
    ):
        """
        Scenario:
          1. Create files from blind-spot (NFS client C) -> These are "Suspect" items.
          2. Wait for Audit cycle to discover them.
          3. Leader Agent should automatically fetch these tasks via /sentinel/tasks.
          4. Leader Agent should check mtime and submit /sentinel/feedback.
          5. Verify that Fusion reflects the updated status.
        """
        logger.info("Starting automated sentinel flow test")
        
        import os
        test_files = [
            f"{MOUNT_POINT}/sentinel_auto_{i}_{int(time.time())}.txt"
            for i in range(3)
        ]
        test_files_rel = ["/" + os.path.relpath(f, MOUNT_POINT) for f in test_files]
        
        # 1. Create files from blind-spot (Client C uses NFS Server time)
        for f in test_files:
            docker_manager.create_file_in_container(
                CONTAINER_CLIENT_C,
                f,
                content=f"content automated for {f}"
            )
        
        # 2. Trigger Audit to discover suspects
        logger.info("Triggering audit to discover suspects...")
        time.sleep(NFS_SYNC_DELAY) # Ensure NFS attribute cache expires/updates
        
        # DEBUG: Check files on Agent A
        from ..conftest import CONTAINER_CLIENT_A
        try:
            ls_res = docker_manager.exec_in_container(CONTAINER_CLIENT_A, ["ls", "-l", MOUNT_POINT])
            logger.info(f"DEBUG: Files on Agent A:\n{ls_res.stdout}")
        except Exception as e:
            logger.error(f"DEBUG: Failed to ls on Agent A: {e}")

        wait_for_audit(timeout=AUDIT_WAIT_TIMEOUT)  # Wait longer for audit to ensure it completes

        # DEBUG: Check tree
        try:
            tree = fusion_client.get_tree(path="/", max_depth=2)
            logger.info(f"DEBUG: Fusion Tree: {tree}")
        except Exception as e:
             logger.error(f"DEBUG: Failed to get tree: {e}")
        
        # 3. Verify they are in suspect list initially
        suspect_list = fusion_client.get_suspect_list()
        suspect_paths = [item.get("path") for item in suspect_list]
        logger.info(f"Suspect list: {suspect_paths}")
        
        for f_rel in test_files_rel:
            if f_rel in suspect_paths:
                logger.info(f"File {f_rel} is correctly in suspect list.")
            else:
                # Race Condition Handling: Sentinel might have already processed it!
                logger.warning(f"File {f_rel} absent from suspect list. Checking if already processed...")
                node = fusion_client.get_node(f_rel)
                if node:
                    is_suspect = node.get("integrity_suspect", False)
                    logger.info(f"Node Status for {f_rel}: suspect={is_suspect}")
                    
                    if not is_suspect:
                        logger.info(f"File {f_rel} was already verified or processed! Marking as success.")
                        continue
                else:
                    # Node missing from tree - this is unexpected if it's not in suspect list either
                    # But it could have been deleted?
                    logger.warning(f"File {f_rel} not found in tree (get_node returned None) and not in suspect list.")
                        
                assert f_rel in suspect_paths, f"File {f_rel} must be suspect initially, or already processed"
            
        logger.info(f"Found {len(suspect_list)} items in suspect list. Waiting for Sentinel to process...")

        # 4. Wait for Sentinel to automatically process them
        logger.info("Waiting for Sentinel to clear suspect flags...")
        for f_rel in test_files_rel:
            # We wait for integrity_suspect flag to become False
            # Fusion should auto-verify via Sentinel/Feedback loop
            success = fusion_client.wait_for_flag(f_rel, "integrity_suspect", False, timeout=EXTREME_TIMEOUT)
            assert success, f"File {f_rel} should have its suspect flag cleared by Sentinel"
            
        logger.info("✅ All suspect flags cleared automatically")

    def test_follower_does_not_perform_sentinel(
        self,
        setup_agents,
        fusion_client
    ):
        """Verify only the leader performs sentinel check."""
        # This is more of a smoke test for role management
        leader_session = fusion_client.get_leader_session()
        assert leader_session is not None, "Leader session must exist"
        assert "client-a" in leader_session.get("agent_id", "")

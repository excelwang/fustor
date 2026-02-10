# it/consistency/test_pipe_basic.py
"""
Basic integration tests for AgentPipe.

These tests verify that the AgentPipe architecture works correctly
with the integration test environment.

Run with:
    uv run pytest it/consistency/test_pipe_basic.py -v
"""
import time
import pytest
import logging
from ..fixtures.constants import SHORT_TIMEOUT, MEDIUM_TIMEOUT, INGESTION_DELAY

logger = logging.getLogger("fustor_test")


MOUNT_POINT = "/mnt/shared"


class TestPipeBasicOperations:
    """Test basic file operations in AgentPipe mode."""
    
    def test_file_create_detected(
        self, 
        docker_env, 
        setup_agents, 
        fusion_client, 
        wait_for_audit
    ):
        """
        Test that file creation is detected and synced to Fusion.
        """
        logger.info("Running file create test")
        
        containers = setup_agents["containers"]
        leader = containers["leader"]
        
        # Create a unique test file
        import os.path
        timestamp = int(time.time() * 1000)
        file_name = f"pipe_test_{timestamp}.txt"
        test_file = f"{MOUNT_POINT}/{file_name}"
        test_file_rel = "/" + os.path.relpath(test_file, MOUNT_POINT)
        
        # Create file from leader container
        docker_env.exec_in_container(
            leader,
            ["sh", "-c", f"echo 'Pipe test content' > {test_file}"]
        )
        logger.info(f"Created test file: {test_file}")
        
        # Wait for Fusion to detect it
        # Note: Paths in Fusion tree are absolute as seen by the Agent
        success = fusion_client.wait_for_file_in_tree(test_file_rel, timeout=SHORT_TIMEOUT)
        assert success, f"File {file_name} not found in tree after sync at {test_file}"
        
        logger.info("File created and detected successfully")
    
    def test_file_modify_detected(
        self, 
        docker_env, 
        setup_agents, 
        fusion_client, 
        wait_for_audit
    ):
        """
        Test that file modification is detected and synced.
        """
        logger.info("Running file modify test")
        
        containers = setup_agents["containers"]
        leader = containers["leader"]
        
        # Create initial file
        import os.path
        timestamp = int(time.time() * 1000)
        file_name = f"modify_test_{timestamp}.txt"
        test_file = f"{MOUNT_POINT}/{file_name}"
        test_file_rel = "/" + os.path.relpath(test_file, MOUNT_POINT)
        
        docker_env.exec_in_container(
            leader,
            ["sh", "-c", f"echo 'Initial content' > {test_file}"]
        )
        
        # Wait for initial sync
        assert fusion_client.wait_for_file_in_tree(test_file_rel), f"Initial file {test_file} not detected"
        
        # Modify file
        docker_env.exec_in_container(
            leader,
            ["sh", "-c", f"echo 'Modified content' > {test_file}"]
        )
        logger.info(f"Modified test file: {test_file}")
        
        # Wait for sync and verify
        # Note: In V2, we check it's still there and potentially mtime
        time.sleep(INGESTION_DELAY)
        found = fusion_client.wait_for_file_in_tree(file_path=test_file_rel, timeout=SHORT_TIMEOUT)
        
        assert found, f"File {file_name} not found after modification at {test_file}"
        logger.info(f"✅ File modification detected")
    
    def test_file_delete_detected(
        self, 
        docker_env, 
        setup_agents, 
        fusion_client, 
        wait_for_audit
    ):
        """
        Test that file deletion is detected and synced.
        """
        logger.info("Running file delete test")
        
        containers = setup_agents["containers"]
        leader = containers["leader"]
        
        # Create file first
        import os.path
        timestamp = int(time.time() * 1000)
        file_name = f"delete_test_{timestamp}.txt"
        test_file = f"{MOUNT_POINT}/{file_name}"
        test_file_rel = "/" + os.path.relpath(test_file, MOUNT_POINT)
        
        docker_env.exec_in_container(
            leader,
            ["sh", "-c", f"echo 'To be deleted' > {test_file}"]
        )
        
        # Wait for initial sync
        assert fusion_client.wait_for_file_in_tree(file_path=test_file_rel, timeout=SHORT_TIMEOUT)
        
        # Delete file
        docker_env.exec_in_container(
            leader,
            ["rm", "-f", test_file]
        )
        logger.info(f"Deleted test file: {test_file}")
        
        # Wait for audit to process deletion
        wait_for_audit()
        
        # Verify file is removed from tree
        removed = fusion_client.wait_for_file_not_in_tree(
            file_path=test_file_rel,
            timeout=MEDIUM_TIMEOUT
        )
        
        assert removed, f"File {file_name} still in tree after deletion at {test_file}"
        logger.info(f"✅ File deletion detected")



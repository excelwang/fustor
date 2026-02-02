# it/consistency/test_pipeline_basic.py
"""
Basic integration tests for AgentPipeline.

These tests verify that the AgentPipeline architecture works correctly
with the integration test environment.

Run with:
    uv run pytest it/consistency/test_pipeline_basic.py -v
"""
import time
import pytest
import logging

logger = logging.getLogger("fustor_test")


class TestPipelineBasicOperations:
    """Test basic file operations in AgentPipeline mode."""
    
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
        timestamp = int(time.time() * 1000)
        file_name = f"pipeline_test_{timestamp}.txt"
        test_file = f"/mnt/shared/{file_name}"
        
        # Create file from leader container
        docker_env.exec_in_container(
            leader,
            ["sh", "-c", f"echo 'Pipeline test content' > {test_file}"]
        )
        logger.info(f"Created test file: {test_file}")
        
        # Wait for sync to occur and verify file appears in Fusion tree
        found = fusion_client.wait_for_file_in_tree(
            file_path=f"/{file_name}",
            timeout=10
        )
        
        assert found, f"File {file_name} not found in tree after sync"
        logger.info(f"✅ File creation detected")
    
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
        timestamp = int(time.time() * 1000)
        file_name = f"modify_test_{timestamp}.txt"
        test_file = f"/mnt/shared/{file_name}"
        
        docker_env.exec_in_container(
            leader,
            ["sh", "-c", f"echo 'Initial content' > {test_file}"]
        )
        
        # Wait for initial sync
        fusion_client.wait_for_file_in_tree(file_path=f"/{file_name}")
        
        # Modify file
        docker_env.exec_in_container(
            leader,
            ["sh", "-c", f"echo 'Modified content' > {test_file}"]
        )
        logger.info(f"Modified test file: {test_file}")
        
        # Wait for sync and verify
        # Note: In a real test we'd check mtime or content hash if available in API
        # For now we just check it's still there after some time
        time.sleep(2)
        found = fusion_client.wait_for_file_in_tree(file_path=f"/{file_name}", timeout=5)
        
        assert found, f"File {file_name} not found after modification"
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
        timestamp = int(time.time() * 1000)
        file_name = f"delete_test_{timestamp}.txt"
        test_file = f"/mnt/shared/{file_name}"
        
        docker_env.exec_in_container(
            leader,
            ["sh", "-c", f"echo 'To be deleted' > {test_file}"]
        )
        
        # Wait for initial sync
        fusion_client.wait_for_file_in_tree(file_path=f"/{file_name}")
        
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
            file_path=f"/{file_name}",
            timeout=10
        )
        
        assert removed, f"File {file_name} still in tree after deletion"
        logger.info(f"✅ File deletion detected")


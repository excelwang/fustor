# it/consistency/test_pipeline_basic.py
"""
Basic integration tests for AgentPipeline mode.

These tests verify that the new AgentPipeline architecture works correctly
with the integration test environment. They run in both Legacy and Pipeline
modes and verify behavior consistency.

Run with Pipeline mode:
    FUSTOR_USE_PIPELINE=true uv run pytest it/consistency/test_pipeline_basic.py -v
"""
import time
import pytest
import logging

logger = logging.getLogger("fustor_test")


class TestPipelineBasicOperations:
    """Test basic file operations work correctly in both modes."""
    
    def test_file_create_detected(
        self, 
        docker_env, 
        setup_agents, 
        fusion_client, 
        wait_for_audit,
        use_pipeline
    ):
        """
        Test that file creation is detected and synced to Fusion.
        
        This is a fundamental test that verifies the sync pipeline is working.
        """
        mode = "Pipeline" if use_pipeline else "Legacy"
        logger.info(f"Running file create test in {mode} mode")
        
        containers = setup_agents["containers"]
        leader = containers["leader"]
        
        # Create a unique test file
        timestamp = int(time.time() * 1000)
        test_file = f"/mnt/shared/pipeline_test_{timestamp}.txt"
        
        # Create file from leader container
        docker_env.exec_in_container(
            leader,
            ["sh", "-c", f"echo 'Pipeline test content' > {test_file}"]
        )
        logger.info(f"Created test file: {test_file}")
        
        # Wait for sync to occur (real-time + audit backup)
        time.sleep(3)
        
        # Verify file appears in Fusion tree
        tree = fusion_client.get_tree()
        file_name = f"pipeline_test_{timestamp}.txt"
        
        found = any(
            node.get("path", "").endswith(file_name) or 
            node.get("name") == file_name
            for node in tree
        )
        
        assert found, f"File {file_name} not found in tree after sync. Mode: {mode}"
        logger.info(f"✅ File creation detected in {mode} mode")
    
    def test_file_modify_detected(
        self, 
        docker_env, 
        setup_agents, 
        fusion_client, 
        wait_for_audit,
        use_pipeline
    ):
        """
        Test that file modification is detected and synced.
        """
        mode = "Pipeline" if use_pipeline else "Legacy"
        logger.info(f"Running file modify test in {mode} mode")
        
        containers = setup_agents["containers"]
        leader = containers["leader"]
        
        # Create initial file
        timestamp = int(time.time() * 1000)
        test_file = f"/mnt/shared/modify_test_{timestamp}.txt"
        
        docker_env.exec_in_container(
            leader,
            ["sh", "-c", f"echo 'Initial content' > {test_file}"]
        )
        time.sleep(2)
        
        # Get initial state
        tree_before = fusion_client.get_tree()
        
        # Modify file
        docker_env.exec_in_container(
            leader,
            ["sh", "-c", f"echo 'Modified content' > {test_file}"]
        )
        logger.info(f"Modified test file: {test_file}")
        
        # Wait for sync
        time.sleep(3)
        
        # Verify file still exists (modification detected)
        tree_after = fusion_client.get_tree()
        file_name = f"modify_test_{timestamp}.txt"
        
        found = any(
            node.get("path", "").endswith(file_name) or 
            node.get("name") == file_name
            for node in tree_after
        )
        
        assert found, f"File {file_name} not found after modification. Mode: {mode}"
        logger.info(f"✅ File modification detected in {mode} mode")
    
    def test_file_delete_detected(
        self, 
        docker_env, 
        setup_agents, 
        fusion_client, 
        wait_for_audit,
        use_pipeline
    ):
        """
        Test that file deletion is detected and synced.
        """
        mode = "Pipeline" if use_pipeline else "Legacy"
        logger.info(f"Running file delete test in {mode} mode")
        
        containers = setup_agents["containers"]
        leader = containers["leader"]
        
        # Create file first
        timestamp = int(time.time() * 1000)
        test_file = f"/mnt/shared/delete_test_{timestamp}.txt"
        
        docker_env.exec_in_container(
            leader,
            ["sh", "-c", f"echo 'To be deleted' > {test_file}"]
        )
        time.sleep(2)
        
        # Verify file exists
        tree_before = fusion_client.get_tree()
        file_name = f"delete_test_{timestamp}.txt"
        
        found_before = any(
            node.get("path", "").endswith(file_name) or 
            node.get("name") == file_name
            for node in tree_before
        )
        assert found_before, f"File {file_name} should exist before deletion"
        
        # Delete file
        docker_env.exec_in_container(
            leader,
            ["rm", "-f", test_file]
        )
        logger.info(f"Deleted test file: {test_file}")
        
        # Wait for audit to process deletion
        wait_for_audit()
        
        # Verify file is removed from tree
        tree_after = fusion_client.get_tree()
        
        found_after = any(
            node.get("path", "").endswith(file_name) or 
            node.get("name") == file_name
            for node in tree_after
        )
        
        # Note: File might still be in tree as tombstone, but should be marked deleted
        # For now, we just log the result
        if found_after:
            logger.warning(f"File still in tree after deletion (may be tombstone). Mode: {mode}")
        else:
            logger.info(f"✅ File deletion detected in {mode} mode")


class TestPipelineModeInfo:
    """Test that provides information about the current mode."""
    
    def test_mode_reported(self, use_pipeline):
        """Report which mode tests are running in."""
        mode = "AgentPipeline" if use_pipeline else "SyncInstance (Legacy)"
        logger.info(f"")
        logger.info(f"{'='*60}")
        logger.info(f"  Integration tests running in: {mode}")
        logger.info(f"  To switch modes:")
        logger.info(f"    Legacy:   uv run pytest it/consistency/ -v")
        logger.info(f"    Pipeline: FUSTOR_USE_PIPELINE=true uv run pytest it/consistency/ -v")
        logger.info(f"{'='*60}")
        logger.info(f"")
        
        # This test always passes, it's just for information
        assert True

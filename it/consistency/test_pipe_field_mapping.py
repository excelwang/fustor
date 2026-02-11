# it/consistency/test_pipe_field_mapping.py
"""
Integration test for Field Mapping in AgentPipe.
"""
import time
import pytest
import logging
from it.fixtures.constants import MOUNT_POINT, FUSION_ENDPOINT, MEDIUM_TIMEOUT, POLL_INTERVAL

logger = logging.getLogger("fustor_test")

class TestPipeFieldMapping:
    """Test field mapping functionality in AgentPipe."""
    
    def test_field_mapping_affects_data(
        self, 
        docker_env, 
        setup_agents, 
        fusion_client
    ):
        """
        Test that field mapping correctly transforms data.
        We will map 'size' to 'remapped_size' so that Fusion sees default size (0).
        """
        logger.info("Running field mapping test")
        
        containers = setup_agents["containers"]
        leader = containers["leader"]
        view_id = setup_agents["view_id"]
        api_key = setup_agents["api_key"]
        
        # 1. Update Agent Config to include fields_mapping
        # Map: path -> path, modified_time -> modified_time, is_directory -> is_directory, size -> remapped_size
        pipe_config = f"""
pipes:
  integration-test-ds:
    fields_mapping:
      - to: "path"
        source: ["path:string"]
      - to: "modified_time"
        source: ["modified_time:number"]
      - to: "is_directory"
        source: ["is_directory:boolean"]
      - to: "created_time"
        source: ["created_time:number"]
      # Test mapping: Map standard 'size' to a custom field 'remapped_size'
      # This verifies that the 'size' field in Fusion becomes 0 (default) because its source was redirected.
      - to: "remapped_size"
        source: ["size:integer"]
"""
        docker_env.create_file_in_container(
            leader, 
            "/root/.fustor/agent-config/pipe-task-1.yaml", 
            pipe_config
        )
        
        # 2. Restart Agent to apply config
        logger.info(f"Restarting agent in {leader} to apply fields_mapping")
        
        # Reset Fusion state to ensure a clean start for the new mapping
        fusion_client.reset()
        
        # To ensure Agent A becomes leader, stop Agent B
        docker_env.exec_in_container(containers["follower"], ["pkill", "-9", "-f", "fustor-agent"])
        docker_env.exec_in_container(leader, ["pkill", "-9", "-f", "fustor-agent"])
        
        # Wait for sessions to clear
        start_wait = time.time()
        while time.time() - start_wait < MEDIUM_TIMEOUT:
            sessions = fusion_client.get_sessions()
            if not sessions:
                break
            for s in sessions:
                try:
                    fusion_client.terminate_session(s["session_id"])
                except Exception:
                    pass
            time.sleep(POLL_INTERVAL)
            
        docker_env.exec_in_container(
            leader, 
            ["sh", "-c", "FUSTOR_USE_PIPELINE=true fustor-agent start -V > /proc/1/fd/1 2>&1"],
            detached=True
        )
        
        # Wait for agent to reconnect and become leader
        logger.info("Waiting for Agent A to become leader...")
        start_wait = time.time()
        while time.time() - start_wait < MEDIUM_TIMEOUT:
            sessions = fusion_client.get_sessions()
            # Ensure it's the ONLY session and it's leader client-a
            if len(sessions) == 1 and sessions[0].get("role") == "leader" and "client-a" in sessions[0].get("agent_id", ""):
                logger.info(f"Agent A successfully became leader: {sessions[0].get('session_id')}")
                break
            time.sleep(POLL_INTERVAL)
        else:
             pytest.fail(f"Agent A failed to become leader. Current sessions: {fusion_client.get_sessions()}")
        
        # 3. Create a file with specific size
        import os.path
        timestamp = int(time.time() * 1000)
        file_name = f"mapping_test_{timestamp}.txt"
        test_file = f"{MOUNT_POINT}/{file_name}"
        test_file_rel = "/" + os.path.relpath(test_file, MOUNT_POINT)
        expected_size = 1234
        
        # Create file with 1234 bytes
        # Use dd for cleaner file creation
        docker_env.exec_in_container(
            leader,
            ["sh", "-c", f"dd if=/dev/zero of={test_file} bs={expected_size} count=1"]
        )
        logger.info(f"Created test file with size {expected_size}: {test_file}")
        
        # 4. Wait for Fusion to detect it
        # FSDriver should use absolute paths by default (matching schema 'Absolute file path')
        expected_path_in_tree = test_file_rel
        success = fusion_client.wait_for_file_in_tree(expected_path_in_tree, timeout=MEDIUM_TIMEOUT)
        assert success, f"File {expected_path_in_tree} not found in tree. Tree: {fusion_client.get_tree()}"
        
        # 5. Verify size in Fusion
        # Because we mapped 'size' to 'remapped_size', and Fusion expects 'size',
        # Fusion should see the default value (0) instead of 1234.
        node = fusion_client.get_node(expected_path_in_tree)
        assert node is not None, f"Node {expected_path_in_tree} should exist but get_node returned None."
        
        logger.info(f"Fusion node data: {node}")
        
        actual_size = node.get('size')
        
        assert actual_size in (0, None), f"Expected size 0 or None (due to mapping), but got {actual_size}"
        logger.info("✅ Field mapping confirmed: 'size' was correctly redirected to 'remapped_size', causing Fusion to see 0.")

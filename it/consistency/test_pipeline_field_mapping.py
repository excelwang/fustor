# it/consistency/test_pipeline_field_mapping.py
"""
Integration test for Field Mapping in AgentPipeline.
"""
import time
import pytest
import logging
from it.fixtures.constants import MOUNT_POINT, FUSION_ENDPOINT, MEDIUM_TIMEOUT, POLL_INTERVAL

logger = logging.getLogger("fustor_test")

class TestPipelineFieldMapping:
    """Test field mapping functionality in AgentPipeline."""
    
    def test_field_mapping_affects_data(
        self, 
        docker_env, 
        setup_agents, 
        fusion_client
    ):
        """
        Test that field mapping correctly transforms data.
        We will map 'size' to 'wrong_size' so that Fusion sees default size (0).
        """
        logger.info("Running field mapping test")
        
        containers = setup_agents["containers"]
        leader = containers["leader"]
        view_id = setup_agents["view_id"]
        api_key = setup_agents["api_key"]
        
        # 1. Update Agent Config to include fields_mapping
        # Map: file_path -> path, modified_time -> modified_time, is_dir -> is_dir, size -> wrong_size
        pipeline_config = f"""
id: "pipeline-task-1"
source: "shared-fs"
sender: "fusion"
disabled: false
fields_mapping:
  - to: "files.path"
    source: ["file_path:string"]
  - to: "files.modified_time"
    source: ["modified_time:number"]
  - to: "files.is_dir"
    source: ["is_dir:boolean"]
  - to: "files.wrong_size"
    source: ["size:integer"]
"""
        docker_env.create_file_in_container(
            leader, 
            "/root/.fustor/agent-pipes-config/pipeline-task-1.yaml", 
            pipeline_config
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
        timestamp = int(time.time() * 1000)
        file_name = f"mapping_test_{timestamp}.txt"
        test_file = f"{MOUNT_POINT}/{file_name}"
        expected_size = 1234
        
        # Create file with 1234 bytes
        docker_env.exec_in_container(
            leader,
            ["sh", "-c", f"head -c {expected_size} /dev/zero > {test_file}"]
        )
        logger.info(f"Created test file with size {expected_size}: {test_file}")
        
        # 4. Wait for Fusion to detect it
        # Based on the tree dump, Fusion stores the absolute path as seen by the Agent
        success = fusion_client.wait_for_file_in_tree(test_file, timeout=MEDIUM_TIMEOUT)
        assert success, f"File {test_file} not found in tree. Tree: {fusion_client.get_tree()}"
        
        # 5. Verify size in Fusion
        # Because we mapped 'size' to 'wrong_size', and Fusion expects 'size',
        # Fusion should see the default value (0) instead of 1234.
        node = fusion_client.get_node(test_file)
        
        logger.info(f"Fusion node data: {node}")
        
        actual_size = node.get('size') if node else None
        
        assert actual_size == 0, f"Expected size 0 (due to mapping), but got {actual_size}"
        logger.info("✅ Field mapping confirmed: 'size' was correctly redirected to 'wrong_size', causing Fusion to see 0.")


"""
Integration tests for advanced deployment scenarios.
- Fan-Out (1 Agent -> Multi Views)
- Aggregation (Multi Pipes -> Single View)
- HA Dynamic Adjustment (Config Reload)
"""
import pytest
import time
import os
import yaml
from pathlib import Path
import logging
import subprocess
from ..fixtures.constants import (
    SHORT_TIMEOUT, MEDIUM_TIMEOUT, INGESTION_DELAY, 
    LONG_TIMEOUT, AGENT_READY_TIMEOUT, EXTREME_TIMEOUT
)

logger = logging.getLogger("fustor_test")

# Container name for Fusion
CONTAINER_FUSION = "fustor-fusion"

# Paths inside containers (processed config, NOT templates)
FUSION_PROCESSED_CONFIG_DIR = "/root/.fustor/fusion-config"
AGENT_PROCESSED_CONFIG_DIR = "/root/.fustor/agent-config"


@pytest.fixture
def extra_fusion_config():
    """Create extra YAML config files in Fusion's processed config directory.
    
    Creates files directly in /root/.fustor/fusion-config/ (the processed directory),
    bypassing envsubst. Values must be already resolved (no ${...} vars).
    Cleans up created files on teardown.
    """
    created_files = []  # list of container config paths
    
    def _create(filename, content):
        """Create a YAML config file directly in Fusion container.
        
        Args:
            filename: Config filename (e.g., "extra_fanout.yaml")
            content: Dict to write as YAML. All values must be resolved.
        """
        container_path = f"{FUSION_PROCESSED_CONFIG_DIR}/{filename}"
        
        # Write YAML content via docker exec
        yaml_str = yaml.dump(content, default_flow_style=False)
        # Escape single quotes in YAML for shell command
        yaml_str_escaped = yaml_str.replace("'", "'\\''")
        
        subprocess.check_call([
            "docker", "exec", CONTAINER_FUSION,
            "sh", "-c", f"cat > {container_path} << 'YAML_EOF'\n{yaml_str}\nYAML_EOF"
        ])
        logger.info(f"Created Fusion extra config: {container_path}")
        created_files.append(container_path)
    
    yield _create
    
    # Cleanup
    for path in created_files:
        try:
            subprocess.call(["docker", "exec", CONTAINER_FUSION, "rm", "-f", path])
            logger.info(f"Removed Fusion extra config: {path}")
        except Exception as e:
            logger.warning(f"Could not remove {path}: {e}")


@pytest.fixture
def extra_agent_config():
    """Create extra YAML config files in Agent's processed config directory.
    
    Similar to extra_fusion_config but targets Agent containers.
    Cleans up on teardown.
    """
    created_files = []  # list of (container, container_path) tuples
    
    def _create(container_name, filename, content):
        """Create a YAML config file directly in Agent container.
        
        Args:
            container_name: Container name
            filename: Config filename
            content: Dict to write as YAML
        """
        container_path = f"{AGENT_PROCESSED_CONFIG_DIR}/{filename}"
        
        yaml_str = yaml.dump(content, default_flow_style=False)
        
        subprocess.check_call([
            "docker", "exec", container_name,
            "sh", "-c", f"cat > {container_path} << 'YAML_EOF'\n{yaml_str}\nYAML_EOF"
        ])
        logger.info(f"Created Agent extra config in {container_name}: {container_path}")
        created_files.append((container_name, container_path))
    
    yield _create
    
    # Cleanup
    for container, path in created_files:
        try:
            subprocess.call(["docker", "exec", container, "rm", "-f", path])
            logger.info(f"Removed Agent extra config {path} from {container}")
        except Exception as e:
            logger.warning(f"Could not remove {path} from {container}: {e}")


class TestAdvancedDeployments:

    def test_fan_out_deployment(
        self, docker_env, setup_agents, fusion_client, extra_fusion_config
    ):
        """
        Test Scenario: Fan-Out (One Agent -> Multiple Views)
        
        The default config already defines the 'archive-fanout' view.
        This test overrides the pipe config to fan-out events to BOTH views.
        """
        view_id = os.environ.get("TEST_VIEW_ID", "integration-test-ds")
        extra_view_id = "archive-fanout"
        
        # The default config already has 'archive-fanout' view defined.
        # We just need to update the pipe to fan-out to both views.
        # Since FusionConfigLoader overwrites pipes with same ID,
        # we create an extra config that redefines the pipe with all required fields.
        extra_fusion_config("extra_fanout.yaml", {
            "pipes": {
                view_id: {
                    "receiver": "http-main",
                    "views": [view_id, extra_view_id],
                    "audit_interval_sec": 10.0,
                    "sentinel_interval_sec": 5.0,
                    "session_timeout_seconds": 5,
                }
            }
        })
        
        # Restart Fusion to pick up the extra config
        subprocess.check_call(["docker", "restart", CONTAINER_FUSION])
        
        logger.info("Waiting for Fusion to reload with fan-out config...")
        assert fusion_client.wait_for_view_ready(timeout=EXTREME_TIMEOUT), \
            "Fusion did not become ready after restart with fan-out config"
        
        # Write data via leader
        containers = setup_agents["containers"]
        leader = containers["leader"]
        timestamp = int(time.time())
        filename = f"fanout_{timestamp}.txt"
        docker_env.exec_in_container(leader, ["sh", "-c", f"echo 'fanout' > /mnt/shared/{filename}"])

        # Verify file appears in primary view
        logger.info(f"Checking file in primary view: {view_id}")
        assert fusion_client.wait_for_file_in_tree(f"/{filename}", timeout=EXTREME_TIMEOUT), \
            f"File not found in primary view {view_id}"
        
        # Verify file also appears in fan-out view
        logger.info(f"Checking file in fan-out view: {extra_view_id}")
        original_view = fusion_client.view_id
        fusion_client.view_id = extra_view_id
        try:
            assert fusion_client.wait_for_file_in_tree(f"/{filename}", timeout=EXTREME_TIMEOUT), \
                f"File not found in fan-out view {extra_view_id}"
        finally:
            fusion_client.view_id = original_view

    def test_aggregation_deployment(
        self, docker_env, setup_agents, fusion_client, 
        extra_fusion_config, extra_agent_config
    ):
        """
        Test Scenario: Aggregation (Multiple Pipes -> Single View)
        
        Create a second pipe (pipe-agg) that feeds into the same view.
        Agent monitors a separate directory via this second pipe.
        """
        view_id = os.environ.get("TEST_VIEW_ID", "integration-test-ds")
        agg_pipe_id = "pipe-agg"
        agg_source_id = "source-agg"
        
        # 1. Add new pipe to Fusion
        extra_fusion_config("extra_agg.yaml", {
            "pipes": {
                agg_pipe_id: {
                    "receiver": "http-main",
                    "views": [view_id],
                    "audit_interval_sec": 10.0,
                    "sentinel_interval_sec": 5.0,
                    "session_timeout_seconds": 5,
                }
            }
        })
        
        # 2. Add new source + pipe to Agent (leader)
        containers = setup_agents["containers"]
        leader = containers["leader"]
        
        extra_agent_config(leader, "extra_agg.yaml", {
            "sources": {
                agg_source_id: {
                    "driver": "fs",
                    "uri": "/mnt/shared/aggregated"
                }
            },
            "pipes": {
                agg_pipe_id: {
                    "source": agg_source_id,
                    "sender": "fusion-main"
                }
            }
        })
        
        # 3. Create the aggregated directory
        docker_env.exec_in_container(leader, ["mkdir", "-p", "/mnt/shared/aggregated"])
        
        # 4. Restart Fusion and reload Agent
        subprocess.check_call(["docker", "restart", CONTAINER_FUSION])
        logger.info("Waiting for Fusion to reload with aggregation config...")
        assert fusion_client.wait_for_view_ready(timeout=EXTREME_TIMEOUT), \
            "Fusion did not become ready after restart with aggregation config"
        
        # Reload agent config via SIGHUP
        docker_env.exec_in_container(leader, ["pkill", "-HUP", "-f", "fustor-agent"])
        logger.info("Sent SIGHUP to agent for config reload. Waiting...")
        time.sleep(10)  # Give agent time to reload config and reconnect
        
        # 5. Write to aggregated source directory
        timestamp = int(time.time())
        filename = f"agg_{timestamp}.txt"
        docker_env.exec_in_container(
            leader, ["sh", "-c", f"echo 'aggregated data' > /mnt/shared/aggregated/{filename}"]
        )

        # 6. Verify in view (the aggregated pipe feeds into the same view)
        logger.info(f"Checking aggregated file in view: {view_id}")
        assert fusion_client.wait_for_file_in_tree(f"/{filename}", timeout=EXTREME_TIMEOUT), \
            f"Aggregated file not found in view {view_id}"

    def test_ha_dynamic_adjustment(
        self, docker_env, setup_agents, fusion_client, extra_fusion_config
    ):
        """
        Test Scenario: HA Cluster Configuration Reload
        
        Verify that global fusion config can be changed dynamically via extra config.
        Creates an extra config with session_cleanup_interval change,
        restarts Fusion, verifies it comes back healthy.
        """
        # Create extra config with modified global setting
        extra_fusion_config("extra_ha.yaml", {
            "fusion": {
                "session_cleanup_interval": 11.0
            }
        })
        
        subprocess.check_call(["docker", "restart", CONTAINER_FUSION])
        logger.info("Waiting for Fusion to reload with HA config...")
        assert fusion_client.wait_for_view_ready(timeout=LONG_TIMEOUT), \
            "Fusion did not become ready after HA config reload"
        
        # Verify Fusion is operational by checking we can get stats
        stats = fusion_client.get_stats()
        assert stats is not None, "Could not get stats after HA config reload"
        logger.info(f"HA config reload successful. Stats: {stats}")

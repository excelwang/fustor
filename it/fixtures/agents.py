# it/fixtures/agents.py
"""
Agent setup and configuration fixtures for integration tests.
"""
import os
import sys
import time
import pytest
import logging
from pathlib import Path

# Ensure parent directory is in path
_fixtures_dir = Path(__file__).parent
_it_dir = _fixtures_dir.parent
if str(_it_dir) not in sys.path:
    sys.path.insert(0, str(_it_dir))

from utils import docker_manager

logger = logging.getLogger("fustor_test")

from .constants import (
    CONTAINER_CLIENT_A, 
    CONTAINER_CLIENT_B, 
    CONTAINER_CLIENT_C, 
    MOUNT_POINT, 
    AUDIT_INTERVAL,
    SENTINEL_INTERVAL,
    FUSION_ENDPOINT,
    HEARTBEAT_INTERVAL,
    THROTTLE_INTERVAL_SEC,
    AGENT_READY_TIMEOUT,
    AGENT_B_READY_TIMEOUT,
    VIEW_READY_TIMEOUT,
    FAST_POLL_INTERVAL,
    SESSION_TIMEOUT
)




def ensure_agent_running(container_name, api_key, view_id, mount_point=MOUNT_POINT):
    """
    Ensure agent is configured and running in the container.
    
    Args:
        container_name: Docker container name
        api_key: API key for authentication
        view_id: View ID for the pipeline
        mount_point: Path to the NFS mount point
    """
    fusion_endpoint = FUSION_ENDPOINT
    
    # Generate unique agent ID
    agent_id = f"{container_name.replace('fustor-nfs-', '')}-{os.urandom(2).hex()}"
    
    # Ensure config dir exists
    docker_manager.exec_in_container(container_name, ["mkdir", "-p", "/root/.fustor"])

    docker_manager.create_file_in_container(
        container_name,
        "/root/.fustor/agent.id",
        content=agent_id
    )

    # 1. Sources Config
    sources_config = f"""
shared-fs:
  driver: "fs"
  uri: "{mount_point}"
  credential:
    user: "unused"
  disabled: false
  driver_params:
    throttle_interval_sec: {THROTTLE_INTERVAL_SEC}
"""
    docker_manager.create_file_in_container(container_name, "/root/.fustor/sources-config.yaml", sources_config)

    # 2. Senders Config
    senders_config = f"""
fusion:
  driver: "fusion"
  uri: "{fusion_endpoint}"
  credential:
    key: "{api_key}"
  disabled: false
  driver_params:
    view_id: {view_id}
    api_version: "pipe"
"""
    docker_manager.create_file_in_container(container_name, "/root/.fustor/senders-config.yaml", senders_config)

    # 3. Pipelines Config
    pipes_dir = "/root/.fustor/agent-pipes-config"
    docker_manager.exec_in_container(container_name, ["mkdir", "-p", pipes_dir])
    
    pipelines_config = f"""
id: "pipeline-task-1"
source: "shared-fs"
sender: "fusion"
disabled: false
audit_interval_sec: {AUDIT_INTERVAL}
sentinel_interval_sec: {SENTINEL_INTERVAL}
heartbeat_interval_sec: {HEARTBEAT_INTERVAL}
"""
    docker_manager.create_file_in_container(container_name, f"{pipes_dir}/pipeline-task-1.yaml", pipelines_config)
    
    # 4. Kill existing agent if running and clean up pid/state files
    docker_manager.exec_in_container(container_name, ["pkill", "-f", "fustor-agent"])
    docker_manager.exec_in_container(container_name, ["rm", "-f", "/root/.fustor/agent.pid"])
    docker_manager.exec_in_container(container_name, ["rm", "-f", "/root/.fustor/agent-state.json"])
    time.sleep(FAST_POLL_INTERVAL)
    
    # 5. Start new agent (Always in Pipeline mode)
    logger.info(f"Starting agent in {container_name} with AgentPipeline mode")
    env_prefix = "FUSTOR_USE_PIPELINE=true "
    
    docker_manager.exec_in_container(
        container_name, 
        ["sh", "-c", f"{env_prefix}fustor-agent start -V > /proc/1/fd/1 2>&1"],
        detached=True
    )


@pytest.fixture
def setup_agents(docker_env, fusion_client, test_api_key, test_view):
    """
    Ensure agents are running and healthy.
    """
    view_id = test_view["id"]
    api_key = test_api_key["key"]
    
    # Start Agent A first (Cleanup handled by conftest.py)
    logger.info(f"Configuring and starting agent in {CONTAINER_CLIENT_A}...")
    ensure_agent_running(CONTAINER_CLIENT_A, api_key, view_id)
    
    # Wait for A to become Leader and Ready
    logger.info("Waiting for Agent A to be ready (Leader + Realtime Ready)...")
    if not fusion_client.wait_for_agent_ready("client-a", timeout=AGENT_READY_TIMEOUT):
        raise RuntimeError(f"Agent A did not become ready (can_realtime=True) within {AGENT_READY_TIMEOUT} seconds")
    
    sessions = fusion_client.get_sessions()
    leader = next((s for s in sessions if "client-a" in s.get("agent_id", "")), None)
    if not leader or leader.get("role") != "leader":
        raise RuntimeError(f"Agent A registered but not as leader ({leader.get('role') if leader else 'not found'})")
    
    logger.info(f"Agent A successfully became leader and is ready: {leader.get('agent_id')}")

    # Wait for View to be READY (Snapshot complete)
    logger.info("Waiting for View to be ready (initial snapshot completion)...")
    if not fusion_client.wait_for_view_ready(timeout=VIEW_READY_TIMEOUT):
        logger.warning("View readiness check timed out. Proceeding anyway.")
    else:
        logger.info("View is READY.")

    # Start Agent B as Follower
    logger.info(f"Configuring and starting agent in {CONTAINER_CLIENT_B}...")
    ensure_agent_running(CONTAINER_CLIENT_B, api_key, view_id)
    
    # Wait for Agent B to be Ready
    logger.info("Waiting for Agent B to be ready (Follower + Realtime Ready)...")
    if not fusion_client.wait_for_agent_ready("client-b", timeout=AGENT_B_READY_TIMEOUT):
        logs = docker_manager.get_logs(CONTAINER_CLIENT_B)
        logger.warning(f"Timeout waiting for Agent B (>{AGENT_B_READY_TIMEOUT}s). Logs:\n{logs}")
        # Proceeding anyway for some tests, though most will fail

    return {
        "api_key": api_key,
        "view_id": view_id,
        "containers": {
            "leader": CONTAINER_CLIENT_A,
            "follower": CONTAINER_CLIENT_B,
            "blind": CONTAINER_CLIENT_C
        },
        "ensure_agent_running": ensure_agent_running
    }

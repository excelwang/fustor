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
    FUSION_ENDPOINT
)




def ensure_agent_running(container_name, api_key, view_id, mount_point=MOUNT_POINT):
    """
    Ensure agent is configured and running in the container.
    
    Args:
        container_name: Docker container name
        api_key: API key for authentication
        view_id: View ID for the sync
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
    throttle_interval_sec: 0.5
"""
    docker_manager.create_file_in_container(container_name, "/root/.fustor/sources-config.yaml", sources_config)

    # 2. Senders Config
    senders_config = f"""
fusion:
  driver: "fusion"
  endpoint: "{fusion_endpoint}"
  credential:
    key: "{api_key}"
  disabled: false
  driver_params:
    view_id: {view_id}
    api_version: "pipe"
"""
    docker_manager.create_file_in_container(container_name, "/root/.fustor/senders-config.yaml", senders_config)

    # 3. Syncs Config
    docker_manager.exec_in_container(container_name, ["mkdir", "-p", "/root/.fustor/syncs-config"])
    syncs_config = f"""
id: "sync-task-1"
source: "shared-fs"
sender: "fusion"
disabled: false
audit_interval_sec: {AUDIT_INTERVAL}
sentinel_interval_sec: {SENTINEL_INTERVAL}
"""
    docker_manager.create_file_in_container(container_name, "/root/.fustor/syncs-config/sync-task-1.yaml", syncs_config)
    
    # 4. Kill existing agent if running and clean up pid/state files
    docker_manager.exec_in_container(container_name, ["pkill", "-f", "fustor-agent"])
    docker_manager.exec_in_container(container_name, ["rm", "-f", "/root/.fustor/agent.pid"])
    docker_manager.exec_in_container(container_name, ["rm", "-f", "/root/.fustor/agent-state.json"])
    time.sleep(0.2)
    
    # 5. Start new agent (Always in Pipeline mode)
    logger.info(f"Starting agent in {container_name} with AgentPipeline mode")
    env_prefix = "FUSTOR_USE_PIPELINE=true "
    
    docker_manager.exec_in_container(
        container_name, 
        ["sh", "-c", f"{env_prefix}fustor-agent start -V > /proc/1/fd/1 2>&1"],
        detached=True
    )


@pytest.fixture(scope="session")
def setup_agents(docker_env, fusion_client, test_api_key, test_view):
    """
    Ensure agents are running and healthy.
    """
    view_id = test_view["id"]
    api_key = test_api_key["key"]
    
    # Clean Slate: Stop all agents first
    logger.info("Cleaning up existing agents preventing stale leadership...")
    for container in [CONTAINER_CLIENT_A, CONTAINER_CLIENT_B, CONTAINER_CLIENT_C]:
        try:
            # First, ensure processes are running so they can receive signals, 
            # then kill them forcefully.
            docker_manager.exec_in_container(container, ["sh", "-c", "pkill -CONT -f fustor-agent || true"])
            docker_manager.exec_in_container(container, ["pkill", "-9", "-f", "fustor-agent"])
        except Exception:
            pass

    # Wait for Fusion sessions to expire (max 5s to safely cover 3s timeout)
    logger.info("Waiting for stale sessions to expire (max 5s)...")
    start_cleanup = time.time()
    while time.time() - start_cleanup < 5:
        sessions = fusion_client.get_sessions()
        if not sessions:
            logger.info("All sessions cleared.")
            break
        time.sleep(0.5)
    else:
        logger.warning(f"Some sessions still active after cleanup: {fusion_client.get_sessions()}")

    # Start Agent A first
    logger.info(f"Configuring and starting agent in {CONTAINER_CLIENT_A}...")
    ensure_agent_running(CONTAINER_CLIENT_A, api_key, view_id)
    
    # Wait for A to become Leader
    logger.info("Waiting for Agent A to register and become Leader...")
    start_wait = time.time()
    while time.time() - start_wait < 30:
        sessions = fusion_client.get_sessions()
        leader = next((s for s in sessions if s.get("role") == "leader"), None)
        if leader:
            agent_id = leader.get("agent_id", "")
            if "client-a" in agent_id:
                logger.info(f"Agent A successfully became leader: {agent_id}")
                break
            else:
                logger.warning(f"Someone else is leader: {agent_id}. Waiting...")
        time.sleep(1)
    else:
        raise RuntimeError("Agent A did not become leader within 30 seconds")

    # Wait for Datastore to be READY (Snapshot complete)
    logger.info("Waiting for Datastore to be ready (initial snapshot completion)...")
    start_ready = time.time()
    while time.time() - start_ready < 30:
        try:
            fusion_client.get_stats()
            logger.info("Datastore is READY.")
            break
        except Exception:
            time.sleep(0.5)
    else:
        logger.warning("Datastore readiness check timed out. Proceeding anyway.")

    # Start Agent B as Follower
    logger.info(f"Configuring and starting agent in {CONTAINER_CLIENT_B}...")
    ensure_agent_running(CONTAINER_CLIENT_B, api_key, view_id)
    
    # Wait for Agent B to register
    logger.info("Waiting for Agent B to register as Follower...")
    start_wait = time.time()
    while time.time() - start_wait < 30:
        sessions = fusion_client.get_sessions()
        agent_b = next((s for s in sessions if "client-b" in s.get("agent_id", "")), None)
        if agent_b:
            logger.info(f"Agent B registered: {agent_b.get('agent_id')} (Role: {agent_b.get('role')})")
            break
        time.sleep(0.5)
    else:
        logs = docker_manager.get_logs(CONTAINER_CLIENT_B)
        logger.warning(f"Timeout waiting for Agent B. Logs:\n{logs}")

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

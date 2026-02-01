"""
Pytest configuration and fixtures for NFS multi-mount consistency integration tests.
"""
import os
import pytest
import time
import logging
from pathlib import Path

from .utils import docker_manager, FusionClient

# Setup test logger
logger = logging.getLogger("fustor_test")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# Test configuration
TEST_TIMEOUT = int(os.getenv("FUSTOR_TEST_TIMEOUT", "600"))

# Timing Hierarchy
# NFS actimeo=1 (set in docker-compose.yml)
ACTIMEO = 1 
AUDIT_INTERVAL = 5

# Container names
CONTAINER_NFS_SERVER = "fustor-nfs-server"
CONTAINER_FUSION = "fustor-fusion"
CONTAINER_CLIENT_A = "fustor-nfs-client-a"
CONTAINER_CLIENT_B = "fustor-nfs-client-b"
CONTAINER_CLIENT_C = "fustor-nfs-client-c"

# Shared mount point inside containers
MOUNT_POINT = "/mnt/shared"

@pytest.fixture(scope="session")
def docker_env():
    """
    Session-scoped fixture that manages the Docker Compose environment.
    Initializes Fusion with static configuration.
    """
    logger.info("Checking Docker Compose environment (Auto-Reuse Mode)")
    # Check if environment is up
    try:
        docker_manager.exec_in_container(CONTAINER_NFS_SERVER, ["ls", "/"])
        is_up = True
    except Exception:
        is_up = False
        
    if not is_up:
        logger.info("Environment not running. Starting it automatically...")
        docker_manager.up(build=True, wait=True)
    else:
        # Check individual container health
        for container in [CONTAINER_NFS_SERVER, CONTAINER_FUSION]:
            if not docker_manager.wait_for_health(container, timeout=30):
                logger.warning(f"Container {container} not healthy. Repairing ecosystem...")
                docker_manager.up(build=True, wait=True)
                break
    
    # --- NEW: Inject Fusion Configuration ---
    logger.info("Injecting static configuration into Fusion...")
    
    # 1. Create .fustor directory
    docker_manager.exec_in_container(CONTAINER_FUSION, ["mkdir", "-p", "/root/.fustor/views-config"])
    
    # 2. Inject Receivers Config (v2: renamed from 'datastores')
    receivers_config = """
integration-test-ds:
  api_key: "test-api-key-123"
  session_timeout_seconds: 3
  allow_concurrent_push: true
"""
    docker_manager.create_file_in_container(CONTAINER_FUSION, "/root/.fustor/receivers-config.yaml", receivers_config)
    
    # 3. Inject View Config
    view_config = """
id: "test-fs"
datastore_id: "integration-test-ds"
driver: "fs"
disabled: false
driver_params:
  uri: "/mnt/shared-view"
  hot_file_threshold: 10.0
"""
    docker_manager.create_file_in_container(CONTAINER_FUSION, "/root/.fustor/views-config/test-fs.yaml", view_config)
    
    # 4. Reload Fusion (Restarting is safer to ensure all services pickup the new YAML)
    docker_manager.restart_container(CONTAINER_FUSION)
    docker_manager.wait_for_health(CONTAINER_FUSION)
    
    # 5. Simulate Clock Skew (Testing Logical Clock synchronization)
    # NFS Server: 2 hours AHEAD (files will have future mtime)
    # Client B: 1 hour BEHIND
    # Fusion & Client A: Normal time (reference)
    logger.info("Simulating clock skew between containers...")
    # 5. Log Environment Skew (configured via libfaketime in docker-compose.yml)
    logger.info("Clock skew environment active: A:+2h, B:-1h, Fusion/NFS/Host:0")
    try:
        for container in [CONTAINER_FUSION, CONTAINER_CLIENT_A, CONTAINER_CLIENT_B, CONTAINER_NFS_SERVER]:
            t = docker_manager.exec_in_container(container, ["date", "-u"])
            logger.info(f"Container {container} UTC time: {t.stdout.strip()}")
    except Exception as e:
        logger.warning(f"Could not log container times: {e}")

    logger.info("All containers healthy and Fusion configured with clock skew.")
    yield docker_manager
    logger.info("Keeping Docker Compose environment running")


@pytest.fixture(scope="session")
def test_datastore() -> dict:
    """Return static test datastore info."""
    return {
        "id": "integration-test-ds",
        "name": "integration-test-ds",
        "allow_concurrent_push": True,
        "session_timeout_seconds": 3
    }


@pytest.fixture(scope="session")
def test_api_key(test_datastore) -> dict:
    """Return static API key info."""
    return {
        "key": "test-api-key-123",
        "datastore_id": "integration-test-ds",
        "name": "integration-test-key"
    }



@pytest.fixture(scope="session")
def fusion_client(docker_env, test_api_key) -> FusionClient:
    """Create Fusion client with API key."""
    client = FusionClient(base_url="http://localhost:18102", view_id="test-fs")
    client.set_api_key(test_api_key["key"])
    
    # Wait for Fusion to be ready to accept requests and sync its cache
    logger.info("Waiting for Fusion to become ready and sync cache...")
    for i in range(60):
        try:
            # Use get_sessions which doesn't require snapshot completion (unlike get_stats/get_tree)
            client.get_sessions()
            logger.info(f"Fusion ready and API key synced after {i+1} seconds")
            break
        except Exception:
            time.sleep(1)
    else:
        raise RuntimeError("Fusion did not become ready within 60 seconds")
    
    return client


def ensure_agent_running(container_name, api_key, datastore_id):
    """Ensure the fustor-agent is configured and running in the given container."""
    # First, guarantee the container itself is running (might have been stopped by failover tests)
    docker_manager.start_container(container_name)
    
    agent_id = "agent-a" if container_name == CONTAINER_CLIENT_A else "agent-b"
    agent_port = 8100
    mount_point = MOUNT_POINT  # /mnt/shared
    fusion_endpoint = "http://fustor-fusion:8102"
    
    # 0. Initialize Fustor home and agent.id
    docker_manager.exec_in_container(container_name, ["mkdir", "-p", "/root/.fustor/schemas"])
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

    # 2. Senders Config (v2: renamed from 'pushers')
    senders_config = f"""
fusion:
  driver: "fusion"
  endpoint: "{fusion_endpoint}"
  credential:
    key: "{api_key}"
  disabled: false
  driver_params:
    datastore_id: {datastore_id}
"""
    docker_manager.create_file_in_container(container_name, "/root/.fustor/senders-config.yaml", senders_config)

    # 3. Syncs Config
    docker_manager.exec_in_container(container_name, ["mkdir", "-p", "/root/.fustor/syncs-config"])
    syncs_config = f"""
id: "sync-task-1"
source: "shared-fs"
sender: "fusion"  # v2: renamed from 'pusher'
disabled: false
audit_interval_sec: {AUDIT_INTERVAL}
sentinel_interval_sec: 1
"""
    docker_manager.create_file_in_container(container_name, "/root/.fustor/syncs-config/sync-task-1.yaml", syncs_config)
    
    # 3. Kill existing agent if running and clean up pid/state files
    docker_manager.exec_in_container(container_name, ["pkill", "-f", "fustor-agent"])
    docker_manager.exec_in_container(container_name, ["rm", "-f", "/root/.fustor/agent.pid"])
    docker_manager.exec_in_container(container_name, ["rm", "-f", "/root/.fustor/agent-state.json"])
    time.sleep(0.2)
    
    # 4. Start new agent
    docker_manager.exec_in_container(
        container_name, 
        ["sh", "-c", "nohup fustor-agent start -V > /data/agent/console.log 2>&1 &"]
    )


@pytest.fixture(scope="session")
def setup_agents(docker_env, fusion_client, test_api_key, test_datastore):
    """
    Configure agents in NFS client containers with API key and datastore.
    Returns a dict with agent configuration info.
    """
    api_key = test_api_key["key"]
    datastore_id = test_datastore["id"]
    
    # --- Clean Slate: Stop all agents first ---
    logger.info("Cleaning up existing agents preventing stale leadership...")
    for container in [CONTAINER_CLIENT_A, CONTAINER_CLIENT_B, CONTAINER_CLIENT_C]:
        try:
             # Just kill the process, don't worry about config yet
             docker_manager.exec_in_container(container, ["pkill", "-f", "fustor-agent"])
        except Exception:
             pass # Ignore errors if not running

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

    # Update agent configs in containers A and B
    # Start Agent A first
    logger.info(f"Configuring and starting agent in {CONTAINER_CLIENT_A}...")
    ensure_agent_running(CONTAINER_CLIENT_A, api_key, datastore_id)
    
    # Wait to ensure A becomes Leader (Effective Synchronization)
    logger.info("Waiting for Agent A to register and become Leader...")
    start_wait = time.time()
    while time.time() - start_wait < 30:
        sessions = fusion_client.get_sessions()
        leader = next((s for s in sessions if s.get("role") == "leader"), None)
        if leader:
            agent_id = leader.get("agent_id", "")
            if "agent-a" in agent_id:
                logger.info(f"Agent A successfully became leader: {agent_id}")
                break
            else:
                logger.warning(f"Someone else is leader: {agent_id}. Waiting...")
        time.sleep(1)
    else:
        raise RuntimeError("Agent A did not become leader within 30 seconds")

    # 4. Wait for Datastore to be READY (Snapshot complete)
    logger.info("Waiting for Datastore to be ready (initial snapshot completion)...")
    start_ready = time.time()
    while time.time() - start_ready < 30:
        try:
            # Stats endpoint checks readiness
            fusion_client.get_stats()
            logger.info("Datastore is READY.")
            break
        except Exception as e:
            # logger.debug(f"Datastore not ready yet: {e}")
            time.sleep(0.5)
    else:
        logger.warning("Datastore readiness check timed out. Proceeding anyway.")

    # 5. Start Agent B as Follower
    logger.info(f"Configuring and starting agent in {CONTAINER_CLIENT_B}...")
    ensure_agent_running(CONTAINER_CLIENT_B, api_key, datastore_id)
    
    # Wait for Agent B to register (Ensure both agents are online)
    logger.info("Waiting for Agent B to register as Follower...")
    start_wait = time.time()
    while time.time() - start_wait < 30:
        sessions = fusion_client.get_sessions()
        # Check if we have 2 sessions and one is agent-b
        agent_b = next((s for s in sessions if "agent-b" in s.get("agent_id", "")), None)
        if agent_b:
            logger.info(f"Agent B registered: {agent_b.get('agent_id')} (Role: {agent_b.get('role')})")
            break
        time.sleep(0.5)
    else:
        logs = docker_manager.get_logs(CONTAINER_CLIENT_B)
        logger.warning(f"Timeout waiting for Agent B. Logs:\n{logs}")

    return {
        "api_key": api_key,
        "datastore_id": datastore_id,
        "containers": {
            "leader": CONTAINER_CLIENT_A,
            "follower": CONTAINER_CLIENT_B,
            "blind": CONTAINER_CLIENT_C
        },
        "ensure_agent_running": ensure_agent_running  # Pass the helper function too
    }


@pytest.fixture
def clean_shared_dir(docker_env):
    """
    Clean up shared directory before each test.
    Note: Fusion state is NOT reset. Tests must use unique file paths to ensure isolation.
    """
    # Clear all files in shared directory
    docker_manager.exec_in_container(
        CONTAINER_NFS_SERVER,
        ["sh", "-c", "rm -rf /exports/* 2>/dev/null || true"]
    )
    # Wait for NFS cache partially
    time.sleep(1.1)
    yield


@pytest.fixture(autouse=True)
def reset_fusion_state(fusion_client):
    """
    Reset Fusion parser state before each test case to ensure isolation.
    """
    try:
        fusion_client.reset()
    except Exception as e:
        # It's okay if it fails (e.g. datastore not yet created in first test)
        logger.debug(f"Fusion reset skipped or failed: {e}")
    yield


@pytest.fixture
def wait_for_audit():
    """Return a function that waits for audit cycle to complete."""
    def _wait(seconds: int = AUDIT_INTERVAL + 1):
        time.sleep(seconds)
    return _wait

@pytest.fixture
def reset_leadership(fusion_client, setup_agents, docker_env):
    """
    Ensure Agent A is Leader and Agent B is Follower before test.
    This effectively resets the cluster state if previous tests messed it up.
    """
    api_key = setup_agents["api_key"]
    datastore_id = setup_agents["datastore_id"]
    ensure_agent_running = setup_agents["ensure_agent_running"]
    
    # Check current state
    sessions = fusion_client.get_sessions()
    leader = next((s for s in sessions if s.get("role") == "leader"), None)
    
    is_clean = False
    if leader and "agent-a" in leader.get("agent_id", ""):
        # Agent A is leader. Check Agent B presence.
        agent_b = next((s for s in sessions if "agent-b" in s.get("agent_id", "")), None)
        if agent_b:
            is_clean = True
            
    if is_clean:
        logger.info("Cluster state is clean (A=Leader, B=Follower). Skipping reset.")
        return

    logger.warning("Cluster state dirty. Forcing leadership reset...")
    
    # Force reset: Stop everyone
    for container in [CONTAINER_CLIENT_A, CONTAINER_CLIENT_B]:
         try:
             docker_manager.exec_in_container(container, ["pkill", "-f", "fustor-agent"])
         except Exception:
             pass

    # Wait for sessions to vanish
    logger.info("Waiting for stale sessions to expire...")
    start_cleanup = time.time()
    while time.time() - start_cleanup < 6:
        if not fusion_client.get_sessions():
            break
        time.sleep(0.5)
        
    # Restart A first
    logger.info("Restarting Agent A...")
    ensure_agent_running(CONTAINER_CLIENT_A, api_key, datastore_id)
    
    # Wait for A to become Leader
    logger.info("Waiting for Agent A to be Leader...")
    start_wait = time.time()
    while time.time() - start_wait < 30:
        sessions = fusion_client.get_sessions()
        leader = next((s for s in sessions if s.get("role") == "leader"), None)
        if leader and "agent-a" in leader.get("agent_id", ""):
            break
        time.sleep(0.5)
        
    # Restart B
    logger.info("Restarting Agent B...")
    ensure_agent_running(CONTAINER_CLIENT_B, api_key, datastore_id)
    
    # Wait for B
    time.sleep(1)
    logger.info("Leadership reset complete.")

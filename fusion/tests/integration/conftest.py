"""
Pytest configuration and fixtures for NFS multi-mount consistency integration tests.
"""
import os
import pytest
import time
from pathlib import Path

from .utils import docker_manager, FusionClient, RegistryClient

# Test configuration
TEST_TIMEOUT = int(os.getenv("FUSTOR_TEST_TIMEOUT", "120"))
AUDIT_INTERVAL = int(os.getenv("FUSTOR_AUDIT_INTERVAL", "30"))
SUSPECT_TTL_MINUTES = 10  # From CONSISTENCY_DESIGN.md

# Container names
CONTAINER_NFS_SERVER = "fustor-nfs-server"
CONTAINER_REGISTRY = "fustor-registry"
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
    
    Set FUSTOR_REUSE_ENV=true to use an already running environment.
    Otherwise, the environment will be built and torn down for each session.
    """
    reuse_env = os.getenv("FUSTOR_REUSE_ENV", "false").lower() == "true"
    
    if reuse_env:
        print("\n=== Using existing Docker Compose environment ===")
        # Just verify containers are running
        containers = [
            CONTAINER_NFS_SERVER,
            CONTAINER_REGISTRY,
            CONTAINER_FUSION,
            CONTAINER_CLIENT_A,
            CONTAINER_CLIENT_B,
            CONTAINER_CLIENT_C
        ]
        for container in containers:
            if not docker_manager.wait_for_health(container, timeout=30):
                raise RuntimeError(f"Container {container} is not healthy. Please start the environment first.")
        
        print("=== All containers healthy ===")
        yield docker_manager
        # Don't tear down when reusing
        print("\n=== Keeping Docker Compose environment running ===")
    else:
        print("\n=== Starting Docker Compose environment ===")
        try:
            # Build and start all services
            docker_manager.up(build=True, wait=True)
            
            # Wait for all containers to be healthy
            containers = [
                CONTAINER_NFS_SERVER,
                CONTAINER_REGISTRY,
                CONTAINER_FUSION,
                CONTAINER_CLIENT_A,
                CONTAINER_CLIENT_B,
                CONTAINER_CLIENT_C
            ]
            for container in containers:
                if not docker_manager.wait_for_health(container, timeout=120):
                    logs = docker_manager.get_logs(container)
                    raise RuntimeError(f"Container {container} failed to become healthy.\nLogs:\n{logs}")
            
            print("=== All containers healthy ===")
            yield docker_manager
            
        finally:
            print("\n=== Tearing down Docker Compose environment ===")
            docker_manager.down(volumes=True)


@pytest.fixture(scope="session")
def registry_client(docker_env) -> RegistryClient:
    """Create and authenticate Registry client."""
    client = RegistryClient(base_url="http://localhost:18101")
    # Wait for registry to be fully ready
    time.sleep(2)
    # Default admin credentials (email, not username)
    client.login("admin@admin.com", "admin")
    return client


@pytest.fixture(scope="session")
def test_datastore(registry_client) -> dict:
    """Create a test datastore or reuse existing one."""
    existing_datastores = registry_client.get_datastores()
    for ds in existing_datastores:
        if ds["name"] == "integration-test-ds":
            print(f"Reusing existing datastore: {ds['id']}")
            # Ensure allow_concurrent_push is True even for reused datastore
            if not ds.get("allow_concurrent_push"):
                 registry_client.update_datastore(ds["id"], allow_concurrent_push=True)
                 ds["allow_concurrent_push"] = True
            return ds

    return registry_client.create_datastore(
        name="integration-test-ds",
        description="Datastore for NFS consistency integration tests",
        allow_concurrent_push=True
    )


@pytest.fixture(scope="session")
def test_api_key(registry_client, test_datastore) -> dict:
    """Create an API key for the test datastore or reuse existing one."""
    existing_keys = registry_client.get_api_keys()
    
    # Filter keys for this datastore
    datastore_keys = [k for k in existing_keys if k.get("datastore_id") == test_datastore["id"]]
    
    for key in datastore_keys:
        if key["name"] == "integration-test-key":
            print(f"Reusing existing API key: {key['name']}")
            return key

    return registry_client.create_api_key(
        datastore_id=test_datastore["id"],
        name="integration-test-key"
    )


@pytest.fixture(scope="session")
def fusion_client(docker_env, test_api_key) -> FusionClient:
    """Create Fusion client with API key."""
    client = FusionClient(base_url="http://localhost:18102")
    client.set_api_key(test_api_key["key"])
    
    # Wait for Fusion to be ready to accept requests and sync its cache
    print("Waiting for Fusion to become ready and sync cache...")
    for i in range(20):
        try:
            # Use get_sessions which doesn't require snapshot completion (unlike get_stats/get_tree)
            client.get_sessions()
            print(f"Fusion ready and API key synced after {i+1} seconds")
            break
        except Exception:
            time.sleep(1)
    else:
        raise RuntimeError("Fusion did not become ready within 20 seconds")
    
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
    
    # 0.1 Bypass schema discovery
    docker_manager.create_file_in_container(
        container_name,
        "/root/.fustor/schemas/source_shared-fs.schema.json",
        content="{}"
    )
    docker_manager.create_file_in_container(
        container_name,
        "/root/.fustor/schemas/source_shared-fs.valid",
        content=""
    )

    # 1. Generate Config Content
    config_content = f"""
sources:
  shared-fs:
    driver: "fs"
    uri: "{mount_point}"
    credential:
      user: "unused"
    disabled: false

pushers:
  fusion:
    driver: "fusion"
    endpoint: "{fusion_endpoint}"
    credential:
      key: "{api_key}"
    disabled: false
    driver_params:
      datastore_id: {datastore_id}

syncs:
  sync-task-1:
    source: "shared-fs"
    pusher: "fusion"
    disabled: false
    audit_interval_sec: {AUDIT_INTERVAL}
    sentinel_interval_sec: {AUDIT_INTERVAL // 2 if AUDIT_INTERVAL > 10 else 5}
"""
    # 2. Write config file
    docker_manager.create_file_in_container(
        container_name, 
        "/root/.fustor/agent-config.yaml", 
        content=config_content
    )
    
    # 3. Kill existing agent if running and clean up pid/state files
    docker_manager.exec_in_container(container_name, ["pkill", "-f", "fustor-agent"])
    docker_manager.exec_in_container(container_name, ["rm", "-f", "/root/.fustor/agent.pid"])
    docker_manager.exec_in_container(container_name, ["rm", "-f", "/root/.fustor/agent-state.json"])
    time.sleep(1)
    
    # 4. Start new agent
    docker_manager.exec_in_container(
        container_name, 
        ["sh", "-c", "nohup fustor-agent start > /data/agent/console.log 2>&1 &"]
    )


@pytest.fixture(scope="session")
def setup_agents(docker_env, fusion_client, test_api_key, test_datastore):
    """
    Configure agents in NFS client containers with API key and datastore.
    Returns a dict with agent configuration info.
    """
    api_key = test_api_key["key"]
    datastore_id = test_datastore["id"]
    
    # Update agent configs in containers A and B
    # Start Agent A first
    print(f"Configuring and starting agent in {CONTAINER_CLIENT_A}...")
    ensure_agent_running(CONTAINER_CLIENT_A, api_key, datastore_id)
    
    # Wait to ensure A becomes Leader
    time.sleep(3)
    
    # Now start Agent B
    print(f"Configuring and starting agent in {CONTAINER_CLIENT_B}...")
    ensure_agent_running(CONTAINER_CLIENT_B, api_key, datastore_id)
    
    # Wait for agents to register with Fusion
    print("Waiting for agents to register...")
    time.sleep(5)
    
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
def clean_shared_dir(docker_env, fusion_client, test_api_key, test_datastore):
    """Clean up shared directory AND reset Fusion parser before each test."""
    # 1. Stop agents first
    for container_name in [CONTAINER_CLIENT_A, CONTAINER_CLIENT_B]:
        docker_manager.exec_in_container(container_name, ["pkill", "-f", "fustor-agent"])
        # Important: clear agent local state so it starts fresh
        docker_manager.exec_in_container(container_name, ["rm", "-f", "/root/.fustor/agent-state.json"])
        
    # 2. Clear files on NFS server
    docker_manager.exec_in_container(
        CONTAINER_NFS_SERVER,
        ["sh", "-c", "rm -rf /exports/* 2>/dev/null || true"]
    )
    
    # 3. Reset Fusion state via API (Tree, Sessions, Leadership)
    try:
        fusion_client.reset_parser()
    except Exception as e:
        print(f"Warning: Failed to reset Fusion parser: {e}")
    
    # Wait for NFS cache to clear and Fusion to settle
    time.sleep(5)
    
    # Force NFS refresh
    docker_manager.exec_in_container(CONTAINER_CLIENT_A, ["ls", "-la", MOUNT_POINT])
        
    # 4. Start agents sequentially
    api_key = test_api_key["key"]
    datastore_id = test_datastore["id"]
    
    # Start Agent A (Leader)
    ensure_agent_running(CONTAINER_CLIENT_A, api_key, datastore_id)
    time.sleep(3)
    # Start Agent B (Follower)
    ensure_agent_running(CONTAINER_CLIENT_B, api_key, datastore_id)
    
    # 5. Synchronization: Wait for BOTH sessions to be active
    start_time = time.time()
    while time.time() - start_time < 30:
        sessions = fusion_client.get_sessions()
        if len(sessions) >= 2:
            break
        time.sleep(1)
        
    # 6. Synchronization: Wait for initial Snapshot completion via marker
    start_marker = f"{MOUNT_POINT}/test_start_marker_{int(time.time()*1000)}.txt"
    docker_manager.create_file_in_container(CONTAINER_CLIENT_A, start_marker, content="started")
    
    found = fusion_client.wait_for_file_in_tree(start_marker, timeout=60)
    if not found:
        logs = docker_manager.exec_in_container(CONTAINER_CLIENT_A, ["cat", "/data/agent/console.log"])
        print(f"Agent A console logs:\n{logs}")
        assert False, f"Agents failed to sync initial state within 60s. Marker: {start_marker}"
        
    yield
    # No teardown needed as setup handles everything and we want to preserve state for debugging on failure


@pytest.fixture
def wait_for_audit():
    """Return a function that waits for audit cycle to complete."""
    def _wait(seconds: int = AUDIT_INTERVAL + 5):
        time.sleep(seconds)
    return _wait

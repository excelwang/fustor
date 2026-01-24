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
    
    # Restart Fusion to force immediate cache sync (workaround for periodic sync issue)
    print("Restarting Fusion container to enforce API key sync...")
    docker_manager.restart_container(CONTAINER_FUSION)
    
    # Wait for Fusion to be healthy again
    if not docker_manager.wait_for_health(CONTAINER_FUSION, timeout=30):
        raise RuntimeError("Fusion container failed to become healthy after restart")
    
    # Wait for Fusion to be ready to accept requests
    print("Waiting for Fusion to become ready...")
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


@pytest.fixture(scope="session")
def setup_agents(docker_env, test_api_key, test_datastore):
    """
    Configure agents in NFS client containers with API key and datastore.
    Rewrites config file and restarts agent process.
    Returns a dict with agent configuration info.
    """
    api_key = test_api_key["key"]
    datastore_id = test_datastore["id"]
    
    fusion_endpoint = "http://fustor-fusion:8102"
    
    # Update agent configs in containers A and B
    for container_name in [CONTAINER_CLIENT_A, CONTAINER_CLIENT_B]:
        agent_id = "agent-a" if container_name == CONTAINER_CLIENT_A else "agent-b"
        agent_port = 8100
        mount_point = MOUNT_POINT  # /mnt/shared
        
        # 0. Initialize Fustor home and agent.id
        docker_manager.exec_in_container(container_name, ["mkdir", "-p", "/root/.fustor/schemas"])
        docker_manager.create_file_in_container(
            container_name,
            "/root/.fustor/agent.id",
            content=agent_id
        )
        
        # 0.1 Bypass schema discovery: Create fake schema and validation marker
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

        # 1. Generate Config Content (Dictionary format as expected by AppConfig)
        config_content = f"""
sources:
  shared-fs:
    driver: "fs"
    uri: "{mount_point}"
    credential:
      user: "unused"
    disabled: false
    driver_params:
      scan_interval: 60
      audit_interval: 300

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
"""
        # 2. Write config file
        print(f"Configuring agent in {container_name} (ID: {agent_id})...")
        docker_manager.create_file_in_container(
            container_name, 
            "/root/.fustor/agent-config.yaml", 
            content=config_content
        )
        
        # 3. Kill existing agent if running
        docker_manager.exec_in_container(container_name, ["pkill", "-f", "fustor-agent"])
        time.sleep(1) # Wait for shutdown
        
        # 4. Start new agent
        print(f"Starting agent in {container_name}...")
        # Start in background with logging to console.log
        docker_manager.exec_in_container(
            container_name, 
            ["sh", "-c", "nohup fustor-agent start > /data/agent/console.log 2>&1 &"]
        )
    
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
        }
    }


@pytest.fixture
def clean_shared_dir(docker_env):
    """Clean up shared directory before each test."""
    # Clear all files in shared directory
    docker_manager.exec_in_container(
        CONTAINER_NFS_SERVER,
        ["sh", "-c", "rm -rf /exports/* 2>/dev/null || true"]
    )
    # Wait for changes to propagate
    time.sleep(2)
    yield
    # Cleanup after test
    docker_manager.exec_in_container(
        CONTAINER_NFS_SERVER,
        ["sh", "-c", "rm -rf /exports/* 2>/dev/null || true"]
    )


@pytest.fixture
def wait_for_audit():
    """Return a function that waits for audit cycle to complete."""
    def _wait(seconds: int = AUDIT_INTERVAL + 5):
        time.sleep(seconds)
    return _wait

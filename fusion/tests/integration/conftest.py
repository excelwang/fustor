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
    Session-scoped fixture that starts the Docker Compose environment.
    Waits for all services to be healthy before yielding.
    Tears down the environment after all tests complete.
    """
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
    client.login("admin", "admin123")
    return client


@pytest.fixture(scope="session")
def test_datastore(registry_client) -> dict:
    """Create a test datastore."""
    return registry_client.create_datastore(
        name="integration-test-ds",
        description="Datastore for NFS consistency integration tests"
    )


@pytest.fixture(scope="session")
def test_api_key(registry_client, test_datastore) -> dict:
    """Create an API key for the test datastore."""
    return registry_client.create_api_key(
        datastore_id=test_datastore["id"],
        name="integration-test-key"
    )


@pytest.fixture(scope="session")
def fusion_client(docker_env, test_api_key) -> FusionClient:
    """Create Fusion client with API key."""
    client = FusionClient(base_url="http://localhost:18102")
    client.set_api_key(test_api_key["key"])
    return client


@pytest.fixture(scope="session")
def setup_agents(docker_env, test_api_key, test_datastore):
    """
    Configure agents in NFS client containers with API key and datastore.
    Returns a dict with agent configuration info.
    """
    api_key = test_api_key["key"]
    datastore_id = test_datastore["id"]
    
    # Update agent configs in containers A and B
    for container in [CONTAINER_CLIENT_A, CONTAINER_CLIENT_B]:
        docker_manager.exec_in_container(
            container,
            ["sh", "-c", f"export API_KEY='{api_key}' && export DATASTORE_ID='{datastore_id}'"]
        )
    
    # Wait for agents to register with Fusion
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

# it/fixtures/docker.py
"""
Docker environment fixtures for integration tests.
"""
import os
import sys
import pytest
import logging
from pathlib import Path

# Ensure parent directory is in path
_fixtures_dir = Path(__file__).parent
_it_dir = _fixtures_dir.parent
if str(_it_dir) not in sys.path:
    sys.path.insert(0, str(_it_dir))

from utils import docker_manager
from .constants import (
    CONTAINER_CLIENT_A,
    CONTAINER_CLIENT_B,
    CONTAINER_CLIENT_C,
    CONTAINER_FUSION,
    MOUNT_POINT,
    SESSION_TIMEOUT
)

logger = logging.getLogger("fustor_test")

# Container names
CONTAINER_NFS_SERVER = "fustor-nfs-server"


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
    
    # Inject Fusion Configuration
    logger.info("Injecting static configuration into Fusion...")
    
    # 1. Create .fustor directory
    docker_manager.exec_in_container(CONTAINER_FUSION, ["mkdir", "-p", "/root/.fustor/views-config"])
    
    # 2. Inject Receivers Config (v2: renamed from 'datastores')
    receivers_config = f"""
http-main:
  driver: "http"
  port: 8102
  api_keys:
    - key: "test-api-key-123"
      pipeline_id: "integration-test-ds"
  session_timeout_seconds: {SESSION_TIMEOUT}
  allow_concurrent_push: true
"""
    docker_manager.create_file_in_container(CONTAINER_FUSION, "/root/.fustor/receivers-config.yaml", receivers_config)
    
    # 3. Inject View Config
    view_config = """
id: "integration-test-ds"
datastore_id: "integration-test-ds"
driver: "fs"
disabled: false
driver_params:
  uri: "/mnt/shared-view"
  hot_file_threshold: 10.0
"""
    docker_manager.create_file_in_container(CONTAINER_FUSION, "/root/.fustor/views-config/integration-test-ds.yaml", view_config)
    
    # 4. Inject Fusion Pipeline Config (V2 binding)
    pipeline_config = f"""
id: "integration-test-ds"
receiver: "http-main"
views:
  - "integration-test-ds"
enabled: true

session_timeout_seconds: {SESSION_TIMEOUT}
allow_concurrent_push: true
extra:
  view_id: "integration-test-ds"
"""
    docker_manager.exec_in_container(CONTAINER_FUSION, ["mkdir", "-p", "/root/.fustor/fusion-pipes-config"])
    docker_manager.create_file_in_container(CONTAINER_FUSION, "/root/.fustor/fusion-pipes-config/integration-test-ds.yaml", pipeline_config)

    # 5. Reload Fusion

    docker_manager.restart_container(CONTAINER_FUSION)
    docker_manager.wait_for_health(CONTAINER_FUSION)
    
    # 5. Log Environment Skew
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


@pytest.fixture
def clean_shared_dir(docker_env):
    """
    Function-scoped fixture to clean the shared directory before each test.
    """
    dm = docker_env
    logger.info("Cleaning shared directory...")
    
    # Clean from all clients to handle NFS cache
    for container in [CONTAINER_CLIENT_A, CONTAINER_CLIENT_B, CONTAINER_CLIENT_C]:
        try:
            dm.exec_in_container(container, ["sh", "-c", f"rm -rf {MOUNT_POINT}/*"])
        except Exception as e:
            logger.warning(f"Could not clean {MOUNT_POINT} in {container}: {e}")
    
    # Also clean from NFS server directly
    try:
        dm.exec_in_container(CONTAINER_NFS_SERVER, ["sh", "-c", "rm -rf /exports/*"])
    except Exception as e:
        logger.warning(f"Could not clean NFS exports: {e}")
    
    yield
    # No cleanup after test - let the next test decide

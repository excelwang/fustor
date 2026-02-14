# tests/e2e/fixtures/docker.py
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
    SESSION_TIMEOUT,
    HOT_FILE_THRESHOLD,
    CONTAINER_HEALTH_TIMEOUT,
    SHORT_TIMEOUT,
    TEST_TOMBSTONE_TTL
)

logger = logging.getLogger("fustor_test")

# Container names
CONTAINER_NFS_SERVER = "fustor-nfs-server"


import hashlib
import glob

def get_env_hash():
    """
    Calculate hash of files that affect the Docker environment.
    """
    files_to_hash = [
        str(_it_dir / "docker-compose.yml"),
        str(_it_dir / "containers/fustor-services/Dockerfile"),
        str(_it_dir / "containers/nfs-client/Dockerfile"),
        str(_it_dir / "containers/nfs-client/entrypoint.sh"),
        str(_it_dir.parent / "pyproject.toml"),
    ]
    # Add all package pyproject.toml files
    files_to_hash.extend(glob.glob(str(_it_dir.parent / "extensions/*/pyproject.toml")))
    files_to_hash.extend(glob.glob(str(_it_dir.parent / "core/pyproject.toml")))
    files_to_hash.extend(glob.glob(str(_it_dir.parent / "agent-sdk/pyproject.toml")))
    files_to_hash.extend(glob.glob(str(_it_dir.parent / "fusion-sdk/pyproject.toml")))
    files_to_hash.extend(glob.glob(str(_it_dir.parent / "agent/pyproject.toml")))
    files_to_hash.extend(glob.glob(str(_it_dir.parent / "fusion/pyproject.toml")))
    
    hasher = hashlib.md5()
    for f in sorted(files_to_hash):
        if os.path.exists(f):
            with open(f, "rb") as fh:
                hasher.update(fh.read())
    return hasher.hexdigest()


@pytest.fixture(scope="session")
def docker_env(request):
    """
    Session-scoped fixture that manages the Docker Compose environment.
    Initializes Fusion with static configuration.
    """
    fast_mode = request.config.getoption("--fast")
    state_file = _it_dir / ".env_state"
    current_hash = get_env_hash()
    
    stored_hash = None
    if state_file.exists():
        stored_hash = state_file.read_text().strip()
    
    # Check if environment is up
    try:
        docker_manager.exec_in_container(CONTAINER_NFS_SERVER, ["ls", "/"])
        is_up = True
    except Exception:
        is_up = False
        
    needs_rebuild = not is_up or (stored_hash != current_hash)
    
    if fast_mode and is_up:
        logger.info("⚡ Fast mode: Skipping hash check and forced rebuild. Assuming environment is good.")
        needs_rebuild = False
    
    if needs_rebuild:
        logger.info(f"Environment needs rebuild (Hash mismatch or not up). Hash: {current_hash}")
        # Stop everything to ensure a clean slate if hash changed
        if is_up:
            docker_manager.down(volumes=True)
        docker_manager.up(build=True, wait=True)
        # Update state file
        state_file.write_text(current_hash)
    else:
        logger.info("Environment hash matches. Reusing existing running containers.")
        # Optional: Fast health check
        for container in [CONTAINER_NFS_SERVER, CONTAINER_FUSION]:
            if not docker_manager.wait_for_health(container, timeout=CONTAINER_HEALTH_TIMEOUT):
                logger.warning(f"Container {container} unhealthy. Repairing...")
                docker_manager.up(build=True, wait=True)
                break
    
    # --- Configuration Hook ---
    # Fusion configuration logic:
    # 1. Templates mounted to /config/fusion-config (read-only from host ./config/fusion-config)
    # 2. Entrypoint runs `envsubst` to inject test variables (TEST_VIEW_ID, TEST_API_KEY)
    # 3. Processed configs written to /root/.fustor/fusion-config (Fusion's active config dir)
    logger.info("Fusion config: envsubst(/config/fusion-config) -> /root/.fustor/fusion-config")

    # 5. Reload Fusion

    # 5. Reload environment if needed
    if needs_rebuild:
        logger.info("Restarting all containers to ensure fresh state after build...")
        for container in [CONTAINER_FUSION, CONTAINER_CLIENT_A, CONTAINER_CLIENT_B, CONTAINER_CLIENT_C]:
            docker_manager.restart_container(container)
            docker_manager.wait_for_health(container)
    elif not fast_mode:
        # If we are reusing, we still might want to restart Fusion once per session 
        # to ensure it picked up the (potentially) updated static configs.
        logger.info("Restarting Fusion to apply static configs...")
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

# it/fixtures/constants.py
"""
Shared constants for integration tests.
"""
import os

# Container names
CONTAINER_CLIENT_A = "fustor-nfs-client-a"
CONTAINER_CLIENT_B = "fustor-nfs-client-b"
CONTAINER_CLIENT_C = "fustor-nfs-client-c"
CONTAINER_FUSION = "fustor-fusion"
CONTAINER_NFS_SERVER = "fustor-nfs-server"

# Shared mount point
MOUNT_POINT = "/mnt/shared"

# Fusion API connection
FUSION_PORT = 8102
FUSION_HOST = "fustor-fusion"
FUSION_ENDPOINT = f"http://{FUSION_HOST}:{FUSION_PORT}"

# Test timing configurations
AUDIT_INTERVAL = 5  # seconds
SENTINEL_INTERVAL = 1  # seconds
SESSION_TIMEOUT = 5   # seconds (used in docker-compose configs as well)
HEARTBEAT_INTERVAL = 1 # seconds

# Orchestration timeouts
TEST_TIMEOUT = int(os.getenv("FUSTOR_TEST_TIMEOUT", "600"))
CONTAINER_HEALTH_TIMEOUT = 120
AGENT_READY_TIMEOUT = 30
VIEW_READY_TIMEOUT = 30

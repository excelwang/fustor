# tests/e2e/fixtures/constants.py
"""
Shared constants for integration tests.

Values are scaled by TIME_SCALE for fast local testing.
Default TIME_SCALE=0.05 compresses production intervals ~20x.
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
FUSION_PORT = 8102  # Main API port (Management)
RECEIVER_PORT = 18888  # Receiver port (Data)
FUSION_HOST = "fustor-fusion"
FUSION_ENDPOINT = f"http://{FUSION_HOST}:{FUSION_PORT}"
RECEIVER_ENDPOINT = f"http://{FUSION_HOST}:{RECEIVER_PORT}"
TEST_VIEW_ID = "integration-test-ds"
TEST_API_KEY = "test-api-key-123"
TEST_QUERY_KEY = "test-query-key-456"

# --- Time Scaling ---
TIME_SCALE = 0.05

def scaled_duration(seconds: float, min_val: float = 0.1) -> float:
    """Scale a production duration for testing, respecting a minimum."""
    return max(seconds * TIME_SCALE, min_val)

# --- Core Intervals ---
AUDIT_INTERVAL = scaled_duration(300.0, min_val=0.5)       # ~15s
SENTINEL_INTERVAL = scaled_duration(100.0, min_val=0.1)     # ~5s
HEARTBEAT_INTERVAL = scaled_duration(10.0, min_val=0.1)     # ~0.5s
SESSION_TIMEOUT = scaled_duration(30.0, min_val=2.0)        # ~2s
if SESSION_TIMEOUT < 3 * HEARTBEAT_INTERVAL:
    SESSION_TIMEOUT = 3 * HEARTBEAT_INTERVAL + 1.0
THROTTLE_INTERVAL_SEC = scaled_duration(5.0, min_val=0.1)   # ~0.25s

# --- FS Thresholds ---
# HOT_FILE_THRESHOLD: MUST match Agent docker config (hot_file_threshold).
# If reducing this, also update docker-compose.yml / agent config.
HOT_FILE_THRESHOLD = scaled_duration(1200.0, min_val=60.0)  # 60s
TEST_TOMBSTONE_TTL = scaled_duration(2.0, min_val=0.5)
TOMBSTONE_CLEANUP_WAIT = scaled_duration(3.0, min_val=0.5)

# --- Orchestration Timeouts ---
TEST_TIMEOUT = int(os.getenv("FUSTOR_TEST_TIMEOUT", "600"))
CONTAINER_HEALTH_TIMEOUT = 120
AGENT_READY_TIMEOUT = 60.0
AGENT_B_READY_TIMEOUT = 60.0
VIEW_READY_TIMEOUT = 60.0
AUDIT_WAIT_TIMEOUT = max(AUDIT_INTERVAL * 2.0, 10.0)
SESSION_VANISH_TIMEOUT = SESSION_TIMEOUT * 2.0

# --- API Wait Timeouts ---
SHORT_TIMEOUT = 5.0
MEDIUM_TIMEOUT = 15.0
LONG_TIMEOUT = 30.0
EXTREME_TIMEOUT = 60.0

# --- NFS/Ingestion Delays ---
NFS_SYNC_DELAY = 3.2    # actimeo=1 + margin
INGESTION_DELAY = 6.0    # Agent scan + network + Fusion process
STRESS_DELAY = 10.0      # Heavy operations (was 15s, reduced)

# --- Polling ---
POLL_INTERVAL = 0.5
FAST_POLL_INTERVAL = 0.1


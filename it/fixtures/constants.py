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

# --- Test Timing Configurations ---

# Driver / Core Intervals
AUDIT_INTERVAL = 5        # seconds
SENTINEL_INTERVAL = 1     # seconds
HEARTBEAT_INTERVAL = 1    # seconds
SESSION_TIMEOUT = 5       # seconds (used in sessions and heartbeats)

# FS Specific Thresholds
HOT_FILE_THRESHOLD = 10.0   # seconds
THROTTLE_INTERVAL_SEC = 0.5 # seconds (event deduplication)

# --- Orchestration Timeouts (Wait Limits) ---

TEST_TIMEOUT = int(os.getenv("FUSTOR_TEST_TIMEOUT", "600"))
CONTAINER_HEALTH_TIMEOUT = 120
AGENT_READY_TIMEOUT = 45
AGENT_B_READY_TIMEOUT = 60 # Agent B usually takes longer due to follower sync
VIEW_READY_TIMEOUT = 45
AUDIT_WAIT_TIMEOUT = 45    # How long to wait for an audit cycle to complete
SESSION_VANISH_TIMEOUT = 12 # How long to wait for stale sessions to expire (SESSION_TIMEOUT + 8s)

# --- Standard API Wait Timeouts ---
SHORT_TIMEOUT = 10         # Quick checks (file creation)
MEDIUM_TIMEOUT = 20        # Moderate operations
LONG_TIMEOUT = 30          # Complex operations (large tree syncs)
EXTREME_TIMEOUT = 60       # Failover / Heavy Audit scenarios

# --- Semantic Sleep Durations (NFS/Ingestion) ---
NFS_SYNC_DELAY = 1.5       # Basic NFS attribute cache delay
INGESTION_DELAY = 2.0      # Time for Agent -> Fusion -> Tree update
STRESS_DELAY = 5.0         # Large operations/NFS heavy load

# Polling intervals
POLL_INTERVAL = 0.5
FAST_POLL_INTERVAL = 0.1

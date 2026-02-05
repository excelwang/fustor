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

# --- Time Scaling Logic ---
# Defaults to 1.0 (no scaling). Set FUSTOR_TIME_SCALE < 1.0 to speed up tests.
TIME_SCALE = 0.05

# Lower bounds to prevent "quantum effects" (instability due to system/network overhead)
# Software lower bound: Don't poll faster than this to avoid CPU saturation/race conditions
MIN_SLEEP_DURATION = 0.1 

def scaled_duration(seconds: float, min_val: float = MIN_SLEEP_DURATION) -> float:
    """
    Scales a duration by TIME_SCALE, respecting a minimum value.
    """
    return max(seconds * TIME_SCALE, min_val)

# NFS Physical lower bound: actimeo=1 means ~1s lag is unavoidable.
# The user specified NFS_PHYSICAL_LATENCY=60 to simulate a standard production NFS environment.
# We then scale this down to test speeds, ensuring we never go below the physical reality (1s).
NFS_PHYSICAL_LATENCY_SEC = 60.0 # Standard Production NFS Attribute Cache
NFS_SAFETY_MARGIN = 0.2
# Effective delay for tests: 60 * 0.05 = 3.0s (safe for actimeo=1)
MIN_NFS_DELAY = scaled_duration(NFS_PHYSICAL_LATENCY_SEC, min_val=1.0) + NFS_SAFETY_MARGIN

# --- Test Timing Configurations ---

# Driver / Core Intervals (Production Defaults from Source)
# Source: agent/src/fustor_agent/config/pipelines.py
AUDIT_INTERVAL = scaled_duration(600.0, min_val=0.5)      # Prod: 10m -> Test: 30s
SENTINEL_INTERVAL = scaled_duration(120.0, min_val=MIN_SLEEP_DURATION) # Prod: 2m -> Test: 6s
HEARTBEAT_INTERVAL = scaled_duration(10.0, min_val=MIN_SLEEP_DURATION) # Prod: 10s -> Test: 0.5s

# Source: fusion/src/fustor_fusion/api/session.py
SESSION_TIMEOUT = scaled_duration(30.0, min_val=2.0)      # Prod: 30s -> Test: 1.5s (Safe > 3*HB=1.5s? Edge case, bump min)
if SESSION_TIMEOUT < 3 * HEARTBEAT_INTERVAL:
    SESSION_TIMEOUT = 3 * HEARTBEAT_INTERVAL + 1.0

# FS Specific Thresholds
# Source: packages/source-fs/src/fustor_source_fs/driver.py
THROTTLE_INTERVAL_SEC = scaled_duration(5.0, min_val=0.1) # Prod: 5s -> Test: 0.25s

# Test Specific Logic (No direct Prod equivalent found in source-fs, treated as test params)
HOT_FILE_THRESHOLD = scaled_duration(30.0, min_val=5.0)   
TEST_TOMBSTONE_TTL = scaled_duration(2.0, min_val=0.5)    
TOMBSTONE_CLEANUP_WAIT = scaled_duration(3.0, min_val=0.5) 

# --- Orchestration Timeouts (Wait Limits) ---
# Timeouts are derived from the intervals they wait for + buffers.

TEST_TIMEOUT = int(os.getenv("FUSTOR_TEST_TIMEOUT", "600"))
CONTAINER_HEALTH_TIMEOUT = 120 # Fixed startup cost
AGENT_READY_TIMEOUT = scaled_duration(90.0, min_val=20.0) 
AGENT_B_READY_TIMEOUT = scaled_duration(120.0, min_val=25.0)
VIEW_READY_TIMEOUT = scaled_duration(60.0, min_val=15.0) # Wait for initial snapshot

# Audit Wait needs to cover a full audit cycle
AUDIT_WAIT_TIMEOUT = AUDIT_INTERVAL * 2.0 
if AUDIT_WAIT_TIMEOUT < 10.0: AUDIT_WAIT_TIMEOUT = 10.0

# Session Vanish needs to wait for Session Timeout
SESSION_VANISH_TIMEOUT = SESSION_TIMEOUT * 2.0

# --- Standard API Wait Timeouts ---
SHORT_TIMEOUT = scaled_duration(10.0, min_val=2.0)         
MEDIUM_TIMEOUT = scaled_duration(30.0, min_val=5.0)        
LONG_TIMEOUT = scaled_duration(60.0, min_val=10.0)          
EXTREME_TIMEOUT = scaled_duration(120.0, min_val=20.0)       

# --- Semantic Sleep Durations (NFS/Ingestion) ---
# NFS Sync Delay: How long to wait for NFS attributes to propagate
NFS_SYNC_DELAY = scaled_duration(1.5 * NFS_PHYSICAL_LATENCY_SEC, min_val=MIN_NFS_DELAY) # 90s -> 4.5s
# Ingestion Delay: Agent Scan + Network + Fusion Process + View Update
INGESTION_DELAY = scaled_duration(2.0 * NFS_PHYSICAL_LATENCY_SEC, min_val=MIN_NFS_DELAY) # 120s -> 6s
STRESS_DELAY = scaled_duration(5.0 * NFS_PHYSICAL_LATENCY_SEC, min_val=MIN_NFS_DELAY) # 300s -> 15s

# Polling intervals
POLL_INTERVAL = 0.5
FAST_POLL_INTERVAL = 0.1

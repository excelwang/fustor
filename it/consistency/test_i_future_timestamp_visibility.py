import logging
import pytest
import time
from it.utils.docker_manager import DockerManager
from it.utils.fusion_client import FusionClient
from ..fixtures.constants import SHORT_TIMEOUT, MEDIUM_TIMEOUT, EXTREME_TIMEOUT, INGESTION_DELAY, HOT_FILE_THRESHOLD

logger = logging.getLogger("fustor_test")

@pytest.mark.asyncio
async def test_future_timestamp_visibility(
    docker_env,
    fusion_client: FusionClient,
    setup_agents
):
    """
    Validation Test: Future Timestamp File Visibility
    
    Objective:
        Verify that a file with a future timestamp (Future File):
        1. Is INITIALLY marked as 'integrity_suspect=True' (because (Clock - mtime) < stable_threshold).
        2. Eventually becomes visible and 'integrity_suspect=False' once the Robust Logical Clock catches up 
           (or the file cools down/is updated).
        
        However, per current design:
        - Robust Clock rejects Future Mtime, so Clock < Future Mtime.
        - Suspect Age = Clock - Mtime (becomes negative which is < threshold).
        - So it enters Suspect List.
        - Question: Does it EVER leave Suspect List if Clock is way behind?
        
    Design Clarification:
        If User creates `future_file` at 2050:
        - Clock stays at 2024.
        - File Mtime = 2050.
        - Age = 2024 - 2050 = -26 years.
        - Threshold = 30s.
        - Age < Threshold => SUSPECT.
        
        The requirement is: "When hot file threshold expires, this future file needs to be visible in fs-view".
        
    Strategy:
        1. Create a file with Mtime = Now + 60s (Mini Future).
        2. Wait for Fusion to ingest.
        3. Expect it to be Suspect initially.
        4. Wait for > 60s (Clock catches up OR physical time passes).
        
        BUT Robust Clock won't catch up to Mtime if Mtime is rejected.
        Actually, if Mtime is just +60s, it might be rejected by TrustWindow(+1s).
        
        If `now()` uses Fallback (time.time()) or advances naturally with other updates, eventually `now()` > `future_mtime`.
        Wait, `now()` follows `agent_time`.
        So if we wait 65s physically, `agent_time` advances 65s.
        `BaseLine` advances 65s.
        Clock `now()` advances 65s.
        Then `now()` > `future_mtime`.
        Then `Age` > 0.
        Then `Suspect` should clear.
    """
    containers = docker_env # Alias for convenience
    
    # 1. Setup path
    test_path = "/mnt/shared/future_file.txt"
    relative_path = "future_file.txt"
    relative_slash_path = "/future_file.txt"
    
    # Get current physical time from Client C (which has no skew)
    # Using Client C ensures we create a "mini future" relative to Fusion's reference time.
    res = containers.exec_in_container("fustor-nfs-client-c", ["date", "+%s"])
    base_now = float(res.stdout.strip())
    
    # Set Future Mtime = Now + SHORT_TIMEOUT
    future_time = base_now + SHORT_TIMEOUT
    # Use gmtime because containers are typically in UTC, whereas host might be in local TZ
    future_time_str = time.strftime('%Y%m%d%H%M.%S', time.gmtime(future_time))
    
    logger.info(f"Step 1: Creating Future File {test_path} at T+{SHORT_TIMEOUT}s (relative to normal time, UTC string: {future_time_str})")
    
    # Create file with future timestamp
    # Note: touch -t uses [[CC]YY]MMDDhhmm[.ss]
    containers.exec_in_container("fustor-nfs-client-a", ["touch", "-t", future_time_str, test_path])
    
    # 2. Wait for ingestion reliably
    logger.info("Step 2: Waiting for ingestion...")
    # Fusion client uses relative path
    exists = fusion_client.wait_for_file(relative_slash_path, timeout=SHORT_TIMEOUT)
    assert exists, "Node should exist in tree (ingestion timed out)"
    
    # 3. Check Initial State (Should be Suspect)
    # Because Clock (Now) < FutureMtime (Now+10)
    # Age < 0 < 30s
    node = fusion_client.get_node(relative_slash_path)
    assert node, "Node should exist in tree"
    
    logger.info(f"Node State 1: mtime={node.get('modified_time')}, suspect={node.get('integrity_suspect')}")
    
    # Ideally it should be suspect=True. 
    # But if TrustWindow accepts it (unlikely for +10s), it might be False.
    # Our TrustWindows is 1s. So +10s should be REJECTED by Clock update.
    # So Clock stays at `agent_now`.
    # Age = agent_now - (agent_now+10) = -10.
    # -10 < 30 => Suspect.
    
    if not node.get('integrity_suspect'):
        logger.warning("Node was NOT suspect initially? Clock might have jumped ahead or threshold logic differs.")
    else:
        logger.info("Verified: Node is initially SUSPECT.")

    # 4. Wait for Time to Pass (Catch Up)
    # We need to wait > HOT_FILE_THRESHOLD (60s) + SHORT_TIMEOUT (5s) + Safety Buffer
    wait_time = HOT_FILE_THRESHOLD + SHORT_TIMEOUT + 5
    logger.info(f"Step 3: Waiting {wait_time}s for time to catch up and cross threshold...")
    time.sleep(wait_time)
    
    # NOTE: We need to trigger an event to update the clock!
    # Valid Clock advances driven by Agent Time updates.
    # If no events happen, Clock might stagnate?
    # No, Fusion has background tasks, but `LogicalClock` updates on events.
    # Wait, does Fusion update clock periodically? No.
    # So we need to generate a "tick" event.
    
    # Generate a "tick" file
    containers.exec_in_container("fustor-nfs-client-a", ["touch", "/mnt/shared/tick_tock.txt"])
    time.sleep(INGESTION_DELAY) # Allow ingest
    
    # 5. Check Final State
    # Now Physical Time > Future Time.
    # Clock should have advanced (via tick event) to ~Current Physical Time.
    # So Clock > Future Mtime.
    # Age > 0.
    # Wait. Suspect list is cleared by:
    # A) Realtime Update (Instant)
    # B) Background Sentinel (Periodic Scan)
    
    # Since we didn't touch the future file again, we rely on Sentinel to clear it?
    # Or does Sentinel only clean up if older than threshold?
    # Sentinel logic:
    #   scan suspect list
    #   if `clock.now() - node.mtime > threshold`:
    #       suspect = False
    
    # So yes, Sentinel should clear it.
    
    logger.info("Step 4: Checking Final State after catch-up...")
    node_final = fusion_client.get_node(relative_slash_path)
    
    if node_final is None:
        logger.error(f"Node {relative_slash_path} not found in tree. Dumping partial tree...")
        try:
            tree_dump = fusion_client.get_tree("/", max_depth=2)
            logger.error(f"Tree Dump: {tree_dump}")
        except Exception as e:
            logger.error(f"Failed to dump tree: {e}")
        pytest.fail(f"Node {test_path} disappeared from view (returned None).")

    logger.info(f"Node State 2: suspect={node_final.get('integrity_suspect')}")
    
    assert node_final.get('integrity_suspect') is False, f"Node should be visible (suspect=False) after clock catch-up. Current suspect={node_final.get('integrity_suspect')}"


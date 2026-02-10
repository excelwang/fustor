
import asyncio
import os
import time
import shutil
import pytest
import logging
import queue
import threading
from fustor_view_fs.state import FSState
from fustor_core.event import MessageSource, EventType
from fustor_source_fs.event_handler import OptimizedWatchEventHandler
from fustor_source_fs.components import _WatchManager
# We don't need Observer
# from watchdog.observers import Observer

logger = logging.getLogger("test_atomic_write")
logging.getLogger("fustor_agent.driver.fs").setLevel(logging.DEBUG)

@pytest.mark.asyncio
async def test_atomic_write_happy_path(tmp_path):
    """
    Verify that atomic operations (mv) are treated as clean writes (not suspect),
    while partial writes are treated as suspect.
    """
    # 1. Setup Environment
    source_path = str(tmp_path / "source")
    os.makedirs(source_path, exist_ok=True)
    
    # Logic Verification: Agent Side
    event_queue = queue.Queue()
    stop_event = threading.Event()
    
    # Correct Initialization Pattern from FSDriver
    wm = _WatchManager(
        root_path=source_path,
        event_handler=None, # Will be set below
        min_monitoring_window_days=30.0,
        stop_driver_event=stop_event,
        throttle_interval=0.1 # FAST throttle for test
    )
    
    handler = OptimizedWatchEventHandler(event_queue, wm)
    # Inject handler back to manager (Circular dependency resolution)
    wm.event_handler = handler
    
    # Explicitly schedule the root path (FSDriver does this in pre-scan)
    wm.schedule(source_path)
    
    # DIAGNOSTIC: Check if path is in cache
    assert source_path in wm.lru_cache, "Source path was not added to WatchManager LRU cache!"
    print(f"DIAGNOSTIC: Watches in cache: {list(wm.lru_cache.cache.keys())}")
    
    # Start Manager (starts inotify thread)
    wm.start()
    
    try:
        # DIAGNOSTIC: Simple creation test
        test_probe = os.path.join(source_path, "probe.txt")
        with open(test_probe, 'w') as f:
            f.write("probe")
        time.sleep(0.5)
        
        probe_events = []
        temp_queue = []
        while not event_queue.empty():
            e = event_queue.get()
            temp_queue.append(e)
            # Expect relative path
            if e.rows[0]['path'] == "/" + os.path.basename(test_probe):
                probe_events.append(e)
        
        # Put back events for verification
        for e in temp_queue:
            # We don't put back actually, we just consumed them.
            # But main test relies on event_queue being fresh or accumulated?
            # Main test loop drains queue.
            pass
            
        if not probe_events:
            print(f"DIAGNOSTIC FAILURE: No events received for simple file creation! Expected path: /{os.path.basename(test_probe)}")
            # If this fails, inotify is broken/not working on this path.
            # We might fallback to polling or skip?
            # But the requirement is integration test for checks.
            # If environment is broken, we should know.
            print(f"DIAGNOSTIC: Dump of first 5 events: {[e.rows[0]['path'] for e in temp_queue[:5]]}")
        else:
            print(f"DIAGNOSTIC SUCCESS: Received events for probe: {probe_events}")
            
        # Scenario A: Atomic Write via Move (from INSIDE watched directory)
        # This tests paired MOVED_FROM + MOVED_TO which generates on_moved
        logger.info("TEST: Performing Atomic Move (inside watched dir)...")
        staging_file = os.path.join(source_path, "staging.tmp")
        with open(staging_file, 'w') as f:
            f.write("content")
        time.sleep(0.3)  # Wait for creation events
        
        # Drain creation events
        while not event_queue.empty():
            event_queue.get()
        
        target_file = os.path.join(source_path, "atomic.txt")
        
        # Move within watched directory - should trigger paired move events
        shutil.move(staging_file, target_file)
        time.sleep(0.5)  # Wait for thread to process
        
        # Verification A
        events = []
        while not event_queue.empty():
            events.append(event_queue.get())
        
        # Check for Update on target (moved file)
        target_rel_path = "/" + os.path.basename(target_file)
        target_events = [e for e in events if e.rows[0]['path'] == target_rel_path]
        assert target_events, f"Should have received event for moved file {target_rel_path}. Got events: {[e.rows[0]['path'] for e in events]}"
        
        # Check flag - moved files should be atomic (no explicit is_atomic_write=False set)
        latest_event = target_events[-1]
        row_data = latest_event.rows[0]
        if isinstance(row_data, dict):
             flag = row_data.get("is_atomic_write", True)  # Default True if not set
        else:
             flag = getattr(row_data, "is_atomic_write", True)
        
        assert flag is not False, f"Atomic move should NOT be marked as partial write. Got: {row_data}"
        logger.info("VERIFIED: Atomic Move produces event compatible with is_atomic_write=True")

        # Scenario B: Partial Write (Modify without Close)
        logger.info("TEST: Performing Partial Write...")
        partial_file = os.path.join(source_path, "partial.txt")
        
        # We need to ensure we catch the dirty state.
        # open -> write -> flush -> fsync -> (wait) -> close
        f = open(partial_file, 'w')
        f.write("chunk1")
        f.flush()
        os.fsync(f.fileno())
        
        # Wait for inotify to pick up modification
        # Throttle interval is 0.1s so it should be fast
        time.sleep(0.5) 
        
        # Verification B
        events = []
        while not event_queue.empty():
            events.append(event_queue.get())
            
        partial_rel_path = "/" + os.path.basename(partial_file)
        mod_events = [e for e in events if e.event_type == EventType.UPDATE and e.rows[0]['path'] == partial_rel_path]
        
        dirty_events = [e for e in mod_events if e.rows[0].get("is_atomic_write") is False]
        assert dirty_events, f"Should have received at least one dirty event for {partial_rel_path}. Events: {mod_events}"
        logger.info("VERIFIED: Modification produces is_atomic_write=False")
        
        # Cleanup B (Close)
        f.close()
        time.sleep(0.5)
        
        # Scenario C: Close Event
        events = []
        while not event_queue.empty():
            events.append(event_queue.get())
            
        close_events = [e for e in events if e.event_type == EventType.UPDATE and e.rows[0]['path'] == partial_rel_path]
        clean_events = [e for e in close_events if e.rows[0].get("is_atomic_write") is True]
        
        assert clean_events, "Should receive clean event after close"
        logger.info("VERIFIED: Close produces is_atomic_write=True")
        
    finally:
        wm.stop()
        
    # Logic Verification: View Side (Arbitrator)
    from fustor_view_fs.arbitrator import FSArbitrator
    from fustor_view_fs.tree import TreeManager
    # Use real FSState to ensure proper initialization (root node, etc.)
    from fustor_view_fs.state import FSState
    
    logger.info("TEST: Verifying Arbitrator Logic...")
    
    # Use real FSState which properly initializes root node
    state = FSState(view_id="test-view")
    tree_manager = TreeManager(state)
    arbitrator = FSArbitrator(state, tree_manager, hot_file_threshold=5.0)
    
    path = "/data/file.txt"
    
    # Test 1: Partial Write -> Should be Suspect
    payload_dirty = {
        "path": path, "modified_time": 1000.0, "is_atomic_write": False, 
        "file_name": "file.txt", "size": 10, "is_directory": False
    }
    dummy_event = type('obj', (object,), {"event_type": "UPDATE", "rows": [payload_dirty]})
    
    await arbitrator._handle_upsert(path, payload_dirty, dummy_event, MessageSource.REALTIME, 1000.0, 1000.0)
    
    node = state.get_node(path)
    if node:
         assert node.integrity_suspect is True, "Node should be suspect"
         assert path in state.suspect_list
    else:
         # Depending on tree manager mock behavior, maybe node wasn't created?
         # TreeManager is real in this test. It should create it if payload fits.
         # But FSArb calls tree_manager.update_node.
         # Check if node exists.
         assert state.get_node(path) is not None
         assert state.get_node(path).integrity_suspect is True
         
    logger.info("VERIFIED: Arbitrator marks dirty write as suspect")
    
    # Test 2: Atomic/Clean Write -> Should Clear Suspect
    payload_clean = {
        "path": path, "modified_time": 1001.0, "is_atomic_write": True,
        "file_name": "file.txt", "size": 20, "is_directory": False
    }
    
    await arbitrator._handle_upsert(path, payload_clean, dummy_event, MessageSource.REALTIME, 1001.0, 1001.0)
    
    node = state.get_node(path)
    assert node.integrity_suspect is False
    assert path not in state.suspect_list
    logger.info("VERIFIED: Arbitrator clears suspect on clean write")

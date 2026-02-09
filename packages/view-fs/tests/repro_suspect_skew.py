
import sys
import os
import time
import asyncio
from unittest.mock import MagicMock, patch

# Setup paths
sys.path.insert(0, os.path.abspath("packages/core/src"))
sys.path.insert(0, os.path.abspath("packages/view-fs/src"))

from fustor_view_fs.state import FSState
from fustor_view_fs.arbitrator import FSArbitrator
from fustor_core.event import MessageSource, EventType

async def run_repro():
    # Use a fixed physical time base
    start_phys = 10000.0
    
    with patch('time.time', return_value=start_phys) as mock_time:
        # Initialize components
        # FSState initializes LogicalClock which calls time.time()
        state = FSState(view_id="repro-view")
        
        # Mock TreeManager to avoid complex setup
        mock_tree = MagicMock()
        mock_tree.update_node = asyncio.iscoroutinefunction(MagicMock()) # fake it
        
        # Real TreeManager.update_node is async, so we need to mock it properly
        async def mock_update(payload, path):
            if path not in state.file_path_map:
                from fustor_view_fs.nodes import FileNode
                name = os.path.basename(path)
                state.file_path_map[path] = FileNode(
                    name=name, 
                    path=path, 
                    size=payload.get('size', 100),
                    modified_time=payload.get('modified_time', 0.0),
                    created_time=payload.get('created_time', 0.0)
                )
            node = state.file_path_map[path]
            node.modified_time = payload.get('modified_time', 0.0)
            node.last_updated_at = time.time()
            return node
            
        mock_tree.update_node = mock_update
        
        arbitrator = FSArbitrator(state, tree_manager=mock_tree, hot_file_threshold=60.0)
        
        print(f"Initial physical time: {start_phys}")
        print(f"Initial logical watermark: {state.logical_clock.get_watermark()}")

        # 1. Simulate Agent A (+2h skew) sending real-time events
        # Agent Time = 10000 + 7200 = 17200
        agent_a_time = 17200.0
        
        event_a = MagicMock()
        event_a.message_source = MessageSource.REALTIME
        event_a.event_type = EventType.UPDATE
        # We need multiple samples to set the 'Mode' in LogicalClock
        event_a.rows = [{'path': '/file_a', 'modified_time': agent_a_time}]
        
        print("Sending 10 events from skewed Agent A (+2h)...")
        for _ in range(10):
            await arbitrator.process_event(event_a)
            
        watermark = state.logical_clock.get_watermark()
        print(f"Watermark after Agent A events: {watermark:.1f} (Skew: {watermark - start_phys:.1f}s)")
        
        # 2. Simulate Audit discovering a file from Client C (No skew)
        # Client C created file "just now" at 10005.0 physical time.
        mock_time.return_value = 10005.0
        
        event_c = MagicMock()
        event_c.message_source = MessageSource.AUDIT
        event_c.event_type = EventType.INSERT
        event_c.rows = [{'path': '/file_c', 'modified_time': 10005.0}]
        
        print(f"Audit discovers /file_c at physical time 10005.0 (file mtime=10005.0)")
        
        # --- PREDICT FIX BEHAVIOR ---
        print("\n--- Applying Fix Hypothesis: age = min(logical_age, physical_age) ---")
        watermark = state.logical_clock.get_watermark()
        logical_age = watermark - 10005.0
        physical_age = mock_time.return_value - 10005.0
        effective_age = min(logical_age, physical_age)
        print(f"Logical Age: {logical_age:.1f}s")
        print(f"Physical Age: {physical_age:.1f}s")
        print(f"Effective Age: {effective_age:.1f}s")
        
        if effective_age < 60.0:
            print(">>> FIX VERIFIED: File would be marked suspect!")
        else:
            print(">>> FIX FAIL: File would still NOT be marked suspect.")

        await arbitrator.process_event(event_c)
        # (This will still fail in real code until we patch arbitrator.py)

if __name__ == "__main__":
    asyncio.run(run_repro())

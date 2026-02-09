
import pytest
import asyncio
from fustor_view_fs.arbitrator import FSArbitrator
from fustor_view_fs.state import FSState
from fustor_view_fs.tree import TreeManager
from fustor_core.event import MessageSource, EventType
from fuzzer_utils import EventFuzzer

# Re-use mocks from test_arbitrator.py
class MockRow:
    def __init__(self, **kwargs):
        self.data = kwargs
    def get(self, key, default=None):
        return self.data.get(key, default)
    def __getitem__(self, key):
        return self.data[key]

class MockEvent:
    def __init__(self, event_type, rows, source=MessageSource.REALTIME, index=0):
        self.event_type = event_type
        self.rows = [MockRow(**row) if isinstance(row, dict) else row for row in rows]
        # Allow string source for compatibility testing
        self.message_source = source
        self.index = index

@pytest.fixture
def fs_state():
    state = FSState("test-view-prop")
    state.logical_clock.reset(500.0)
    return state

@pytest.fixture
def tree_manager(fs_state):
    return TreeManager(fs_state)

@pytest.fixture
def arbitrator(fs_state, tree_manager):
    return FSArbitrator(fs_state, tree_manager, hot_file_threshold=300.0)

@pytest.mark.asyncio
async def test_arbitrator_convergence(arbitrator, fs_state):
    """
    Property: The state of a file should eventually converge to the latest Realtime update,
    ignoring stale Audit/Snapshot events.
    """
    fuzzer = EventFuzzer()
    fuzzer.log_seed()
    
    events = fuzzer.generate_events(count=100)
    
    # Track expected state (Oracle)
    # Map path -> (latest_realtime_mtime, latest_realtime_deleted)
    oracle = {}
    
    for evt_data in events:
        source = evt_data['message_source']
        is_delete = (evt_data['event_type'] == EventType.DELETE)
        
        # Oracle must process ALL rows, just like the Arbitrator
        for row in evt_data['rows']:
            path = row['path']
            mtime = row['modified_time']
            
            # Update oracle logic to mirror System Behavior:
            # 1. Realtime events are applied blindly (Stream of Truth).
            # 2. Non-Realtime events satisfy Smart Merge (mtime check).
            
            current = oracle.get(path, {'mtime': 0.0, 'deleted': True})
            
            should_apply = False
            if source == MessageSource.REALTIME:
                should_apply = True
            else:
                # Audit/Snapshot Logic
                if is_delete:
                    # Delete accepts ties (>=)
                    if mtime >= current['mtime'] - 1e-6:
                        should_apply = True
                else:
                    # Upsert rejects ties strictly (>)
                    if mtime > current['mtime'] + 1e-6:
                        should_apply = True
                        
            if should_apply:
                oracle[path] = {'mtime': mtime, 'deleted': is_delete}
            
        # Convert dict to MockEvent
            
        # Convert dict to MockEvent
        mock_evt = MockEvent(
            evt_data['event_type'],
            evt_data['rows'],
            source=source,
            index=evt_data['index']
        )
        await arbitrator.process_event(mock_evt)
        
    # Verify convergence
    for path, expected in oracle.items():
        node = fs_state.get_node(path)
        
        if expected['deleted']:
            # Should be deleted (or tombstoned)
            # If node exists, it MUST NOT be older than the delete time (which is impossible if we track correctly)
            # OR inconsistent state if Smart Merge failed
            if node:
                # If node exists, its mtime must be newer than the delete (Reincarnation)
                # But our oracle only tracked Realtime. If a newer Realtime Insert happened, expected['deleted'] would be false.
                # So if expected['deleted'] is True, node should strictly be None.
                assert node is None, f"Node {path} should be deleted. Oracle mtime: {expected['mtime']}, Node mtime: {node.modified_time}"
        else:
            # Should exist
            assert node is not None, f"Node {path} missing. Expected mtime: {expected['mtime']}"
            # Node mtime should be AT LEAST the expected realtime mtime
            # (Audit could have updated it if it was newer, which is allowed)
            assert node.modified_time >= expected['mtime'] - 1e-6, \
                f"Node {path} stale. Expected >= {expected['mtime']}, Got {node.modified_time}"

@pytest.mark.asyncio
async def test_tombstone_effectiveness(arbitrator, fs_state):
    """
    Property: A Realtime Delete should create a Tombstone that blocks
    any subsequent Non-Realtime event with an older mtime.
    """
    from unittest.mock import patch
    
    # Patch time.time() to match the clock.reset(500.0) in fixture
    with patch('time.time', return_value=500.0):
        path = "/tombstone_test.txt"
        base_time = 1000.0
        
        # 1. Realtime Insert
        await arbitrator.process_event(MockEvent(
            EventType.INSERT,
            [{"path": path, "modified_time": base_time, "size": 100}],
            source=MessageSource.REALTIME
        ))
        
        # 2. Realtime Delete (Creates Tombstone at ~1000.0)
        await arbitrator.process_event(MockEvent(
            EventType.DELETE,
            [{"path": path, "modified_time": base_time + 10}],
            source=MessageSource.REALTIME
        ))
        
        # Verify Tombstone exists
        assert path in fs_state.tombstone_list
        
        # 3. Attempt to resurrect with Stale Snapshot (Time < Delete Time)
        await arbitrator.process_event(MockEvent(
            EventType.UPDATE,
            [{"path": path, "modified_time": base_time + 5, "size": 999}],
            source=MessageSource.SNAPSHOT
        ))
        
        # Verify stays deleted
        assert fs_state.get_node(path) is None
        
        # 4. Attempt to resurrect with Newer Snapshot (Time > Delete Time) -> Reincarnation
        await arbitrator.process_event(MockEvent(
            EventType.UPDATE,
            [{"path": path, "modified_time": base_time + 20, "size": 200}],
            source=MessageSource.SNAPSHOT
        ))
        
        # Verify resurrected
        node = fs_state.get_node(path)
        assert node is not None
        assert node.size == 200
        assert path not in fs_state.tombstone_list

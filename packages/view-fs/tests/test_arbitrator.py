
import pytest
import time
import asyncio
from unittest.mock import MagicMock
from fustor_view_fs.arbitrator import FSArbitrator
from fustor_view_fs.state import FSState
from fustor_view_fs.tree import TreeManager
from fustor_core.event import MessageSource, EventType, EventBase

@pytest.fixture
def fs_state():
    state = FSState("test-view")
    # Reset to a fixed small value for predictable tests
    state.logical_clock.reset(500.0)
    return state

@pytest.fixture
def tree_manager(fs_state):
    return TreeManager(fs_state)

@pytest.fixture
def arbitrator(fs_state, tree_manager):
    return FSArbitrator(fs_state, tree_manager, hot_file_threshold=300.0)

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
        self.message_source = source
        self.index = index

@pytest.mark.asyncio
async def test_arbitrator_realtime_upsert(arbitrator, fs_state):
    event = MockEvent(
        EventType.INSERT,
        [{"path": "/file.txt", "size": 100, "modified_time": 1000.0}],
        source=MessageSource.REALTIME
    )
    
    await arbitrator.process_event(event)
    
    node = fs_state.get_node("/file.txt")
    assert node is not None
    assert node.size == 100
    assert node.modified_time == 1000.0
    assert node.known_by_agent is True

@pytest.mark.asyncio
async def test_arbitrator_tombstone_protection(arbitrator, fs_state):
    # 1. Delete in realtime (creates tombstone)
    delete_event = MockEvent(
        EventType.DELETE,
        [{"path": "/ghost.txt", "modified_time": 2000.0}],
        source=MessageSource.REALTIME
    )
    await arbitrator.process_event(delete_event)
    assert "/ghost.txt" in fs_state.tombstone_list
    
    # 2. Older snapshot event (should be blocked)
    snapshot_event = MockEvent(
        EventType.INSERT,
        [{"path": "/ghost.txt", "size": 500, "modified_time": 1500.0}],
        source=MessageSource.SNAPSHOT
    )
    await arbitrator.process_event(snapshot_event)
    assert fs_state.get_node("/ghost.txt") is None
    
    # 3. Newer snapshot event after tombstone (should clear tombstone and progress)
    # Using index as reference timestamp (simulating newer logical event)
    new_snapshot_event = MockEvent(
        EventType.INSERT,
        [{"path": "/ghost.txt", "size": 500, "modified_time": 2500.0}],
        source=MessageSource.SNAPSHOT,
        index=2500.0 # 2500s (Seconds)
    )
    await arbitrator.process_event(new_snapshot_event)
    assert "/ghost.txt" not in fs_state.tombstone_list
    assert fs_state.get_node("/ghost.txt") is not None

@pytest.mark.asyncio
async def test_arbitrator_suspect_management(arbitrator, fs_state):
    # Set watermark to 2000.0
    fs_state.logical_clock.update(2000.0)
    
    # Audit arrival of a "hot" file (mtime = 1950, age = 50 < 300)
    audit_event = MockEvent(
        EventType.INSERT,
        [{"path": "/hot.txt", "size": 10, "modified_time": 1950.0}],
        source=MessageSource.AUDIT
    )
    
    await arbitrator.process_event(audit_event)
    
    node = fs_state.get_node("/hot.txt")
    assert node.integrity_suspect is True
    assert "/hot.txt" in fs_state.suspect_list
    assert "/hot.txt" in fs_state.blind_spot_additions
    
    # Real-time event clears suspect
    rt_event = MockEvent(
        EventType.UPDATE,
        [{"path": "/hot.txt", "size": 10, "modified_time": 1960.0}],
        source=MessageSource.REALTIME
    )
    await arbitrator.process_event(rt_event)
    assert node.integrity_suspect is False
    assert "/hot.txt" not in fs_state.suspect_list
    assert "/hot.txt" not in fs_state.blind_spot_additions

@pytest.mark.asyncio
async def test_arbitrator_smart_merge_logic(arbitrator, fs_state):
    # Existing file at T100
    await arbitrator.process_event(MockEvent(
        EventType.INSERT,
        [{"path": "/data.bin", "size": 100, "modified_time": 100.0}],
        source=MessageSource.SNAPSHOT
    ))
    
    # Stale Snapshot/Audit at T50 (should be ignored)
    await arbitrator.process_event(MockEvent(
        EventType.UPDATE,
        [{"path": "/data.bin", "size": 50, "modified_time": 50.0}],
        source=MessageSource.SNAPSHOT
    ))
    assert fs_state.get_node("/data.bin").size == 100
    
    # Newer Snapshot/Audit at T150 (should be applied)
    await arbitrator.process_event(MockEvent(
        EventType.UPDATE,
        [{"path": "/data.bin", "size": 150, "modified_time": 150.0}],
        source=MessageSource.SNAPSHOT
    ))
    assert fs_state.get_node("/data.bin").size == 150

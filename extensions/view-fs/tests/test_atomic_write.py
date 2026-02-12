
import asyncio
import pytest
from unittest.mock import MagicMock
from fustor_view_fs.arbitrator import FSArbitrator
from fustor_view_fs.state import FSState
from fustor_view_fs.tree import TreeManager
from fustor_core.event import EventType, MessageSource

@pytest.mark.asyncio
async def test_atomic_write_integrity():
    # Setup
    state = FSState("test_view")
    tree = TreeManager(state)
    arb = FSArbitrator(state, tree, hot_file_threshold=5.0)
    
    path = "/test/file.txt"
    
    # 1. Simulate Partial Write (IN_MODIFY) - is_atomic_write=False
    event_partial = MagicMock()
    event_partial.message_source = MessageSource.REALTIME
    event_partial.event_type = EventType.UPDATE
    event_partial.rows = [{
        "path": path,
        "file_name": "file.txt",
        "size": 100,
        "modified_time": 1000.0,
        "is_directory": False,
        "is_atomic_write": False # PARTIAL
    }]
    
    await arb.process_event(event_partial)
    
    node = state.get_node(path)
    assert node is not None
    assert node.integrity_suspect is True, "Partial write should be marked SUSPECT"
    assert path in state.suspect_list
    
    # 2. Simulate Completed Write (IN_CLOSE_WRITE) - is_atomic_write=True
    event_complete = MagicMock()
    event_complete.message_source = MessageSource.REALTIME
    event_complete.event_type = EventType.UPDATE
    event_complete.rows = [{
        "path": path,
        "file_name": "file.txt",
        "size": 200,
        "modified_time": 1001.0, # Time advanced
        "is_directory": False,
        "is_atomic_write": True # COMPLETE
    }]
    
    await arb.process_event(event_complete)
    
    node = state.get_node(path)
    assert node is not None
    assert node.integrity_suspect is False, "Atomic write should CLEAR suspect flag"
    assert path not in state.suspect_list

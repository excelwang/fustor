
import pytest
import asyncio
from unittest.mock import MagicMock
from fustor_view_fs.arbitrator import FSArbitrator
from fustor_view_fs.state import FSState
from fustor_view_fs.tree import TreeManager
from fustor_core.event import MessageSource, EventType
from fuzzer_utils import EventFuzzer

@pytest.fixture
def state():
    return FSState(view_id="test_view", config={})

@pytest.fixture
def tree_manager(state):
    tm = MagicMock(spec=TreeManager)
    # Mock update_node to actually update state if needed, or just return
    async def mock_update(payload, path):
        # We don't need full tree logic for arbitration test
        pass
    async def mock_delete(path):
        pass
    tm.update_node.side_effect = mock_update
    tm.delete_node.side_effect = mock_delete
    return tm

@pytest.fixture
def arbitrator(state, tree_manager):
    return FSArbitrator(state, tree_manager, hot_file_threshold=10.0)

class MockEvent:
    def __init__(self, data):
        self.rows = [MagicMock(**row) for row in data["rows"]]
        for i, row in enumerate(self.rows):
            # Ensure row has desired attributes
            row.get = lambda k, i=i: data["rows"][i].get(k)
        self.event_type = data["event_type"]
        self.message_source = data["message_source"]
        self.index = data["index"]

@pytest.mark.asyncio
async def test_tombstone_resurrection_logic(arbitrator, state):
    """
    Verify that tombstones properly block stale events and allow newer events.
    """
    from unittest.mock import patch
    
    # Patch time.time() to match clock.reset(1000.0) called below
    with patch('time.time', return_value=1000.0):
        path = "/data/resurrection.txt"
        
        # 1. DELETE event (Realtime) establishes a tombstone
        # Set logical clock watermark
        state.logical_clock.reset(1000.0)
        
        del_event = MockEvent({
            "event_type": EventType.DELETE,
            "rows": [{"path": path, "modified_time": 1000.1}],
            "message_source": MessageSource.REALTIME,
            "index": 1000100 # 1000.1s
        })
        
        await arbitrator.process_event(del_event)
        assert path in state.tombstone_list
        tombstone_ts, _ = state.tombstone_list[path]
        assert tombstone_ts >= 1000.0  # With simplified watermark, equals time.time() when no skew

        # 2. STALE event (Snapshot) should be blocked
        stale_event = MockEvent({
            "event_type": EventType.UPDATE,
            "rows": [{"path": path, "modified_time": 999.0}],
            "message_source": MessageSource.SNAPSHOT,
            "index": 999000 # 999.0s
        })
        
        await arbitrator.process_event(stale_event)
        # Should still be in tombstone list, and tree_manager.update_node should NOT have been called for this path
        assert path in state.tombstone_list
        arbitrator.tree_manager.update_node.assert_not_called()

        # 3. NEW activity (Realtime/Audit) should resurrect
        resurrect_event = MockEvent({
            "event_type": EventType.UPDATE,
            "rows": [{"path": path, "modified_time": 1005.0}],
            "message_source": MessageSource.REALTIME,
            "index": 1005100
        })
        
        await arbitrator.process_event(resurrect_event)
        assert path not in state.tombstone_list
        arbitrator.tree_manager.update_node.assert_called_once()

@pytest.mark.asyncio
async def test_fuzzed_arbitration_stability(arbitrator, state):
    """
    Run 100 fuzzed events and ensure no crashes or obvious consistency violations.
    """
    fuzzer = EventFuzzer(seed=42)
    raw_events = fuzzer.generate_events(count=100)
    
    for raw_evt in raw_events:
        evt = MockEvent(raw_evt)
        await arbitrator.process_event(evt)
    
    # Just verify it survived and clock advanced
    assert state.logical_clock.get_watermark() > 1000.0
    print(f"Final Watermark: {state.logical_clock.get_watermark()}")
